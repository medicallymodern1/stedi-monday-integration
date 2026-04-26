"""
routes/stedi_webhook.py
========================
Handles incoming webhooks FROM Stedi.

Stedi fires this webhook when:
- A 277 acknowledgement is ready
- An 835 ERA is ready

Endpoint: POST /webhooks/stedi
"""

import json
import logging
import os
from typing import Any, Dict

from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

from services.era_parser_service import (
    match_era_rows_to_claim_item,
    parse_era_from_string,
    summarize_era_row_for_monday,
)
from services.monday_service import populate_era_data_on_claims_item, update_277_on_claims_board
from services.monday_service import run_query
from services.stedi_service import get_era_as_835_file, get_277_report

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Stedi Webhook"])


# @router.post("/webhook")
# async def stedi_webhook(request: Request, background_tasks: BackgroundTasks):
#     """
#     Receives Stedi transaction.processed.v2 events.
#     Stedi wraps events in AWS SQS Records format:
#     {
#       "event": {
#         "Records": [{
#           "body": {
#             "detail-type": "transaction.processed.v2",
#             "detail": { "transactionId": "...", "x12": {...} }
#           }
#         }]
#       }
#     }
#     """
#     body: Dict[str, Any] = await request.json()
#     logger.info(f"Stedi webhook raw payload: {body}")
#
#     # Extract records from SQS wrapper
#     records = (
#         body.get("event", {}).get("Records", []) or
#         body.get("Records", []) or
#         []
#     )
#
#     if not records:
#         # Try direct event format (non-SQS)
#         records = [{"body": body}]
#
#     for record in records:
#         record_body = record.get("body", {})
#         if isinstance(record_body, str):
#             import json
#             record_body = json.loads(record_body)
#         background_tasks.add_task(handle_stedi_event, record_body)
#
#     return JSONResponse({"status": "received"}, status_code=200)
#
# async def handle_stedi_event(event: dict) -> None:
#     """Process a single Stedi event"""
#
#     event_id   = event.get("id", "")
#     event_type = event.get("detail-type", "")
#     detail     = event.get("detail", {})
#
#     logger.info(f"Stedi event: id={event_id} | type={event_type}")
#
#     if event_type != "transaction.processed.v2":
#         logger.info(f"Ignored event type: {event_type}")
#         return
#
#     transaction_id = detail.get("transactionId", "")
#
#     # Get transaction set identifier — can be int or string
#     tx_set = str(
#         detail.get("x12", {})
#         .get("metadata", {})
#         .get("transaction", {})
#         .get("transactionSetIdentifier", "")
#     )
#
#     logger.info(f"Transaction: id={transaction_id} | set={tx_set}")
#
#     if tx_set == "277":
#         await handle_277_event(transaction_id, detail)
#     elif tx_set == "835":
#         await handle_835_event(transaction_id, detail)
#     else:
#         logger.info(f"Unhandled transaction set: {tx_set}")

@router.post("/webhook")
async def stedi_webhook(request: Request, background_tasks: BackgroundTasks):
    """Receives Stedi transaction.processed.v2 events"""
    body: Dict[str, Any] = await request.json()

    # Return 200 immediately — Stedi requires response within 5 seconds
    background_tasks.add_task(handle_stedi_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


async def handle_stedi_event(body: dict) -> None:
    """Process Stedi event asynchronously"""

    # Stedi wraps the event under "event" key
    event = body.get("event", body)  # fallback to body itself if not wrapped

    event_id   = event.get("id", "")
    event_type = event.get("detail-type", "")
    detail     = event.get("detail", {})

    logger.info(f"Stedi event: id={event_id} | type={event_type}")

    if event_type != "transaction.processed.v2":
        logger.info(f"Ignored event type: {event_type}")
        return

    transaction_id = detail.get("transactionId", "")

    # transactionSetIdentifier can be int or string
    x12_meta = detail.get("x12", {}).get("metadata", {}).get("transaction", {})
    tx_set   = str(x12_meta.get("transactionSetIdentifier", ""))

    logger.info(f"Transaction: id={transaction_id} | set={tx_set}")

    if tx_set == "277":
        await handle_277_event(transaction_id, detail)
    elif tx_set == "835":
        await handle_835_event(transaction_id, detail)
    else:
        logger.info(f"Unhandled transaction set: {tx_set}")

async def handle_277_event(transaction_id: str, detail: dict) -> None:
    logger.info(f"[277] Processing transaction_id={transaction_id}")
    try:
        report = get_277_report(transaction_id)
        status, rejection_reason, pcn, claim_id, category_code, payer_claim_number = parse_277_status(report)
        logger.info(f"[277] status={status} | category={category_code} | pcn={pcn} | claim_id={claim_id} | payer_claim={payer_claim_number}")

        monday_status = _map_277_status(status, report, category_code)
        logger.info(f"[277] monday_status={monday_status}")

        claims_item_id = ""
        if pcn:
            claims_item_id = _find_claims_item_by_pcn(pcn)
        if not claims_item_id and claim_id:
            logger.info(f"[277] PCN lookup failed, trying claim_id={claim_id}")
            claims_item_id = _find_claims_item_by_correlation_id(claim_id)

        if not claims_item_id:
            logger.warning(f"[277] No Claims Board item for pcn={pcn} or claim_id={claim_id}")
            return

        update_277_on_claims_board(claims_item_id, monday_status, rejection_reason)
        logger.info(f"[277] Updated Claims Board item {claims_item_id} to {monday_status}")

        # Write payer claim number to Claims Board (needed for corrected/void resubmissions)
        if payer_claim_number:
            PAYER_CLAIM_NUM_COL = "text_mm2nfytt"
            mutation = """
            mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
              change_column_value(item_id: $itemId, board_id: $boardId,
                                  column_id: $columnId, value: $value) { id }
            }
            """
            try:
                run_query(mutation, {
                    "itemId": str(claims_item_id),
                    "boardId": str(os.getenv("MONDAY_CLAIMS_BOARD_ID")),
                    "columnId": PAYER_CLAIM_NUM_COL,
                    "value": json.dumps(str(payer_claim_number)),
                })
                logger.info(f"[277] Wrote payer claim number {payer_claim_number} to item {claims_item_id}")
            except Exception as e:
                logger.warning(f"[277] Failed to write payer claim number: {e}")

    except Exception as e:
        logger.error(f"[277] Failed: {e}", exc_info=True)


def _map_277_status(raw_status: str, report: dict, category_code: str = "") -> str:
    """
    Map raw 277 status to Monday 277 Status label.
    PRD 14 values: Stedi Accepted, Stedi Rejected, Payer Accepted, Payer Rejected

    Logic:
    - A0 is always Stedi-level (clearinghouse acknowledgement)
    - A2 is always Payer-level (accepted into adjudication)
    - A1 can be either — use source metadata (STEDI INC / CLEARINGHOUSE) to decide
    - Rejections (A3, A6, A7, A8, DR05-07) are always Payer-level
    """

    # A2 is always payer-level — accepted into adjudication system
    if category_code == "A2":
        return "Payer Accepted"

    # Rejection codes are always payer-level
    if category_code in ("A3", "A6", "A7", "A8", "DR05", "DR06", "DR07"):
        return "Payer Rejected"

    payer = {}
    try:
        payer = (
            report.get("transactions", [{}])[0]
            .get("payers", [{}])[0]
        )
    except Exception:
        payer = {}

    # ── Step 1: Source metadata takes priority ────────────────────────────────
    source_name = (payer.get("organizationName") or "").strip().upper()
    source_type = (payer.get("entityIdentifierCodeValue") or "").strip().upper()

    is_stedi_level = (
        source_name == "STEDI INC"
        or source_type == "CLEARINGHOUSE"
    )

    # ── Step 2: Fallback to old A0 heuristic only if source metadata
    #           did not already prove this is Stedi-level ──────────────────────
    provider_cat = ""
    try:
        provider_cat = (
            payer.get("claimStatusTransactions", [{}])[0]
            .get("providerClaimStatuses", [{}])[0]
            .get("providerStatuses", [{}])[0]
            .get("healthCareClaimStatusCategoryCode", "")
        )
    except Exception:
        provider_cat = ""

    has_any_a0 = False
    try:
        claim_status_txns = payer.get("claimStatusTransactions", [{}])
        for cst in claim_status_txns:
            for csd in cst.get("claimStatusDetails", []):
                for pcsd in csd.get("patientClaimStatusDetails", []):
                    for claim in pcsd.get("claims", []):
                        for ics in claim.get("claimStatus", {}).get("informationClaimStatuses", []):
                            for info in ics.get("informationStatuses", []):
                                if info.get("healthCareClaimStatusCategoryCode", "") == "A0":
                                    has_any_a0 = True
                                    break
                            if has_any_a0:
                                break
                        if has_any_a0:
                            break
                    if has_any_a0:
                        break
                if has_any_a0:
                    break
            if has_any_a0:
                break
    except Exception:
        pass

    if not is_stedi_level:
        is_stedi_level = (provider_cat == "A0") or has_any_a0

    logger.info(
        f"[277] _map_277_status: raw_status={raw_status!r} "
        f"source_name={source_name!r} source_type={source_type!r} "
        f"provider_cat={provider_cat!r} has_any_a0={has_any_a0} "
        f"is_stedi_level={is_stedi_level}"
    )

    if raw_status == "Accepted":
        return "Stedi Accepted" if is_stedi_level else "Payer Accepted"
    elif raw_status == "Rejected":
        return "Stedi Rejected" if is_stedi_level else "Payer Rejected"

    return "Stedi Accepted" if is_stedi_level else "Payer Accepted"


# ---------------------------------------------------------------------------
# Field-name probes for the recursive 277 walker
# ---------------------------------------------------------------------------
# Stedi has shipped two distinct JSON shapes for 277 reports over time:
#
# 1. Legacy "logical" shape:
#      transactions[0].payers[0].claimStatusTransactions[0]
#        .claimStatusDetails[0].patientClaimStatusDetails[0].claims[0]
#        .claimStatus.informationClaimStatuses[0].informationStatuses[0]
#
# 2. X12-typed shape (what Fidelis returned 2026-04-23):
#      detail.information_source_level_HL_loop[0]
#        .information_receiver_level_HL_loop[0]
#        .billing_provider_of_service_level_HL_loop[0]
#        .patient_level_HL_loop[0]
#        .claim_status_tracking_number_TRN_loop[0]
#        .claim_level_status_information_STC[0]
#          .health_care_claim_status_01.health_care_claim_status_category_code_01
#
# Rather than walk both fragile paths, we recursively search the full
# report for field names from a probe list. This is robust to future
# shape drift and lets us fall back gracefully when one shape's fields
# are missing.

_CATEGORY_PROBES = (
    "healthCareClaimStatusCategoryCode",
    "health_care_claim_status_category_code_01",
    "statusCategoryCode",
)
_STATUS_CODE_PROBES = (
    "healthCareClaimStatusCode",
    "health_care_claim_status_code_02",
    "statusCode",
)
_STATUS_VALUE_PROBES = (
    "statusCodeValue",
    "healthCareClaimStatusCodeValue",
    "statusDescription",
    "description",
)
_PCN_PROBES = (
    "patientAccountNumber",
    "patient_control_number_02",
    "patientControlNumber",
)
_PAYER_CLAIM_PROBES = (
    "tradingPartnerClaimNumber",
    "payerClaimControlNumber",
    "claimControlNumber",
    "payer_claim_control_number_01",
    "payer_claim_control_number_02",
)
_BATCH_PROBES = (
    "claimTransactionBatchNumber",
    "claim_transaction_batch_number_02",
)


def _walk_first(obj, keys: tuple) -> str:
    """
    Depth-first walk through ``obj`` and return the first non-empty string
    value whose key matches any in ``keys``. Returns "" if not found.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            if k in keys:
                if isinstance(v, (str, int, float)) and str(v).strip():
                    return str(v).strip()
                if isinstance(v, dict):
                    # Some payers wrap the value in an object; pull a
                    # likely sub-field if present.
                    for sub in ("value", "code"):
                        sv = v.get(sub)
                        if isinstance(sv, (str, int, float)) and str(sv).strip():
                            return str(sv).strip()
            # Recurse regardless — match may be deeper than this node.
            sub = _walk_first(v, keys)
            if sub:
                return sub
    elif isinstance(obj, list):
        for item in obj:
            sub = _walk_first(item, keys)
            if sub:
                return sub
    return ""


def parse_277_status(report: dict) -> tuple:
    """
    Extract claim status from a 277 report.

    Returns (status, rejection_reason, patient_account_number,
             claim_id, category_code, payer_claim_number)

    Recursive over the full report: works against both the legacy
    "logical" Stedi JSON shape and the newer X12-typed shape (where
    fields are nested inside ``detail.information_source_level_HL_loop``
    etc.). When a field appears in multiple places, we take the first
    non-empty match — which biases toward whatever the payer surfaced
    most prominently.
    """
    try:
        category_code           = _walk_first(report, _CATEGORY_PROBES)
        status_code_value       = _walk_first(report, _STATUS_VALUE_PROBES)
        status_code             = _walk_first(report, _STATUS_CODE_PROBES)
        patient_account_number  = _walk_first(report, _PCN_PROBES)
        claim_id                = _walk_first(report, _BATCH_PROBES)
        payer_claim_number      = _walk_first(report, _PAYER_CLAIM_PROBES)

        # Status mapping (unchanged from previous behaviour)
        REJECTION_CODES = {"A3", "A6", "A7", "A8", "DR05", "DR06", "DR07"}
        if category_code in ("A0", "A1"):
            status = "Accepted"
            rejection_reason = ""
        elif category_code == "A2":
            status = "Accepted"
            rejection_reason = ""
        elif category_code in REJECTION_CODES:
            status = "Rejected"
            rejection_reason = status_code_value
        elif category_code == "A4":
            status = "Pending"
            rejection_reason = ""
        else:
            # Unknown code — log but don't assume rejected
            status = "Pending"
            rejection_reason = status_code_value

        logger.info(
            f"[277] category={category_code} | status_code={status_code} | "
            f"status={status} | pcn={patient_account_number} | "
            f"claim_id={claim_id} | payer_claim={payer_claim_number}"
        )

        # Diagnostic — when ICN extraction comes back empty, dump a
        # truncated view of the raw report so future "where's the
        # column write" questions can be answered from logs.
        if not payer_claim_number:
            try:
                excerpt = json.dumps(report, default=str)[:2000]
            except Exception:
                excerpt = repr(report)[:2000]
            logger.warning(
                f"[277] No payer claim number resolved from report. "
                f"Excerpt: {excerpt}"
            )

        return (
            status,
            rejection_reason,
            patient_account_number,
            claim_id,
            category_code,
            payer_claim_number,
        )

    except Exception as e:
        logger.error(f"[277] parse failed: {e}", exc_info=True)
        return "Unknown", "", "", "", "", ""

#
# def find_order_item_by_pcn(patient_control_number: str) -> str:
#     """
#     Find Order Board item by patientControlNumber.
#     PCN is stored in the Order Board when claim is submitted.
#     We need to search by it.
#     """
#     from services.monday_service import run_query
#     import os
#
#     board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
#     if not board_id or not patient_control_number:
#         return ""
#
#     # Search all items for matching PCN
#     query = """
#     query FindItem($boardId: ID!) {
#       boards(ids: [$boardId]) {
#         items_page(limit: 200) {
#           items {
#             id
#             name
#             column_values { id text }
#           }
#         }
#       }
#     }
#     """
#     try:
#         result = run_query(query, {"boardId": board_id})
#         items = (
#             result.get("data", {})
#             .get("boards", [{}])[0]
#             .get("items_page", {})
#             .get("items", [])
#         )
#         for item in items:
#             for col in item.get("column_values", []):
#                 # if col.get("text") == patient_control_number:
#                 if col.get("id") == "text_mm1ra2v1" and col.get("text") == patient_control_number:
#                     logger.info(f"Found Order item {item['id']} for PCN={patient_control_number}")
#                     return item["id"]
#     except Exception as e:
#         logger.error(f"Order item search failed: {e}")
#     return ""

def find_order_item_by_pcn(patient_control_number: str) -> str:
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
    if not board_id or not patient_control_number:
        return ""

    query = """
    query FindItem($boardId: ID!) {
      boards(ids: [$boardId]) {
        items_page(limit: 200) {
          items {
            id
            name
            column_values { id text }
          }
        }
      }
    }
    """
    try:
        result = run_query(query, {"boardId": board_id})
        items = (
            result.get("data", {})
            .get("boards", [{}])[0]
            .get("items_page", {})
            .get("items", [])
        )
        for item in items:
            for col in item.get("column_values", []):
                if col.get("id") == "text_mm1ra2v1":
                    # ── Support comma-separated PCNs for multi-claim orders ──
                    stored_pcns = [v.strip() for v in (col.get("text") or "").split(",")]
                    if patient_control_number in stored_pcns:
                        logger.info(f"Found Order item {item['id']} for PCN={patient_control_number}")
                        return item["id"]
    except Exception as e:
        logger.error(f"Order item search failed: {e}")
    return ""

async def handle_835_event(transaction_id: str, detail: dict) -> None:
    """
    Handle 835 ERA payment.
    1. Fetch ERA report from Stedi
    2. Parse ERA JSON
    3. Find matching Claims Board item
    4. Populate Monday Claims Board
    """
    logger.info(f"[835] Processing transaction_id={transaction_id}")
    try:
        # Step 1: Fetch ERA from Stedi
        era_content = get_era_as_835_file(transaction_id)
        if not era_content:
            logger.warning(f"[835] Empty ERA for {transaction_id}")
            return

        logger.info(f"[835] ERA fetched, length={len(era_content)}")

        # Step 2: Parse ERA JSON
        from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday

        era_rows = parse_era_from_string(era_content)
        if not era_rows:
            logger.warning(f"[835] No rows parsed — check ERA format in logs above")
            return

        logger.info(f"[835] Parsed {len(era_rows)} ERA row(s)")

        # Step 3 & 4: For each parsed row, find Claims Board item and populate
        for era_row in era_rows:
            parent = era_row.get("parent", {})
            patient_control_num = parent.get("raw_patient_control_num", "")

            logger.info(
                f"[835] PCN={patient_control_num} | "
                f"paid={parent.get('primary_paid')} | "
                f"pr={parent.get('pr_amount')}"
            )

            claims_item_id = _find_claims_item_by_correlation_id(transaction_id)
            if not claims_item_id:
                logger.info(f"[835] No item by transaction_id, trying PCN={patient_control_num}")
                claims_item_id = _find_claims_item_by_pcn(patient_control_num)

            if not claims_item_id:
                logger.warning(f"[835] No Claims Board item found for PCN={patient_control_num}")
                continue

            logger.info(f"[835] Found Claims Board item: {claims_item_id}")

            summary = summarize_era_row_for_monday(era_row)
            from services.monday_service import populate_era_data_on_claims_item
            populate_era_data_on_claims_item(claims_item_id, summary)
            logger.info(f"[835] Populated Claims Board item {claims_item_id}")

    except Exception as e:
        logger.error(f"[835] Failed: {e}", exc_info=True)


def _find_claims_item_by_pcn(patient_control_num: str) -> str:
    """Find Claims Board item by patient control number stored in text_mkwzbcme"""
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    if not claims_board_id or not patient_control_num:
        return ""

    query = """
    query FindItem($boardId: ID!) {
      boards(ids: [$boardId]) {
        items_page(limit: 200) {
          items {
            id
            column_values { id text }
          }
        }
      }
    }
    """
    try:
        result = run_query(query, {"boardId": claims_board_id})
        items = (
            result.get("data", {})
            .get("boards", [{}])[0]
            .get("items_page", {})
            .get("items", [])
        )
        for item in items:
            for col in item.get("column_values", []):
                if col.get("id") == "text_mkwzbcme" and col.get("text") == patient_control_num:
                    logger.info(f"Found Claims Board item {item['id']} for pcn={patient_control_num}")
                    return item["id"]
    except Exception as e:
        logger.error(f"Claims Board search failed: {e}")
    return ""

async def process_era_response(
    era_id: str,
    claim_id: str,
    patient_control_number: str,
) -> None:
    """
    Full ERA processing pipeline.
    1. Fetch raw 835 from Stedi
    2. Parse ERA JSON
    3. Find matching Claims Board item by correlationId/claim_id
    4. Populate parent columns with ERA data
    """
    try:
        logger.info(f"[ERA {era_id}] Fetching from Stedi")
        era_content = get_era_as_835_file(era_id)

        if not era_content:
            logger.warning(f"[ERA {era_id}] Empty content")
            return

        logger.info(f"[ERA {era_id}] Parsing ERA")
        era_rows = parse_era_from_string(era_content)

        if not era_rows:
            logger.warning(f"[ERA {era_id}] No rows parsed")
            return

        # Match by patient control number if provided
        if patient_control_number:
            era_rows = match_era_rows_to_claim_item(era_rows, patient_control_number)

        if not era_rows:
            logger.warning(f"[ERA {era_id}] No rows matched PCN={patient_control_number}")
            return

        # Find Claims Board item
        claims_item_id = _find_claims_item_by_claim_id(claim_id)
        if not claims_item_id:
            logger.warning(f"[ERA {era_id}] No Claims Board item for claim_id={claim_id}")
            return

        # Populate Monday Claims Board
        for era_row in era_rows:
            summary = summarize_era_row_for_monday(era_row)
            populate_era_data_on_claims_item(claims_item_id, summary)
            logger.info(f"[ERA {era_id}] Populated claims item {claims_item_id}")

    except Exception as e:
        logger.error(f"[ERA {era_id}] ERA processing failed: {e}", exc_info=True)


def _find_claims_item_by_claim_id(claim_id: str) -> str:
    """
    Search the Monday Claims Board for the item whose
    'text_stedi_claim_id' column matches the given claim_id.
    """
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    query = """
    query FindClaimsItem($boardId: ID!) {
      boards(ids: [$boardId]) {
        items_page(limit: 200) {
          items {
            id
            column_values { id text }
          }
        }
      }
    }
    """
    try:
        result = run_query(query, {"boardId": claims_board_id})
        items = (
            result.get("data", {})
            .get("boards", [{}])[0]
            .get("items_page", {})
            .get("items", [])
        )
        for item in items:
            for col in item.get("column_values", []):
                if col.get("id") == "text_stedi_claim_id" and col.get("text") == claim_id:
                    return item["id"]
    except Exception as e:
        logger.error(f"Error searching Claims Board: {e}")

    return ""

def _find_claims_item_by_correlation_id(correlation_id: str) -> str:
    """Find Claims Board item by Stedi correlationId stored in text_mkwzbcme"""
    from services.monday_service import run_query
    import os

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    if not claims_board_id or not correlation_id:
        return ""

    query = """
    query FindItem($boardId: ID!) {
      boards(ids: [$boardId]) {
        items_page(limit: 200) {
          items {
            id
            column_values { id text }
          }
        }
      }
    }
    """
    try:
        result = run_query(query, {"boardId": claims_board_id})
        items = (
            result.get("data", {})
            .get("boards", [{}])[0]
            .get("items_page", {})
            .get("items", [])
        )
        for item in items:
            for col in item.get("column_values", []):
                if col.get("id") == "text_mkwzbcme" and col.get("text") == correlation_id:
                    logger.info(f"Found Claims item {item['id']} by correlationId={correlation_id}")
                    return item["id"]
    except Exception as e:
        logger.error(f"Claims Board search failed: {e}")
    return ""

@router.post("/277")
async def stedi_277_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    background_tasks.add_task(handle_stedi_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


@router.post("/835")
async def stedi_835_webhook(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()
    background_tasks.add_task(handle_stedi_event, body)
    return JSONResponse({"status": "received"}, status_code=200)
