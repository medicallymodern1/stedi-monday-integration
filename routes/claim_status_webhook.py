"""
routes/claim_status_webhook.py
===============================
FastAPI router for the Stedi Claim Status (276/277) flow on the
**Claims Board** (board id in MONDAY_CLAIMS_BOARD_ID).

Symmetric with ``routes/subscription_eligibility_webhook.py`` — same
worker-pool pattern, same ACK-fast-then-process discipline. Only
touches: Claims Board read + Claims Board write of the claim-status
columns + Notes & Activity append + trigger reset.

Trigger:
  Monday Claims Board -> "Claim Status Check" (color_mm2qq1f9) -> "Run"
  -> POST /claim-status/trigger

Manual run (no Monday change required):
  POST /claim-status/run/{item_id}

Test endpoints (no Monday write):
  POST /claim-status/test/{item_id}          — dry run, returns writeback
  POST /claim-status/test/payload/{item_id}  — build + return payload only

Debug:
  GET  /claim-status/board-columns           — list Claims Board cols
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.monday_service import run_query
from services.claim_status_monday_service import (
    run_and_write_claim_status,
    CLAIMS_BOARD_ID,
)
from services.eligibility_worker_pool import submit as pool_submit, pool_stats
from services.claim_status_service import (
    run_claim_status_check,
    extract_claim_status_inputs,
    _resolve_payer,
)
from stedi_claim_status_builder import build_claim_status_payload

logger = logging.getLogger(__name__)
router = APIRouter()

CLAIMS_COL_RUN_CHECK  = "color_mm2qq1f9"  # "Claim Status Check" trigger
RUN_TRIGGER_VALUE     = "Run"


# =============================================================================
# Utilities
# =============================================================================

def _ack(challenge: str | None = None) -> JSONResponse:
    if challenge:
        return JSONResponse({"challenge": challenge})
    return JSONResponse({"status": "ok"})


def fetch_claims_item(item_id: str) -> dict:
    """Fetch a single Claims Board item with all column_values."""
    query = """
    query GetClaimsItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values { id text value }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"Item {item_id} not found on Claims Board")
    return items[0]


# =============================================================================
# BACKGROUND WORKER
# =============================================================================

def _process_claim_status_job(item_id: str) -> None:
    """
    Runs in the eligibility worker-pool thread. Never raises; all errors
    logged so the ACK that already went back to Monday is never affected.
    """
    try:
        monday_item = fetch_claims_item(item_id)
        writeback   = run_and_write_claim_status(item_id, monday_item)
        logger.info(
            f"[CS-WEBHOOK] OK Complete | item={item_id} "
            f"category={writeback.get('Claim Status Category')!r} "
            f"paid={writeback.get('277 Paid Amount')} "
            f"icn={writeback.get('277 ICN')!r}"
        )
    except Exception as e:
        logger.error(
            f"[CS-WEBHOOK] Worker error | item={item_id}: {e}",
            exc_info=True,
        )


# =============================================================================
# WEBHOOK TRIGGER
# =============================================================================

@router.post("/trigger")
async def claim_status_trigger(request: Request) -> JSONResponse:
    """
    Monday webhook: fires when the "Claim Status Check" column on the
    Claims Board changes to "Run".

    ACKs within Monday's 5-second window, then processes in the pool.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    if "challenge" in body:
        return _ack(body["challenge"])

    event   = body.get("event", {})
    item_id = str(event.get("pulseId") or event.get("itemId") or "")
    col_id  = str(event.get("columnId") or "")

    event_value = event.get("value", {})
    if isinstance(event_value, dict):
        new_value = str(event_value.get("label", {}).get("text", "")).strip()
    else:
        new_value = ""

    logger.info(
        f"[CS-WEBHOOK] Received | item={item_id} col={col_id} value={new_value!r}"
    )

    if col_id and col_id != CLAIMS_COL_RUN_CHECK:
        logger.debug(
            f"[CS-WEBHOOK] Ignoring — column {col_id} is not the Claim Status "
            f"Check trigger"
        )
        return _ack()

    if new_value.lower() != RUN_TRIGGER_VALUE.lower():
        logger.debug(
            f"[CS-WEBHOOK] Ignoring — value {new_value!r} is not 'Run'"
        )
        return _ack()

    if not item_id:
        logger.warning("[CS-WEBHOOK] No item_id in event — skipping")
        return _ack()

    pool_submit(_process_claim_status_job, item_id)
    logger.info(f"[CS-WEBHOOK] Queued | item={item_id} pool={pool_stats()}")
    return _ack()


# =============================================================================
# MANUAL RUN
# =============================================================================

@router.post("/run/{item_id}")
async def run_claim_status_and_write(item_id: str) -> JSONResponse:
    """Manually trigger a full Claim Status check + write to Monday."""
    try:
        monday_item = fetch_claims_item(item_id)
        writeback   = run_and_write_claim_status(item_id, monday_item)
        return JSONResponse({
            "status":  "success",
            "item_id": item_id,
            "results": writeback,
        })
    except Exception as e:
        logger.error(
            f"[CS-RUN] Error | item={item_id}: {e}",
            exc_info=True,
        )
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# =============================================================================
# TEST ENDPOINTS — no Monday writes
# =============================================================================

@router.post("/test/{item_id}")
async def test_claim_status(item_id: str) -> JSONResponse:
    """
    Dry run: fetch Claims item -> run full 277 pipeline -> return writeback.
    Does NOT write to Monday.
    """
    try:
        monday_item = fetch_claims_item(item_id)
        row         = extract_claim_status_inputs(monday_item)
        writeback   = run_claim_status_check(monday_item)
        return JSONResponse({
            "status":              "success",
            "item_id":             item_id,
            "input_row":           row,
            "claim_status_results": writeback,
        })
    except Exception as e:
        logger.error(
            f"[CS-TEST] Error | item={item_id}: {e}",
            exc_info=True,
        )
        return JSONResponse(
            {"status": "error", "item_id": item_id, "error": str(e)},
            status_code=500,
        )


@router.post("/test/payload/{item_id}")
async def test_claim_status_payload(item_id: str) -> JSONResponse:
    """
    Build + return the Stedi 276 payload without sending it. Useful for
    verifying payer mapping + DOS window before a live call.
    """
    try:
        monday_item = fetch_claims_item(item_id)
        row         = extract_claim_status_inputs(monday_item)
        payer_id, partner_name = _resolve_payer(row)
        payload = build_claim_status_payload(
            row, payer_id=payer_id, partner_name=partner_name,
        )
        display = {k: v for k, v in payload.items() if k != "_meta"}
        return JSONResponse({
            "status":       "ok",
            "item_id":      item_id,
            "input_row":    row,
            "payer_id":     payer_id or payload.get("tradingPartnerServiceId"),
            "partner_name": partner_name or payload.get("_meta", {}).get("tradingPartnerName"),
            "payload":      display,
        })
    except Exception as e:
        logger.error(
            f"[CS-PAYLOAD-TEST] Error | item={item_id}: {e}",
            exc_info=True,
        )
        return JSONResponse({"status": "error", "error": str(e)}, status_code=400)


# =============================================================================
# DEBUG
# =============================================================================

@router.get("/board-columns")
async def get_claims_board_columns() -> JSONResponse:
    """Return all column IDs and types from the Claims Board."""
    if not CLAIMS_BOARD_ID:
        return JSONResponse(
            {"error": "MONDAY_CLAIMS_BOARD_ID env var not set"},
            status_code=400,
        )
    query = """
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns { id title type }
      }
    }
    """
    result = run_query(query, {"boardId": CLAIMS_BOARD_ID})
    cols   = result.get("data", {}).get("boards", [{}])[0].get("columns", [])
    return JSONResponse(
        [{"id": c["id"], "title": c["title"], "type": c["type"]} for c in cols]
    )
