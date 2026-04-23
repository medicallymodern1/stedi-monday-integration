"""
routes/subscription_eligibility_webhook.py
===========================================
FastAPI router for the Stedi eligibility flow on the **Subscription Board**
(board 18407459988).

Completely isolated from the Intake Board eligibility flow
(routes/eligibility_webhook.py) and from the claims/ERA pipeline. Only
touches: Subscription Board reads + Subscription Board writes of the 5
Stedi eligibility columns.

Trigger:
  Monday Subscription Board -> "Run Check" column (color_mm2nnjam) -> "Run"
  -> POST /subscription-eligibility/trigger  (Monday webhook)

Manual run (same result as trigger, no Monday column change needed):
  POST /subscription-eligibility/run/{item_id}

Test endpoints (no Monday write):
  POST /subscription-eligibility/test/{item_id}          — dry run, returns writeback dict
  POST /subscription-eligibility/test/payload/{item_id}  — build + return payload only

Debug:
  GET  /subscription-eligibility/board-columns           — list all Subscription Board cols
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.monday_service import run_query
from services.subscription_eligibility_monday_service import (
    run_and_write_subscription_eligibility,
    SUBSCRIPTION_BOARD_ID,
)
from services.eligibility_worker_pool import submit as pool_submit, pool_stats
from services.subscription_eligibility_service import (
    run_subscription_eligibility_check,
    extract_subscription_eligibility_inputs,
    _resolve_subscription_payer,
)
from stedi_eligibility_builder import build_eligibility_payload

logger = logging.getLogger(__name__)
router = APIRouter()

SUBSCRIPTION_COL_RUN_CHECK = "color_mm2nnjam"   # "Run Check" (verified board export)
RUN_TRIGGER_VALUE          = "Run"              # status label that fires the check


# =============================================================================
# Utilities
# =============================================================================

def _ack(challenge: str | None = None) -> JSONResponse:
    """Monday challenge handshake — always respond immediately."""
    if challenge:
        return JSONResponse({"challenge": challenge})
    return JSONResponse({"status": "ok"})


def fetch_subscription_item(item_id: str) -> dict:
    """Fetch a single Subscription Board item with all column_values."""
    query = """
    query GetSubscriptionItem($itemId: ID!) {
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
        raise ValueError(f"Item {item_id} not found on Subscription Board")
    return items[0]


# =============================================================================
# BACKGROUND WORKER — runs the full pipeline off the event loop
# =============================================================================

def _process_subscription_job(item_id: str) -> None:
    """
    Fetch the Monday item, run eligibility, write back. Runs in the
    eligibility worker-pool thread — NEVER raises; all errors logged.
    """
    try:
        monday_item = fetch_subscription_item(item_id)
        writeback   = run_and_write_subscription_eligibility(item_id, monday_item)
        logger.info(
            f"[SUB-ELG-WEBHOOK] ✓ Complete | item={item_id} "
            f"active={writeback.get('Sub Stedi Active?')!r} "
            f"payer={writeback.get('Stedi Payer Name')!r}"
        )
    except Exception as e:
        logger.error(
            f"[SUB-ELG-WEBHOOK] ✗ Worker error | item={item_id}: {e}",
            exc_info=True,
        )


# =============================================================================
# WEBHOOK TRIGGER — Monday fires this when "Run Check" -> "Run"
# =============================================================================

@router.post("/trigger")
async def subscription_eligibility_trigger(request: Request) -> JSONResponse:
    """
    Monday webhook: fires when "Run Check" (color_mm2nnjam) on the
    Subscription Board changes to "Run".

    Responds to Monday immediately (within 5s), then runs the pipeline.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    # Monday challenge handshake
    if "challenge" in body:
        return _ack(body["challenge"])

    event = body.get("event", {})
    item_id = str(event.get("pulseId") or event.get("itemId") or "")
    col_id  = str(event.get("columnId") or "")

    # Extract the new label from the event value
    event_value = event.get("value", {})
    if isinstance(event_value, dict):
        new_value = str(event_value.get("label", {}).get("text", "")).strip()
    else:
        new_value = ""

    logger.info(
        f"[SUB-ELG-WEBHOOK] Received | item={item_id} col={col_id} value={new_value!r}"
    )

    # Only process the correct trigger column
    if col_id and col_id != SUBSCRIPTION_COL_RUN_CHECK:
        logger.debug(
            f"[SUB-ELG-WEBHOOK] Ignoring — column {col_id} is not the Run Check trigger"
        )
        return _ack()

    # Only process when value == "Run"
    if new_value.lower() != RUN_TRIGGER_VALUE.lower():
        logger.debug(
            f"[SUB-ELG-WEBHOOK] Ignoring — value {new_value!r} is not 'Run'"
        )
        return _ack()

    if not item_id:
        logger.warning("[SUB-ELG-WEBHOOK] No item_id in event — skipping")
        return _ack()

    # Enqueue the job and ACK instantly (within Monday's 5s window).
    # The pool runs up to ELIGIBILITY_POOL_MAX_WORKERS jobs concurrently,
    # bounded by STEDI_MAX_CONCURRENT at the HTTP layer.
    pool_submit(_process_subscription_job, item_id)
    logger.info(
        f"[SUB-ELG-WEBHOOK] Queued | item={item_id} pool={pool_stats()}"
    )
    return _ack()


# =============================================================================
# MANUAL RUN — same as webhook but triggered via API call
# =============================================================================

@router.post("/run/{item_id}")
async def run_subscription_eligibility_and_write(item_id: str) -> JSONResponse:
    """
    Manually trigger a full Subscription eligibility check + write to Monday.
    Useful for re-running without changing the Run Check column value.
    """
    try:
        monday_item = fetch_subscription_item(item_id)
        writeback   = run_and_write_subscription_eligibility(item_id, monday_item)
        return JSONResponse({
            "status":  "success",
            "item_id": item_id,
            "results": writeback,
        })
    except Exception as e:
        logger.error(
            f"[SUB-ELG-RUN] Error | item={item_id}: {e}",
            exc_info=True,
        )
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# =============================================================================
# TEST ENDPOINTS — no Monday writes
# =============================================================================

@router.post("/test/{item_id}")
async def test_subscription_eligibility(item_id: str) -> JSONResponse:
    """
    Dry run: fetch Subscription item -> run full eligibility pipeline ->
    return writeback dict. Does NOT write to Monday.
    """
    try:
        monday_item = fetch_subscription_item(item_id)
        row         = extract_subscription_eligibility_inputs(monday_item)
        writeback   = run_subscription_eligibility_check(monday_item)
        return JSONResponse({
            "status":              "success",
            "item_id":             item_id,
            "input_row":           row,
            "eligibility_results": writeback,
        })
    except Exception as e:
        logger.error(
            f"[SUB-ELG-TEST] Error | item={item_id}: {e}",
            exc_info=True,
        )
        return JSONResponse(
            {"status": "error", "item_id": item_id, "error": str(e)},
            status_code=500,
        )


@router.post("/test/payload/{item_id}")
async def test_subscription_eligibility_payload(item_id: str) -> JSONResponse:
    """
    Build + return the Stedi payload without sending it. Useful for
    verifying payer mapping (Primary Insurance -> Stedi payer ID) before a
    live call.
    """
    try:
        monday_item = fetch_subscription_item(item_id)
        row         = extract_subscription_eligibility_inputs(monday_item)
        payer_id, partner_name = _resolve_subscription_payer(row["Primary Insurance"])
        payload = build_eligibility_payload(
            row, payer_id=payer_id, partner_name=partner_name,
        )
        display = {k: v for k, v in payload.items() if k != "_meta"}
        return JSONResponse({
            "status":       "ok",
            "item_id":      item_id,
            "input_row":    row,
            "payer_id":     payer_id,
            "partner_name": partner_name,
            "payload":      display,
        })
    except Exception as e:
        logger.error(
            f"[SUB-ELG-PAYLOAD-TEST] Error | item={item_id}: {e}",
            exc_info=True,
        )
        return JSONResponse({"status": "error", "error": str(e)}, status_code=400)


# =============================================================================
# DEBUG
# =============================================================================

@router.get("/board-columns")
async def get_subscription_board_columns() -> JSONResponse:
    """
    Return all column IDs and types from the Subscription Board. Use this to
    verify column IDs after any board changes.
    """
    if not SUBSCRIPTION_BOARD_ID:
        return JSONResponse(
            {"error": "MONDAY_SUBSCRIPTION_BOARD_ID env var not set"},
            status_code=400,
        )
    query = """
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns { id title type }
      }
    }
    """
    result = run_query(query, {"boardId": SUBSCRIPTION_BOARD_ID})
    cols   = result.get("data", {}).get("boards", [{}])[0].get("columns", [])
    return JSONResponse(
        [{"id": c["id"], "title": c["title"], "type": c["type"]} for c in cols]
    )
