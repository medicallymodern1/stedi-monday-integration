"""
routes/eligibility_webhook.py
==============================
FastAPI router for the Stedi eligibility flow.

This router is completely isolated from the claims/ERA pipeline.
It only touches: Intake Board reads + Intake Board writes (Stedi output columns).

Trigger:
  Monday Intake Board → "Run Stedi Eligibility" column → set to "Run"
  → POST /eligibility/trigger  (Monday webhook)

Manual run (same result as trigger, no Monday column change needed):
  POST /eligibility/run/{item_id}

Test endpoints (no Monday write):
  POST /eligibility/test/{item_id}           — full dry run, returns writeback dict
  POST /eligibility/test/payload/{item_id}   — build + return payload only (no Stedi call)

Debug:
  GET  /eligibility/intake-board-columns     — list all Intake Board column IDs
"""

from __future__ import annotations

import logging
import os
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.monday_service import run_query
from services.eligibility_monday_service import run_and_write_eligibility
from services.eligibility_service import run_eligibility_check, extract_eligibility_inputs
from stedi_eligibility_builder import build_eligibility_payload

logger = logging.getLogger(__name__)
router = APIRouter()

INTAKE_BOARD_ID     = os.getenv("MONDAY_INTAKE_BOARD_ID", "")
INTAKE_COL_RUN_ELIG = "color_mm1yeksx"   # "Run Stedi Eligibility" — verified from board export
RUN_TRIGGER_VALUE   = "Run"              # The status label that fires the check


# =============================================================================
# Utilities
# =============================================================================

def _ack(challenge: str | None = None) -> JSONResponse:
    """Monday challenge handshake — always respond immediately."""
    if challenge:
        return JSONResponse({"challenge": challenge})
    return JSONResponse({"status": "ok"})


def fetch_intake_item(item_id: str) -> dict:
    """Fetch a single Intake Board item with all column_values."""
    query = """
    query GetIntakeItem($itemId: ID!) {
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
        raise ValueError(f"Item {item_id} not found on Intake Board")
    return items[0]


# =============================================================================
# WEBHOOK TRIGGER — Monday fires this when "Run Stedi Eligibility" → "Run"
# =============================================================================

@router.post("/trigger")
async def eligibility_trigger(request: Request) -> JSONResponse:
    """
    Monday webhook: fires when "Run Stedi Eligibility" column changes to "Run".

    Responds to Monday immediately (within 5s), then runs pipeline.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "Invalid JSON"}, status_code=400)

    # Monday challenge handshake
    if "challenge" in body:
        return _ack(body["challenge"])

    event = body.get("event", {})
    item_id   = str(event.get("pulseId") or event.get("itemId") or "")
    col_id    = str(event.get("columnId") or "")

    # Extract the new label from the event value
    event_value = event.get("value", {})
    if isinstance(event_value, dict):
        new_value = str(event_value.get("label", {}).get("text", "")).strip()
    else:
        new_value = ""

    logger.info(
        f"[ELG-WEBHOOK] Received | item={item_id} col={col_id} value={new_value!r}"
    )

    # Only process the correct trigger column
    if col_id and col_id != INTAKE_COL_RUN_ELIG:
        logger.debug(f"[ELG-WEBHOOK] Ignoring — column {col_id} is not the run trigger")
        return _ack()

    # Only process when value = "Run"
    if new_value.lower() != RUN_TRIGGER_VALUE.lower():
        logger.debug(f"[ELG-WEBHOOK] Ignoring — value {new_value!r} is not 'Run'")
        return _ack()

    if not item_id:
        logger.warning("[ELG-WEBHOOK] No item_id in event — skipping")
        return _ack()

    # Run pipeline (ACK already sent above via return)
    try:
        monday_item = fetch_intake_item(item_id)
        writeback   = run_and_write_eligibility(item_id, monday_item)
        logger.info(
            f"[ELG-WEBHOOK] ✓ Complete | item={item_id} "
            f"part_b_active={writeback.get('Stedi Part B Active?')!r} "
            f"coverage_type={writeback.get('Stedi Coverage Type')!r}"
        )
    except Exception as e:
        logger.error(f"[ELG-WEBHOOK] ✗ Error | item={item_id}: {e}", exc_info=True)

    return _ack()


# =============================================================================
# MANUAL RUN — same as webhook but triggered via API call
# =============================================================================

@router.post("/run/{item_id}")
async def run_eligibility_and_write(item_id: str) -> JSONResponse:
    """
    Manually trigger a full eligibility check + write to Monday for a given item.
    Same as the webhook trigger but invoked directly — useful for re-running
    without changing the Monday column value.
    """
    try:
        monday_item = fetch_intake_item(item_id)
        writeback   = run_and_write_eligibility(item_id, monday_item)
        return JSONResponse({
            "status":  "success",
            "item_id": item_id,
            "results": writeback,
        })
    except Exception as e:
        logger.error(f"[ELG-RUN] Error | item={item_id}: {e}", exc_info=True)
        return JSONResponse({"status": "error", "error": str(e)}, status_code=500)


# =============================================================================
# TEST ENDPOINTS — no Monday writes
# =============================================================================

@router.post("/test/{item_id}")
async def test_eligibility(item_id: str) -> JSONResponse:
    """
    Dry run: fetch item → run full eligibility pipeline → return writeback dict.
    Does NOT write to Monday. Use for testing before the webhook is wired.
    Returns all 23 V1 output fields.
    """
    try:
        monday_item = fetch_intake_item(item_id)
        row         = extract_eligibility_inputs(monday_item)
        writeback   = run_eligibility_check(monday_item)
        return JSONResponse({
            "status":              "success",
            "item_id":             item_id,
            "input_row":           row,
            "eligibility_results": writeback,
        })
    except Exception as e:
        logger.error(f"[ELG-TEST] Error | item={item_id}: {e}", exc_info=True)
        return JSONResponse({"status": "error", "item_id": item_id, "error": str(e)}, status_code=500)


@router.post("/test/payload/{item_id}")
async def test_eligibility_payload(item_id: str) -> JSONResponse:
    """
    Build + return the Stedi payload without sending it.
    Use to verify payer mapping and subscriber fields before a live call.
    """
    try:
        monday_item = fetch_intake_item(item_id)
        row         = extract_eligibility_inputs(monday_item)
        payload     = build_eligibility_payload(row)
        # Strip internal meta before returning to caller
        display = {k: v for k, v in payload.items() if k != "_meta"}
        return JSONResponse({
            "status":    "ok",
            "item_id":   item_id,
            "input_row": row,
            "payload":   display,
        })
    except Exception as e:
        logger.error(f"[ELG-PAYLOAD-TEST] Error | item={item_id}: {e}", exc_info=True)
        return JSONResponse({"status": "error", "error": str(e)}, status_code=400)


# =============================================================================
# DEBUG
# =============================================================================

@router.get("/intake-board-columns")
async def get_intake_board_columns() -> JSONResponse:
    """
    Return all column IDs and types from the Intake Board.
    Use this to verify column IDs after any board changes.
    """
    if not INTAKE_BOARD_ID:
        return JSONResponse(
            {"error": "MONDAY_INTAKE_BOARD_ID env var not set"},
            status_code=400,
        )
    query = """
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns { id title type }
      }
    }
    """
    result  = run_query(query, {"boardId": INTAKE_BOARD_ID})
    cols    = result.get("data", {}).get("boards", [{}])[0].get("columns", [])
    return JSONResponse([{"id": c["id"], "title": c["title"], "type": c["type"]} for c in cols])