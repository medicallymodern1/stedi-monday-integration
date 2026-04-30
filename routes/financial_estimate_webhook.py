"""
routes/financial_estimate_webhook.py
=====================================
FastAPI router for the Subscription Board "Calculate Financials"
trigger. When the trigger column flips to "Calculate", we read the
row's Primary Insurance, Subscription, and Inf Qty 1/2, compute
estimated sensors + supplies revenue/cost/GP, write them to Monday,
then clear the trigger (or flip to "Failed" on any error).

Trigger:
  Monday Subscription Board -> "Calculate Financials" (color_mm2w74y8) -> "Calculate"
  -> POST /financial-estimate/trigger

Manual run (same outcome, no Monday column change needed):
  POST /financial-estimate/run/{item_id}

Test endpoint (no Monday writes):
  POST /financial-estimate/test/{item_id}
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from services.eligibility_worker_pool import submit as pool_submit, pool_stats
from services.financial_estimate_monday_service import (
    SUB_FIN_COL,
    CALCULATE_TRIGGER_LABEL,
    extract_financial_inputs,
    run_and_write_financial_estimate,
)
from services.financial_estimate_service import (
    estimate_sensors,
    estimate_supplies,
)
from services.monday_service import run_query

logger = logging.getLogger(__name__)
router = APIRouter()

CALCULATE_FINANCIALS_COL_ID = SUB_FIN_COL["calculate_financials"]


# =============================================================================
# Helpers
# =============================================================================

def _ack(challenge: str | None = None) -> JSONResponse:
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
        raise ValueError(f"Item {item_id} not found")
    return items[0]


# =============================================================================
# Background worker
# =============================================================================

def _process_financial_estimate_job(item_id: str) -> None:
    """Off-the-event-loop worker. Never raises — all errors logged."""
    try:
        item = fetch_subscription_item(item_id)
        result = run_and_write_financial_estimate(item_id, item)
        logger.info(
            f"[FIN-EST-WEBHOOK] ✓ Complete | item={item_id} ok={result.get('ok')}"
        )
    except Exception as e:
        logger.error(
            f"[FIN-EST-WEBHOOK] ✗ Worker error | item={item_id}: {e}",
            exc_info=True,
        )


# =============================================================================
# Webhook trigger
# =============================================================================

@router.post("/trigger")
async def financial_estimate_trigger(request: Request) -> JSONResponse:
    """
    Monday webhook: fires when "Calculate Financials" (color_mm2w74y8)
    on the Subscription Board changes to "Calculate".

    Responds within Monday's 5s window, processes in background.
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
        f"[FIN-EST-WEBHOOK] Received | item={item_id} col={col_id} value={new_value!r}"
    )

    if col_id and col_id != CALCULATE_FINANCIALS_COL_ID:
        logger.debug(
            f"[FIN-EST-WEBHOOK] Ignoring — column {col_id} is not the trigger"
        )
        return _ack()

    if new_value.lower() != CALCULATE_TRIGGER_LABEL.lower():
        # Ignore "Failed" and blank — only "Calculate" runs the job.
        logger.debug(
            f"[FIN-EST-WEBHOOK] Ignoring — value {new_value!r} is not 'Calculate'"
        )
        return _ack()

    if not item_id:
        logger.warning("[FIN-EST-WEBHOOK] No item_id in event — skipping")
        return _ack()

    pool_submit(_process_financial_estimate_job, item_id)
    logger.info(
        f"[FIN-EST-WEBHOOK] Queued | item={item_id} pool={pool_stats()}"
    )
    return _ack()


# =============================================================================
# Manual run
# =============================================================================

@router.post("/run/{item_id}")
async def run_financial_estimate(item_id: str) -> JSONResponse:
    """Manually run the financial estimate for an item — same outcome as the trigger."""
    try:
        item = fetch_subscription_item(item_id)
        result = run_and_write_financial_estimate(item_id, item)
        return JSONResponse({"item_id": item_id, "result": result})
    except Exception as e:
        logger.error(f"[FIN-EST-MANUAL] ✗ Error | item={item_id}: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)


# =============================================================================
# Test (no Monday writes)
# =============================================================================

@router.post("/test/{item_id}")
async def test_financial_estimate(item_id: str) -> JSONResponse:
    """Dry run: compute the estimate but don't write anything to Monday."""
    try:
        item = fetch_subscription_item(item_id)
        inputs = extract_financial_inputs(item)
        primary      = inputs["primary_insurance"]
        subscription = (inputs["subscription"] or "").lower()
        sets         = inputs["sets"]

        sensors  = estimate_sensors(primary) if subscription in ("sensors", "sensors & supplies") else None
        supplies = estimate_supplies(primary, sets) if subscription in ("supplies", "sensors & supplies") else None

        return JSONResponse({
            "item_id": item_id,
            "inputs":  inputs,
            "sensors": sensors,
            "supplies": supplies,
        })
    except Exception as e:
        logger.error(f"[FIN-EST-TEST] ✗ Error | item={item_id}: {e}", exc_info=True)
        return JSONResponse({"error": str(e)}, status_code=500)
