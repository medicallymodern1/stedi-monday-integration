"""
services/financial_estimate_monday_service.py
==============================================
Monday writeback for the Subscription Board "Calculate Financials"
feature. Reads inputs (Primary Insurance, Subscription, Inf Qty 1+2),
calls the pure-math estimator, and writes the 6 numeric output
columns plus the trigger-column flip.

Trigger column behaviour:
  - On success, the trigger column ("Calculate Financials") is cleared.
  - On any failure, the trigger column is flipped to "Failed".

Failure cases (any of these flip Failed):
  - Missing Primary Insurance
  - Primary Insurance has no rate in PAYER_RATE_SCHEDULE
  - Required rate is None
  - Subscription side requires sets but Inf Qty 1 + Inf Qty 2 == 0
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from services.financial_estimate_service import (
    estimate_sensors,
    estimate_supplies,
)
from services.monday_service import run_query

logger = logging.getLogger(__name__)

# Subscription Board (board 18407459988) column IDs — verified from board export.
SUBSCRIPTION_BOARD_ID = os.getenv("MONDAY_SUBSCRIPTION_BOARD_ID")

SUB_FIN_COL = {
    # Inputs
    "primary_insurance":     "color_mm254qxj",  # status
    "subscription":          "color_mm273mv8",  # status: Sensors / Supplies / Sensors & Supplies
    "inf_qty_1":             "numeric_mkw839ks",
    "inf_qty_2":             "numeric_mkwac234",
    # Trigger
    "calculate_financials":  "color_mm2w74y8",  # status: "Calculate" / "Failed" / blank
    # Outputs
    "sensors_revenue":       "numeric_mkxj6a3d",
    "sensors_cost":          "numeric_mkxjxmga",
    "sensors_gp":            "numeric_mkxjyw32",
    "supplies_revenue":      "numeric_mm27rypj",
    "supplies_cost":         "numeric_mm27hem2",
    "supplies_gp":           "numeric_mm2785ag",
}

CALCULATE_TRIGGER_LABEL = "Calculate"
FAILED_TRIGGER_LABEL    = "Failed"


_UPDATE_MUTATION = """
mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
  change_column_value(
    item_id: $itemId,
    board_id: $boardId,
    column_id: $columnId,
    value: $value
  ) { id }
}
"""


# ---------------------------------------------------------------------------
# Input extraction
# ---------------------------------------------------------------------------

def _read_text(item: dict, col_id: str) -> str:
    for c in item.get("column_values", []) or []:
        if c.get("id") == col_id:
            return (c.get("text") or "").strip()
    return ""


def _read_int(item: dict, col_id: str) -> int:
    s = _read_text(item, col_id)
    if not s:
        return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def extract_financial_inputs(monday_item: dict) -> dict[str, Any]:
    """Pull the four inputs we need off a Subscription Board item."""
    primary       = _read_text(monday_item, SUB_FIN_COL["primary_insurance"])
    subscription  = _read_text(monday_item, SUB_FIN_COL["subscription"])
    inf_qty_1     = _read_int(monday_item, SUB_FIN_COL["inf_qty_1"])
    inf_qty_2     = _read_int(monday_item, SUB_FIN_COL["inf_qty_2"])
    return {
        "primary_insurance": primary,
        "subscription":      subscription,
        "inf_qty_1":         inf_qty_1,
        "inf_qty_2":         inf_qty_2,
        "sets":              inf_qty_1 + inf_qty_2,
    }


# ---------------------------------------------------------------------------
# Monday writes
# ---------------------------------------------------------------------------

def _write_column(item_id: str, col_id: str, value: Any) -> None:
    if not SUBSCRIPTION_BOARD_ID:
        logger.error(
            "[FIN-EST-MONDAY] MONDAY_SUBSCRIPTION_BOARD_ID env var not set — "
            "cannot write to Monday."
        )
        return
    run_query(_UPDATE_MUTATION, {
        "itemId":   str(item_id),
        "boardId":  str(SUBSCRIPTION_BOARD_ID),
        "columnId": col_id,
        "value":    json.dumps(value) if not isinstance(value, str) else json.dumps(value),
    })


def _write_number(item_id: str, col_id: str, n: float) -> None:
    """Numeric columns: Monday accepts a JSON-encoded string."""
    _write_column(item_id, col_id, str(n))


def _set_trigger(item_id: str, label: str) -> None:
    """
    Flip the Calculate Financials trigger column.
    - label="Failed"  -> mark Failed
    - label=""        -> clear (success)
    """
    value: dict[str, str] = {"label": label} if label else {"label": ""}
    _write_column(item_id, SUB_FIN_COL["calculate_financials"], value)


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------

def run_and_write_financial_estimate(item_id: str, monday_item: dict) -> dict[str, Any]:
    """
    Full pipeline: read inputs -> estimate -> write outputs -> flip trigger.
    Returns a summary dict for logging / inspection.
    """
    inputs = extract_financial_inputs(monday_item)
    primary      = inputs["primary_insurance"]
    subscription = (inputs["subscription"] or "").lower()
    sets         = inputs["sets"]

    logger.info(
        f"[FIN-EST] Start | item={item_id} primary={primary!r} "
        f"subscription={inputs['subscription']!r} "
        f"inf_qty_1={inputs['inf_qty_1']} inf_qty_2={inputs['inf_qty_2']} sets={sets}"
    )

    needs_sensors  = subscription in ("sensors", "sensors & supplies")
    needs_supplies = subscription in ("supplies", "sensors & supplies")

    if not (needs_sensors or needs_supplies):
        msg = f"Subscription {inputs['subscription']!r} is empty or unrecognized"
        logger.warning(f"[FIN-EST] ! Failed | item={item_id} reason={msg}")
        _set_trigger(item_id, FAILED_TRIGGER_LABEL)
        return {"ok": False, "reason": msg}

    failures: list[str] = []
    sensors_result: dict[str, Any] | None = None
    supplies_result: dict[str, Any] | None = None

    if needs_sensors:
        sensors_result = estimate_sensors(primary)
        if not sensors_result["ok"]:
            failures.append(f"Sensors: {sensors_result['reason']}")

    if needs_supplies:
        supplies_result = estimate_supplies(primary, sets)
        if not supplies_result["ok"]:
            failures.append(f"Supplies: {supplies_result['reason']}")

    # Write whichever side(s) succeeded
    if sensors_result and sensors_result["ok"]:
        _write_number(item_id, SUB_FIN_COL["sensors_revenue"], sensors_result["revenue"])
        _write_number(item_id, SUB_FIN_COL["sensors_cost"],    sensors_result["cost"])
        _write_number(item_id, SUB_FIN_COL["sensors_gp"],      sensors_result["gp"])
        logger.info(
            f"[FIN-EST] Sensors written | item={item_id} "
            f"rev={sensors_result['revenue']} cost={sensors_result['cost']} "
            f"gp={sensors_result['gp']}"
        )

    if supplies_result and supplies_result["ok"]:
        _write_number(item_id, SUB_FIN_COL["supplies_revenue"], supplies_result["revenue"])
        _write_number(item_id, SUB_FIN_COL["supplies_cost"],    supplies_result["cost"])
        _write_number(item_id, SUB_FIN_COL["supplies_gp"],      supplies_result["gp"])
        logger.info(
            f"[FIN-EST] Supplies written | item={item_id} "
            f"rev={supplies_result['revenue']} cost={supplies_result['cost']} "
            f"gp={supplies_result['gp']} "
            f"payer={supplies_result.get('supplies_payer')!r} "
            f"inf_units={supplies_result.get('infusion_units')} "
            f"cart_units={supplies_result.get('cartridge_units')}"
        )

    # Flip trigger column
    if failures:
        logger.warning(
            f"[FIN-EST] ! Failed | item={item_id} reasons={failures}"
        )
        _set_trigger(item_id, FAILED_TRIGGER_LABEL)
        return {"ok": False, "reasons": failures,
                "sensors": sensors_result, "supplies": supplies_result}

    logger.info(f"[FIN-EST] ✓ Done | item={item_id}")
    _set_trigger(item_id, "")
    return {"ok": True, "sensors": sensors_result, "supplies": supplies_result}
