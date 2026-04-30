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
SUBSCRIPTION_BOARD_ID = os.getenv("MONDAY_SUBSCRIPTION_BOARD_ID", "18407459988")

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
    # Per-fill totals — computed by us (replaces the previous Monday formula
    # columns so all financial math lives in one place).
    "total_revenue":         "numeric_mm2xsjm5",
    "total_cost":            "numeric_mm2xgvxx",
    "total_gp":              "numeric_mm2xvjc1",
    # Annualised totals — written when no side fails.
    "arr":                   "numeric_mm2xsqyd",  # Annual Recurring Revenue
    "arp":                   "numeric_mm2xdsvh",  # Annual Recurring Profit
}

# Patients whose primary insurance bills bi-monthly (6 fills/year)
# instead of quarterly (4). Brandon's spec: "if it's medicaid, x6".
# Low-Cost / CHP / Essential plans are NOT included by default since
# they're subsidized-commercial rather than pure Medicaid — confirm
# with Brandon if you want them in.
MEDICAID_FILLS_PER_YEAR = {
    "Fidelis Medicaid",
    "Anthem BCBS Medicaid (JLJ)",
    "United Medicaid",
    "Medicaid",
}
ARR_QUARTERLY_MULTIPLIER = 4
ARR_MEDICAID_MULTIPLIER  = 6

CALCULATE_TRIGGER_LABEL = "Calculate"
FAILED_TRIGGER_LABEL    = "Failed"


# One mutation per row instead of 11, so 500 rows in a row don't blow
# through Monday's per-minute complexity budget. The whole writeback
# (numeric columns + trigger flip) goes in a single call.
_UPDATE_MULTI_MUTATION = """
mutation UpdateMulti($itemId: ID!, $boardId: ID!, $columnValues: JSON!) {
  change_multiple_column_values(
    item_id: $itemId,
    board_id: $boardId,
    column_values: $columnValues
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

def _write_all(item_id: str, column_values: dict[str, Any]) -> None:
    """
    Single Monday API call that updates every populated column in one shot.

    column_values is a {column_id: value} dict where each value is in
    Monday's native shape for that column type:
      - Numeric columns: a string (e.g., "954.00")
      - Status columns:  {"label": "Failed"} or {"label": ""} to clear

    No-op if there's nothing to write.
    """
    if not SUBSCRIPTION_BOARD_ID:
        logger.error(
            "[FIN-EST-MONDAY] MONDAY_SUBSCRIPTION_BOARD_ID env var not set — "
            "cannot write to Monday."
        )
        return
    if not column_values:
        return
    run_query(_UPDATE_MULTI_MUTATION, {
        "itemId":       str(item_id),
        "boardId":      str(SUBSCRIPTION_BOARD_ID),
        "columnValues": json.dumps(column_values),
    })


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
        _write_all(item_id, {
            SUB_FIN_COL["calculate_financials"]: {"label": FAILED_TRIGGER_LABEL},
        })
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

    # Build the column-values payload incrementally so the whole writeback
    # goes in ONE Monday mutation per row (was 11 — kicking off 500 rows in
    # a row used to blow through Monday's per-minute complexity budget).
    payload: dict[str, Any] = {}

    if sensors_result and sensors_result["ok"]:
        payload[SUB_FIN_COL["sensors_revenue"]] = str(sensors_result["revenue"])
        payload[SUB_FIN_COL["sensors_cost"]]    = str(sensors_result["cost"])
        payload[SUB_FIN_COL["sensors_gp"]]      = str(sensors_result["gp"])

    if supplies_result and supplies_result["ok"]:
        payload[SUB_FIN_COL["supplies_revenue"]] = str(supplies_result["revenue"])
        payload[SUB_FIN_COL["supplies_cost"]]    = str(supplies_result["cost"])
        payload[SUB_FIN_COL["supplies_gp"]]      = str(supplies_result["gp"])

    # Failure path — only write the side(s) that succeeded, then flip the
    # trigger to Failed in the same mutation.
    if failures:
        payload[SUB_FIN_COL["calculate_financials"]] = {"label": FAILED_TRIGGER_LABEL}
        _write_all(item_id, payload)
        logger.warning(
            f"[FIN-EST] ! Failed | item={item_id} reasons={failures}"
        )
        return {"ok": False, "reasons": failures,
                "sensors": sensors_result, "supplies": supplies_result}

    # Success path — also compute totals + annualised values.
    total_revenue = 0.0
    total_cost    = 0.0
    total_gp      = 0.0
    if sensors_result and sensors_result["ok"]:
        total_revenue += sensors_result["revenue"]
        total_cost    += sensors_result["cost"]
        total_gp      += sensors_result["gp"]
    if supplies_result and supplies_result["ok"]:
        total_revenue += supplies_result["revenue"]
        total_cost    += supplies_result["cost"]
        total_gp      += supplies_result["gp"]

    total_revenue = round(total_revenue, 2)
    total_cost    = round(total_cost,    2)
    total_gp      = round(total_gp,      2)

    payload[SUB_FIN_COL["total_revenue"]] = str(total_revenue)
    payload[SUB_FIN_COL["total_cost"]]    = str(total_cost)
    payload[SUB_FIN_COL["total_gp"]]      = str(total_gp)

    # Canonical primary (apply alias) so e.g. "Magnacare" matches the same
    # casing used in the Medicaid set if/when it lands there.
    from services.financial_estimate_service import _canonical
    canonical_primary = _canonical((primary or "").strip())
    multiplier = (ARR_MEDICAID_MULTIPLIER
                  if canonical_primary in MEDICAID_FILLS_PER_YEAR
                  else ARR_QUARTERLY_MULTIPLIER)
    arr = round(total_revenue * multiplier, 2)
    arp = round(total_gp      * multiplier, 2)
    payload[SUB_FIN_COL["arr"]] = str(arr)
    payload[SUB_FIN_COL["arp"]] = str(arp)

    # Clear the trigger column on success — same mutation.
    payload[SUB_FIN_COL["calculate_financials"]] = {"label": ""}

    _write_all(item_id, payload)
    logger.info(
        f"[FIN-EST] ✓ Done | item={item_id} "
        f"sensors={sensors_result and sensors_result.get('revenue')} "
        f"supplies={supplies_result and supplies_result.get('revenue')} "
        f"total_rev={total_revenue} total_gp={total_gp} "
        f"x{multiplier} ARR={arr} ARP={arp}"
    )
    return {
        "ok": True,
        "sensors":       sensors_result,
        "supplies":      supplies_result,
        "total_revenue": total_revenue,
        "total_cost":    total_cost,
        "total_gp":      total_gp,
        "arr":           arr,
        "arp":           arp,
        "multiplier":    multiplier,
    }
