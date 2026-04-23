"""
services/subscription_eligibility_monday_service.py
====================================================
Writes the 5 subscription-side Stedi eligibility results back to the Monday
Subscription Board (board 18407459988).

Unlike the Intake Board — whose Stedi columns are all ``text`` and therefore
use the same stringified-JSON write for every field — the Subscription Board
stores these 5 fields in 4 different column types (status / date / dropdown /
number). One ``change_multiple_column_values`` call with
``create_labels_if_missing: true`` handles the mix cleanly and lets the
Payer Name / Plan Name dropdowns auto-add labels for unseen payers and plans.

Column IDs verified against live board export 2026-04:

  color_mm2nzm33   "Active?"           (status)   labels: 1="Active" / 2="Inactive"
  date_mm2n4b26    "Date Plan Begin"   (date)
  dropdown_mm2nz3wd "Stedi Payer Name"  (dropdown) labels auto-created
  dropdown_mm2n7ps1 "Stedi Plan Name"   (dropdown) labels auto-created
  numeric_mm2nkcfx "Ded. Remaining"    (numbers)  $ unit

Writeback keys come from the full parser output, same as the Intake flow.
We pick the five we need and encode them per Monday's per-type JSON shapes.

Blank values are skipped — existing cells are preserved (parity with
Intake's write_eligibility_to_monday).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from services.monday_service import run_query

logger = logging.getLogger(__name__)

# Default is the confirmed board ID; env var wins so a local/test board can
# be pointed at without changing code.
SUBSCRIPTION_BOARD_ID = os.getenv("MONDAY_SUBSCRIPTION_BOARD_ID", "18407459988")

# Column IDs on the Subscription Board (verified 2026-04)
SUBSCRIPTION_OUTPUT_COL = {
    "active":      "color_mm2nzm33",   # status: "Active" / "Inactive"
    "plan_begin":  "date_mm2n4b26",    # date
    "payer_name":  "dropdown_mm2nz3wd",# dropdown (auto-create labels)
    "plan_name":   "dropdown_mm2n7ps1",# dropdown (auto-create labels)
    "deductible":  "numeric_mm2nkcfx", # numbers
}

# change_multiple_column_values is the only Monday mutation that accepts
# create_labels_if_missing, which we need for the two dropdowns.
_UPDATE_MULTI_MUTATION = """
mutation ChangeMulti(
  $itemId: ID!,
  $boardId: ID!,
  $columnValues: JSON!,
  $createLabels: Boolean!
) {
  change_multiple_column_values(
    item_id: $itemId,
    board_id: $boardId,
    column_values: $columnValues,
    create_labels_if_missing: $createLabels
  ) { id }
}
"""


def _encode_subscription_columns(writeback: dict[str, Any]) -> dict[str, Any]:
    """
    Pick the 5 relevant fields out of the full eligibility writeback dict
    and encode them in the per-type JSON shapes Monday expects.

    - status    -> {"label": "..."}            (must be one of the column's labels)
    - date      -> {"date": "YYYY-MM-DD"}      (parser already returns that format)
    - dropdown  -> {"labels": ["..."]}         (create_labels_if_missing handles new ones)
    - numbers   -> "500.00"                    (plain stringified number)

    Returns a dict keyed by Monday column ID. Only columns with non-blank
    values are included — blanks are dropped so we never overwrite existing
    Monday data with empty strings.
    """
    # Prefer the Subscription-specific active flag (computed in
    # subscription_eligibility_service._compute_subscription_active). Fall
    # back to the Medicare-centric Stedi Part B Active? only if the new
    # key is absent, so callers that build a writeback dict by hand (tests,
    # one-off scripts) keep working.
    active_yn  = (
        writeback.get("Sub Stedi Active?")
        or writeback.get("Stedi Part B Active?")
        or ""
    ).strip()
    plan_begin = (writeback.get("Stedi Plan Begin Date") or "").strip()
    payer_name = (writeback.get("Stedi Payer Name") or "").strip()
    plan_name  = (writeback.get("Stedi Plan Name") or "").strip()
    ded_rem    = writeback.get("Stedi Individual Deductible Remaining")

    values: dict[str, Any] = {}

    # --- Active? (status) -------------------------------------------------
    # Parser returns "Yes" / "No". Board labels are "Active" / "Inactive".
    if active_yn == "Yes":
        values[SUBSCRIPTION_OUTPUT_COL["active"]] = {"label": "Active"}
    elif active_yn == "No":
        values[SUBSCRIPTION_OUTPUT_COL["active"]] = {"label": "Inactive"}
    # Anything else (blank, unexpected) -> leave the cell untouched.

    # --- Date Plan Begin (date) ------------------------------------------
    # Parser already formats YYYY-MM-DD when the source is YYYYMMDD; if we
    # get anything non-empty but oddly formatted, skip rather than risk a
    # bad write (Monday's date column is strict about format).
    if plan_begin and len(plan_begin) == 10 and plan_begin[4] == "-" and plan_begin[7] == "-":
        values[SUBSCRIPTION_OUTPUT_COL["plan_begin"]] = {"date": plan_begin}
    elif plan_begin:
        logger.warning(
            f"[SUB-ELG-MONDAY] Skipping Date Plan Begin — unexpected format: "
            f"{plan_begin!r} (expected YYYY-MM-DD)"
        )

    # --- Stedi Payer Name (dropdown) -------------------------------------
    if payer_name:
        values[SUBSCRIPTION_OUTPUT_COL["payer_name"]] = {"labels": [payer_name]}

    # --- Stedi Plan Name (dropdown) --------------------------------------
    if plan_name:
        values[SUBSCRIPTION_OUTPUT_COL["plan_name"]] = {"labels": [plan_name]}

    # --- Ded. Remaining (numbers) ----------------------------------------
    # Parser returns "" if no deductible-remaining benefit row was found;
    # otherwise a numeric string like "500" or "1500.00".
    if ded_rem not in (None, "", "Not returned"):
        try:
            # Let Monday store the exact value; coerce to str to satisfy JSON.
            float(ded_rem)  # validate numeric
            values[SUBSCRIPTION_OUTPUT_COL["deductible"]] = str(ded_rem)
        except (TypeError, ValueError):
            logger.warning(
                f"[SUB-ELG-MONDAY] Skipping Ded. Remaining — not numeric: "
                f"{ded_rem!r}"
            )

    return values


def write_subscription_eligibility_to_monday(
    item_id: str,
    writeback: dict[str, Any],
) -> None:
    """
    Write the 5 Subscription Board eligibility columns for ``item_id``.

    One single change_multiple_column_values mutation covers all 5 columns.
    Partial writeback is fine — any blank fields are just omitted.
    """
    if not SUBSCRIPTION_BOARD_ID:
        logger.error(
            "[SUB-ELG-MONDAY] MONDAY_SUBSCRIPTION_BOARD_ID env var is not set "
            "and no default is baked in — results cannot be written back."
        )
        return

    values = _encode_subscription_columns(writeback)
    if not values:
        logger.warning(
            f"[SUB-ELG-MONDAY] Nothing to write for item {item_id} — "
            f"all 5 target fields were blank or unmapped. "
            f"writeback keys: {list(writeback.keys())}"
        )
        return

    try:
        run_query(_UPDATE_MULTI_MUTATION, {
            "itemId":       str(item_id),
            "boardId":      str(SUBSCRIPTION_BOARD_ID),
            "columnValues": json.dumps(values),
            "createLabels": True,
        })
        logger.info(
            f"[SUB-ELG-MONDAY] ✓ wrote {len(values)} column(s) for item {item_id}: "
            f"{sorted(values.keys())}"
        )
    except Exception as e:
        # Parity with the Intake writer: warn, never raise. Webhook ACK has
        # already been sent by the route; we don't want a transient Monday
        # failure to 500 the webhook.
        logger.warning(
            f"[SUB-ELG-MONDAY] ✗ change_multiple_column_values failed for "
            f"item {item_id}: {e}"
        )


def run_and_write_subscription_eligibility(
    item_id: str,
    monday_item: dict,
) -> dict:
    """
    Convenience: run the Subscription pipeline + write to Monday in one call.

    Used by the Subscription webhook trigger handler. Returns the full
    writeback dict for logging / debugging.
    """
    # Imported lazily to keep the module-import graph clean and match the
    # pattern used by services/eligibility_monday_service.py.
    from services.subscription_eligibility_service import (
        run_subscription_eligibility_check,
    )

    writeback = run_subscription_eligibility_check(monday_item)
    write_subscription_eligibility_to_monday(item_id, writeback)
    return writeback
