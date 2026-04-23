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

  color_mm2nzm33   "Active?"           (status)   labels: "Active" / "Inactive" / "Medicare Advantage"
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
    "active":               "color_mm2nzm33",   # status: "Active" / "Inactive" / "Medicare Advantage"
    "plan_begin":           "date_mm2n4b26",    # date
    "member_id":            "text_mm2phve4",    # text  ("Stedi Member ID")
    "payer_name":           "dropdown_mm2nz3wd",# dropdown (auto-create labels)
    "plan_name":            "dropdown_mm2n7ps1",# dropdown (auto-create labels)
    "deductible":           "numeric_mm2nkcfx", # numbers
    "prior_auth_required":  "color_mm2pj23n",   # status: "Yes" / "No" / "Evaluate"
}

# Run Check is the trigger column (user flips it to "Run" to fire the
# webhook). When the eligibility check can't produce a usable answer
# (Stedi COVERAGE_INFORMATION_UNAVAILABLE, or validation / HTTP error),
# we flip it to "Failed" so the row is visually distinct and never
# mistaken for a real "Inactive" result.
SUBSCRIPTION_RUN_CHECK_COL   = "color_mm2nnjam"
SUBSCRIPTION_RUN_CHECK_FAILED_LABEL = "Failed"

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
    member_id  = (writeback.get("Stedi Member ID") or "").strip()
    payer_name = (writeback.get("Stedi Payer Name") or "").strip()
    plan_name  = (writeback.get("Stedi Plan Name") or "").strip()
    ded_rem    = writeback.get("Stedi Individual Deductible Remaining")
    pa_req     = (writeback.get("Sub Prior Auth Req?") or "").strip()

    values: dict[str, Any] = {}

    # --- Active? (status) -------------------------------------------------
    # Parser returns "Yes", "No", or "Medicare Advantage".
    # Board labels are "Active", "Inactive", and "Medicare Advantage"
    # (the MA label was added on the board so billing knows not to bill
    # CMS 16013 for those patients).
    if active_yn == "Yes":
        values[SUBSCRIPTION_OUTPUT_COL["active"]] = {"label": "Active"}
    elif active_yn == "No":
        values[SUBSCRIPTION_OUTPUT_COL["active"]] = {"label": "Inactive"}
    elif active_yn == "Medicare Advantage":
        values[SUBSCRIPTION_OUTPUT_COL["active"]] = {"label": "Medicare Advantage"}
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

    # --- Stedi Member ID (text) ------------------------------------------
    # Just pass through whatever the 271 gave us; Monday text columns take
    # a plain string. Skip when blank so we don't overwrite a good value
    # with "" on a response that happened to lack subscriber.memberId.
    if member_id:
        values[SUBSCRIPTION_OUTPUT_COL["member_id"]] = member_id

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

    # --- Prior Auth Req? (status) ----------------------------------------
    # Labels on the board are "Yes" / "No" / "Evaluate". We write whatever
    # the service-layer resolver produced; create_labels_if_missing on the
    # mutation means the label will auto-create if it hasn't been added
    # yet (so adding "Evaluate" to the column on Monday is optional from a
    # robustness standpoint, though the user is adding it manually).
    if pa_req in ("Yes", "No", "Evaluate"):
        values[SUBSCRIPTION_OUTPUT_COL["prior_auth_required"]] = {"label": pa_req}

    return values


def write_subscription_eligibility_to_monday(
    item_id: str,
    writeback: dict[str, Any],
) -> None:
    """
    Write the Subscription Board eligibility columns for ``item_id``.

    Two branches:

    * "Failed" path — when ``writeback`` carries ``_subscription_failed``
      (set by the service layer for COVERAGE_INFORMATION_UNAVAILABLE,
      validation errors, or HTTP errors), write ONLY the Run Check
      column -> "Failed". All 5 result columns are deliberately left
      untouched so we don't clobber whatever was already there (or
      overwrite it with misleading blanks).

    * Normal path — encode the 5 Subscription output columns and fire
      one change_multiple_column_values mutation.

    Partial writeback in the normal path is fine — any blank fields
    are just omitted.
    """
    if not SUBSCRIPTION_BOARD_ID:
        logger.error(
            "[SUB-ELG-MONDAY] MONDAY_SUBSCRIPTION_BOARD_ID env var is not set "
            "and no default is baked in — results cannot be written back."
        )
        return

    # --- Failed path -----------------------------------------------------
    if writeback.get("_subscription_failed"):
        reason = str(writeback.get("_failure_reason") or "").strip()
        values = {
            SUBSCRIPTION_RUN_CHECK_COL: {
                "label": SUBSCRIPTION_RUN_CHECK_FAILED_LABEL
            }
        }
        try:
            run_query(_UPDATE_MULTI_MUTATION, {
                "itemId":       str(item_id),
                "boardId":      str(SUBSCRIPTION_BOARD_ID),
                "columnValues": json.dumps(values),
                # create_labels_if_missing handles status labels too, so
                # if "Failed" hasn't been added to the Run Check column
                # yet Monday will create it on first use.
                "createLabels": True,
            })
            logger.info(
                f"[SUB-ELG-MONDAY] ✓ Run Check -> Failed for item {item_id} "
                f"(reason: {reason!r})"
            )
        except Exception as e:
            logger.warning(
                f"[SUB-ELG-MONDAY] ✗ Failed-path write failed for "
                f"item {item_id}: {e}"
            )
        return

    # --- Normal path -----------------------------------------------------
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
