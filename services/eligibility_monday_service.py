"""
services/eligibility_monday_service.py
=======================================
Writes Stedi eligibility results back to the Monday Onboarding Board.

Column IDs must be updated once Brandon creates the 16 output columns
and exports the board. Run GET /test/onboarding-board-columns to retrieve them.

Until column IDs are confirmed, set each via environment variable:
  ELIG_OUT_ACTIVE, ELIG_OUT_IN_NETWORK, ... (see mapping below)
"""

from __future__ import annotations

import json
import logging
import os

from services.monday_service import run_query

logger = logging.getLogger(__name__)

ONBOARDING_BOARD_ID = os.getenv("MONDAY_ONBOARDING_BOARD_ID", "")

# ── Output column ID map ──────────────────────────────────────────────────────
# Keys  = Monday column names (from MONDAY_ELIGIBILITY_OUTPUT_COLUMN_MAP)
# Values = Monday column IDs — set via env vars or replace with real IDs once confirmed
#
# To find real IDs: GET /test/onboarding-board-columns
ELIGIBILITY_OUTPUT_COLUMN_IDS: dict[str, str] = {
    "Stedi Eligibility Active?":             os.getenv("ELIG_OUT_ACTIVE",       ""),
    "Stedi In Network?":                     os.getenv("ELIG_OUT_IN_NETWORK",   ""),
    "Stedi Plan Name":                       os.getenv("ELIG_OUT_PLAN_NAME",    ""),
    "Stedi Prior Auth Required?":            os.getenv("ELIG_OUT_PRIOR_AUTH",   ""),
    "Stedi Copay":                           os.getenv("ELIG_OUT_COPAY",        ""),
    "Stedi Coinsurance %":                   os.getenv("ELIG_OUT_COINSURANCE",  ""),
    "Stedi Individual Deductible":           os.getenv("ELIG_OUT_IND_DED",      ""),
    "Stedi Individual Deductible Remaining": os.getenv("ELIG_OUT_IND_DED_REM",  ""),
    "Stedi Family Deductible":               os.getenv("ELIG_OUT_FAM_DED",      ""),
    "Stedi Family Deductible Remaining":     os.getenv("ELIG_OUT_FAM_DED_REM",  ""),
    "Stedi Individual OOP Max":              os.getenv("ELIG_OUT_IND_OOP",      ""),
    "Stedi Individual OOP Max Remaining":    os.getenv("ELIG_OUT_IND_OOP_REM",  ""),
    "Stedi Family OOP Max":                  os.getenv("ELIG_OUT_FAM_OOP",      ""),
    "Stedi Family OOP Max Remaining":        os.getenv("ELIG_OUT_FAM_OOP_REM",  ""),
    "Stedi Plan Begin Date":                 os.getenv("ELIG_OUT_PLAN_BEGIN",   ""),
    "Stedi Eligibility Error Description":   os.getenv("ELIG_OUT_ERROR",        ""),
}

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


def write_eligibility_to_monday(item_id: str, writeback_payload: dict) -> None:
    """
    Write eligibility results to the Monday Onboarding Board item.

    writeback_payload keys are Monday column names from
    MONDAY_ELIGIBILITY_OUTPUT_COLUMN_MAP (e.g. "Stedi Plan Name").

    Skips columns with blank values or missing column IDs (PRD FR6 pattern).
    Always writes eligibility_error_description if present, even on failure.
    """
    if not ONBOARDING_BOARD_ID:
        logger.warning("[ELG] MONDAY_ONBOARDING_BOARD_ID not set — skipping writeback")
        return

    wrote_any = False

    for column_name, value in writeback_payload.items():
        if value is None or value == "":
            continue

        col_id = ELIGIBILITY_OUTPUT_COLUMN_IDS.get(column_name, "")
        if not col_id:
            logger.warning(f"[ELG] No column ID configured for: {column_name!r} — skipped")
            continue

        try:
            run_query(_UPDATE_MUTATION, {
                "itemId":   str(item_id),
                "boardId":  str(ONBOARDING_BOARD_ID),
                "columnId": col_id,
                "value":    json.dumps(str(value)),
            })
            logger.info(f"[ELG] Wrote {column_name!r} = {value!r}")
            wrote_any = True
        except Exception as e:
            logger.warning(f"[ELG] Failed to write {column_name!r}: {e}")

    if not wrote_any:
        logger.warning(f"[ELG] No columns written for item {item_id} — check column ID env vars")


def run_and_write_eligibility(item_id: str, monday_item: dict) -> dict:
    """
    Convenience: run eligibility check + write results to Monday in one call.
    Returns the writeback payload for inspection/logging.
    """
    from services.eligibility_service import run_eligibility_check

    writeback = run_eligibility_check(monday_item)
    write_eligibility_to_monday(item_id, writeback)
    return writeback