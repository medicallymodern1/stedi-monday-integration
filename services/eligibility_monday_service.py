"""
services/eligibility_monday_service.py
=======================================
Writes Stedi eligibility results back to the Monday Intake Board.

PRD rules enforced here:
  - Blank values are NOT written (existing Monday cells preserved)
  - "Not returned" IS written (valid PRD value for Stedi Secondary / Medicaid ID)
  - Per-column write failures are warned, never raised (partial writeback is OK)
  - Error description is always attempted when present

Column IDs verified against live board export (document provided 2024-04).
All 23 output column IDs confirmed correct.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any

from services.monday_service import run_query

logger = logging.getLogger(__name__)

INTAKE_BOARD_ID = os.getenv("MONDAY_INTAKE_BOARD_ID", "")

# ---------------------------------------------------------------------------
# OUTPUT column IDs — keyed by exact PRD Monday column name.
# All IDs verified against live board export.
#
# NOTE: The parser returns "Stedi Part B Active?" as the key.
#       The Monday board column title is "Stedi Eligibility Active?"
#       The column ID text_mm1xpgy2 is correct — it maps to that board column.
#       The internal key name difference does NOT affect writes (we look up by key).
# ---------------------------------------------------------------------------
ELIGIBILITY_OUTPUT_COLUMN_IDS: dict[str, str] = {
    # Board title: "Stedi Eligibility Active?"
    "Stedi Part B Active?":               "text_mm1xpgy2",
    "Stedi Coverage Type":                "text_mm25pxed",
    "Stedi Payer Name":                   "text_mm25wrxw",
    "Stedi Plan Name":                    "text_mm1xdcet",
    "Stedi Medicare Advantage?":          "text_mm25j9aj",
    "Stedi Medicare Advantage Carrier":   "text_mm25pyfx",
    "Stedi Medicare Advantage Member ID": "text_mm25j9j7",
    "Stedi QMB?":                         "text_mm25zsdd",
    "Stedi Secondary / Medicaid ID":      "text_mm25bjz7",
    "Stedi In Network?":                  "text_mm1xehx8",
    "Stedi Prior Auth Required?":         "text_mm1xhymg",
    "Stedi Copay":                        "text_mm1xzqe0",
    "Stedi Coinsurance %":                "text_mm1xssyw",
    "Stedi Individual Deductible":            "text_mm1x46kd",
    "Stedi Individual Deductible Remaining":  "text_mm1xyga2",
    "Stedi Family Deductible":                "text_mm1x7hkk",
    "Stedi Family Deductible Remaining":      "text_mm1xyzqx",
    "Stedi Individual OOP Max":               "text_mm1xdtxq",
    "Stedi Individual OOP Max Remaining":     "text_mm1x32jw",
    "Stedi Family OOP Max":                   "text_mm1xqmg9",
    "Stedi Family OOP Max Remaining":         "text_mm1xkdgq",
    "Stedi Plan Begin Date":                  "text_mm1xsa9",
    "Stedi Eligibility Error Description":    "text_mm1x9tje",
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


def write_eligibility_to_monday(item_id: str, writeback: dict[str, Any]) -> None:
    """
    Write all non-blank eligibility results to the Monday Intake Board item.

    writeback keys are exact PRD Monday column names (as returned by the parser).
    Blank values ("") are skipped — existing Monday cell values are preserved.
    "Not returned" IS written — it is a valid PRD value.
    """
    if not INTAKE_BOARD_ID:
        logger.error(
            "[ELG-MONDAY] MONDAY_INTAKE_BOARD_ID env var is not set — "
            "eligibility results cannot be written back to Monday. "
            "Add MONDAY_INTAKE_BOARD_ID to your .env file."
        )
        return

    wrote, errors = 0, []

    for col_name, value in writeback.items():
        # Skip blank — do not overwrite existing Monday values with nothing
        if value is None or value == "":
            continue

        col_id = ELIGIBILITY_OUTPUT_COLUMN_IDS.get(col_name, "")
        if not col_id:
            logger.warning(
                f"[ELG-MONDAY] No column ID mapped for key {col_name!r} — "
                f"add it to ELIGIBILITY_OUTPUT_COLUMN_IDS if this is a new field."
            )
            continue

        try:
            run_query(_UPDATE_MUTATION, {
                "itemId":   str(item_id),
                "boardId":  str(INTAKE_BOARD_ID),
                "columnId": col_id,
                "value":    json.dumps(str(value)),
            })
            logger.info(f"[ELG-MONDAY] ✓ wrote {col_name!r} = {value!r}")
            wrote += 1
        except Exception as e:
            msg = f"[ELG-MONDAY] ✗ {col_name!r}: {e}"
            logger.warning(msg)
            errors.append(msg)

    if wrote == 0:
        logger.warning(
            f"[ELG-MONDAY] Nothing written for item {item_id}. "
            f"Check: (1) MONDAY_INTAKE_BOARD_ID env var is set correctly, "
            f"(2) writeback dict has non-blank values."
        )
    else:
        logger.info(
            f"[ELG-MONDAY] Writeback complete — "
            f"item={item_id} wrote={wrote} errors={len(errors)}"
        )


def run_and_write_eligibility(item_id: str, monday_item: dict) -> dict:
    """
    Convenience: run full eligibility pipeline + write to Monday in one call.

    Used by the eligibility webhook trigger handler.
    Returns the writeback dict for logging / inspection.
    """
    from services.eligibility_service import run_eligibility_check

    writeback = run_eligibility_check(monday_item)
    write_eligibility_to_monday(item_id, writeback)
    return writeback