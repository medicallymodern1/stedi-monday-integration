"""
services/claim_status_monday_service.py
========================================
Writes the Claim Status 277 results back to the Monday Claims Board.

Columns (verified against live Claims Board export 2026-04):
  date_mm2qrazz       "Last Claim Status Check"    (date)
  color_mm2qbcpy      "Claim Status Category"      (status)
  long_text_mm2qapj6  "Claim Status Detail"        (long_text)
  text_mm2nfytt       "Payer Claim Number" → ICN   (text, repurposed)
  numeric_mm2qt479    "277 Paid Amount"            (numbers $)

Plus two "side-effect" writes done in the same mutation so the row
reflects the new activity immediately without a second API round-trip:
  long_text_mkzrx7ke  "Notes & Activity"   — append a timestamped line
  color_mm2qq1f9      "Claim Status Check" — reset Run -> blank (index 5)

Single ``change_multiple_column_values`` mutation with
``create_labels_if_missing: true`` so the Claim Status Category column
auto-creates any labels that aren't already on the board.
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
from typing import Any

from services.monday_service import run_query

logger = logging.getLogger(__name__)

CLAIMS_BOARD_ID = os.getenv("MONDAY_CLAIMS_BOARD_ID", "")


# ---------------------------------------------------------------------------
# Column IDs
# ---------------------------------------------------------------------------

CLAIM_STATUS_OUTPUT_COL = {
    "last_check_date":   "date_mm2qrazz",        # date
    "category":          "color_mm2qbcpy",       # status
    "detail":            "long_text_mm2qapj6",   # long_text
    "icn":               "text_mm2nfytt",        # text (repurposed)
    "paid_amount":       "numeric_mm2qt479",     # numbers
    "notes_activity":    "long_text_mkzrx7ke",   # long_text (append)
    "run_check_trigger": "color_mm2qq1f9",       # status — reset from "Run"
}

# Reset-to-blank on a Monday status column is "index 5" by convention for
# this board's Claim Status Check column (the labels config carries only
# label 1 = "Run"; index 5 is the empty slot).
CLAIM_STATUS_CHECK_BLANK_INDEX = 5


# ---------------------------------------------------------------------------
# Canonical labels — keep identical to the board's dropdown options.
# ---------------------------------------------------------------------------

VALID_CATEGORY_LABELS = {
    "Acknowledged",
    "Pending",
    "In Process",
    "Paid",
    "Denied",
    "Requests Info",
    "No Match",
    "Error",
}


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


# ---------------------------------------------------------------------------
# Notes & Activity log helper
# ---------------------------------------------------------------------------

def _format_activity_line(writeback: dict[str, Any]) -> str:
    """
    Build a single-line audit entry for the Notes & Activity column.

    Shape (kept short enough to stay readable in the long_text preview):
        [YYYY-MM-DD HH:MM] 277 via Stedi: <Category> (<cat><code>) — $<paid> · ICN <icn>
    """
    ts   = _dt.datetime.now().strftime("%Y-%m-%d %H:%M")
    cat  = writeback.get("Claim Status Category") or ""
    code = f"{writeback.get('_category_code', '')}{writeback.get('_status_code', '')}".strip()
    paid = writeback.get("277 Paid Amount") or 0.0
    icn  = writeback.get("277 ICN") or ""

    try:
        paid_txt = f"${float(paid):,.2f}"
    except (TypeError, ValueError):
        paid_txt = f"${paid}"

    parts = [f"[{ts}] 277 via Stedi: {cat}"]
    if code:
        parts[-1] += f" ({code})"
    parts.append(paid_txt)
    if icn:
        parts.append(f"ICN {icn}")
    return " \u00b7 ".join(parts)


def _append_to_notes(existing: str, new_line: str) -> str:
    """
    Prepend the new line so the newest entry sits at the top of the column
    (makes it visible without scrolling). Keep the old content verbatim.
    """
    if existing.strip():
        return f"{new_line}\n{existing}"
    return new_line


# ---------------------------------------------------------------------------
# Value encoder
# ---------------------------------------------------------------------------

def _encode_claim_status_columns(
    writeback: dict[str, Any],
    existing_notes: str = "",
) -> dict[str, Any]:
    """
    Pick the fields we care about out of the writeback dict and encode
    them in the per-type JSON shapes Monday expects.

      - status    -> {"label": "..."}
      - date      -> {"date": "YYYY-MM-DD"}
      - text      -> "plain string"
      - long_text -> {"text": "..."}
      - numbers   -> "1234.56"  (stringified)

    Blanks are dropped so we never overwrite existing Monday data with "".
    Trigger reset + notes append are always included.
    """
    category   = (writeback.get("Claim Status Category") or "").strip()
    detail     = (writeback.get("Claim Status Detail") or "").strip()
    icn        = (writeback.get("277 ICN") or "").strip()
    paid       = writeback.get("277 Paid Amount")
    check_date = (writeback.get("Last Claim Status Check") or "").strip()

    values: dict[str, Any] = {}

    if category in VALID_CATEGORY_LABELS:
        values[CLAIM_STATUS_OUTPUT_COL["category"]] = {"label": category}
    elif category:
        logger.warning(
            f"[CS-MONDAY] Unknown Claim Status Category {category!r}; "
            f"writing as new label (create_labels_if_missing will auto-add)."
        )
        values[CLAIM_STATUS_OUTPUT_COL["category"]] = {"label": category}

    if detail:
        values[CLAIM_STATUS_OUTPUT_COL["detail"]] = {"text": detail}

    if icn:
        values[CLAIM_STATUS_OUTPUT_COL["icn"]] = icn

    try:
        if paid is not None and float(paid) != 0.0:
            values[CLAIM_STATUS_OUTPUT_COL["paid_amount"]] = str(float(paid))
    except (TypeError, ValueError):
        pass

    if check_date and len(check_date) == 10 and check_date[4] == "-" and check_date[7] == "-":
        values[CLAIM_STATUS_OUTPUT_COL["last_check_date"]] = {"date": check_date}

    # Notes & Activity — always append so there's an audit trail.
    values[CLAIM_STATUS_OUTPUT_COL["notes_activity"]] = {
        "text": _append_to_notes(existing_notes, _format_activity_line(writeback))
    }

    # Reset the trigger so the user can fire it again later.
    values[CLAIM_STATUS_OUTPUT_COL["run_check_trigger"]] = {
        "index": CLAIM_STATUS_CHECK_BLANK_INDEX
    }

    return values


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------

def _fetch_existing_notes(item_id: str) -> str:
    """
    Read the current Notes & Activity cell so we can prepend the new
    entry without losing prior history. Non-fatal — on any failure we
    just start with an empty string.
    """
    query = """
    query NotesForItem($itemId: ID!) {
      items(ids: [$itemId]) {
        column_values(ids: ["long_text_mkzrx7ke"]) { id text }
      }
    }
    """
    try:
        result = run_query(query, {"itemId": str(item_id)})
        items  = result.get("data", {}).get("items", [])
        if not items:
            return ""
        cv = items[0].get("column_values", []) or []
        if not cv:
            return ""
        return (cv[0].get("text") or "").strip()
    except Exception as e:
        logger.warning(f"[CS-MONDAY] Could not read existing Notes & Activity: {e}")
        return ""


def write_claim_status_to_monday(
    item_id: str,
    writeback: dict[str, Any],
) -> None:
    """
    Write the Claim Status columns for ``item_id``.

    Partial writebacks are fine — any blank fields are omitted. The two
    side-effect writes (Notes & Activity append, Claim Status Check
    trigger reset) always go through so the Monday view reflects the
    fact that a check ran, even when the check itself failed.
    """
    if not CLAIMS_BOARD_ID:
        logger.error(
            "[CS-MONDAY] MONDAY_CLAIMS_BOARD_ID env var is not set — "
            "results cannot be written back."
        )
        return

    existing_notes = _fetch_existing_notes(item_id)
    values         = _encode_claim_status_columns(writeback, existing_notes)

    try:
        run_query(_UPDATE_MULTI_MUTATION, {
            "itemId":       str(item_id),
            "boardId":      str(CLAIMS_BOARD_ID),
            "columnValues": json.dumps(values),
            "createLabels": True,
        })
        logger.info(
            f"[CS-MONDAY] OK wrote {len(values)} col(s) for item {item_id}: "
            f"{sorted(values.keys())}"
        )
    except Exception as e:
        # Parity with the other writers — warn but don't raise so a
        # transient Monday failure doesn't 500 the webhook (which has
        # already ACKed).
        logger.warning(
            f"[CS-MONDAY] change_multiple_column_values failed for "
            f"item {item_id}: {e}"
        )


def run_and_write_claim_status(
    item_id: str,
    monday_item: dict,
) -> dict:
    """
    Convenience: run the claim-status pipeline + write to Monday in one call.
    Used by the webhook trigger handler. Returns the full writeback dict
    for logging / debugging.
    """
    from services.claim_status_service import run_claim_status_check

    writeback = run_claim_status_check(monday_item)
    write_claim_status_to_monday(item_id, writeback)
    return writeback
