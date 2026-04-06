"""
services/eligibility_service.py
================================
Stage A + B + C for Stedi real-time eligibility checks (V1 PRD).

Architecture (per PRD Section 13):
  A. Input layer     — extract Monday item fields into a normalized row dict
  B. Business logic  — validate, build payload, call Stedi (via stedi_eligibility.py)
  C. Output layer    — parse response, build Monday writeback (via stedi_eligibility_parser.py
                       and stedi_eligibility_monday_mapping.py)

The client-provided files are the source of truth for business logic:
  stedi_eligibility.py          — payload builder + HTTP sender
  stedi_eligibility_parser.py   — response parser
  stedi_eligibility_monday_mapping.py — column name mappings
"""

from __future__ import annotations

import logging
import re
from typing import Any

from stedi_eligibility import (
    build_eligibility_payload_from_monday_row,
    send_realtime_eligibility_check,
)
from stedi_eligibility_parser import parse_stedi_eligibility_response
from stedi_eligibility_monday_mapping import build_monday_writeback_payload
from stedi_eligibility_parser import DEFAULT_REQUESTED_SERVICE_TYPE_CODE

logger = logging.getLogger(__name__)

# ── Onboarding Board column IDs ───────────────────────────────────────────────
# These are the column IDs on the New Onboarding Board.
# Update once Brandon exports the live board column IDs.
ONBOARDING_COL = {
    "primary_insurance": "color_mm18jhq5",   # Primary Insurance Final (status)
    "member_id":         "text_mm18s3fe",    # Member ID (text)
    "dob":               "text_mm187t6a",    # Patient Date of Birth (text)
}
# First name and last name come from the item name, not a column.


# =============================================================================
# A. INPUT LAYER
# =============================================================================

def _clean_name(name: str) -> str:
    """Remove test brackets, copy markers, and extra whitespace from item name."""
    if not name:
        return ""
    name = re.sub(r"\(.*?\)", "", name)       # remove (copy), (test), etc.
    name = re.sub(r"^\[.*?\]\s*", "", name)   # remove [Test]N prefix
    name = re.sub(r"\s+", " ", name)          # collapse whitespace
    return name.strip()


def _split_name(full_name: str) -> tuple[str, str]:
    """Split 'First Last' into (first, last). Returns ('', '') if blank."""
    clean = _clean_name(full_name)
    parts = clean.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def extract_eligibility_inputs(monday_item: dict) -> dict[str, Any]:
    """
    Extract Onboarding Board column values into a normalized row dict
    shaped exactly as stedi_eligibility.py expects.

    PRD Section 4: required inputs are payer label, member ID,
    first name, last name, DOB.
    """
    cols = {
        c.get("id"): (c.get("text") or "").strip()
        for c in monday_item.get("column_values", [])
    }

    first_name, last_name = _split_name(monday_item.get("name", ""))

    return {
        # Keys match MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP values
        "Primary Insurance Final": cols.get(ONBOARDING_COL["primary_insurance"], ""),
        "Member ID":               cols.get(ONBOARDING_COL["member_id"], ""),
        "First Name":              first_name,
        "Last Name":               last_name,
        "Patient Date of Birth":   cols.get(ONBOARDING_COL["dob"], ""),
        # Optional identifiers for logging / controlNumber
        "Pulse ID":  str(monday_item.get("id", "")),
        "Name":      monday_item.get("name", ""),
    }


# =============================================================================
# B. BUSINESS LOGIC LAYER
# =============================================================================

def run_eligibility_check(monday_item: dict) -> dict[str, Any]:
    """
    Main entry point: Monday item → eligibility writeback payload.

    Returns a normalized writeback dict (column name → value) in all cases:
    - success
    - validation failure (missing fields, unknown payer)
    - HTTP / Stedi error

    PRD Section 15: errors must always be available for Monday writeback.
    """
    item_id   = str(monday_item.get("id", ""))
    item_name = monday_item.get("name", "")

    logger.info(f"[ELG] Starting eligibility check | item={item_id} name={item_name!r}")

    try:
        row = extract_eligibility_inputs(monday_item)

        logger.info(
            f"[ELG] Inputs | item={item_id} "
            f"payer={row.get('Primary Insurance Final')!r} "
            f"member_id={row.get('Member ID')!r}"
        )

        # build_eligibility_payload_from_monday_row validates all required fields
        # and raises ValueError with a clear message if anything is missing.
        payload = build_eligibility_payload_from_monday_row(row)

        logger.info(
            f"[ELG] Sending request | item={item_id} "
            f"tradingPartnerServiceId={payload.get('tradingPartnerServiceId')}"
        )

        raw_response = send_realtime_eligibility_check(payload)

        logger.info(f"[ELG] Response received | item={item_id}")

        parsed = parse_stedi_eligibility_response(
            raw_response,
            requested_service_type_code=DEFAULT_REQUESTED_SERVICE_TYPE_CODE,
        )

        writeback = build_monday_writeback_payload(parsed)

        logger.info(
            f"[ELG] Success | item={item_id} "
            f"active={parsed.get('eligibility_active')} "
            f"plan={parsed.get('eligibility_plan_name')!r}"
        )

        return writeback

    except ValueError as e:
        # Validation errors (missing fields, unknown payer, invalid DOB, etc.)
        error_msg = str(e)
        logger.warning(f"[ELG] Validation error | item={item_id}: {error_msg}")
        return _error_writeback(error_msg)

    except Exception as e:
        # HTTP errors, network failures, Stedi API errors
        error_msg = str(e)
        logger.error(f"[ELG] Unexpected error | item={item_id}: {error_msg}", exc_info=True)
        return _error_writeback(error_msg)


def _error_writeback(error_description: str) -> dict[str, Any]:
    """
    Return a minimal writeback payload with the error description populated.
    All other fields are blank so Monday columns are not overwritten.
    PRD Section 18: always return a consistent structured result shape.
    """
    return build_monday_writeback_payload({
        "eligibility_error_description": error_description,
    })