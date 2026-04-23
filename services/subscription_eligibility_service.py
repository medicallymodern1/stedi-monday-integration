"""
services/subscription_eligibility_service.py
=============================================
Orchestrates the Stedi eligibility pipeline for the Subscription Board.

Mirrors services/eligibility_service.py but reads from the Subscription Board
(18407459988) instead of the Intake Board, and resolves the Stedi payer ID
from the **specific** Primary Insurance label (e.g. "Fidelis Low-Cost") via
``claim_assumptions.PAYER_ID_MAP`` — the same source-of-truth map the claims
submission pipeline uses.

Architecture (same four layers as the Intake flow):
  A. Input layer      — extract Subscription Board columns -> row dict
  B. Business logic   — validate + map payer + build payload (stedi_eligibility_builder)
  C. HTTP layer       — send to Stedi                        (stedi_eligibility_client)
  D. Output layer     — parse response                       (stedi_eligibility_parser)

Entry points:
  run_subscription_eligibility_check(monday_item)
  extract_subscription_eligibility_inputs(monday_item)

Subscription Board INPUT column IDs (verified against live board export
2026-04 — see `query { boards(ids: [18407459988]) { columns { id title type } } }`):
  color_mm254qxj   Primary Insurance   (status — specific label, e.g. "Fidelis Low-Cost")
  text_mkvp6zfg    Member ID 1         (text)
  text_mkvdefh1    DOB                 (text)
  color_mm2nnjam   Run Check           (trigger — status, label "Run")
  name             Item name           ("Firstname Lastname", split here)

This module is completely isolated from the Intake flow — importing it must
never change any Intake Board behaviour.
"""

from __future__ import annotations

import logging
from typing import Any

from claim_assumptions import (
    PAYER_ID_MAP,
    STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID,
)
from services.eligibility_service import _split_name  # reuse the exact same name parser
from stedi_eligibility_builder import build_eligibility_payload
from stedi_eligibility_client import send_eligibility_request
from stedi_eligibility_parser import parse_eligibility_response, error_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Subscription Board — INPUT column IDs (verified against live board export)
# ---------------------------------------------------------------------------
SUBSCRIPTION_COL = {
    "primary_insurance": "color_mm254qxj",  # "Primary Insurance" — status, .text = label
    "member_id":         "text_mkvp6zfg",   # "Member ID 1"
    "dob":               "text_mkvdefh1",   # "DOB"
    "run_check":         "color_mm2nnjam",  # "Run Check" — trigger column
}


# =============================================================================
# A. INPUT LAYER
# =============================================================================

def extract_subscription_eligibility_inputs(monday_item: dict) -> dict[str, Any]:
    """
    Map Subscription Board column_values -> normalised row dict.

    Keys match what stedi_eligibility_builder.build_eligibility_payload expects,
    with the addition of "Primary Insurance" (the specific label, for payer
    resolution via claim_assumptions.PAYER_ID_MAP).
    """
    cols: dict[str, str] = {
        c.get("id", ""): (c.get("text") or "").strip()
        for c in monday_item.get("column_values", [])
    }

    primary_insurance = cols.get(SUBSCRIPTION_COL["primary_insurance"], "").strip()
    member_id         = cols.get(SUBSCRIPTION_COL["member_id"], "").strip()
    dob               = cols.get(SUBSCRIPTION_COL["dob"], "").strip()

    # Subscription Board items are named "Firstname Lastname" (e.g. "Margaret Purifoy")
    first_name, last_name = _split_name(monday_item.get("name", ""))

    logger.debug(
        f"[SUB-ELG-INPUT] primary_insurance={primary_insurance!r} "
        f"member_id={member_id!r} dob={dob!r} "
        f"first={first_name!r} last={last_name!r}"
    )

    return {
        "Primary Insurance":     primary_insurance,
        "Member ID":             member_id,
        "First Name":            first_name,
        "Last Name":             last_name,
        "Patient Date of Birth": dob,
        # Identifiers for logging / controlNumber
        "Pulse ID": str(monday_item.get("id", "")),
        "Name":     monday_item.get("name", ""),
    }


# =============================================================================
# B. PAYER RESOLUTION
# =============================================================================

def _resolve_subscription_payer(primary_insurance: str) -> tuple[str, str]:
    """
    Resolve the specific Primary Insurance label -> (payer_id, partner_name)
    using ``claim_assumptions`` as the single source of truth.

    Raises ValueError with an actionable message if either lookup fails.
    """
    if not primary_insurance:
        raise ValueError("Missing required field: Primary Insurance")

    payer_id = PAYER_ID_MAP.get(primary_insurance, "")
    if not payer_id:
        raise ValueError(
            f"Unknown payer mapping for Primary Insurance: {primary_insurance!r}. "
            f"Add it to claim_assumptions.PAYER_ID_MAP."
        )

    partner_name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "")
    if not partner_name:
        raise ValueError(
            f"No Stedi trading partner name for payer ID: {payer_id!r} "
            f"(from Primary Insurance: {primary_insurance!r}). "
            f"Add it to claim_assumptions.STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID."
        )

    return payer_id, partner_name


# =============================================================================
# C + D. ORCHESTRATION
# =============================================================================

def run_subscription_eligibility_check(monday_item: dict) -> dict[str, Any]:
    """
    Full pipeline: Subscription Board item -> eligibility writeback dict.

    Always returns a complete 23-field writeback dict (same shape as the
    Intake flow). The Monday writer filters this down to the 5 Subscription
    Board columns; keeping the dict full-shape means the parser and error
    handling are unchanged.
    """
    item_id   = str(monday_item.get("id", ""))
    item_name = monday_item.get("name", "")

    logger.info(
        f"[SUB-ELG] ── Subscription eligibility check start ── "
        f"item={item_id} name={item_name!r}"
    )

    try:
        # A. Extract inputs from Monday item
        row = extract_subscription_eligibility_inputs(monday_item)

        # B1. Resolve payer from claim_assumptions (NOT from GENERAL_PAYER_ID_MAP)
        payer_id, partner_name = _resolve_subscription_payer(row["Primary Insurance"])
        logger.info(
            f"[SUB-ELG] Payer resolved | item={item_id} "
            f"primary_insurance={row['Primary Insurance']!r} "
            f"payer_id={payer_id} partner={partner_name!r}"
        )

        # B2. Build + validate payload (builder skips GENERAL map since we passed payer_id)
        payload = build_eligibility_payload(
            row,
            payer_id=payer_id,
            partner_name=partner_name,
        )
        logger.info(
            f"[SUB-ELG] Sending | item={item_id} "
            f"tradingPartnerServiceId={payload.get('tradingPartnerServiceId')}"
        )

        # C. Send to Stedi (same client as Intake flow)
        raw_response = send_eligibility_request(payload)
        logger.info(f"[SUB-ELG] Response received | item={item_id}")

        # D. Parse response -> full writeback dict (same parser as Intake flow)
        writeback = parse_eligibility_response(raw_response)
        logger.info(
            f"[SUB-ELG] ✓ Done | item={item_id} "
            f"active={writeback.get('Stedi Part B Active?')!r} "
            f"payer_name={writeback.get('Stedi Payer Name')!r} "
            f"plan_name={writeback.get('Stedi Plan Name')!r}"
        )
        return writeback

    except ValueError as e:
        msg = str(e)
        logger.warning(f"[SUB-ELG] ✗ Validation error | item={item_id}: {msg}")
        return error_response(msg)

    except Exception as e:
        msg = str(e)
        logger.error(
            f"[SUB-ELG] ✗ Unexpected error | item={item_id}: {msg}",
            exc_info=True,
        )
        return error_response(msg)
