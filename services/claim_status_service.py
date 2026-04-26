"""
services/claim_status_service.py
=================================
Orchestrates the Stedi Claim Status (276/277) pipeline for the Claims Board.

Mirrors the 4-layer shape of ``services/subscription_eligibility_service.py``:
  A. Input layer      — extract Claims Board columns -> row dict
  B. Business logic   — resolve payer, build payload  (stedi_claim_status_builder)
  C. HTTP layer       — send to Stedi                  (stedi_claim_status_client)
  D. Output layer     — parse 277 response             (stedi_claim_status_parser)

Entry points:
  run_claim_status_check(monday_item)
  extract_claim_status_inputs(monday_item)

Claims Board INPUT column IDs (verified against live board export 2026-04):
  color_mm2qq1f9   Claim Status Check  (trigger — status, label "Run")
  name             Item name           ("Firstname Lastname" — split here)
  text_mkp3y5ax    DOB
  text_mktat89m    Member ID
  color_mkxmhypt   Primary Payor       (status — generic label e.g. "Cigna")
  color_mkxmmm77   Insurance Type      (Commercial / Medicare / Medicaid)
  date_mkwr7spz    DOS                 (date — Date of Service)
  text_mm1gcz3y    PR Payor ID         (text — optional explicit payer ID)
"""

from __future__ import annotations

import logging
from typing import Any

from services.eligibility_service import _split_name  # reuse the exact name parser
from stedi_claim_status_builder import build_claim_status_payload
from stedi_claim_status_client  import send_claim_status_request
from stedi_claim_status_parser  import (
    parse_claim_status_response,
    error_writeback,
)

logger = logging.getLogger(__name__)


# Verified live against the Claims Board export (2026-04).
CLAIMS_BOARD_INPUT_COL = {
    "run_check":          "color_mm2qq1f9",   # trigger
    "dob":                "text_mkp3y5ax",
    "member_id":          "text_mktat89m",
    "primary_payor":      "color_mkxmhypt",   # status (generic label)
    "insurance_type":     "color_mkxmmm77",   # status (Commercial/Medicare/Medicaid)
    "dos":                "date_mkwr7spz",
    "pr_payor_id":        "text_mm1gcz3y",    # text (optional)
    # Optional claim-pinpoint fields. Sending these tightens the payer's
    # match — Cigna in particular returns "missing or invalid information"
    # (E0/21) on a 276 that lacks a patient control number.
    "patient_control_number": "text_mm1gkf40",   # "Raw Patient Control Number"
    "claim_id_alt":           "text_mm1zpzrs",   # "Claim ID" — fallback PCN source
    "claim_charge_amount":    "numeric_mm1ghydj",# "Raw Claim Charge Amount"
}

CLAIM_STATUS_FAILED_FLAG = "_claim_status_failed"


# =============================================================================
# A. INPUT LAYER
# =============================================================================

def extract_claim_status_inputs(monday_item: dict) -> dict[str, Any]:
    """
    Map Claims Board column_values -> normalised row dict that the
    builder accepts.
    """
    cols: dict[str, str] = {
        c.get("id", ""): (c.get("text") or "").strip()
        for c in monday_item.get("column_values", [])
    }

    primary_payor  = cols.get(CLAIMS_BOARD_INPUT_COL["primary_payor"], "").strip()
    insurance_type = cols.get(CLAIMS_BOARD_INPUT_COL["insurance_type"], "").strip()
    member_id      = cols.get(CLAIMS_BOARD_INPUT_COL["member_id"], "").strip()
    dob            = cols.get(CLAIMS_BOARD_INPUT_COL["dob"], "").strip()
    dos            = cols.get(CLAIMS_BOARD_INPUT_COL["dos"], "").strip()
    pr_payor_id    = cols.get(CLAIMS_BOARD_INPUT_COL["pr_payor_id"], "").strip()
    pcn            = (
        cols.get(CLAIMS_BOARD_INPUT_COL["patient_control_number"], "").strip()
        or cols.get(CLAIMS_BOARD_INPUT_COL["claim_id_alt"], "").strip()
    )
    claim_amount   = cols.get(CLAIMS_BOARD_INPUT_COL["claim_charge_amount"], "").strip()

    first_name, last_name = _split_name(monday_item.get("name", ""))

    # ``General Insurance`` is the key the builder validates on when no
    # explicit payer_id is passed. The builder's GENERAL_PAYER_ID_MAP
    # uses generic labels ("Cigna", "Aetna", ...) which is exactly what
    # the Claims Board stores in color_mkxmhypt.
    row = {
        "General Insurance":     primary_payor,
        "Insurance Type":        insurance_type,
        "Member ID":              member_id,
        "First Name":            first_name,
        "Last Name":             last_name,
        "Patient Date of Birth": dob,
        "Date of Service":       dos,
        "Pulse ID":              str(monday_item.get("id", "")),
        "Name":                  monday_item.get("name", ""),
        # Pass-through for the service-level payer override.
        "_pr_payor_id":          pr_payor_id,
        # Optional 276 fields — payers often need these to pinpoint
        # the right claim record (Cigna especially).
        "Patient Control Number": pcn,
        "Claim Charge Amount":    claim_amount,
    }

    logger.debug(
        f"[CS-INPUT] primary_payor={primary_payor!r} "
        f"insurance_type={insurance_type!r} "
        f"member_id={member_id!r} dob={dob!r} dos={dos!r} "
        f"first={first_name!r} last={last_name!r} "
        f"pr_payor_id={pr_payor_id!r} pcn={pcn!r} "
        f"claim_amount={claim_amount!r}"
    )

    return row


# =============================================================================
# B. PAYER RESOLUTION
# =============================================================================

def _resolve_payer(row: dict[str, Any]) -> tuple[str | None, str | None]:
    """
    Resolve the Stedi payer ID + trading-partner name for a Claims Board row.

    Priority:
      1. If ``_pr_payor_id`` is populated on the row, use it as a hard
         override. This lets billing pin an exact payer ID when Monday's
         generic Primary Payor label is ambiguous (e.g. a BCBS sub-plan
         whose Stedi ID is different from the parent BCBS entry).
      2. Otherwise fall through to the builder which does
         GENERAL_PAYER_ID_MAP["Primary Payor"] resolution on its own.
    """
    override = (row.get("_pr_payor_id") or "").strip()
    if override:
        from stedi_eligibility_builder import STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID
        partner = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(override, "")
        logger.info(
            f"[CS] Payer override via PR Payor ID | payer_id={override} "
            f"partner={partner!r}"
        )
        return override, partner or None
    return None, None


# =============================================================================
# C + D. ORCHESTRATION
# =============================================================================

def _failed_writeback(reason: str) -> dict[str, Any]:
    """
    Writeback used when we can't produce a live 277 result. The Monday
    writer branches on ``CLAIM_STATUS_FAILED_FLAG`` to flip the Claim
    Status Category -> "Error" and dump the reason into Claim Status
    Detail, without clobbering unrelated cells.
    """
    base = error_writeback(reason)
    base[CLAIM_STATUS_FAILED_FLAG] = True
    base["_failure_reason"]        = reason
    return base


def run_claim_status_check(monday_item: dict) -> dict[str, Any]:
    """
    Full pipeline: Claims Board item -> 277 writeback dict.

    Always returns a writeback dict. Errors are encoded via
    ``_failed_writeback`` so the Monday writer can surface them
    visually rather than silently swallowing.
    """
    item_id   = str(monday_item.get("id", ""))
    item_name = monday_item.get("name", "")

    logger.info(
        f"[CS] -- Claim Status check start -- "
        f"item={item_id} name={item_name!r}"
    )

    try:
        # A. Extract inputs
        row = extract_claim_status_inputs(monday_item)

        # B1. Payer (may be None -> builder resolves via GENERAL map)
        payer_id, partner_name = _resolve_payer(row)

        # B2. Build + validate payload
        payload = build_claim_status_payload(
            row,
            payer_id=payer_id,
            partner_name=partner_name,
        )
        logger.info(
            f"[CS] Sending | item={item_id} "
            f"tradingPartnerServiceId={payload.get('tradingPartnerServiceId')}"
        )

        # C. Send
        raw_response = send_claim_status_request(payload)

        # D. Parse
        writeback = parse_claim_status_response(raw_response)

        logger.info(
            f"[CS] Done | item={item_id} "
            f"category={writeback.get('Claim Status Category')!r} "
            f"paid={writeback.get('277 Paid Amount')} "
            f"icn={writeback.get('277 ICN')!r} "
            f"n_claims={writeback.get('_n_claims_returned')}"
        )
        return writeback

    except ValueError as e:
        msg = str(e)
        logger.warning(f"[CS] Validation error | item={item_id}: {msg}")
        return _failed_writeback(f"Validation error: {msg}")

    except Exception as e:
        msg = str(e)
        logger.error(
            f"[CS] Unexpected error | item={item_id}: {msg}",
            exc_info=True,
        )
        return _failed_writeback(f"Unexpected error: {msg}")
