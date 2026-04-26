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
    "gender":             "color_mm1zy5f2",   # status: "Male"/"Female"
    # Reserved for a future fallback retry attempt — not in the base
    # request, per Stedi's "send minimal info first" guidance. Read here
    # so the row dict carries them; the builder ignores until the
    # fallback path is wired.
    "patient_control_number": "text_mm1gkf40",   # "Raw Patient Control Number"
    "claim_id_alt":           "text_mm1zpzrs",   # "Claim ID" — fallback PCN source
    "claim_charge_amount":    "numeric_mm1ghydj",# "Raw Claim Charge Amount"
    "tradingpartner_claim_number": "text_mm2nfytt",  # payer's ICN ("Payer Claim Number")
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
    gender_raw     = cols.get(CLAIMS_BOARD_INPUT_COL["gender"], "").strip()
    gender         = {"Male": "M", "Female": "F"}.get(gender_raw, "")

    pcn            = (
        cols.get(CLAIMS_BOARD_INPUT_COL["patient_control_number"], "").strip()
        or cols.get(CLAIMS_BOARD_INPUT_COL["claim_id_alt"], "").strip()
    )
    claim_amount   = cols.get(CLAIMS_BOARD_INPUT_COL["claim_charge_amount"], "").strip()
    tp_claim_num   = cols.get(CLAIMS_BOARD_INPUT_COL["tradingpartner_claim_number"], "").strip()

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
        # Gender (M/F) — used in subscriber + dependent blocks per Stedi
        # docs ("recommended for best results").
        "Gender":                gender,
        # Always-sent: the claim charge amount surfaced as
        # encounter.submittedAmount in the 276 request. Empirical
        # confirmation (Fidelis, 2026-04-26): some payers refuse to
        # match the claim without it.
        "Claim Charge Amount":   claim_amount,
        # Pass-through for the service-level payer override.
        "_pr_payor_id":          pr_payor_id,
        # Fallback-retry inputs. Stedi: "if base request returns no
        # data, retry with tradingPartnerClaimNumber + taxId."
        # The builder reads "Tradingpartner Claim Number" when called
        # with fallback_mode=True; keys with leading underscore are
        # the raw cell values for logging / future extensions.
        "Tradingpartner Claim Number": tp_claim_num,
        "_pcn":                  pcn,
        "_claim_charge_amount":  claim_amount,
        "_tradingpartner_claim_number": tp_claim_num,
    }

    logger.debug(
        f"[CS-INPUT] primary_payor={primary_payor!r} "
        f"insurance_type={insurance_type!r} "
        f"member_id={member_id!r} dob={dob!r} dos={dos!r} "
        f"gender={gender!r} first={first_name!r} last={last_name!r} "
        f"pr_payor_id={pr_payor_id!r} pcn={pcn!r} "
        f"claim_amount={claim_amount!r} tp_claim_num={tp_claim_num!r}"
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

        def _send_and_parse(*, fallback_mode: bool) -> dict:
            payload = build_claim_status_payload(
                row,
                payer_id=payer_id,
                partner_name=partner_name,
                fallback_mode=fallback_mode,
            )
            logger.info(
                f"[CS] Sending | item={item_id} "
                f"tradingPartnerServiceId={payload.get('tradingPartnerServiceId')} "
                f"fallback={'yes' if fallback_mode else 'no'}"
            )
            raw = send_claim_status_request(payload)
            return parse_claim_status_response(raw)

        # First attempt: documented base request (minimal fields).
        writeback = _send_and_parse(fallback_mode=False)

        # Stedi's recommended retry pattern: when the base request
        # returns "Data Search Unsuccessful" (category D*), AND we have
        # something extra to add (the payer's ICN), retry once with
        # tradingPartnerClaimNumber + provider taxId. Use the retry
        # response only if it found more than the base response did.
        base_cat = (writeback.get("_category_code") or "").upper()[:1]
        n_base   = writeback.get("_n_claims_returned", 0) or 0
        tp_claim = (row.get("Tradingpartner Claim Number") or "").strip()
        # Treat both D-category and zero-claim envelopes as "no data".
        no_data  = base_cat == "D" or n_base == 0

        if no_data and tp_claim:
            logger.info(
                f"[CS] Base request returned no data ({base_cat or 'empty'}); "
                f"retrying with tradingPartnerClaimNumber={tp_claim!r} + taxId."
            )
            try:
                fb = _send_and_parse(fallback_mode=True)
                fb_cat = (fb.get("_category_code") or "").upper()[:1]
                fb_n   = fb.get("_n_claims_returned", 0) or 0
                # Take the fallback response if it produced ANY actionable
                # category (F/P/A/R/E with claims), else stick with base.
                if fb_n > 0 and fb_cat and fb_cat != "D":
                    logger.info(
                        f"[CS] Fallback retry succeeded | item={item_id} "
                        f"category={fb.get('Claim Status Category')!r}"
                    )
                    writeback = fb
                else:
                    logger.info(
                        f"[CS] Fallback retry also returned no data; "
                        f"keeping base response."
                    )
            except Exception as e:
                # Don't let a fallback failure mask the base result.
                logger.warning(
                    f"[CS] Fallback retry errored | item={item_id}: {e}"
                )

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
