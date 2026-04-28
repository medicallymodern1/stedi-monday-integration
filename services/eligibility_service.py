"""
services/eligibility_service.py
================================
Orchestrates the full eligibility check pipeline — V1 PRD.

Architecture (per PRD):
  A. Input layer      — extract Intake Board columns → normalised row dict
  B. Business logic   — validate + map payer + build payload  (stedi_eligibility_builder)
  C. HTTP layer       — send to Stedi                          (stedi_eligibility_client)
  D. Output layer     — parse response → 23-field writeback    (stedi_eligibility_parser)

Entry points:
  run_eligibility_check(monday_item)       — full pipeline for one Monday item
  extract_eligibility_inputs(monday_item)  — input layer only (testing / reuse)

Intake Board INPUT column IDs (verified against live board export):
  color_mm24ap4j  General Insurance   (status — .text gives label e.g. "Medicare A&B")
  text_mm1x2qk2   Member ID 1         (text)
  text_mm1xvxst   DOB                 (text)
  color_mm1yeksx  Run Stedi Eligibility (trigger — status)
  name            Item name           (used to derive First/Last when no dedicated cols)
"""

from __future__ import annotations

import logging
import re
from typing import Any

from stedi_eligibility_builder import build_eligibility_payload
from stedi_eligibility_client import send_eligibility_request
from stedi_eligibility_parser import parse_eligibility_response, error_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Intake Board — INPUT column IDs (verified against live board export)
# ---------------------------------------------------------------------------
INTAKE_COL = {
    "general_insurance": "color_mm24ap4j",  # "General Insurance" — status type, .text = label
    "member_id":         "text_mm1x2qk2",   # "Member ID 1"
    "first_name":        "",                # no dedicated column — split from item name
    "last_name":         "",                # no dedicated column — split from item name
    "dob":               "text_mm1xvxst",   # "DOB"
    "run_eligibility":   "color_mm1yeksx",  # "Run Stedi Eligibility" — trigger column
}


# =============================================================================
# A. INPUT LAYER
# =============================================================================

def _clean_name(name: str) -> str:
    """Strip test markers, copy labels, and extra whitespace from item name."""
    if not name:
        return ""
    name = re.sub(r"\(.*?\)", "", name)
    name = re.sub(r"^\[.*?\]\s*", "", name)
    name = re.sub(r"\s+", " ", name)
    return name.strip()


def _split_name(full_name: str) -> tuple[str, str]:
    """'First Last' → ('First', 'Last'). Returns ('', '') if blank."""
    clean = _clean_name(full_name)
    parts = clean.split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def extract_eligibility_inputs(monday_item: dict) -> dict[str, Any]:
    """
    Map Intake Board column_values → normalised row dict.

    Keys match exactly what stedi_eligibility_builder.build_eligibility_payload expects:
      "General Insurance", "Member ID", "First Name", "Last Name",
      "Patient Date of Birth", "Pulse ID", "Name"

    General Insurance is a status column — Monday returns the label text via .text,
    which matches the keys in GENERAL_PAYER_ID_MAP in stedi_eligibility_builder.py.

    Falls back to splitting the item name for First/Last since there are no
    dedicated First Name / Last Name columns on the Intake Board.
    """
    cols: dict[str, str] = {
        c.get("id", ""): (c.get("text") or "").strip()
        for c in monday_item.get("column_values", [])
    }

    general_insurance = cols.get(INTAKE_COL["general_insurance"], "").strip()
    member_id         = cols.get(INTAKE_COL["member_id"], "").strip()

    # No dedicated first/last name columns — always split from item name
    first_name, last_name = _split_name(monday_item.get("name", ""))

    dob = cols.get(INTAKE_COL["dob"], "").strip()

    logger.debug(
        f"[ELG-INPUT] general_insurance={general_insurance!r} "
        f"member_id={member_id!r} dob={dob!r} "
        f"first={first_name!r} last={last_name!r}"
    )

    return {
        "General Insurance":     general_insurance,
        "Member ID":             member_id,
        "First Name":            first_name,
        "Last Name":             last_name,
        "Patient Date of Birth": dob,
        # Identifiers for logging / controlNumber — not sent to Stedi
        "Pulse ID": str(monday_item.get("id", "")),
        "Name":     monday_item.get("name", ""),
    }


# =============================================================================
# B + C + D. ORCHESTRATION
# =============================================================================




# ---------------------------------------------------------------------------
# Coverage-unavailable detection
# ---------------------------------------------------------------------------

def _is_coverage_unavailable(raw_response: dict) -> tuple[bool, str]:
    """
    Stedi flags responses where the payer found the patient but did not
    return coverage status with a top-level warning of code
    COVERAGE_INFORMATION_UNAVAILABLE. The 271 typically has empty
    planStatus and a benefitsInformation row of code "U" pointing at a
    different entity (e.g. NY Medicaid eMedNY pointing at Healthfirst
    PHSP for managed care members).

    Without this short-circuit the parser falls through and writes
    Active=No, which is misleading: the payer never said inactive,
    they said "I don't know — go ask someone else." Treat it as a
    failure and surface the reason in the error description column.

    Mirrors the same handler in subscription_eligibility_service.
    """
    for w in raw_response.get("warnings") or []:
        if not isinstance(w, dict):
            continue
        if str(w.get("code", "")).strip().upper() == "COVERAGE_INFORMATION_UNAVAILABLE":
            return True, (str(w.get("description") or "").strip()
                          or "Coverage information unavailable")
    return False, ""

def run_eligibility_check(monday_item: dict) -> dict[str, Any]:
    """
    Full pipeline: Monday Intake Board item → eligibility writeback dict.

    Always returns a complete 23-field writeback dict:
      - Success:            all fields populated from Stedi response
      - Validation failure: only error field populated, rest blank
      - HTTP / API error:   only error field populated, rest blank

    The caller (eligibility_monday_service) writes the result to Monday.
    """
    item_id   = str(monday_item.get("id", ""))
    item_name = monday_item.get("name", "")

    logger.info(
        f"[ELG] ── Eligibility check start ── "
        f"item={item_id} name={item_name!r}"
    )

    try:
        # A. Extract inputs from Monday item
        row = extract_eligibility_inputs(monday_item)
        logger.info(
            f"[ELG] Inputs | item={item_id} "
            f"general_insurance={row.get('General Insurance')!r} "
            f"member_id={row.get('Member ID')!r} "
            f"dob={row.get('Patient Date of Birth')!r} "
            f"name={row.get('First Name')!r} {row.get('Last Name')!r}"
        )

        # B. Build + validate payload
        payload = build_eligibility_payload(row)
        logger.info(
            f"[ELG] Sending | item={item_id} "
            f"tradingPartnerServiceId={payload.get('tradingPartnerServiceId')} "
            f"tradingPartner={payload.get('_meta', {}).get('tradingPartnerName')!r}"
        )

        # C. Send to Stedi
        raw_response = send_eligibility_request(payload)
        logger.info(f"[ELG] Response received | item={item_id}")

        # C2. Short-circuit on COVERAGE_INFORMATION_UNAVAILABLE — see helper.
        # Common pattern: NY Medicaid managed-care members where eMedNY
        # punts to the MCO (Healthfirst, etc.). Without this, parser would
        # fall through to Active=No which misleadingly reads as "Inactive".
        is_unavail, unavail_reason = _is_coverage_unavailable(raw_response)
        if is_unavail:
            logger.warning(
                f"[ELG] ! Coverage unavailable | item={item_id} "
                f"reason={unavail_reason!r}"
            )
            return error_response(unavail_reason)

        # D. Parse response → 23-field writeback
        writeback = parse_eligibility_response(raw_response)
        logger.info(
            f"[ELG] ✓ Done | item={item_id} "
            f"part_b_active={writeback.get('Stedi Part B Active?')!r} "
            f"coverage_type={writeback.get('Stedi Coverage Type')!r} "
            f"ma={writeback.get('Stedi Medicare Advantage?')!r}"
        )

        # Diagnostic: when Active comes back No but no error description was
        # written, dump the raw 271 so we can see exactly what shape the
        # payer returned. Targeted log — only fires for the puzzling cases.
        try:
            if (writeback.get("Stedi Part B Active?") == "No"
                and not (writeback.get("Stedi Eligibility Error Description") or "").strip()):
                import json as _json
                logger.warning(
                    f"[ELG] ⚠ Active=No without error — raw 271 dump | "
                    f"item={item_id} | "
                    f"plan_status={_json.dumps(raw_response.get('planStatus') or [])} | "
                    f"benefits_codes={_json.dumps([{'code': r.get('code'), 'name': r.get('name'), 'serviceTypeCodes': r.get('serviceTypeCodes'), 'statusCode': r.get('statusCode')} for r in (raw_response.get('benefitsInformation') or [])])}"
                )
        except Exception:
            pass

        return writeback

    except ValueError as e:
        msg = str(e)
        logger.warning(f"[ELG] ✗ Validation error | item={item_id}: {msg}")
        return error_response(msg)

    except Exception as e:
        msg = str(e)
        logger.error(f"[ELG] ✗ Unexpected error | item={item_id}: {msg}", exc_info=True)
        return error_response(msg)