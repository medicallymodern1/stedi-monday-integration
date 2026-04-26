"""
stedi_claim_status_builder.py
==============================
Validates inputs and builds the Stedi real-time Claim Status (276/277)
request payload.

Mirrors the shape of ``stedi_eligibility_builder.build_eligibility_payload``
so the two flows feel symmetric. The 276 JSON endpoint is documented at:
https://www.stedi.com/docs/healthcare/api-reference/post-healthcare-claim-status

Key differences from eligibility:
  - Providers is an ARRAY (not a single object), each with ``providerType``
    plus ``taxId`` (the billing provider's TIN/EIN).
  - Required date range: ``beginningDateOfService`` / ``endDateOfService``.
    We widen DOS to a 60-days-before/60-days-after window because some
    payers reject pinpoint DOS matches on 276.
  - No encounter.serviceTypeCodes — 276 is claim-level, not benefit-level.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from stedi_eligibility_builder import (
    STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID,
    _normalize_dob,
    _resolve_payer_id,
    _resolve_trading_partner_name,
    _safe_str,
)

logger = logging.getLogger(__name__)


STEDI_CLAIM_STATUS_ENDPOINT = (
    "https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/claimstatus/v2"
)

CLAIM_STATUS_DATE_WINDOW_DAYS_BEFORE = 60
CLAIM_STATUS_DATE_WINDOW_DAYS_AFTER  = 60


def _validate_inputs(row: dict[str, Any], *, payer_id: str | None = None) -> None:
    """Raise ValueError with a clear message for any missing required field."""
    if payer_id is None:
        general_insurance = _safe_str(row.get("General Insurance"))
        if not general_insurance:
            raise ValueError("Missing required field: General Insurance")
        _resolve_payer_id(general_insurance)

    if not _safe_str(row.get("Member ID")):
        raise ValueError("Missing required field: Member ID")
    if not _safe_str(row.get("First Name")):
        raise ValueError("Missing required field: First Name")
    if not _safe_str(row.get("Last Name")):
        raise ValueError("Missing required field: Last Name")

    dob_raw = row.get("Patient Date of Birth")
    if not _normalize_dob(dob_raw):
        raise ValueError(
            f"Missing or unparseable Patient Date of Birth: {dob_raw!r}. "
            f"Expected formats: MM/DD/YYYY, MM/DD/YY, YYYY-MM-DD"
        )

    if not _safe_str(row.get("Date of Service")):
        raise ValueError("Missing required field: Date of Service (Monday DOS column)")

    try:
        from claim_assumptions import (
            BILLING_PROVIDER_NPI,
            BILLING_PROVIDER_ORGANIZATION_NAME,
            BILLING_PROVIDER_EIN,
        )
        if not _safe_str(BILLING_PROVIDER_NPI):
            raise ValueError("BILLING_PROVIDER_NPI is blank in claim_assumptions.py")
        if not _safe_str(BILLING_PROVIDER_ORGANIZATION_NAME):
            raise ValueError(
                "BILLING_PROVIDER_ORGANIZATION_NAME is blank in claim_assumptions.py"
            )
        if not _safe_str(BILLING_PROVIDER_EIN):
            raise ValueError("BILLING_PROVIDER_EIN is blank in claim_assumptions.py")
    except ImportError:
        raise ValueError(
            "claim_assumptions.py not found — cannot load billing provider defaults"
        )


def _dos_window(dos_raw: Any) -> tuple[str, str]:
    """
    Normalise a single Date of Service into a (begin, end) window in
    YYYYMMDD — widened by the before/after constants above.
    """
    dos = _normalize_dob(dos_raw)  # reuses the same parser
    if not dos:
        raise ValueError(f"Could not normalise Date of Service: {dos_raw!r}")
    d = datetime.strptime(dos, "%Y%m%d")
    begin = (d - timedelta(days=CLAIM_STATUS_DATE_WINDOW_DAYS_BEFORE)).strftime("%Y%m%d")
    end   = (d + timedelta(days=CLAIM_STATUS_DATE_WINDOW_DAYS_AFTER)).strftime("%Y%m%d")
    return begin, end


def build_claim_status_payload(
    row: dict[str, Any],
    *,
    payer_id: str | None = None,
    partner_name: str | None = None,
) -> dict[str, Any]:
    """
    Validate inputs and return a Stedi-compatible 276 claim-status payload.

    Row keys (from services.claim_status_service.extract_claim_status_inputs):
      "General Insurance"     — generic payer label (e.g. "Cigna")
      "Member ID"             — subscriber's member ID
      "First Name"            — split from the Claims Board item name
      "Last Name"
      "Patient Date of Birth" — free-text DOB
      "Date of Service"       — DOS (single date; widened to window)
      "Pulse ID", "Name"      — for controlNumber + logs
    """
    _validate_inputs(row, payer_id=payer_id)

    from claim_assumptions import (
        BILLING_PROVIDER_NPI,
        BILLING_PROVIDER_ORGANIZATION_NAME,
        BILLING_PROVIDER_EIN,
    )

    if payer_id is None:
        general_insurance = _safe_str(row["General Insurance"])
        payer_id          = _resolve_payer_id(general_insurance)
        partner_name      = _resolve_trading_partner_name(payer_id)
    else:
        general_insurance = _safe_str(
            row.get("General Insurance") or row.get("Primary Insurance") or ""
        )
        if not _safe_str(partner_name):
            partner_name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "")
        if not _safe_str(partner_name):
            raise ValueError(
                f"No Stedi trading partner name for payer ID: {payer_id!r}. "
                f"Pass partner_name explicitly, or add it to "
                f"STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID."
            )

    dob         = _normalize_dob(row.get("Patient Date of Birth"))
    member_id   = _safe_str(row["Member ID"])
    begin, end  = _dos_window(row.get("Date of Service"))

    control_number = (
        _safe_str(row.get("Pulse ID"))
        or _safe_str(row.get("Name"))
        or member_id
    )[:80]

    encounter: dict[str, Any] = {
        "beginningDateOfService": begin,
        "endDateOfService":       end,
    }
    # Optional pinpoint fields — payers use these to disambiguate when
    # multiple claims share the same DOS window. Cigna returns E0/21
    # ("missing or invalid information") on 276s that don't include
    # at least the patient control number, so we always send it when
    # we have one.
    pcn = _safe_str(row.get("Patient Control Number"))
    if pcn:
        encounter["patientControlNumber"] = pcn[:38]  # X12 max for REF*EJ

    raw_amt = _safe_str(row.get("Claim Charge Amount"))
    if raw_amt:
        try:
            encounter["totalAmount"] = float(raw_amt.replace("$", "").replace(",", ""))
        except ValueError:
            pass  # silently skip — non-fatal

    payload: dict[str, Any] = {
        "controlNumber":          control_number,
        "tradingPartnerServiceId": payer_id,
        "providers": [
            {
                "organizationName": BILLING_PROVIDER_ORGANIZATION_NAME,
                "npi":              BILLING_PROVIDER_NPI,
                "taxId":            BILLING_PROVIDER_EIN,
                "providerType":     "BillingProvider",
            }
        ],
        "subscriber": {
            "memberId":    member_id,
            "firstName":   _safe_str(row["First Name"]),
            "lastName":    _safe_str(row["Last Name"]),
            "dateOfBirth": dob,
        },
        "encounter": encounter,
        "_meta": {
            "generalInsurance":   general_insurance,
            "tradingPartnerName": partner_name,
            "pulseId":            _safe_str(row.get("Pulse ID")),
            "itemName":           _safe_str(row.get("Name")),
            "dosWindow":          f"{begin}..{end}",
            "patientControlNumber": pcn,
            "claimChargeAmount":  raw_amt,
        },
    }

    logger.info(
        f"[CS-BUILDER] Payload ready | "
        f"general_insurance={general_insurance!r} -> payer_id={payer_id} | "
        f"partner={partner_name!r} | member_id={member_id!r} | "
        f"dos_window={begin}..{end} | "
        f"pcn={pcn!r} charge={raw_amt!r}"
    )

    return payload
