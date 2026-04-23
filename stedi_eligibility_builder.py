"""
stedi_eligibility_builder.py
==============================
Validates inputs and builds the Stedi real-time eligibility request payload.

PRD decisions:
- Payer routing via GENERAL_PAYER_ID_MAP (simplified General Insurance labels)
- Provider defaults from claim_assumptions.py (BILLING_PROVIDER_NPI / ORG_NAME)
- Subscriber-only check; no dependents
- Service type code hardcoded to ["12"] (DME) for V1
- dateOfService intentionally omitted (today-check)
- controlNumber = Pulse ID → Name → memberId (fallback chain)
"""

from __future__ import annotations

import logging
import os
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Payer maps — per new PRD (simplified General Insurance labels)
# ---------------------------------------------------------------------------

GENERAL_PAYER_ID_MAP: dict[str, str] = {
    "Anthem BCBS":        "803",
    "Fidelis":            "11315",
    "Medicare A&B":       "16013",
    "NYSHIP Empire":      "87726",
    "United Healthcare":  "87726",
    "UMR":                "87726",
    "Aetna":              "60054",
    "Wellcare":           "14163",
    "Humana":             "61101",
    "Cigna":              "62308",
    "Medicaid":           "MCDNY",
    "Midlands Choice":    "47080",
    "MagnaCare":          "MAGNACARE_PLACEHOLDER",
    "Stedi":              "STEDITEST"
}

STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID: dict[str, str] = {
    "803":                    "Anthem Blue Cross Blue Shield of New York",
    "11315":                  "Fidelis Care",
    "16013":                  "Medicare Part B",
    "87726":                  "UnitedHealthcare",
    "60054":                  "Aetna",
    "14163":                  "Wellcare",
    "61101":                  "Humana",
    "62308":                  "Cigna",
    "MCDNY":                  "Medicaid New York",
    "47080":                  "Midlands Choice",
    "11303":                  "MagnaCare",
    "STEDITEST":              "Stedi Test Payer",
}

GENERAL_CLAIM_FILING_CODE_MAP: dict[str, str] = {
    "Anthem BCBS":        "CI",
    "Fidelis":            "CI",
    "Medicare A&B":       "MB",
    "NYSHIP Empire":      "CI",
    "United Healthcare":  "CI",
    "UMR":                "CI",
    "Aetna":              "CI",
    "Wellcare":           "MC",
    "Humana":             "CI",
    "Cigna":              "CI",
    "Medicaid":           "MC",
    "Midlands Choice":    "CI",
    "MagnaCare":          "CI",
}

DEFAULT_CLAIM_FILING_CODE = "CI"

DEFAULT_SERVICE_TYPE_CODES = ["12"]

STEDI_ELIGIBILITY_ENDPOINT = "https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/eligibility/v3"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _normalize_dob(raw: Any) -> str:
    """
    Convert common Monday DOB text formats into YYYYMMDD.
    Monday stores DOB as free text — must handle all realistic variants.
    """
    if raw is None:
        return ""

    if hasattr(raw, "strftime"):
        try:
            return raw.strftime("%Y%m%d")
        except Exception:
            pass

    date_str = _safe_str(raw)
    if not date_str:
        return ""

    for fmt in ("%m/%d/%y", "%m/%d/%Y", "%Y-%m-%d", "%m-%d-%y", "%m-%d-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(date_str, fmt).strftime("%Y%m%d")
        except ValueError:
            continue

    return ""


# ---------------------------------------------------------------------------
# Payer resolution
# ---------------------------------------------------------------------------

def _resolve_payer_id(general_insurance: str) -> str:
    payer_id = GENERAL_PAYER_ID_MAP.get(general_insurance, "")
    if not payer_id:
        raise ValueError(
            f"Unknown payer mapping for General Insurance: {general_insurance!r}. "
            f"Add it to GENERAL_PAYER_ID_MAP."
        )
    if payer_id == "MAGNACARE_PLACEHOLDER":
        raise ValueError(
            "MagnaCare payer ID is still a placeholder — confirm the real ID before sending."
        )
    return payer_id


def _resolve_trading_partner_name(payer_id: str) -> str:
    name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "")
    if not name:
        raise ValueError(
            f"No Stedi trading partner name for payer ID: {payer_id!r}. "
            f"Add it to STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID."
        )
    return name


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

def _validate_inputs(row: dict[str, Any], *, payer_id: str | None = None) -> None:
    """
    Raise ValueError with a clear message for any missing required field.

    If ``payer_id`` is supplied the caller has already resolved the payer
    (e.g. the Subscription Board flow uses claim_assumptions.PAYER_ID_MAP),
    so the "General Insurance" field is not required and the
    GENERAL_PAYER_ID_MAP lookup is skipped.
    """
    if payer_id is None:
        general_insurance = _safe_str(row.get("General Insurance"))
        if not general_insurance:
            raise ValueError("Missing required field: General Insurance")
        # Payer mapping check (also validates MagnaCare placeholder)
        _resolve_payer_id(general_insurance)

    if not _safe_str(row.get("Member ID")):
        raise ValueError("Missing required field: Member ID")
    if not _safe_str(row.get("First Name")):
        raise ValueError("Missing required field: First Name")
    if not _safe_str(row.get("Last Name")):
        raise ValueError("Missing required field: Last Name")

    dob_raw = row.get("Patient Date of Birth")
    dob = _normalize_dob(dob_raw)
    if not dob:
        raise ValueError(
            f"Missing or unparseable Patient Date of Birth: {dob_raw!r}. "
            f"Expected formats: MM/DD/YYYY, MM/DD/YY, YYYY-MM-DD"
        )

    # Provider — load from claim_assumptions
    try:
        from claim_assumptions import BILLING_PROVIDER_NPI, BILLING_PROVIDER_ORGANIZATION_NAME
        if not _safe_str(BILLING_PROVIDER_NPI):
            raise ValueError("BILLING_PROVIDER_NPI is blank in claim_assumptions.py")
        if not _safe_str(BILLING_PROVIDER_ORGANIZATION_NAME):
            raise ValueError("BILLING_PROVIDER_ORGANIZATION_NAME is blank in claim_assumptions.py")
    except ImportError:
        raise ValueError("claim_assumptions.py not found — cannot load billing provider defaults")


# ---------------------------------------------------------------------------
# Payload builder
# ---------------------------------------------------------------------------

def build_eligibility_payload(
    row: dict[str, Any],
    *,
    service_type_codes: list[str] | None = None,
    payer_id: str | None = None,
    partner_name: str | None = None,
) -> dict[str, Any]:
    """
    Validate inputs and return a Stedi-compatible eligibility request payload.

    Intake Board usage (default):
      row keys (from eligibility_service.extract_eligibility_inputs):
        "General Insurance", "Member ID", "First Name", "Last Name",
        "Patient Date of Birth", "Pulse ID", "Name"
      payer ID is resolved from GENERAL_PAYER_ID_MAP via ``General Insurance``.

    Subscription Board usage:
      Caller (services.subscription_eligibility_service) pre-resolves the
      Stedi payer via ``claim_assumptions.PAYER_ID_MAP`` (keyed on the
      specific Primary Insurance label e.g. "Fidelis Low-Cost") and passes
      both ``payer_id`` and ``partner_name`` here. In that mode, the
      "General Insurance" field is not required; the row may instead carry
      a "Primary Insurance" key for logging.

    Raises ValueError for any validation failure so the caller can return
    a structured error to Monday without crashing.
    """
    _validate_inputs(row, payer_id=payer_id)

    from claim_assumptions import BILLING_PROVIDER_NPI, BILLING_PROVIDER_ORGANIZATION_NAME

    if payer_id is None:
        # Intake flow — resolve from GENERAL_PAYER_ID_MAP
        general_insurance = _safe_str(row["General Insurance"])
        payer_id          = _resolve_payer_id(general_insurance)
        partner_name      = _resolve_trading_partner_name(payer_id)
    else:
        # Caller (Subscription flow) supplied the payer ID directly.
        # Prefer the caller-supplied partner_name; fall back to the local
        # STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID map, and only then raise.
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

    dob               = _normalize_dob(row.get("Patient Date of Birth"))
    member_id         = _safe_str(row["Member ID"])

    # Control number: Pulse ID → Name → member_id
    control_number = (
        _safe_str(row.get("Pulse ID"))
        or _safe_str(row.get("Name"))
        or member_id
    )[:80]  # Stedi max

    codes = service_type_codes or DEFAULT_SERVICE_TYPE_CODES

    payload: dict[str, Any] = {
        "controlNumber":          control_number,
        "tradingPartnerServiceId": payer_id,
        "provider": {
            "organizationName": BILLING_PROVIDER_ORGANIZATION_NAME,
            "npi":              BILLING_PROVIDER_NPI,
        },
        "subscriber": {
            "memberId":    member_id,
            "firstName":   _safe_str(row["First Name"]),
            "lastName":    _safe_str(row["Last Name"]),
            "dateOfBirth": dob,
        },
        "encounter": {
            "serviceTypeCodes": codes,
        },
        # Internal metadata — not sent to Stedi; useful for logs
        "_meta": {
            "generalInsurance":  general_insurance,
            "tradingPartnerName": partner_name,
            "pulseId":           _safe_str(row.get("Pulse ID")),
            "itemName":          _safe_str(row.get("Name")),
        },
    }

    logger.info(
        f"[ELG-BUILDER] Payload ready | "
        f"general_insurance={general_insurance!r} → payer_id={payer_id} | "
        f"partner={partner_name!r} | member_id={member_id!r}"
    )

    return payload
