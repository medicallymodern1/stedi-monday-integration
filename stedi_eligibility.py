from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Any

import requests

from claim_assumptions import (
    BILLING_PROVIDER_NPI,
    BILLING_PROVIDER_ORGANIZATION_NAME,
    PAYER_ID_MAP,
)
from stedi_eligibility_parser import parse_stedi_eligibility_response
from stedi_eligibility_monday_mapping import build_monday_writeback_payload


# ============================================================
# STEDI ELIGIBILITY CONFIG
# ============================================================
# This file builds and sends real-time Stedi eligibility checks.
#
# Design decisions for V1:
# - Source board is the onboarding board
# - Payer routing comes from "Primary Insurance Final"
# - Provider is our facility / billing provider, not the doctor
# - Subscriber fields come from Member ID / First Name / Last Name / DOB
# - Service Type Code is always 12 for DME
# - Date of service is omitted for "today" checks
# - Output parsing follows the final PRD V1 fields only
# ============================================================

STEDI_ELIGIBILITY_ENDPOINT = "https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/eligibility/v3"
DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_SERVICE_TYPE_CODES = ["12"]


# Payer ID -> Stedi canonical payer / network name
STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID = {
    "803": "Anthem Blue Cross Blue Shield of New York",
    "11315": "Fidelis Care",
    "87726": "UnitedHealthcare",
    "60054": "Aetna",
    "14163": "Wellcare",
    "61101": "Humana",
    "62308": "Cigna",
    "MCDNY": "Medicaid New York",
    "47080": "Midlands Choice",
    "22099": "Horizon Blue Cross and Blue Shield of New Jersey",
    "SB890": "BlueCross BlueShield of Tennessee",
    "BCBSF": "Florida Blue",
}


# ============================================================
# MONDAY COLUMN MAP: ONBOARDING BOARD -> ELIGIBILITY INPUTS
# ============================================================
MONDAY_ELIGIBILITY_COLUMN_MAP = {
    "payer_label": "Primary Insurance Final",
    "member_id": "Member ID",
    "first_name": "First Name",
    "last_name": "Last Name",
    "date_of_birth": "Patient Date of Birth",
}


# ============================================================
# HELPERS
# ============================================================

def safe_str(value: Any) -> str:
    """Convert a value to a trimmed string. Returns '' for None."""
    if value is None:
        return ""
    return str(value).strip()



def normalize_date_for_stedi(date_value: Any) -> str:
    """
    Convert common date formats into YYYYMMDD.

    Supports examples like:
    - 3/6/26
    - 03/06/2026
    - 2026-03-06
    - datetime/date objects
    """
    if date_value is None:
        return ""

    if hasattr(date_value, "strftime"):
        try:
            return date_value.strftime("%Y%m%d")
        except Exception:
            pass

    date_str = safe_str(date_value)
    if not date_str:
        return ""

    possible_formats = [
        "%m/%d/%y",
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%m-%d-%y",
        "%m-%d-%Y",
        "%Y/%m/%d",
    ]

    for fmt in possible_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y%m%d")
        except ValueError:
            continue

    return ""


# ============================================================
# PAYER RESOLUTION
# ============================================================

def resolve_eligibility_payer_label(row: dict[str, Any]) -> str:
    """Read the internal payer label from the Monday row."""
    return safe_str(row.get(MONDAY_ELIGIBILITY_COLUMN_MAP["payer_label"]))



def resolve_eligibility_payer_id(payer_label: str) -> str:
    """Convert internal payer label -> payer ID used for Stedi routing."""
    payer_id = PAYER_ID_MAP.get(safe_str(payer_label), "")
    if not payer_id:
        raise ValueError(
            f"No payer ID mapping found for payer label: {payer_label!r}"
        )
    if payer_id == "PLACEHOLDER_MEDICARE_A_B_PAYER_ID":
        raise ValueError(
            "Payer ID for 'Medicare A&B' is still a placeholder. "
            "Replace it before sending eligibility requests."
        )
    return payer_id



def resolve_trading_partner_name(payer_id: str) -> str:
    """Convert payer ID -> Stedi canonical trading partner name."""
    trading_partner_name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(safe_str(payer_id), "")
    if not trading_partner_name:
        raise ValueError(
            f"No Stedi trading partner name mapping found for payer ID: {payer_id!r}"
        )
    return trading_partner_name


# ============================================================
# FIELD EXTRACTION
# ============================================================

def extract_subscriber_fields(row: dict[str, Any]) -> dict[str, str]:
    """Extract and normalize subscriber fields from one Monday row."""
    member_id = safe_str(row.get(MONDAY_ELIGIBILITY_COLUMN_MAP["member_id"]))
    first_name = safe_str(row.get(MONDAY_ELIGIBILITY_COLUMN_MAP["first_name"]))
    last_name = safe_str(row.get(MONDAY_ELIGIBILITY_COLUMN_MAP["last_name"]))
    date_of_birth = normalize_date_for_stedi(
        row.get(MONDAY_ELIGIBILITY_COLUMN_MAP["date_of_birth"])
    )

    if not member_id:
        raise ValueError("Missing required member ID.")
    if not first_name:
        raise ValueError("Missing required subscriber first name.")
    if not last_name:
        raise ValueError("Missing required subscriber last name.")
    if not date_of_birth:
        raise ValueError("Missing or invalid subscriber date of birth.")

    return {
        "memberId": member_id,
        "firstName": first_name,
        "lastName": last_name,
        "dateOfBirth": date_of_birth,
    }


# ============================================================
# PAYLOAD BUILDER
# ============================================================

def build_eligibility_payload_from_monday_row(
    row: dict[str, Any],
    *,
    service_type_codes: list[str] | None = None,
) -> dict[str, Any]:
    """
    Build one Stedi real-time eligibility payload from one Monday row.

    Important V1 behavior:
    - provider is our facility / billing provider
    - dateOfService is intentionally omitted for "today" checks
    - dependents / secondary information is intentionally omitted
    """
    payer_label = resolve_eligibility_payer_label(row)
    if not payer_label:
        raise ValueError("Missing payer label in 'Primary Insurance Final'.")

    payer_id = resolve_eligibility_payer_id(payer_label)
    trading_partner_name = resolve_trading_partner_name(payer_id)
    subscriber = extract_subscriber_fields(row)

    payload = {
        "controlNumber": safe_str(row.get("Pulse ID")) or safe_str(row.get("Name")) or subscriber["memberId"],
        "tradingPartnerServiceId": payer_id,
        "provider": {
            "organizationName": BILLING_PROVIDER_ORGANIZATION_NAME,
            "npi": BILLING_PROVIDER_NPI,
        },
        "subscriber": subscriber,
        "encounter": {
            "serviceTypeCodes": service_type_codes or DEFAULT_SERVICE_TYPE_CODES,
        },
        # Not required by Stedi for routing, but useful for logging / debugging.
        "metadata": {
            "internalPayerLabel": payer_label,
            "tradingPartnerName": trading_partner_name,
        },
    }

    return payload


# ============================================================
# HTTP SENDER
# ============================================================

def send_realtime_eligibility_check(
    payload: dict[str, Any],
    *,
    api_key: str | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """Send one real-time eligibility check to Stedi."""
    resolved_api_key = safe_str(api_key) or safe_str(os.getenv("STEDI_API_KEY"))
    if not resolved_api_key:
        raise ValueError(
            "Missing Stedi API key. Pass api_key=... or set STEDI_API_KEY in your environment."
        )

    headers = {
        "Authorization": resolved_api_key,
        "Content-Type": "application/json",
    }

    response = requests.post(
        STEDI_ELIGIBILITY_ENDPOINT,
        headers=headers,
        json=payload,
        timeout=timeout_seconds,
    )

    try:
        response_json = response.json()
    except ValueError:
        response.raise_for_status()
        raise ValueError("Stedi returned a non-JSON response.")

    if not response.ok:
        # Still return the response body structure to the caller if they want to parse it.
        raise ValueError(
            "Eligibility request failed. "
            f"HTTP {response.status_code}: {json.dumps(response_json, indent=2)}"
        )

    return response_json


# ============================================================
# CONVENIENCE WRAPPERS
# ============================================================

def run_realtime_eligibility_from_monday_row(
    row: dict[str, Any],
    *,
    api_key: str | None = None,
    service_type_codes: list[str] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """
    One-step helper:
    Monday row -> payload -> raw Stedi response
    """
    payload = build_eligibility_payload_from_monday_row(
        row,
        service_type_codes=service_type_codes,
    )
    return send_realtime_eligibility_check(
        payload,
        api_key=api_key,
        timeout_seconds=timeout_seconds,
    )



def run_and_parse_realtime_eligibility_from_monday_row(
    row: dict[str, Any],
    *,
    api_key: str | None = None,
    service_type_codes: list[str] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """
    Monday row -> payload -> raw Stedi response -> normalized parsed output.

    This is useful when the caller wants only the V1 parsed values.
    """
    response_json = run_realtime_eligibility_from_monday_row(
        row,
        api_key=api_key,
        service_type_codes=service_type_codes,
        timeout_seconds=timeout_seconds,
    )

    requested_service_type_code = (service_type_codes or DEFAULT_SERVICE_TYPE_CODES)[0]
    return parse_stedi_eligibility_response(
        response_json,
        requested_service_type_code=requested_service_type_code,
    )



def run_parse_and_build_monday_writeback(
    row: dict[str, Any],
    *,
    api_key: str | None = None,
    service_type_codes: list[str] | None = None,
    timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
) -> dict[str, Any]:
    """
    Monday row -> payload -> raw Stedi response -> normalized parsed output
    -> Monday column-name writeback payload.

    This is the cleanest handoff point for developers who already know how they
    want to call Monday's update APIs.
    """
    normalized_output = run_and_parse_realtime_eligibility_from_monday_row(
        row,
        api_key=api_key,
        service_type_codes=service_type_codes,
        timeout_seconds=timeout_seconds,
    )
    return build_monday_writeback_payload(normalized_output)


# ============================================================
# LOCAL TEST EXAMPLE
# ============================================================
if __name__ == "__main__":
    example_row = {
        "Primary Insurance Final": "Anthem BCBS Commercial",
        "Member ID": "ABC123456789",
        "First Name": "Jane",
        "Last Name": "Doe",
        "Patient Date of Birth": "03/06/1985",
        "Pulse ID": "PULSE-001",
        "Name": "Jane Doe",
    }

    payload = build_eligibility_payload_from_monday_row(example_row)
    print("=== ELIGIBILITY PAYLOAD ===")
    print(json.dumps(payload, indent=2))

    print("\n=== EXPECTED MONDAY OUTPUT COLUMNS ===")
    print(json.dumps(build_monday_writeback_payload({}), indent=2))
