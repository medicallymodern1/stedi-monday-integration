import logging
from typing import Dict, Any
import os
import requests

logger = logging.getLogger(__name__)


def check_eligibility(monday_item: dict) -> dict:
    try:
        row = extract_eligibility_inputs(monday_item)

        validate_inputs(row)

        payload = build_stedi_payload(row)

        response = call_stedi_api(payload)

        parsed = parse_response(response)
        print("check_eligibility:", parsed)

        return parsed

    except Exception as e:
        logger.error(f"Eligibility check failed: {e}", exc_info=True)
        return {
            "eligibility_error_description": str(e)
        }

def extract_eligibility_inputs(monday_item: dict) -> dict:
    cols = {c.get("id"): c.get("text", "") for c in monday_item.get("column_values", [])}

    first, last = split_name(monday_item.get("name", ""))

    return {
        "payer": cols.get("color_mm18jhq5", ""),
        "member_id": cols.get("text_mm18s3fe", ""),
        "first_name": first,
        "last_name": last,
        "dob": cols.get("text_mm187t6a", ""),
    }

def validate_inputs(row: dict):
    if not row.get("member_id"):
        raise ValueError("Missing Member ID")

    if not row.get("payer"):
        raise ValueError("Missing Insurance")

    if not row.get("dob"):
        raise ValueError("Missing DOB")

    if not row.get("first_name"):
        raise ValueError("Invalid First Name")

    if not row.get("last_name"):
        raise ValueError("Invalid Last Name")

import re


def clean_name(name: str) -> str:
    if not name:
        return ""

    # Remove brackets content (test, copy, etc.)
    name = re.sub(r"\(.*?\)", "", name)

    # Remove leading non-alphanumeric characters
    name = re.sub(r"^[^a-zA-Z0-9]+", "", name)

    # Remove extra spaces
    name = " ".join(name.split())

    return name.strip()


def split_name(full_name: str) -> tuple[str, str]:
    clean = clean_name(full_name)

    parts = clean.split()

    if not parts:
        return "", ""

    if len(parts) == 1:
        return parts[0], ""

    return parts[0], " ".join(parts[1:])

def format_dob(dob: str) -> str:
    if "-" in dob:
        parts = dob.split("-")
        return "".join(parts)
    if "/" in dob:
        parts = dob.split("/")
        return f"{parts[2]}{parts[0].zfill(2)}{parts[1].zfill(2)}"
    return dob

def build_stedi_payload(row: dict) -> dict:
    from services.stedi_service import lookup_payer_name_by_internal
    from claim_assumptions import resolve_payer_id

    payer_name = row["payer"]

    # Convert internal payer → payer_id (IMPORTANT)
    payer_id = resolve_payer_id(payer_name)

    if not payer_id:
        raise ValueError(f"Unable to resolve payer_id for: {payer_name}")

    return {
        "tradingPartnerServiceId": payer_id,
        "provider": {
            "npi": "1023042348",
            "organizationName": "Your Org Name"
        },
        "subscriber": {
            "memberId": row["member_id"],
            "firstName": row["first_name"],
            "lastName": 'test last',
            "dateOfBirth": format_dob(row["dob"]),
        },
        "encounter": {
            "serviceTypeCodes": ["30"]  # 30 = General Medical (safe default)
        },
        "externalPatientId": row["member_id"]
    }


def call_stedi_api(payload: dict) -> dict:
    import os
    import requests

    api_key = os.getenv("STEDI_API_KEY")

    if not api_key:
        raise ValueError("Missing STEDI_API_KEY")

    url = "https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/eligibility/v3"

    response = requests.post(
        url,
        json=payload,
        headers={
            "Authorization": api_key,
            "Content-Type": "application/json",
        },
        timeout=30,
    )
    print("Stedi API response:", response.status_code, response.text)
    print("Payload sent:", payload)
    if not response.ok:
        raise ValueError(f"Stedi API error: {response.text}")

    return response.json()


def parse_response(response: dict) -> dict:
    try:
        benefits = response.get("benefitsInformation", [])

        active = "No"
        if benefits:
            active = "Yes"

        return {
            "eligibility_active": active,
            "eligibility_plan_name": response.get("plan", {}).get("name", ""),
            "eligibility_error_description": "",
        }

    except Exception as e:
        return {
            "eligibility_error_description": str(e)
        }