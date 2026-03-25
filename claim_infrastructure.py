from __future__ import annotations

import csv
import re
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Optional

from claim_assumptions import (
    USAGE_INDICATOR,
    BILLING_PROVIDER_ORGANIZATION_NAME,
    BILLING_PROVIDER_ADDRESS_1,
    BILLING_PROVIDER_ADDRESS_2,
    BILLING_PROVIDER_CITY,
    BILLING_PROVIDER_STATE,
    BILLING_PROVIDER_POSTAL_CODE,
    BILLING_PROVIDER_NPI,
    BILLING_PROVIDER_EIN,
    BILLING_PROVIDER_TAXONOMY_CODE,
    BILLING_PROVIDER_CONTACT_NAME,
    BILLING_PROVIDER_CONTACT_PHONE_NUMBER,
    SUBMITTER_ORGANIZATION_NAME,
    SUBMITTER_IDENTIFICATION,
    SUBMITTER_CONTACT_NAME,
    SUBMITTER_PHONE_NUMBER,
    DEFAULT_CLAIM_FREQUENCY_CODE,
    DEFAULT_SIGNATURE_INDICATOR,
    DEFAULT_PLAN_PARTICIPATION_CODE,
    DEFAULT_BENEFITS_ASSIGNMENT_CERTIFICATION_INDICATOR,
    DEFAULT_RELEASE_INFORMATION_CODE,
    generate_patient_control_number,
    generate_provider_control_number,
    resolve_payer_name,
    resolve_payer_id,
    resolve_claim_filing_code,
    resolve_place_of_service_code,
    resolve_procedure_code,
    resolve_service_unit_count,
    resolve_procedure_modifiers,
    resolve_line_item_charge_amount,
    sum_claim_charge_amount,
)


# ============================================================
# INFRASTRUCTURE / PIPELINE FILE
# ============================================================
# This file contains the plumbing:
# - parsing Monday exports
# - normalizing raw data
# - grouping into claims
# - formatting into Stedi JSON
#
# This is the file the Tuesday team will mostly care about.
# ============================================================


# ============================================================
# SHARED GENERIC HELPERS
# ============================================================

def safe_str(value: Any) -> str:
    """Convert a value to a clean string. Returns '' for None."""
    if value is None:
        return ""
    return str(value).strip()


def normalize_spaces(text: str) -> str:
    """Collapse multiple spaces into a single space."""
    return re.sub(r"\s+", " ", safe_str(text)).strip()


def split_full_name(full_name: str) -> tuple[str, str]:
    """Split a full name into first and last name."""
    full_name = normalize_spaces(full_name)
    if not full_name:
        return "", ""

    parts = full_name.split(" ")
    if len(parts) == 1:
        return parts[0], ""

    first_name = parts[0]
    last_name = " ".join(parts[1:])
    return first_name, last_name


def normalize_gender(gender: str) -> str:
    """Convert gender text into M / F / U."""
    gender = safe_str(gender).upper()

    if gender in {"M", "MALE"}:
        return "M"
    if gender in {"F", "FEMALE"}:
        return "F"

    return "U"


def normalize_date(date_str: str) -> str:
    """
    Convert a date into YYYYMMDD format.
    Supports common formats like:
    - 3/6/26
    - 03/06/2026
    - 2026-03-06
    """
    from datetime import datetime

    date_str = safe_str(date_str)
    if not date_str:
        return ""

    possible_formats = [
        "%m/%d/%y",
        "%m/%d/%Y",
        "%Y-%m-%d",
        "%m-%d-%y",
        "%m-%d-%Y",
    ]

    for fmt in possible_formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            return dt.strftime("%Y%m%d")
        except ValueError:
            continue

    return ""


def clean_numeric_string(value: str) -> str:
    """Remove commas and surrounding spaces from a numeric-looking field."""
    return safe_str(value).replace(",", "")


# ============================================================
# ADDRESS PARSING CONSTANTS
# ============================================================

STATE_ABBR = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas",
    "CA": "California", "CO": "Colorado", "CT": "Connecticut", "DE": "Delaware",
    "FL": "Florida", "GA": "Georgia", "HI": "Hawaii", "ID": "Idaho",
    "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas",
    "KY": "Kentucky", "LA": "Louisiana", "ME": "Maine", "MD": "Maryland",
    "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska",
    "NV": "Nevada", "NH": "New Hampshire", "NJ": "New Jersey",
    "NM": "New Mexico", "NY": "New York", "NC": "North Carolina",
    "ND": "North Dakota", "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon",
    "PA": "Pennsylvania", "RI": "Rhode Island", "SC": "South Carolina",
    "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah",
    "VT": "Vermont", "VA": "Virginia", "WA": "Washington",
    "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
    "DC": "District of Columbia", "PR": "Puerto Rico",
    "VI": "U.S. Virgin Islands", "GU": "Guam",
    "MP": "Northern Mariana Islands", "AS": "American Samoa",
}

STATE_FULL_TO_ABBR = {
    full_name.lower(): abbr for abbr, full_name in STATE_ABBR.items()
}

COUNTRY_TAIL_RE = re.compile(
    r",?\s*(?:USA|US|U\.S\.A\.?|United\s+States(?:\s+of\s+America)?)\s*$",
    re.IGNORECASE,
)

ZIP_RE = re.compile(r"\b\d{5}(?:-\d{4})?\b")

STATE_ABBR_LIST = "|".join(STATE_ABBR.keys())
STATE_FULL_LIST = "|".join(re.escape(v) for v in STATE_ABBR.values())

PATTERN_ABBR = re.compile(
    rf"^(.*?)\s*,\s*([A-Za-z .'-]+?)\s*,\s*({STATE_ABBR_LIST})(?:\s*|,\s*)(\d{{5}}(?:-\d{{4}})?)?$",
    re.IGNORECASE,
)

PATTERN_FULL = re.compile(
    rf"^(.*?)\s*,\s*([A-Za-z .'-]+?)\s*,\s*({STATE_FULL_LIST})(?:\s*|,\s*)(\d{{5}}(?:-\d{{4}})?)?$",
    re.IGNORECASE,
)

PATTERN_ABBR_WEAK = re.compile(
    rf"^(.*?)\s*,\s*([A-Za-z .'-]+?)\s+({STATE_ABBR_LIST})(?:\s*|,\s*)(\d{{5}}(?:-\d{{4}})?)?$",
    re.IGNORECASE,
)

PATTERN_FULL_WEAK = re.compile(
    rf"^(.*?)\s*,\s*([A-Za-z .'-]+?)\s+({STATE_FULL_LIST})(?:\s*|,\s*)(\d{{5}}(?:-\d{{4}})?)?$",
    re.IGNORECASE,
)

UNIT_TAIL_RE = re.compile(
    r"\s*(?:,\s*)?(?:\b(?:apt|apartment|unit|ste|suite|fl|floor|bldg|building|rm|room|bsmt|basement)\b\.?\s*\w[\w-]*|#\s*\w[\w-]*|\d+(?:st|nd|rd|th)\s+floor)\s*$",
    re.IGNORECASE,
)

PATIENT_UNIT_TAIL_RE = re.compile(
    r"\s*(?:,\s*)?(?:\b(?:apt|apartment|unit|ste|suite|fl|floor|bldg|building|rm|room|bsmt|basement)\b\.?(?:\s*\w[\w-]*)?|#\s*\w[\w-]*|\d+(?:st|nd|rd|th)\s+floor|\b(?=[A-Za-z0-9-]*[A-Za-z])(?=[A-Za-z0-9-]*\d)[A-Za-z0-9-]+\b)\s*$",
    re.IGNORECASE,
)

CITY_UNIT_PREFIX_RE = re.compile(
    r"^(?:(?:apt|apartment|unit|suite|ste|fl|floor|bldg|building|rm|room|dept|department)\b\.?\s*\w*|\d+(?:st|nd|rd|th)\s+floor)\s+",
    re.IGNORECASE,
)


# ============================================================
# ADDRESS HELPERS
# ============================================================

def title_case(text: str) -> str:
    """Convert text to title case."""
    return safe_str(text).lower().title()


def strip_country_tail(text: str) -> str:
    """Remove trailing country names like USA / US / United States."""
    text = safe_str(text)
    return COUNTRY_TAIL_RE.sub("", text).strip()


def normalize_address_text(text: str) -> str:
    """Clean extra spaces and normalize comma spacing."""
    text = strip_country_tail(text)
    text = re.sub(r"\s{2,}", " ", text)
    text = re.sub(r"\s*,\s*", ", ", text)
    text = re.sub(r",\s*USA\b", "", text, flags=re.IGNORECASE)
    return text.strip()


def finalize_street_city(
    street: str,
    city: str,
    state: str,
    postal_code: str,
    drop_units: bool = False,
) -> dict:
    """
    Final cleanup step for parsed addresses.
    """
    street = safe_str(street)
    city = safe_str(city)
    state = safe_str(state)
    postal_code = safe_str(postal_code)

    if street and city and street == city:
        tokens = street.split()
        if len(tokens) >= 2:
            street_suffixes = {
                "st", "street", "rd", "road", "ave", "avenue", "blvd", "boulevard",
                "dr", "drive", "ln", "lane", "ct", "court", "pl", "place",
                "pkwy", "parkway", "hwy", "highway", "terrace", "ter", "way"
            }
            unit_words = {
                "apt", "apartment", "unit", "suite", "ste", "bldg", "building",
                "rm", "room", "bsmt", "basement", "fl", "floor"
            }

            city_start = -1
            for i in range(len(tokens) - 1, -1, -1):
                raw_tok = tokens[i]
                tok = raw_tok.lower().replace(".", "").replace(",", "")
                has_digit = any(ch.isdigit() for ch in tok)
                is_suffix = tok in street_suffixes
                is_unit = tok in unit_words
                is_hash = tok.startswith("#")

                if not has_digit and not is_suffix and not is_unit and not is_hash:
                    city_start = i
                    break

            if city_start > 0:
                while city_start > 0:
                    prev = tokens[city_start - 1].lower().replace(".", "").replace(",", "")
                    has_digit_prev = any(ch.isdigit() for ch in prev)
                    is_suffix_prev = prev in street_suffixes
                    is_unit_prev = prev in unit_words
                    is_hash_prev = prev.startswith("#")
                    if has_digit_prev or is_suffix_prev or is_unit_prev or is_hash_prev:
                        break
                    city_start -= 1

                street = " ".join(tokens[:city_start])
                city = " ".join(tokens[city_start:])

    address1 = street.strip()
    address2 = ""

    if CITY_UNIT_PREFIX_RE.search(city):
        city = CITY_UNIT_PREFIX_RE.sub("", city).strip()

    if drop_units:
        address1 = UNIT_TAIL_RE.sub("", address1).strip().rstrip(",")
        address2 = ""
    else:
        unit_match = PATIENT_UNIT_TAIL_RE.search(address1)
        if unit_match:
            address2 = unit_match.group(0).lstrip(", ").strip()
            address1 = address1[:unit_match.start()].strip().rstrip(",")

    if postal_code:
        zip_match = re.search(r"\d{5}", postal_code)
        postal_code = zip_match.group(0) if zip_match else ""

    return {
        "address1": address1,
        "address2": address2,
        "city": title_case(city),
        "state": state.upper() if state else "",
        "postal_code": postal_code,
    }


def parse_address(address_text: str, drop_units: bool = False) -> dict:
    """
    Parse a free-text address into:
    - address1
    - address2
    - city
    - state
    - postal_code
    """
    raw = normalize_address_text(address_text)
    if not raw:
        return {
            "address1": "",
            "address2": "",
            "city": "",
            "state": "",
            "postal_code": "",
        }

    match = PATTERN_ABBR.match(raw) or PATTERN_FULL.match(raw)

    if not match:
        match = PATTERN_ABBR_WEAK.match(raw) or PATTERN_FULL_WEAK.match(raw)

    if match:
        street = safe_str(match.group(1))
        city = safe_str(match.group(2))
        state = safe_str(match.group(3))
        postal_code = safe_str(match.group(4))

        if len(state) > 2:
            state = STATE_FULL_TO_ABBR.get(state.lower(), state)

        return finalize_street_city(street, city, state, postal_code, drop_units=drop_units)

    city_state_zip_match = re.match(
        rf"^(.*?),\s*({STATE_ABBR_LIST}|{STATE_FULL_LIST}),\s*(\d{{5}}(?:-\d{{4}})?)$",
        raw,
        re.IGNORECASE,
    )
    if city_state_zip_match:
        head = safe_str(city_state_zip_match.group(1))
        state = safe_str(city_state_zip_match.group(2))
        postal_code = safe_str(city_state_zip_match.group(3))

        if len(state) > 2:
            state = STATE_FULL_TO_ABBR.get(state.lower(), state)

        return finalize_street_city(head, head, state, postal_code, drop_units=drop_units)

    last_state_abbr = re.search(
        rf"(?:,\s*|\s+)({STATE_ABBR_LIST})(?:\s+{ZIP_RE.pattern})?\s*$",
        raw,
        re.IGNORECASE,
    )
    last_state_full = re.search(
        rf"(?:,\s*|\s+)({STATE_FULL_LIST})(?:\s+{ZIP_RE.pattern})?\s*$",
        raw,
        re.IGNORECASE,
    )
    fallback_match = last_state_abbr or last_state_full

    if fallback_match:
        tail_start = fallback_match.start()
        head = raw[:tail_start].rstrip(", ").strip()
        tail = raw[tail_start:].strip()

        parts = [p.strip() for p in head.split(",") if p.strip()]
        city = parts[-1] if parts else ""
        street = ", ".join(parts[:-1]) if len(parts) > 1 else head

        zip_match = ZIP_RE.search(tail)
        postal_code = zip_match.group(0) if zip_match else ""

        state = safe_str(fallback_match.group(1))
        if len(state) > 2:
            state = STATE_FULL_TO_ABBR.get(state.lower(), state)

        return finalize_street_city(street, city, state, postal_code, drop_units=drop_units)

    city_zip_only_match = re.match(
        r"^(.*?),\s*([^,]+),\s*(\d{5}(?:-\d{4})?)$",
        raw,
        re.IGNORECASE,
    )
    if city_zip_only_match:
        street = safe_str(city_zip_only_match.group(1))
        city = safe_str(city_zip_only_match.group(2))
        postal_code = safe_str(city_zip_only_match.group(3))
        return finalize_street_city(street, city, "", postal_code, drop_units=drop_units)

    return {
        "address1": raw,
        "address2": "",
        "city": "",
        "state": "",
        "postal_code": "",
    }


# ============================================================
# NORMALIZED INPUT SCHEMA
# ============================================================

NORMALIZED_ORDER_TEMPLATE = {
    "source_parent_name": "",
    "source_child_name": "",
    "customer_id": "",
    "claim_status": "",
    "order_status": "",
    "patient_full_name": "",
    "patient_first_name": "",
    "patient_last_name": "",
    "patient_dob": "",
    "patient_gender": "",
    "patient_phone": "",
    "patient_address_1": "",
    "patient_address_2": "",
    "patient_city": "",
    "patient_state": "",
    "patient_postal_code": "",
    "member_id": "",
    "primary_insurance_name": "",
    "payer_name": "",
    "secondary_member_id": "",
    "subscription_type": "",
    "diagnosis_code": "",
    "cgm_coverage": "",
    "doctor_name": "",
    "doctor_first_name": "",
    "doctor_last_name": "",
    "doctor_npi": "",
    "doctor_address_1": "",
    "doctor_address_2": "",
    "doctor_city": "",
    "doctor_state": "",
    "doctor_postal_code": "",
    "doctor_phone": "",
    "order_date": "",
    "product_category": "",
    "item": "",
    "variant": "",
    "quantity": "",
    "units": "",
    "auth_id": "",
    "payer_id": "",
    "claim_filing_code": "",
    "place_of_service_code": "",
    "claim_charge_amount": "",
    "patient_control_number": "",
    "service_date": "",
    "procedure_code": "",
    "procedure_modifiers": [],
    "service_unit_count": "",
    "line_item_charge_amount": "",
    "provider_control_number": "",
}


def build_normalized_order_template() -> dict:
    """Return a fresh copy of the normalized order template."""
    return deepcopy(NORMALIZED_ORDER_TEMPLATE)


# ============================================================
# CSV HELPERS
# ============================================================

def read_raw_csv_lines(csv_path: str | Path) -> list[list[str]]:
    """
    Read the CSV as raw rows, not as a normal DictReader.
    """
    raw_rows: list[list[str]] = []

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        for row in reader:
            raw_rows.append(row)

    return raw_rows


def row_is_effectively_blank(row: list[str]) -> bool:
    """Return True if every cell in the row is blank."""
    return not any(safe_str(cell) for cell in row)


def make_row_dict(header_row: list[str], data_row: list[str]) -> dict:
    """
    Combine a header row and data row into a dictionary.
    """
    max_len = max(len(header_row), len(data_row))
    padded_header = header_row + [""] * (max_len - len(header_row))
    padded_data = data_row + [""] * (max_len - len(data_row))

    row_dict = {}
    for header, value in zip(padded_header, padded_data):
        header = safe_str(header)
        if header:
            row_dict[header] = safe_str(value)

    return row_dict


# ============================================================
# MONDAY -> NORMALIZED ORDER MAPPING
# ============================================================

def extract_parent_row(parent_row: dict) -> dict:
    """
    Pull patient / insurance / provider info from a parent row and
    map it into normalized field names.
    """
    normalized = build_normalized_order_template()

    normalized["source_parent_name"] = normalize_spaces(parent_row.get("Name", ""))
    normalized["customer_id"] = safe_str(parent_row.get("Customer ID", ""))
    normalized["claim_status"] = safe_str(parent_row.get("Claim Status", ""))

    patient_full_name = normalize_spaces(parent_row.get("Name", ""))
    patient_first_name, patient_last_name = split_full_name(patient_full_name)

    patient_address = parse_address(parent_row.get("Patient Address", ""))

    normalized["patient_full_name"] = patient_full_name
    normalized["patient_first_name"] = patient_first_name
    normalized["patient_last_name"] = patient_last_name
    normalized["patient_dob"] = normalize_date(parent_row.get("DOB", ""))
    normalized["patient_gender"] = normalize_gender(parent_row.get("Gender", ""))
    normalized["patient_phone"] = safe_str(parent_row.get("Phone", ""))
    normalized["patient_address_1"] = patient_address["address1"]
    normalized["patient_address_2"] = patient_address["address2"]
    normalized["patient_city"] = patient_address["city"]
    normalized["patient_state"] = patient_address["state"]
    normalized["patient_postal_code"] = patient_address["postal_code"]

    normalized["member_id"] = safe_str(parent_row.get("Member ID", ""))
    normalized["primary_insurance_name"] = safe_str(parent_row.get("Primary Insurance", ""))
    normalized["payer_name"] = safe_str(parent_row.get("PR Payor", ""))
    normalized["secondary_member_id"] = safe_str(parent_row.get("Secondary ID", ""))
    normalized["subscription_type"] = safe_str(parent_row.get("Subscription Type", ""))

    doctor_name = normalize_spaces(parent_row.get("Doctor Name", ""))
    doctor_first_name, doctor_last_name = split_full_name(doctor_name)
    doctor_address = parse_address(parent_row.get("Doctor Address", ""), drop_units=True)

    normalized["diagnosis_code"] = safe_str(parent_row.get("Diagnosis Code", ""))
    normalized["cgm_coverage"] = safe_str(parent_row.get("CGM Coverage", ""))
    normalized["doctor_name"] = doctor_name
    normalized["doctor_first_name"] = doctor_first_name
    normalized["doctor_last_name"] = doctor_last_name
    normalized["doctor_npi"] = safe_str(parent_row.get("Doctor NPI", ""))
    normalized["doctor_address_1"] = doctor_address["address1"]
    normalized["doctor_address_2"] = doctor_address["address2"]
    normalized["doctor_city"] = doctor_address["city"]
    normalized["doctor_state"] = doctor_address["state"]
    normalized["doctor_postal_code"] = doctor_address["postal_code"]
    normalized["doctor_phone"] = safe_str(parent_row.get("Doctor Phone", ""))

    return normalized


def extract_child_row(child_row: dict) -> dict:
    """
    Pull order-line info from a child/subitem row and map it into
    normalized field names.
    """
    normalized = build_normalized_order_template()

    normalized["source_child_name"] = normalize_spaces(child_row.get("Name", ""))
    normalized["order_status"] = safe_str(child_row.get("Order Status", ""))
    normalized["order_date"] = normalize_date(child_row.get("Order Date", ""))

    normalized["item"] = normalize_spaces(child_row.get("Name", ""))
    normalized["variant"] = normalize_spaces(child_row.get("CGM Type", ""))
    normalized["payer_name"] = safe_str(child_row.get("Primary", ""))
    normalized["primary_insurance_name"] = safe_str(child_row.get("Primary", ""))
    normalized["member_id"] = safe_str(child_row.get("Member ID", ""))
    normalized["secondary_member_id"] = safe_str(child_row.get("Secondary ID", ""))
    normalized["quantity"] = clean_numeric_string(child_row.get("Quantity", ""))
    normalized["units"] = safe_str(child_row.get("Units", ""))
    normalized["auth_id"] = safe_str(child_row.get("Auth ID", ""))
    normalized["product_category"] = safe_str(child_row.get("Plan Name", ""))

    return normalized


def combine_parent_and_child(parent_data: dict, child_data: dict) -> dict:
    """
    Merge parent and child normalized data into one normalized order object.
    Child values win if there is overlap.
    """
    combined = build_normalized_order_template()

    for key, value in parent_data.items():
        combined[key] = deepcopy(value)

    for key, value in child_data.items():
        if value not in ("", [], {}, None):
            combined[key] = deepcopy(value)

    return combined


def normalize_parent_child_to_order(parent_row: dict, child_row: dict) -> dict:
    """
    Convert one Monday parent row + one Monday child row into one normalized order dict.
    """
    parent_data = extract_parent_row(parent_row)
    child_data = extract_child_row(child_row)
    return combine_parent_and_child(parent_data, child_data)


def load_monday_export(csv_path: str | Path) -> list[dict]:
    """
    Parse the Monday export into normalized order dicts.
    Output:
    - one normalized dict per child row
    """
    raw_rows = read_raw_csv_lines(csv_path)

    normalized_orders: list[dict] = []

    parent_header: Optional[list[str]] = None
    child_header: Optional[list[str]] = None
    current_parent_row_dict: Optional[dict] = None

    i = 0
    while i < len(raw_rows):
        row = raw_rows[i]

        if row_is_effectively_blank(row):
            i += 1
            continue

        first_cell = safe_str(row[0]) if row else ""

        if first_cell == "Current Order":
            i += 1
            continue

        if "Name" in row and "Diagnosis Code" in row and "Primary Insurance" in row:
            parent_header = row
            i += 1
            continue

        if "Subitems" in row and "Order Status" in row:
            child_header = row
            i += 1
            continue

        if parent_header is not None:
            parent_candidate = make_row_dict(parent_header, row)

            has_parent_name = bool(safe_str(parent_candidate.get("Name", "")))
            has_diag = bool(safe_str(parent_candidate.get("Diagnosis Code", "")))
            has_primary_ins = bool(safe_str(parent_candidate.get("Primary Insurance", "")))
            looks_like_child = safe_str(parent_candidate.get("Subitems", "")) == "Subitems"

            if has_parent_name and has_diag and has_primary_ins and not looks_like_child:
                current_parent_row_dict = parent_candidate
                i += 1
                continue

        if current_parent_row_dict is not None and child_header is not None:
            child_candidate = make_row_dict(child_header, row)

            has_child_name = bool(safe_str(child_candidate.get("Name", "")))
            has_order_status = bool(safe_str(child_candidate.get("Order Status", "")))

            if has_child_name and has_order_status:
                normalized_order = normalize_parent_child_to_order(
                    current_parent_row_dict,
                    child_candidate,
                )
                normalized_orders.append(normalized_order)
                i += 1
                continue

        i += 1

    return normalized_orders


# ============================================================
# INTERNAL GROUPED CLAIM SCHEMA
# ============================================================

GROUPED_CLAIM_TEMPLATE = {
    "claim_key": "",
    "patient_control_number": "",
    "customer_id": "",
    "patient_full_name": "",
    "patient_first_name": "",
    "patient_last_name": "",
    "patient_dob": "",
    "patient_gender": "",
    "patient_phone": "",
    "patient_address_1": "",
    "patient_address_2": "",
    "patient_city": "",
    "patient_state": "",
    "patient_postal_code": "",
    "member_id": "",
    "primary_insurance_name": "",
    "payer_name": "",
    "payer_id": "",
    "claim_filing_code": "",
    "place_of_service_code": "",
    "secondary_member_id": "",
    "subscription_type": "",
    "diagnosis_code": "",
    "doctor_name": "",
    "doctor_first_name": "",
    "doctor_last_name": "",
    "doctor_npi": "",
    "doctor_address_1": "",
    "doctor_address_2": "",
    "doctor_city": "",
    "doctor_state": "",
    "doctor_postal_code": "",
    "doctor_phone": "",
    "order_date": "",
    "claim_status": "",
    "order_status": "",
    "claim_charge_amount": "",
    "service_lines": [],
}


def build_grouped_claim_template() -> dict:
    """Return a fresh copy of the grouped claim template."""
    return deepcopy(GROUPED_CLAIM_TEMPLATE)


# ============================================================
# GROUPING / CLAIM BUILDING
# ============================================================

def build_claim_group_key(normalized_order: dict) -> str:
    """
    Build the grouping key for deciding which normalized line items
    belong on the same claim.
    """
    customer_id = safe_str(normalized_order.get("customer_id", ""))
    if customer_id:
        return customer_id

    patient_name = safe_str(normalized_order.get("patient_full_name", ""))
    member_id = safe_str(normalized_order.get("member_id", ""))
    order_date = safe_str(normalized_order.get("order_date", ""))

    return f"{patient_name}|{member_id}|{order_date}"


def build_service_line_from_normalized_order(normalized_order: dict) -> dict:
    """
    Convert one normalized order row into a simple internal service-line object.
    """
    from claim_assumptions import add_days_to_yyyymmdd

    order_date = safe_str(normalized_order.get("order_date", ""))
    service_date = add_days_to_yyyymmdd(order_date, 1)

    payer_name = resolve_payer_name(normalized_order)
    item_name = safe_str(normalized_order.get("item", ""))
    variant = safe_str(normalized_order.get("variant", ""))
    quantity = safe_str(normalized_order.get("quantity", ""))
    cgm_coverage = safe_str(normalized_order.get("cgm_coverage", ""))

    procedure_code = resolve_procedure_code(payer_name, item_name)
    service_unit_count = resolve_service_unit_count(
        payer_name=payer_name,
        item_name=item_name,
        variant=variant,
        quantity=quantity,
        procedure_code=procedure_code,
    )
    procedure_modifiers = resolve_procedure_modifiers(
        payer_name=payer_name,
        procedure_code=procedure_code,
        cgm_coverage=cgm_coverage,
    )
    line_item_charge_amount = resolve_line_item_charge_amount(
        payer_name=payer_name,
        procedure_code=procedure_code,
        service_unit_count=service_unit_count,
)

    return {
        "source_child_name": safe_str(normalized_order.get("source_child_name", "")),
        "order_date": order_date,
        "service_date": service_date,
        "product_category": safe_str(normalized_order.get("product_category", "")),
        "item": item_name,
        "variant": variant,
        "quantity": quantity,
        "units": safe_str(normalized_order.get("units", "")),
        "auth_id": safe_str(normalized_order.get("auth_id", "")),
        "procedure_code": procedure_code,
        "procedure_modifiers": procedure_modifiers,
        "service_unit_count": service_unit_count,
        "line_item_charge_amount": line_item_charge_amount,
        "provider_control_number": generate_provider_control_number(),
    }


def build_grouped_claim_from_normalized_order(normalized_order: dict) -> dict:
    """
    Create a new grouped claim object using the claim-level fields
    from a normalized order row.
    """
    grouped_claim = build_grouped_claim_template()

    payer_name = resolve_payer_name(normalized_order)
    payer_id = resolve_payer_id(payer_name)
    claim_filing_code = resolve_claim_filing_code(payer_name)
    patient_state = safe_str(normalized_order.get("patient_state", ""))
    place_of_service_code = resolve_place_of_service_code(payer_name, patient_state)

    grouped_claim["claim_key"] = build_claim_group_key(normalized_order)
    grouped_claim["patient_control_number"] = generate_patient_control_number()
    grouped_claim["customer_id"] = safe_str(normalized_order.get("customer_id", ""))
    grouped_claim["patient_full_name"] = safe_str(normalized_order.get("patient_full_name", ""))
    grouped_claim["patient_first_name"] = safe_str(normalized_order.get("patient_first_name", ""))
    grouped_claim["patient_last_name"] = safe_str(normalized_order.get("patient_last_name", ""))
    grouped_claim["patient_dob"] = safe_str(normalized_order.get("patient_dob", ""))
    grouped_claim["patient_gender"] = safe_str(normalized_order.get("patient_gender", ""))
    grouped_claim["patient_phone"] = safe_str(normalized_order.get("patient_phone", ""))
    grouped_claim["patient_address_1"] = safe_str(normalized_order.get("patient_address_1", ""))
    grouped_claim["patient_address_2"] = safe_str(normalized_order.get("patient_address_2", ""))
    grouped_claim["patient_city"] = safe_str(normalized_order.get("patient_city", ""))
    grouped_claim["patient_state"] = safe_str(normalized_order.get("patient_state", ""))
    grouped_claim["patient_postal_code"] = safe_str(normalized_order.get("patient_postal_code", ""))
    grouped_claim["member_id"] = safe_str(normalized_order.get("member_id", ""))
    grouped_claim["primary_insurance_name"] = safe_str(normalized_order.get("primary_insurance_name", ""))
    grouped_claim["payer_name"] = payer_name
    grouped_claim["payer_id"] = payer_id
    grouped_claim["claim_filing_code"] = claim_filing_code
    grouped_claim["place_of_service_code"] = place_of_service_code
    grouped_claim["secondary_member_id"] = safe_str(normalized_order.get("secondary_member_id", ""))
    grouped_claim["subscription_type"] = safe_str(normalized_order.get("subscription_type", ""))
    grouped_claim["diagnosis_code"] = safe_str(normalized_order.get("diagnosis_code", ""))
    grouped_claim["doctor_name"] = safe_str(normalized_order.get("doctor_name", ""))
    grouped_claim["doctor_first_name"] = safe_str(normalized_order.get("doctor_first_name", ""))
    grouped_claim["doctor_last_name"] = safe_str(normalized_order.get("doctor_last_name", ""))
    grouped_claim["doctor_npi"] = safe_str(normalized_order.get("doctor_npi", ""))
    grouped_claim["doctor_address_1"] = safe_str(normalized_order.get("doctor_address_1", ""))
    grouped_claim["doctor_address_2"] = safe_str(normalized_order.get("doctor_address_2", ""))
    grouped_claim["doctor_city"] = safe_str(normalized_order.get("doctor_city", ""))
    grouped_claim["doctor_state"] = safe_str(normalized_order.get("doctor_state", ""))
    grouped_claim["doctor_postal_code"] = safe_str(normalized_order.get("doctor_postal_code", ""))
    grouped_claim["doctor_phone"] = safe_str(normalized_order.get("doctor_phone", ""))
    grouped_claim["order_date"] = safe_str(normalized_order.get("order_date", ""))
    grouped_claim["claim_status"] = safe_str(normalized_order.get("claim_status", ""))
    grouped_claim["order_status"] = safe_str(normalized_order.get("order_status", ""))

    return grouped_claim


def group_normalized_orders_into_claims(normalized_orders: list[dict]) -> list[dict]:
    """
    Group normalized line items into grouped claim objects.
    """
    grouped_claim_map: dict[str, dict] = {}

    for normalized_order in normalized_orders:
        claim_key = build_claim_group_key(normalized_order)

        if claim_key not in grouped_claim_map:
            grouped_claim_map[claim_key] = build_grouped_claim_from_normalized_order(
                normalized_order
            )

        service_line = build_service_line_from_normalized_order(normalized_order)
        grouped_claim_map[claim_key]["service_lines"].append(service_line)

    for claim in grouped_claim_map.values():
        claim["claim_charge_amount"] = sum_claim_charge_amount(claim["service_lines"])

    return list(grouped_claim_map.values())


# ============================================================
# STEDI PROFESSIONAL CLAIM TEMPLATE
# ============================================================

STEDI_PROFESSIONAL_CLAIM_TEMPLATE = {
    "usageIndicator": USAGE_INDICATOR,
    "tradingPartnerServiceId": "",
    "tradingPartnerName": "",
    "submitter": {
        "organizationName": SUBMITTER_ORGANIZATION_NAME,
        "submitterIdentification": SUBMITTER_IDENTIFICATION,
        "contactInformation": {
            "name": SUBMITTER_CONTACT_NAME,
            "phoneNumber": SUBMITTER_PHONE_NUMBER,
        },
    },
    "receiver": {
        "organizationName": "",
    },
    "subscriber": {
        "memberId": "",
        "paymentResponsibilityLevelCode": "P",
        "subscriberGroupName": "",
        "firstName": "",
        "lastName": "",
        "gender": "",
        "dateOfBirth": "",
        "groupNumber": "",
        "address": {
            "address1": "",
            "address2": "",
            "city": "",
            "state": "",
            "postalCode": "",
        },
    },
    "billing": {
        "providerType": "BillingProvider",
        "npi": BILLING_PROVIDER_NPI,
        "employerId": BILLING_PROVIDER_EIN,
        "taxonomyCode": BILLING_PROVIDER_TAXONOMY_CODE,
        "organizationName": BILLING_PROVIDER_ORGANIZATION_NAME,
        "address": {
            "address1": BILLING_PROVIDER_ADDRESS_1,
            "address2": BILLING_PROVIDER_ADDRESS_2,
            "city": BILLING_PROVIDER_CITY,
            "state": BILLING_PROVIDER_STATE,
            "postalCode": BILLING_PROVIDER_POSTAL_CODE,
        },
        "contactInformation": {
            "name": BILLING_PROVIDER_CONTACT_NAME,
            "phoneNumber": BILLING_PROVIDER_CONTACT_PHONE_NUMBER,
        },
    },
    "claimInformation": {
        "claimFilingCode": "",
        "patientControlNumber": "",
        "claimChargeAmount": "",
        "placeOfServiceCode": "",
        "claimFrequencyCode": DEFAULT_CLAIM_FREQUENCY_CODE,
        "signatureIndicator": DEFAULT_SIGNATURE_INDICATOR,
        "planParticipationCode": DEFAULT_PLAN_PARTICIPATION_CODE,
        "benefitsAssignmentCertificationIndicator": DEFAULT_BENEFITS_ASSIGNMENT_CERTIFICATION_INDICATOR,
        "releaseInformationCode": DEFAULT_RELEASE_INFORMATION_CODE,
        "healthCareCodeInformation": [
            {
                "diagnosisTypeCode": "ABK",
                "diagnosisCode": "",
            }
        ],
        "serviceLines": [
            {
                "serviceDate": "",
                "professionalService": {
                    "procedureIdentifier": "HC",
                    "procedureCode": "",
                    "procedureModifiers": [],
                    "lineItemChargeAmount": "",
                    "measurementUnit": "UN",
                    "serviceUnitCount": "",
                    "compositeDiagnosisCodePointers": {
                        "diagnosisCodePointers": ["1"]
                    },
                },
                "providerControlNumber": "",
            }
        ],
    },
}


def build_base_claim_template() -> dict:
    """Return a fresh copy of the base Stedi professional claim template."""
    return deepcopy(STEDI_PROFESSIONAL_CLAIM_TEMPLATE)


# ============================================================
# GROUPED CLAIM -> STEDI JSON
# ============================================================

def build_stedi_claim_json(grouped_claim: dict) -> dict:
    """
    Convert one grouped internal claim into final Stedi JSON.
    """
    validate_grouped_claim(grouped_claim)
    
    claim_json = build_base_claim_template()

    payer_name = safe_str(grouped_claim.get("payer_name", ""))
    payer_id = safe_str(grouped_claim.get("payer_id", ""))
    diagnosis_code = safe_str(grouped_claim.get("diagnosis_code", ""))

    claim_json["tradingPartnerServiceId"] = payer_id
    claim_json["tradingPartnerName"] = payer_name

    claim_json["receiver"]["organizationName"] = payer_name

    claim_json["subscriber"]["memberId"] = safe_str(grouped_claim.get("member_id", ""))
    claim_json["subscriber"]["subscriberGroupName"] = ""
    claim_json["subscriber"]["firstName"] = safe_str(grouped_claim.get("patient_first_name", ""))
    claim_json["subscriber"]["lastName"] = safe_str(grouped_claim.get("patient_last_name", ""))
    claim_json["subscriber"]["gender"] = safe_str(grouped_claim.get("patient_gender", ""))
    claim_json["subscriber"]["dateOfBirth"] = safe_str(grouped_claim.get("patient_dob", ""))
    claim_json["subscriber"]["groupNumber"] = ""
    claim_json["subscriber"]["address"]["address1"] = safe_str(grouped_claim.get("patient_address_1", ""))
    claim_json["subscriber"]["address"]["address2"] = safe_str(grouped_claim.get("patient_address_2", ""))
    claim_json["subscriber"]["address"]["city"] = safe_str(grouped_claim.get("patient_city", ""))
    claim_json["subscriber"]["address"]["state"] = safe_str(grouped_claim.get("patient_state", ""))
    claim_json["subscriber"]["address"]["postalCode"] = safe_str(grouped_claim.get("patient_postal_code", ""))

    claim_json["claimInformation"]["claimFilingCode"] = safe_str(grouped_claim.get("claim_filing_code", ""))
    claim_json["claimInformation"]["patientControlNumber"] = safe_str(grouped_claim.get("patient_control_number", ""))
    claim_json["claimInformation"]["claimChargeAmount"] = safe_str(grouped_claim.get("claim_charge_amount", ""))
    claim_json["claimInformation"]["placeOfServiceCode"] = safe_str(grouped_claim.get("place_of_service_code", ""))
    claim_json["claimInformation"]["healthCareCodeInformation"][0]["diagnosisCode"] = diagnosis_code

    service_lines = []
    for line in grouped_claim.get("service_lines", []):
        service_lines.append({
            "serviceDate": safe_str(line.get("service_date", "")),
            "professionalService": {
                "procedureIdentifier": "HC",
                "procedureCode": safe_str(line.get("procedure_code", "")),
                "procedureModifiers": deepcopy(line.get("procedure_modifiers", [])),
                "lineItemChargeAmount": safe_str(line.get("line_item_charge_amount", "")),
                "measurementUnit": "UN",
                "serviceUnitCount": safe_str(line.get("service_unit_count", "")),
                "compositeDiagnosisCodePointers": {
                    "diagnosisCodePointers": ["1"]
                },
            },
            "providerControlNumber": safe_str(line.get("provider_control_number", "")),
        })

    claim_json["claimInformation"]["serviceLines"] = service_lines

    validate_stedi_claim_json(claim_json)
    
    return claim_json

# ============================================================
# VALIDATION HELPERS
# ============================================================

def validate_grouped_claim(grouped_claim: dict) -> None:
    """
    Validate the internal grouped claim object before building final Stedi JSON.
    Raises ValueError with a clear message if required data is missing.
    """
    claim_key = safe_str(grouped_claim.get("claim_key", ""))

    required_claim_fields = [
        ("payer_name", grouped_claim.get("payer_name", "")),
        ("payer_id", grouped_claim.get("payer_id", "")),
        ("claim_filing_code", grouped_claim.get("claim_filing_code", "")),
        ("member_id", grouped_claim.get("member_id", "")),
        ("diagnosis_code", grouped_claim.get("diagnosis_code", "")),
        ("place_of_service_code", grouped_claim.get("place_of_service_code", "")),
        ("patient_control_number", grouped_claim.get("patient_control_number", "")),
        ("claim_charge_amount", grouped_claim.get("claim_charge_amount", "")),
    ]

    for field_name, field_value in required_claim_fields:
        if not safe_str(field_value):
            raise ValueError(
                f"Grouped claim validation failed for claim_key={claim_key}: "
                f"missing required claim field '{field_name}'."
            )

    service_lines = grouped_claim.get("service_lines", [])
    if not service_lines:
        raise ValueError(
            f"Grouped claim validation failed for claim_key={claim_key}: "
            "claim has no service lines."
        )

    for index, line in enumerate(service_lines, start=1):
        required_line_fields = [
            ("service_date", line.get("service_date", "")),
            ("procedure_code", line.get("procedure_code", "")),
            ("service_unit_count", line.get("service_unit_count", "")),
            ("line_item_charge_amount", line.get("line_item_charge_amount", "")),
            ("provider_control_number", line.get("provider_control_number", "")),
        ]

        for field_name, field_value in required_line_fields:
            if not safe_str(field_value):
                raise ValueError(
                    f"Grouped claim validation failed for claim_key={claim_key}, "
                    f"service line #{index}: missing required line field '{field_name}'."
                )


def validate_stedi_claim_json(claim_json: dict) -> None:
    """
    Validate the final Stedi JSON payload after it is built.
    Raises ValueError with a clear message if required fields are missing.
    """
    required_top_level_fields = [
        ("tradingPartnerServiceId", claim_json.get("tradingPartnerServiceId", "")),
        ("tradingPartnerName", claim_json.get("tradingPartnerName", "")),
    ]

    for field_name, field_value in required_top_level_fields:
        if not safe_str(field_value):
            raise ValueError(
                f"Stedi claim validation failed: missing required top-level field '{field_name}'."
            )

    subscriber = claim_json.get("subscriber", {})
    required_subscriber_fields = [
        ("memberId", subscriber.get("memberId", "")),
        ("firstName", subscriber.get("firstName", "")),
        ("lastName", subscriber.get("lastName", "")),
        ("dateOfBirth", subscriber.get("dateOfBirth", "")),
    ]

    for field_name, field_value in required_subscriber_fields:
        if not safe_str(field_value):
            raise ValueError(
                f"Stedi claim validation failed: missing subscriber field '{field_name}'."
            )

    claim_info = claim_json.get("claimInformation", {})
    required_claim_info_fields = [
        ("claimFilingCode", claim_info.get("claimFilingCode", "")),
        ("patientControlNumber", claim_info.get("patientControlNumber", "")),
        ("claimChargeAmount", claim_info.get("claimChargeAmount", "")),
        ("placeOfServiceCode", claim_info.get("placeOfServiceCode", "")),
    ]

    for field_name, field_value in required_claim_info_fields:
        if not safe_str(field_value):
            raise ValueError(
                f"Stedi claim validation failed: missing claimInformation field '{field_name}'."
            )

    diagnosis_list = claim_info.get("healthCareCodeInformation", [])
    if not diagnosis_list:
        raise ValueError(
            "Stedi claim validation failed: healthCareCodeInformation is missing."
        )

    diagnosis_code = safe_str(diagnosis_list[0].get("diagnosisCode", ""))
    if not diagnosis_code:
        raise ValueError(
            "Stedi claim validation failed: diagnosisCode is missing."
        )

    service_lines = claim_info.get("serviceLines", [])
    if not service_lines:
        raise ValueError(
            "Stedi claim validation failed: serviceLines is missing or empty."
        )

    for index, line in enumerate(service_lines, start=1):
        professional_service = line.get("professionalService", {})

        required_line_fields = [
            ("serviceDate", line.get("serviceDate", "")),
            ("providerControlNumber", line.get("providerControlNumber", "")),
            ("procedureCode", professional_service.get("procedureCode", "")),
            ("serviceUnitCount", professional_service.get("serviceUnitCount", "")),
            ("lineItemChargeAmount", professional_service.get("lineItemChargeAmount", "")),
        ]

        for field_name, field_value in required_line_fields:
            if not safe_str(field_value):
                raise ValueError(
                    f"Stedi claim validation failed for service line #{index}: "
                    f"missing field '{field_name}'."
                )


# ============================================================
# END-TO-END PIPELINE HELPERS
# ============================================================

def load_and_group_claims(csv_path: str | Path) -> list[dict]:
    """
    Read Monday CSV -> normalized orders -> grouped claims.
    """
    normalized_orders = load_monday_export(csv_path)
    return group_normalized_orders_into_claims(normalized_orders)


def load_and_build_stedi_claims(csv_path: str | Path) -> list[dict]:
    """
    Read Monday CSV -> grouped claims -> final Stedi claim JSON payloads.
    """
    grouped_claims = load_and_group_claims(csv_path)
    return [build_stedi_claim_json(claim) for claim in grouped_claims]


# ============================================================
# DEBUG / TEST HELPERS
# ============================================================

def print_normalized_orders(csv_path: str | Path) -> None:
    """
    Development helper:
    Read a CSV export and print the normalized orders.
    """
    normalized_orders = load_monday_export(csv_path)

    print(f"Found {len(normalized_orders)} normalized order(s).")
    print("-" * 80)

    for i, order in enumerate(normalized_orders, start=1):
        print(f"ORDER #{i}")
        for key, value in order.items():
            print(f"{key}: {value}")
        print("-" * 80)


def print_grouped_claims(csv_path: str | Path) -> None:
    """
    Development helper:
    Read a CSV export, build normalized line items, then group them into claims.
    """
    grouped_claims = load_and_group_claims(csv_path)

    print(f"Found {len(grouped_claims)} grouped claim(s).")
    print("=" * 80)

    for i, claim in enumerate(grouped_claims, start=1):
        print(f"CLAIM #{i}")
        print(f"claim_key: {claim['claim_key']}")
        print(f"patient_full_name: {claim['patient_full_name']}")
        print(f"customer_id: {claim['customer_id']}")
        print(f"member_id: {claim['member_id']}")
        print(f"primary_insurance_name: {claim['primary_insurance_name']}")
        print(f"payer_name: {claim['payer_name']}")
        print(f"payer_id: {claim['payer_id']}")
        print(f"claim_filing_code: {claim['claim_filing_code']}")
        print(f"place_of_service_code: {claim['place_of_service_code']}")
        print(f"diagnosis_code: {claim['diagnosis_code']}")
        print(f"order_date: {claim['order_date']}")
        print(f"claim_charge_amount: {claim['claim_charge_amount']}")
        print(f"service_line_count: {len(claim['service_lines'])}")

        print("service_lines:")
        for j, line in enumerate(claim["service_lines"], start=1):
            print(f"  LINE #{j}")
            print(f"    source_child_name: {line['source_child_name']}")
            print(f"    quantity: {line['quantity']}")
            print(f"    auth_id: {line['auth_id']}")
            print(f"    service_date: {line['service_date']}")
            print(f"    item: {line['item']}")
            print(f"    procedure_code: {line['procedure_code']}")
            print(f"    procedure_modifiers: {line['procedure_modifiers']}")
            print(f"    line_item_charge_amount: {line['line_item_charge_amount']}")
            print(f"    service_unit_count: {line['service_unit_count']}")
            print(f"    provider_control_number: {line['provider_control_number']}")


        print("=" * 80)


def print_stedi_claims(csv_path: str | Path) -> None:
    stedi_claims = load_and_build_stedi_claims(csv_path)

    print(f"Found {len(stedi_claims)} Stedi claim payload(s).")

    output_dir = Path("example_payloads")
    output_dir.mkdir(exist_ok=True)

    for i, claim in enumerate(stedi_claims, start=1):

        print("=" * 80)
        print(f"STEDI CLAIM #{i}")
        print(json.dumps(claim, indent=2))

        file_path = output_dir / f"example_claim_{i}.json"

        with open(file_path, "w") as f:
            json.dump(claim, f, indent=2)

        print(f"Saved → {file_path}")


if __name__ == "__main__":
    csv_file = "order_board.csv"

    if Path(csv_file).exists():
        print_stedi_claims(csv_file)
    else:
        print("CSV file not found.")
        print("Update the 'csv_file' variable at the bottom of the script.")