"""
services/claim_board_service.py
=================================
Stage A — PRD Section 5:
  Order Board status = "Process Claim"
  → fetch order item (no subitems)
  → resolve HCPC / modifiers / qty / charge via claim_assumptions
  → create Claims Board parent item
  → create one subitem per billed service line

All column IDs verified against live board exports provided by client.
"""

import os
import re
import json
import logging
from datetime import date
from services.monday_service import run_query

logger = logging.getLogger(__name__)
CLAIMS_BOARD_ID = os.getenv("MONDAY_CLAIMS_BOARD_ID")


# =============================================================================
# ORDER BOARD — column IDs  (verified from client export)
# =============================================================================
ORDER_COL = {
    # Identity
    "order_status":        "status",              # Order Status
    "order_date":          "date_mm1ssf5g",       # Order Date  →  DOS

    # Patient
    "gender":              "color_mm1svmyk",      # Gender  (status)
    "dob":                 "text_mm187t6a",        # DOB  (text)
    "phone":               "phone_mm18rr9v",       # Phone  (phone)
    "patient_address":     "location_mm187v29",    # Patient Address  (location)

    # Insurance
    "primary_insurance":   "color_mm18jhq5",       # Primary Insurance  (status)
    "member_id":           "text_mm18s3fe",         # Member ID  (text)
    "secondary_insurance": "color_mm18h6yn",       # Secondary Insurance  (status)
    "secondary_id":        "text_mm18c6z4",         # Secondary ID  (text)
    "subscription_type":   "color_mm18h05q",       # Subscription Type  (status)

    # Clinical
    "diagnosis_code":      "color_mm189t0b",       # Diagnosis Code  (status)
    "cgm_coverage":        "color_mm18ds28",       # CGM Coverage  (status)

    # Doctor
    "doctor_name":         "text_mm18w2y4",         # Doctor Name  (text)
    "doctor_npi":          "text_mm18x1kj",         # Doctor NPI  (text)
    "doctor_address":      "location_mm18qfed",    # Doctor Address  (location)
    "doctor_phone":        "phone_mm18t5ct",       # Doctor Phone  (phone)

    # Order details
    "auth_id":             "text_mm1snsw3",         # Auth ID  (text)
    "order_type":          "color_mm1s96z2",       # Order Type  (status)
    "order_frequency":     "color_mm1s8tz0",       # Order Frequency  (status)
    "referral":            "color_mm1seak5",       # Referral  (status on Order Board)

    # Product quantities
    "qty_pump":            "numeric_mm1smjyx",     # Qty: Pump
    "qty_infusion_set_1":  "numeric_mm1shc1v",     # Qty: Infusion Set 1
    "qty_infusion_set_2":  "numeric_mm1svn8d",     # Qty: Infusion Set 2
    "qty_cartridge":       "numeric_mm1s9qxd",     # Qty: Cartridge
    "qty_cgm_sensors":     "numeric_mm1s49bj",     # Qty: CGM Sensors
    "qty_cgm_monitor":     "numeric_mm1s431c",     # Qty: CGM Monitor

    # Product variants — needed by claim_assumptions resolvers
    "insulin_pump_brand":  "color_mm1stny0",       # Insulin Pump Brand  (status)
    "insulin_pump_type":   "color_mm1s45wm",       # Insulin Pump Type  (status)
    "cartridge_type":      "color_mm1szdck",       # Cartridge Type  (status)
    "infusion_set_type_1": "color_mm1saxyg",       # Infusion Set Type 1  (status)
    "infusion_set_type_2": "color_mm1sp64",        # Infusion Set Type 2  (status)
    "cgm_type":            "color_mm1sjy4y",       # CGM Type  (status) — sensor divisor
}


# =============================================================================
# CLAIMS BOARD PARENT — column IDs  (verified from client export)
# =============================================================================
CLAIMS_PARENT_COL = {
    # PRD 8.1 — direct copy fields
    "dob":              "text_mkp3y5ax",       # DOB  (text — stored as-is from Order Board)
    "member_id":        "text_mktat89m",        # Member ID  (text)
    "doctor":           "text_mkxrh4a4",        # Doctor  (text)
    "npi":              "text_mkxr2r9b",        # NPI  (text)
    "secondary_id":     "text_mkxwcqfy",        # Secondary ID  (text)
    "auth":             "text_mkwrb2t9",         # Authorization  (text)
    "patient_phone":    "phone_mm1znnww",       # Pt. Phone  (phone)
    "dr_phone":         "phone_mm1zy789",       # Dr. Phone  (phone)
    "address":          "location_mkxxpesw",    # Address  (location)
    "doctor_address":   "location_mkxr251b",    # Doctor Address  (location)
    "dos":              "date_mkwr7spz",         # DOS  (date)
    "referral":         "dropdown_mkwry7z9",    # Referral?  (dropdown on Claims Board)
    "gender":           "color_mm1zy5f2",       # Gender  (status)
    "diagnosis":        "color_mky2gpz5",       # Diagnosis  (status)
    "secondary_payer":  "color_mkxq1a2p",       # Secondary Payer  (status)
    "subscription_type":"color_mky1qvcf",       # Subscription Type  (status)
    "cgm_coverage":     "color_mm1ze7b4",       # CGM Coverage  (status)
    "order_frequency":  "color_mky4mb3y",       # Frequency  (status)

    # PRD 8.2 — python-derived fields
    "primary_payor":    "color_mkxmhypt",       # Primary Payor  (status)
    "pr_payor_id":      "text_mm1gcz3y",         # PR Payor ID  (text)

    # PRD 8.3 — blank at creation; written after submission
    "claim_id":         "text_mm1zpzrs",         # Claim ID
    "claim_sent_date":  "date_mm14rk8d",         # Claim Sent Date

    # Transitional optional fields — PRD FR6: must not error if later deleted
    "pump_qty":         "numeric_mkwz4zkt",     # Pump Qty
    "infusion_1":       "numeric_mkwz337y",     # Infusion 1
    "infusion_2":       "numeric_mkwz9g9f",     # Infusion 2
    "cgm_units":        "numeric_mkwz251j",     # A4239 Units
    "monitor_qty":      "numeric_mkwzr5js",     # Monitor Qty
    "e0784_units":      "numeric_mkwz41cr",     # E0784 Units
    "e2103_units":      "numeric_mkwzb2f4",     # E2103 Units
    "a4224_units":      "numeric_mkwzecd2",     # A4224
    "a4225_units":      "numeric_mkwzxh55",     # A4225
    "a4232_units":      "numeric_mkwzpqqx",     # A4232
    "a4230_units":      "numeric_mkwzhnce",     # A4230
    # Add these to CLAIMS_PARENT_COL (get column IDs from Monday board export):
    # "pump_rate":     "formula_mm18p0vv",   # Pump Rate
    # "infusion_rate": "formula_mm18s9pq",   # Infusion Rate
    # "cartridge_rate":"formula_mm181kvv",   # Cartridge Rate
    # "sensor_rate":   "formula_mm18gtsn",   # Sensor Rate
    # "monitor_rate":  "formula_mm18mss8",   # Monitor Rate
    # "a4239_units":   "numeric_mkwz251j",   # A4239 Units (missing from current CLAIMS_PARENT_COL)
}


# =============================================================================
# CLAIMS BOARD SUBITEMS — column IDs  (verified from client export)
# =============================================================================
CLAIMS_SUBITEM_COL = {
    "hcpc_code":         "color_mm1cdvq8",      # HCPC Code  (status)
    "modifiers":         "dropdown_mm1z7je9",   # Modifiers  (dropdown)
    "primary_insurance": "color_mm1cjcmg",      # Primary Insurance  (status)
    "auth_id":           "text_mm1z8nks",        # Auth ID  (text)
    "order_frequency":   "color_mm1cnfsb",      # Order Frequency  (status)
    "secondary_payer":   "color_mm1zxvky",      # Secondary Payer  (status)
    "secondary_id":      "text_mm1zx3da",        # Secondary ID  (text)
    "order_quantity":    "numeric_mm1czbyg",    # Order Quantity  (numbers)
    "claim_quantity":    "numeric_mm20r76b",    # Claim Quantity  (numbers)
    "charge_amount":     "numeric_mm1za8v5",    # Charge Amount  (numbers)
    "est_pay":           "numeric_mm1zspsy",    # Est. Pay  (numbers)
}


# =============================================================================
# STATUS INDEX MAPS — parent board columns
# Values must match exact label indexes on the Claims Board status columns.
# =============================================================================
STATUS_INDEX_MAP = {
    # Gender (color_mm1zy5f2)
    "color_mm1zy5f2": {"Male": 3, "Female": 4, "M": 3, "F": 4},

    # CGM Coverage (color_mm1ze7b4)
    "color_mm1ze7b4": {"Yes": 1, "No": 0, "Insulin": 2},

    # Frequency (color_mky4mb3y)
    "color_mky4mb3y": {"30-Day": 2, "60-Day": 0, "90-Day": 1},

    # Diagnosis (color_mky2gpz5)
    "color_mky2gpz5": {
        "E10.9": 0,    "E10.65": 1,   "E11.9": 2,    "E08.43": 3,
        "E11.21": 4,   "E11.22": 6,   "E11.40": 8,   "E11.45": 9,
        "E11.59": 11,  "E11.65": 12,  "E11.69": 13,  "E11.8": 14,
        "E13.65": 15,  "E11.42": 16,  "E10.649": 17, "E10.10": 18,
        "E10.29": 19,  "E11.3292": 101,"E10.8": 102, "E10.69": 103,
        "E13.9": 104,  "E10.42": 105, "10.649": 106, "E10.3559": 107,
        "E10.40": 108, "E10.22": 109, "O24.111": 110,
    },

    # Secondary Payer (color_mkxq1a2p)
    # color_mkxq1a2p — Secondary Payer (parent board)
# Currently wrong in STATUS_INDEX_MAP — update to match:
    "color_mkxq1a2p": {
        "Patient":         1,
        "NY Medicaid":     2,
        "Medicare Suppl.": 3,
        "Bad Debt":        4,
        "Horizon BCBS NJ": 7,
        "Cigna":           8,
        "Molina":          9,
    },

    # Subscription Type (color_mky1qvcf)
    "color_mky1qvcf": {
        "Insulin Pump + CGM": 0, "Insulin Pump": 1, "CGM": 2,
        "Supplies Only": 3, "Insulin Pump & CGM": 6,
    },

    # Primary Payor (color_mkxmhypt)
    "color_mkxmhypt": {
        "MetroPlus": 0,    "Anthem BCBS": 1,    "Aetna": 2,
        "Fidelis": 3,      "Wellcare": 4,        "Medicare A & B": 6,
        "Cigna": 7,        "Humana": 8,          "health first": 9,
        "NYSHIP Empire": 10, "Medicaid": 11,     "BCBS Wyoming": 12,
        "UMR": 13,         "MagnaCare": 14,      "Midlands Choice": 15,
        "United Healthcare": 16, "1199": 17,     "BCBS NJ (Horizon)": 18,
        "Horizon BCBS": 19,
        "Anthem BCBS Commercial": 101,  "Anthem BCBS Medicare": 102,
        "Anthem BCBS Medicaid (JLJ)": 103, "Fidelis Commercial": 104,
        "Fidelis Medicaid": 105,  "Medicare A&B": 106,  "NYSHIP": 107,
        "United Commercial": 108, "United Medicare": 109, "Aetna Commercial": 110,
        "Aetna Medicare": 151,    "BCBS TN": 152,   "BCBS FL": 153,
        "Fidelis CHP": 154,       "United Medicaid": 155, "BCBS WY": 156,
        "Oregon Care": 157,       "Stedi": 158,
    },

    # 277 Status (color_mm1z1pb2)
    "color_mm1z1pb2": {
        "Stedi Accepted": 0, "Stedi Rejected": 1,
        "Payer Accepted": 2, "Payer Rejected": 3,
    },
}


# =============================================================================
# STATUS INDEX MAPS — subitem columns
# =============================================================================
SUBITEM_STATUS_INDEX_MAP = {
    # HCPC Code (color_mm1cdvq8)
    "color_mm1cdvq8": {
        "E0784": 0,
        "A4224": 1,
        "A4225": 2,
        "E2103": 3,   # ← CGM Monitor (was 8, now 3)
        "A4239": 4,   # ← CGM Sensors (was 7, now 4)
        "A4232": 6,
        "A4230": 7,   # ← Infusion Set Anthem Commercial (was 4, now 7)
        "A4231": 8,
    },

    # Primary Insurance (color_mm1cjcmg)
    # NOTE: If index 101+ doesn't exist on the subitem board,
    #       SUBITEM_PAYER_FALLBACK collapses it to the base label.
    # "color_mm1cjcmg": {
    #     "MetroPlus": 0,    "Anthem BCBS": 1,    "Aetna": 2,
    #     "Fidelis": 3,      "Wellcare": 4,        "Medicare A & B": 6,
    #     "Cigna": 7,        "Humana": 8,          "health first": 9,
    #     "NYSHIP Empire": 10, "Medicaid": 11,     "BCBS Wyoming": 12,
    #     "UMR": 13,         "MagnaCare": 14,      "Midlands Choice": 15,
    #     "United Healthcare": 16, "1199": 17,     "BCBS NJ (Horizon)": 18,
    #     "Horizon BCBS": 19,
    #     "Anthem BCBS Commercial": 101,  "Anthem BCBS Medicare": 102,
    #     "Anthem BCBS Medicaid (JLJ)": 103, "Fidelis Commercial": 104,
    #     "Fidelis Medicaid": 105, "Medicare A&B": 106, "NYSHIP": 107,
    #     "United Commercial": 108, "United Medicare": 109, "Aetna Commercial": 110,
    #     "Aetna Medicare": 151, "BCBS TN": 152,  "BCBS FL": 153, "Stedi": 158,
    # },
    "color_mm1cjcmg": {
        "Medicare A & B":          0,
        "Cigna Commercial":        1,
        "Fidelis CHP":             2,
        "Anthem BCBS Commercial":  3,
        "Humana":                  4,
        "Aetna Commercial":        6,
        "Stedi Test":              7,
    },

    # Order Frequency (color_mm1cnfsb)
    "color_mm1cnfsb": {"30-Day": 2, "60-Day": 0, "90-Day": 1},

    # color_mm1zxvky — Secondary Payer (subitem board)
    "color_mm1zxvky": {
        "Medicare Suppl.": 0,
        "NY Medicaid":     1,
        "Patient":         2,
        "Bad Debt":        3,
        "Horizon BCBS NJ": 4,
        "Cigna":           6,
        "Molina":          7,
    },
}

# Extended payer label → base label fallback for subitem board
SUBITEM_PAYER_FALLBACK = {
    # Collapse all variants → exact subitem board label
    "Anthem BCBS":                "Anthem BCBS Commercial",
    "Anthem BCBS Medicare":       "Anthem BCBS Commercial",
    "Anthem BCBS Medicaid (JLJ)": "Anthem BCBS Commercial",
    "Fidelis":                    "Fidelis CHP",
    "Fidelis Commercial":         "Fidelis CHP",
    "Fidelis Medicaid":           "Fidelis CHP",
    "Aetna":                      "Aetna Commercial",
    "Aetna Medicare":             "Aetna Commercial",
    "Medicare A&B":               "Medicare A & B",
    "Cigna":                      "Cigna Commercial",
    "NYSHIP":                     "Medicare A & B",   # closest available
    "NYSHIP Empire":              "Medicare A & B",
}

# =============================================================================
# HELPERS
# =============================================================================

def safe_int(val) -> int:
    try:
        return int(float(val or 0))
    except (ValueError, TypeError):
        return 0


def safe_claim_qty(cqty_str, fallback: int) -> int:
    """
    Convert a resolved claim quantity string to int.

    Replaces the brittle `int(cqty) if str(cqty).isdigit() else fallback` pattern.
    Handles all formats returned by claim_assumptions resolvers:
      "1"   -> 1         (whole number string)
      "1.0" -> 1         (float string -- isdigit() would have failed here!)
      "1.5" -> 1         (fractional -> truncate, still a valid claim unit count)
      ""    -> fallback  (resolver returned nothing -> use order qty as fallback)
    """
    try:
        val = str(cqty_str).strip()
        if not val:
            return fallback
        return int(float(val))
    except (ValueError, TypeError):
        return fallback


def normalize_date_iso(value: str) -> str:
    """
    Convert any common date format → YYYY-MM-DD for Monday date columns.
    Handles: MM/DD/YYYY, MM/DD/YY, YYYY-MM-DD, "Mar 15, 2026", etc.
    """
    from datetime import datetime
    if not value:
        return ""
    value = str(value).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", value):
        return value
    if "/" in value:
        parts = value.split("/")
        if len(parts) == 3:
            mm, dd, yyyy = parts
            if len(yyyy) == 2:
                yyyy = "20" + yyyy
            return f"{yyyy}-{mm.zfill(2)}-{dd.zfill(2)}"
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%d %b %Y", "%d %B %Y",
                "%m-%d-%Y", "%m-%d-%y", "%Y/%m/%d"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return ""


def resolve_payer_id(payer_name: str) -> str:
    try:
        from claim_assumptions import resolve_payer_id as _r
        return _r(payer_name) or ""
    except Exception:
        return ""


def _build_location_payload(address_str: str) -> str:
    """
    Build the Monday-compliant JSON for a location column.

    Monday's EXACT required schema (confirmed from API error messages):
    {
      "address":      string | null,
      "lat":          number | null,
      "lng":          number | null,
      "placeId":      string | null,
      "country":      {"long_name": str, "short_name": str},   ← OBJECT not string
      "city":         {"long_name": str, "short_name": str},   ← OBJECT not string
      "street":       {"long_name": str, "short_name": str},   ← OBJECT not string
      "streetNumber": {"long_name": str, "short_name": str},   ← OBJECT not string
    }
    """
    addr = str(address_str).strip()
    if not addr:
        return None

    parts = [p.strip() for p in addr.split(",")]
    street_full = parts[0] if len(parts) > 0 else addr
    city_str    = parts[1] if len(parts) > 1 else ""
    state_zip   = parts[2] if len(parts) > 2 else ""
    country_raw = parts[3] if len(parts) > 3 else "USA"

    sz = re.match(r"([A-Z]{2})\s+(\d{5}(?:-\d{4})?)", state_zip.strip())
    state_str = sz.group(1) if sz else state_zip.strip()

    sm = re.match(r"^(\d+)\s+(.+)$", street_full.strip())
    street_number = sm.group(1) if sm else ""
    street_name   = sm.group(2) if sm else street_full

    country_map = {"USA": "United States", "US": "United States", "": "United States"}
    country_long = country_map.get(country_raw.strip(), country_raw.strip())
    city_long = f"{city_str.strip()}, {state_str}".strip(", ") if state_str else city_str.strip()

    return json.dumps({
        "address":      addr,
        "lat":          None,
        "lng":          None,
        "placeId":      None,
        "country":      {"long_name": country_long,   "short_name": "US"},
        "city":         {"long_name": city_long,      "short_name": city_str.strip()},
        "street":       {"long_name": street_name,    "short_name": street_name},
        "streetNumber": {"long_name": street_number,  "short_name": street_number},
    })


def format_monday_value(col_type: str, value, column_id: str = None,
                        use_subitem_map: bool = False):
    """
    Format a value for the Monday API.
    Returns None to skip the write (blank/zero values, unmapped statuses).
    """
    if value in [None, "", [], 0, 0.0]:
        return None

    if col_type == "text":
        return json.dumps(str(value))

    if col_type == "status":
        mapping = (
            SUBITEM_STATUS_INDEX_MAP.get(column_id, {})
            if use_subitem_map
            else STATUS_INDEX_MAP.get(column_id, {})
        )
        sv = str(value)
        index = mapping.get(sv)

        # Normalize trailing 's': "90-Days" → "90-Day"
        if index is None and sv.endswith("s"):
            index = mapping.get(sv[:-1])

        # Subitem Primary Insurance fallback: collapse extended label to base
        if index is None and use_subitem_map and column_id == "color_mm1cjcmg":
            base = SUBITEM_PAYER_FALLBACK.get(sv)
            if base:
                index = mapping.get(base)
                if index is not None:
                    logger.info(f"[STATUS] Subitem payer fallback: '{sv}' → '{base}' (index={index})")

        if index is None:
            logger.warning(f"[STATUS] No mapping: column={column_id} value='{value}' — skipped")
            return None
        return json.dumps({"index": index})

    if col_type == "dropdown":
        if not value:
            return None
        labels = [v.strip() for v in str(value).split(",") if v.strip()]
        return json.dumps({"labels": labels}) if labels else None

    if col_type == "numbers":
        try:
            n = float(value)
            return str(n) if n != 0 else None
        except (ValueError, TypeError):
            return None

    if col_type == "date":
        v = normalize_date_iso(str(value))
        return json.dumps({"date": v}) if v else None

    if col_type == "phone":
        digits = re.sub(r"\D", "", str(value))
        return json.dumps({"phone": str(value), "countryShortName": "US"}) if digits else None

    if col_type == "location":
        if not value:
            return json.dumps({})
        parts = [p.strip() for p in str(value).split(",")]
        return json.dumps({
            "address": str(value),
            "lat": "0",  # ← must be "0" string, not None/null
            "lng": "0",  # ← must be "0" string, not None/null
            "placeId": "",  # ← must be empty string, not None/null
            "country": {"long_name": "United States", "short_name": "US"},
            "city": {
                "long_name": parts[1].strip() if len(parts) > 1 else "",
                "short_name": parts[1].strip() if len(parts) > 1 else ""
            },
            "street": {
                "long_name": parts[0].strip() if parts else "",
                "short_name": parts[0].strip() if parts else ""
            },
            "streetNumber": {
                "long_name": parts[0].split()[0] if parts else "",
                "short_name": parts[0].split()[0] if parts else ""
            },
        })

    return json.dumps(str(value))


# =============================================================================
# EXTRACT ORDER BOARD COLUMNS
# =============================================================================

def extract_order_cols(order_item: dict) -> dict:
    """Extract all Order Board column values using .text (display value)."""
    raw = {
        c.get("id"): (c.get("text") or "").strip()
        for c in order_item.get("column_values", [])
    }
    result = {logical: raw.get(col_id, "") for logical, col_id in ORDER_COL.items()}
    result["name"]    = order_item.get("name", "")
    result["item_id"] = order_item.get("id", "")
    return result


# =============================================================================
# SERVICE LINE RESOLVER — module-level so payer_name is always in scope
# =============================================================================

def _resolve_line(payer_name: str, cgm_coverage: str, cgm_type: str,
                  item_name: str, hcpc: str, qty: int,
                  order_frequency: str = "") -> tuple:
    """
    Resolve (hcpc, modifiers, claim_qty_str, charge_str) for one service line.

    order_frequency is passed through to resolve_cgm_service_unit_count so that
    CGM Sensors claim quantity matches the Monday Claim Quantity Formula:
      90-Days → 3 units
      60-Days → 2 units
    """
    try:
        from claim_assumptions import (
            resolve_procedure_code,
            resolve_procedure_modifiers,
            resolve_service_unit_count,
            resolve_line_item_charge_amount,
            resolve_cgm_service_unit_count,
        )

        # Resolve HCPC if not pre-set (infusion / cartridge are payer-specific)
        if not hcpc:
            hcpc = resolve_procedure_code(payer_name, item_name) or ""

        # Resolve claim quantity
        if item_name == "CGM Sensors":
            # Use frequency-based logic to match Monday formula (90-Days=3, 60-Days=2)
            claim_qty_str = resolve_cgm_service_unit_count(cgm_type, qty, order_frequency) or str(qty)
        else:
            claim_qty_str = resolve_service_unit_count(
                payer_name, item_name, "", qty, hcpc, order_frequency
            ) or str(qty)

        mods   = resolve_procedure_modifiers(payer_name, hcpc, cgm_coverage)
        charge = resolve_line_item_charge_amount(payer_name, hcpc, claim_qty_str)
        return hcpc, mods, claim_qty_str, charge

    except Exception as e:
        logger.warning(f"[CLAIMS] resolver failed for '{item_name}': {e}")
        return hcpc or "", [], str(qty), ""


# =============================================================================
# BUILD SERVICE LINES
# =============================================================================

def build_service_lines(cols: dict) -> list:
    """
    Build service line objects from Order Board columns.

    PRD rules:
    - Only create a subitem if qty > 0  (PRD 6.3)
    - Exactly ONE infusion set subitem; combine inf1+inf2 qty  (PRD 6.4)
    - HCPC / modifiers / claim_qty / charge resolved via claim_assumptions
    """
    payer_name   = cols.get("primary_insurance", "")
    cgm_coverage = cols.get("cgm_coverage", "")
    cgm_type     = cols.get("cgm_type", "")   # e.g. "Dexcom G7" — used for sensor divisor

    pump_qty        = safe_int(cols.get("qty_pump"))
    infusion_1_qty  = safe_int(cols.get("qty_infusion_set_1"))
    infusion_2_qty  = safe_int(cols.get("qty_infusion_set_2"))
    cartridge_qty   = safe_int(cols.get("qty_cartridge"))
    cgm_sensor_qty  = safe_int(cols.get("qty_cgm_sensors"))
    cgm_monitor_qty = safe_int(cols.get("qty_cgm_monitor"))

    dos = normalize_date_iso(cols.get("order_date", "")) or date.today().isoformat()
    lines = []

    # ── Insulin Pump ──────────────────────────────────────────────────────────
    if pump_qty > 0:
        hcpc, mods, cqty, charge = _resolve_line(
            payer_name, cgm_coverage, cgm_type,
            "Insulin Pump", "E0784", pump_qty
        )
        lines.append({
            "line_name":           "Insulin Pump",
            "product_family":      "pump",
            "resolved_hcpc_code":  hcpc or "E0784",
            "resolved_modifiers":  mods,
            "line_order_quantity": pump_qty,
            "line_claim_quantity": pump_qty,   # pump claim qty is always order qty (=1)
            "line_charge_amount":  charge,
            "line_est_pay":        charge,
            "service_date":        dos,
        })
        logger.info(f"[CLAIMS] Insulin Pump  hcpc={hcpc} order={pump_qty} claim={cqty} charge={charge} mods={mods}")

    # ── Infusion Set — PRD 6.4: ONE subitem, combine inf1 + inf2 ─────────────
    total_infusion = infusion_1_qty + infusion_2_qty
    if total_infusion > 0:
        # Must pass "Infusion Set 1" — that is the exact key in INFUSION_SET_ITEM_NAMES
        hcpc, mods, cqty, charge = _resolve_line(
            payer_name, cgm_coverage, cgm_type,
            "Infusion Set 1", "", total_infusion
        )
        lines.append({
            "line_name":           "Infusion Set",
            "product_family":      "infusion",
            "resolved_hcpc_code":  hcpc or "A4221",
            "resolved_modifiers":  mods,
            "line_order_quantity": total_infusion,
            "line_claim_quantity": safe_claim_qty(cqty, total_infusion),
            "line_charge_amount":  charge,
            "line_est_pay":        charge,
            "service_date":        dos,
        })
        logger.info(f"[CLAIMS] Infusion Set  hcpc={hcpc} order={total_infusion}(inf1={infusion_1_qty}+inf2={infusion_2_qty}) claim={cqty} charge={charge} mods={mods}")

    # ── Cartridge ─────────────────────────────────────────────────────────────
    if cartridge_qty > 0:
        # Must pass "Cartridge" — that is the key in CARTRIDGE_ITEM_NAMES
        hcpc, mods, cqty, charge = _resolve_line(
            payer_name, cgm_coverage, cgm_type,
            "Cartridge", "", cartridge_qty
        )
        lines.append({
            "line_name":           "Cartridge",
            "product_family":      "cartridge",
            "resolved_hcpc_code":  hcpc or "A4224",
            "resolved_modifiers":  mods,
            "line_order_quantity": cartridge_qty,
            "line_claim_quantity": safe_claim_qty(cqty, cartridge_qty),
            "line_charge_amount":  charge,
            "line_est_pay":        charge,
            "service_date":        dos,
        })
        logger.info(f"[CLAIMS] Cartridge  hcpc={hcpc} order={cartridge_qty} claim={cqty} charge={charge} mods={mods}")

    # ── CGM Sensors ───────────────────────────────────────────────────────────
    if cgm_sensor_qty > 0:
        # Pass order_frequency so claim_qty matches the Monday formula (90-Days=3, 60-Days=2)
        hcpc, mods, cqty, charge = _resolve_line(
            payer_name, cgm_coverage, cgm_type,
            "CGM Sensors", "A4239", cgm_sensor_qty,
            order_frequency=cols.get("order_frequency", ""),
        )
        lines.append({
            "line_name":           "CGM Sensors",
            "product_family":      "cgm_sensor",
            "resolved_hcpc_code":  hcpc or "A4239",
            "resolved_modifiers":  mods,
            "line_order_quantity": cgm_sensor_qty,
            "line_claim_quantity": safe_claim_qty(cqty, cgm_sensor_qty),
            "line_charge_amount":  charge,
            "line_est_pay":        charge,
            "service_date":        dos,
        })
        logger.info(f"[CLAIMS] CGM Sensors  hcpc={hcpc} cgm_type={cgm_type} order={cgm_sensor_qty} claim={cqty} charge={charge} mods={mods}")

    # ── CGM Monitor ───────────────────────────────────────────────────────────
    if cgm_monitor_qty > 0:
        hcpc, mods, cqty, charge = _resolve_line(
            payer_name, cgm_coverage, cgm_type,
            "CGM Monitor", "E2103", cgm_monitor_qty
        )
        lines.append({
            "line_name":           "CGM Monitor",
            "product_family":      "cgm_monitor",
            "resolved_hcpc_code":  hcpc or "E2103",
            "resolved_modifiers":  mods,
            "line_order_quantity": cgm_monitor_qty,
            "line_claim_quantity": cgm_monitor_qty,  # monitor claim qty = 1 always
            "line_charge_amount":  charge,
            "line_est_pay":        charge,
            "service_date":        dos,
        })
        logger.info(f"[CLAIMS] CGM Monitor  hcpc={hcpc} order={cgm_monitor_qty} charge={charge} mods={mods}")

    return lines


# =============================================================================
# GROUP LINES BY PAYER  (PRD 6.1)
# Implements payer-split logic per Tanvi_Payer_Split_Logic_Handoff.docx
# =============================================================================

# HCPC codes by product family — used for split routing
_PUMP_HCPCS       = {"E0784"}
_CGM_HCPCS        = {"A4239", "E2103"}
_INFUSION_HCPCS   = {"A4224", "A4230", "A4231"}
_CARTRIDGE_HCPCS  = {"A4225", "A4232"}
_SUPPLY_HCPCS     = _INFUSION_HCPCS | _CARTRIDGE_HCPCS  # go to Medicaid on split

# Anthem JLJ plan names that trigger a split
_ANTHEM_JLJ_MEDICAID_PLANS = {
    "NEW YORK MEDICAID",
    "NY SSI HARP",
    "NY CHIP - STATE BILLING ONLY RISK",
}

# Medicaid ID regex: exactly 2 letters + 5 digits + 1 letter  (e.g. AA12345B)
_MEDICAID_ID_RE = re.compile(r"^[A-Za-z]{2}\d{5}[A-Za-z]$")

# Stedi payer ID for NY Medicaid (MCDNY)
_MEDICAID_PAYER_ID   = "MCDNY"
_MEDICAID_PAYER_NAME = "Medicaid"


def _build_claim_group(cols: dict, lines: list, payer_name: str,
                        payer_id: str, member_id: str, dos: str) -> dict:
    """Build a single claim group dict from cols + a (possibly filtered) line list."""

    def _units_for(hcpc: str):
        for ln in lines:
            if ln.get("resolved_hcpc_code") == hcpc:
                return ln.get("line_claim_quantity")
        return None

    return {
        "source_order_item_id":      cols.get("item_id"),
        "patient_name":              cols.get("name"),
        "dob":                       cols.get("dob", ""),
        "gender":                    cols.get("gender"),
        "patient_phone":             cols.get("phone"),
        "patient_address":           cols.get("patient_address"),
        "member_id":                 member_id,
        "diagnosis_code":            cols.get("diagnosis_code"),
        "doctor_name":               cols.get("doctor_name"),
        "doctor_npi":                cols.get("doctor_npi"),
        "doctor_address":            cols.get("doctor_address"),
        "doctor_phone":              cols.get("doctor_phone"),
        "secondary_payer":           cols.get("secondary_insurance"),
        "secondary_id":              cols.get("secondary_id"),
        "subscription_type":         cols.get("subscription_type"),
        "cgm_coverage":              cols.get("cgm_coverage"),
        "order_frequency":           cols.get("order_frequency"),
        "auth_id":                   cols.get("auth_id"),
        "referral":                  cols.get("referral"),
        "resolved_primary_payor":    payer_name,
        "resolved_primary_payor_id": payer_id,
        "computed_dos":              dos,
        # Qty fields (always from original cols)
        "pump_order_qty":            cols.get("qty_pump"),
        "infusion_1_order_qty":      cols.get("qty_infusion_set_1"),
        "infusion_2_order_qty":      cols.get("qty_infusion_set_2"),
        "sensor_order_qty":          cols.get("qty_cgm_sensors"),
        "monitor_order_qty":         cols.get("qty_cgm_monitor"),
        # Unit fields — derived from this group's service lines
        "e0784_units":               _units_for("E0784"),
        "e2103_units":               _units_for("E2103"),
        "a4224_units":               _units_for("A4224"),
        "a4225_units":               _units_for("A4225"),
        "a4230_units":               _units_for("A4230"),
        "a4232_units":               _units_for("A4232"),
        "service_lines":             lines,
    }


def group_lines_by_payer(cols: dict, lines: list) -> list:
    """
    Route service lines into one or two claim groups based on payer split rules.

    Rule 1 — Fidelis Medicaid:
      Claim A: pump + CGM lines  (primary payer)
      Claim B: infusion + cartridge lines -> Medicaid / MCDNY (if any exist)
      CGM lines fall through to Claim A (should never exist in practice for this payer).

    Rule 2 — Anthem BCBS Medicaid (JLJ):
      Split triggered if plan_name in approved list OR secondary_id matches Medicaid ID regex.
      Claim A: pump + CGM lines  (primary payer)
      Claim B: infusion + cartridge lines -> Medicaid / MCDNY (if any exist)
      No split -> all lines on Anthem JLJ as single claim.

    Default: single claim group on primary insurance.
    """
    payer_name   = cols.get("primary_insurance", "")
    payer_id     = resolve_payer_id(payer_name)
    dos          = normalize_date_iso(cols.get("order_date", "")) or date.today().isoformat()
    secondary_id = cols.get("secondary_id", "")
    plan_name    = cols.get("plan_name", "")  # from subitem; confirm column ID with Brandon

    if not lines:
        return []

    # ── Rule 1: Fidelis Medicaid ──────────────────────────────────────────────
    if payer_name == "Fidelis Medicaid":
        primary_lines  = [ln for ln in lines if ln.get("resolved_hcpc_code") in (_PUMP_HCPCS | _CGM_HCPCS)]
        medicaid_lines = [ln for ln in lines if ln.get("resolved_hcpc_code") in _SUPPLY_HCPCS]
        # CGM lines fall through to Claim A per client instruction.
        # In practice a Fidelis Medicaid order should never have CGM quantities,
        # but if they exist they stay on the primary claim rather than being dropped.

        groups = []
        if primary_lines:
            groups.append(_build_claim_group(
                cols, primary_lines, payer_name, payer_id,
                cols.get("member_id", ""), dos,
            ))
        if medicaid_lines:
            groups.append(_build_claim_group(
                cols, medicaid_lines, _MEDICAID_PAYER_NAME, _MEDICAID_PAYER_ID,
                secondary_id, dos,
            ))

        logger.info(
            f"[CLAIMS] Fidelis Medicaid split: "
            f"Claim A ({len(primary_lines)} pump lines), "
            f"Claim B ({len(medicaid_lines)} supply lines)"
        )
        return groups

    # ── Rule 2: Anthem BCBS Medicaid (JLJ) ───────────────────────────────────
    if payer_name == "Anthem BCBS Medicaid (JLJ)":
        plan_match       = plan_name.strip().upper() in {p.upper() for p in _ANTHEM_JLJ_MEDICAID_PLANS}
        medicaid_id_match = bool(_MEDICAID_ID_RE.match(secondary_id.strip())) if secondary_id else False

        if plan_match or medicaid_id_match:
            primary_lines  = [ln for ln in lines if ln.get("resolved_hcpc_code") in (_PUMP_HCPCS | _CGM_HCPCS)]
            medicaid_lines = [ln for ln in lines if ln.get("resolved_hcpc_code") in _SUPPLY_HCPCS]

            groups = []
            if primary_lines:
                groups.append(_build_claim_group(
                    cols, primary_lines, payer_name, payer_id,
                    cols.get("member_id", ""), dos,
                ))
            if medicaid_lines:
                groups.append(_build_claim_group(
                    cols, medicaid_lines, _MEDICAID_PAYER_NAME, _MEDICAID_PAYER_ID,
                    secondary_id, dos,
                ))

            trigger = "plan_name" if plan_match else "medicaid_id_regex"
            logger.info(
                f"[CLAIMS] Anthem JLJ split triggered by {trigger}: "
                f"Claim A ({len(primary_lines)} primary lines), "
                f"Claim B ({len(medicaid_lines)} supply lines)"
            )
            return groups

        else:
            # No split — all lines stay on Anthem JLJ
            logger.info("[CLAIMS] Anthem JLJ — no split condition met, single claim")
            return [_build_claim_group(
                cols, lines, payer_name, payer_id,
                cols.get("member_id", ""), dos,
            )]

    # ── Default: single claim group on primary payer ──────────────────────────
    return [_build_claim_group(
        cols, lines, payer_name, payer_id,
        cols.get("member_id", ""), dos,
    )]

# =============================================================================
# MAIN ENTRY POINT — Stage A
# =============================================================================

def create_claims_board_items_from_order(order_item: dict) -> list:
    """PRD Section 5 Stage A: Order Board item → Claims Board parent + subitems."""
    cols = extract_order_cols(order_item)
    logger.info(f"[CLAIMS] Processing order: {cols.get('name')} | payer: {cols.get('primary_insurance')}")

    lines = build_service_lines(cols)
    if not lines:
        logger.warning(f"[CLAIMS] No service lines for {cols.get('item_id')} — skipping")
        return []

    claim_groups = group_lines_by_payer(cols, lines)
    logger.info(f"[CLAIMS] {len(claim_groups)} claim group(s) | {len(lines)} service line(s)")

    created_ids = []
    for claim in claim_groups:
        try:
            item_id = _create_parent_item(claim)
            if item_id:
                _create_subitems(item_id, claim)
                created_ids.append(item_id)
                logger.info(f"[CLAIMS] Created Claims Board item {item_id} for {claim['patient_name']}")
        except Exception as e:
            logger.error(f"[CLAIMS] Failed to create claim item: {e}", exc_info=True)

    return created_ids


# =============================================================================
# COLUMN WRITE HELPER
# =============================================================================

def _write_column(item_id: str, board_id: str, col_id: str,
                  formatted: str, mutation: str) -> None:
    """Write one column. Skips gracefully on error (PRD FR6)."""
    if not col_id or not formatted:
        return
    try:
        run_query(mutation, {
            "itemId":   str(item_id),
            "boardId":  str(board_id),
            "columnId": col_id,
            "value":    formatted,
        })
    except Exception as e:
        logger.warning(f"[CLAIMS] Skipped column {col_id}: {e}")


# =============================================================================
# CREATE PARENT ITEM  (PRD Section 8)
# =============================================================================

def _create_parent_item(claim: dict) -> str:
    """Create Claims Board parent item per PRD Section 8."""
    if not CLAIMS_BOARD_ID:
        logger.warning("[CLAIMS] MONDAY_CLAIMS_BOARD_ID not set")
        return ""

    patient_name = claim.get("patient_name", "Unknown")
    payer_name   = claim.get("resolved_primary_payor", "")
    item_name    = f"{patient_name} - {payer_name}" if payer_name else patient_name

    create_mut = """
    mutation CreateItem($boardId: ID!, $itemName: String!) {
      create_item(board_id: $boardId, item_name: $itemName) { id }
    }
    """
    result  = run_query(create_mut, {"boardId": CLAIMS_BOARD_ID, "itemName": item_name})
    item_id = result.get("data", {}).get("create_item", {}).get("id", "")
    if not item_id:
        logger.warning("[CLAIMS] Failed to create parent item — no ID returned")
        return ""
    logger.info(f"[CLAIMS] Created parent item {item_id}: {item_name}")

    update_mut = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(item_id: $itemId, board_id: $boardId,
                          column_id: $columnId, value: $value) { id }
    }
    """

    # PRD 8.1 + 8.2 — all fields to write at creation
    fields = [
        # Text
        (CLAIMS_PARENT_COL["dob"],              "text",     claim.get("dob")),
        (CLAIMS_PARENT_COL["member_id"],        "text",     claim.get("member_id")),
        (CLAIMS_PARENT_COL["doctor"],           "text",     claim.get("doctor_name")),
        (CLAIMS_PARENT_COL["npi"],              "text",     claim.get("doctor_npi")),
        (CLAIMS_PARENT_COL["secondary_id"],     "text",     claim.get("secondary_id")),
        (CLAIMS_PARENT_COL["auth"],             "text",     claim.get("auth_id")),
        (CLAIMS_PARENT_COL["pr_payor_id"],      "text",     claim.get("resolved_primary_payor_id")),
        # Phone
        (CLAIMS_PARENT_COL["patient_phone"],    "phone",    claim.get("patient_phone")),
        (CLAIMS_PARENT_COL["dr_phone"],         "phone",    claim.get("doctor_phone")),
        # Location
        (CLAIMS_PARENT_COL["address"],          "location", claim.get("patient_address")),
        (CLAIMS_PARENT_COL["doctor_address"],   "location", claim.get("doctor_address")),
        # Date
        (CLAIMS_PARENT_COL["dos"],              "date",     claim.get("computed_dos")),
        # Dropdown
        (CLAIMS_PARENT_COL["referral"],         "dropdown", claim.get("referral")),
        # Status
        (CLAIMS_PARENT_COL["gender"],           "status",   claim.get("gender")),
        (CLAIMS_PARENT_COL["diagnosis"],        "status",   claim.get("diagnosis_code")),
        (CLAIMS_PARENT_COL["secondary_payer"],  "status",   claim.get("secondary_payer")),
        (CLAIMS_PARENT_COL["subscription_type"],"status",   claim.get("subscription_type")),
        (CLAIMS_PARENT_COL["cgm_coverage"],     "status",   claim.get("cgm_coverage")),
        (CLAIMS_PARENT_COL["order_frequency"],  "status",   claim.get("order_frequency")),
        (CLAIMS_PARENT_COL["primary_payor"],    "status",   claim.get("resolved_primary_payor")),
    ]
    for col_id, col_type, value in fields:
        _write_column(item_id, CLAIMS_BOARD_ID, col_id,
                      format_monday_value(col_type, value, col_id), update_mut)

    # PRD 8.2 transitional optional qty fields (FR6: must not error if deleted)
    optional = [
        (CLAIMS_PARENT_COL.get("pump_qty"), "numbers", claim.get("pump_order_qty")),
        (CLAIMS_PARENT_COL.get("infusion_1"), "numbers", claim.get("infusion_1_order_qty")),
        (CLAIMS_PARENT_COL.get("infusion_2"), "numbers", claim.get("infusion_2_order_qty")),
        (CLAIMS_PARENT_COL.get("cgm_units"), "numbers", claim.get("sensor_order_qty")),
        (CLAIMS_PARENT_COL.get("monitor_qty"), "numbers", claim.get("monitor_order_qty")),
        # unit fields — these ARE writable numeric columns
        (CLAIMS_PARENT_COL.get("e0784_units"), "numbers", claim.get("e0784_units")),
        (CLAIMS_PARENT_COL.get("e2103_units"), "numbers", claim.get("e2103_units")),
        (CLAIMS_PARENT_COL.get("a4224_units"), "numbers", claim.get("a4224_units")),
        (CLAIMS_PARENT_COL.get("a4225_units"), "numbers", claim.get("a4225_units")),
        (CLAIMS_PARENT_COL.get("a4230_units"), "numbers", claim.get("a4230_units")),
        (CLAIMS_PARENT_COL.get("a4232_units"), "numbers", claim.get("a4232_units")),
        # NOTE: a4239_units and cgm_units both point to numeric_mkwz251j — use only one
    ]
    for col_id, col_type, value in optional:
        if not col_id:
            continue
        _write_column(item_id, CLAIMS_BOARD_ID, col_id,
                      format_monday_value(col_type, value, col_id), update_mut)

    return item_id


# =============================================================================
# CREATE SUBITEMS  (PRD Section 9)
# =============================================================================

def _create_subitems(parent_item_id: str, claim: dict) -> None:
    """Create one subitem per service line (PRD 6.3). PRD 6.4 already enforced."""
    create_mut = """
    mutation CreateSubitem($parentId: ID!, $itemName: String!) {
      create_subitem(parent_item_id: $parentId, item_name: $itemName) {
        id board { id }
      }
    }
    """
    update_mut = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(item_id: $itemId, board_id: $boardId,
                          column_id: $columnId, value: $value) { id }
    }
    """

    payer_name = claim.get("resolved_primary_payor", "")
    auth_id    = claim.get("auth_id", "")
    frequency  = claim.get("order_frequency", "")
    sec_payer  = claim.get("secondary_payer", "")
    sec_id     = claim.get("secondary_id", "")

    for line in claim.get("service_lines", []):
        line_name = line.get("line_name", "Unknown")
        try:
            result = run_query(create_mut, {
                "parentId": str(parent_item_id),
                "itemName": line_name,
            })
            subitem_id       = result.get("data", {}).get("create_subitem", {}).get("id", "")
            subitem_board_id = result.get("data", {}).get("create_subitem", {}).get("board", {}).get("id", "")

            if not subitem_id or not subitem_board_id:
                logger.warning(f"[CLAIMS] Failed to create subitem '{line_name}' — no ID returned")
                continue
            logger.info(f"[CLAIMS] Created subitem {subitem_id}: {line_name}")

            # Modifiers: join list → comma string for dropdown
            mods_str = ", ".join(line.get("resolved_modifiers", []))

            # Charge: store as float for numbers column
            charge_num = None
            try:
                charge_num = float(line.get("line_charge_amount", "") or 0)
            except (ValueError, TypeError):
                pass

            # PRD 9.2 — subitem field mapping
            subitem_fields = [
                (CLAIMS_SUBITEM_COL["hcpc_code"],        "status",   line.get("resolved_hcpc_code", ""), True),
                (CLAIMS_SUBITEM_COL["primary_insurance"], "status",  payer_name,                         True),
                (CLAIMS_SUBITEM_COL["order_frequency"],  "status",   frequency,                          True),
                (CLAIMS_SUBITEM_COL["secondary_payer"],  "status",   sec_payer,                          True),
                (CLAIMS_SUBITEM_COL["modifiers"],        "dropdown", mods_str,                           False),
                (CLAIMS_SUBITEM_COL["auth_id"],          "text",     auth_id,                            False),
                (CLAIMS_SUBITEM_COL["secondary_id"],     "text",     sec_id,                             False),
                (CLAIMS_SUBITEM_COL["order_quantity"],   "numbers",  line.get("line_order_quantity"),    False),
                (CLAIMS_SUBITEM_COL["claim_quantity"],   "numbers",  line.get("line_claim_quantity"),    False),
                (CLAIMS_SUBITEM_COL["charge_amount"],    "numbers",  charge_num,                         False),
                (CLAIMS_SUBITEM_COL["est_pay"],          "numbers",  line.get("line_est_pay"),           False),
            ]

            for col_id, col_type, value, use_sub in subitem_fields:
                if not col_id:
                    continue
                formatted = format_monday_value(col_type, value, col_id, use_subitem_map=use_sub)
                if not formatted:
                    continue
                try:
                    run_query(update_mut, {
                        "itemId":   str(subitem_id),
                        "boardId":  str(subitem_board_id),
                        "columnId": col_id,
                        "value":    formatted,
                    })
                except Exception as e:
                    logger.warning(f"[CLAIMS] Subitem '{line_name}' col {col_id}: {e}")

        except Exception as e:
            logger.warning(f"[CLAIMS] Failed creating subitem '{line_name}': {e}", exc_info=True)


# =============================================================================
# 277 WRITE-BACK  (PRD Section 14)
# =============================================================================

def update_277_on_claims_board(item_id: str, status: str,
                                rejection_reason: str = "") -> None:
    """PRD 14: Update 277 Status and Rejected Reason. PRD 14.4: clear reason on accept."""
    STATUS_277_COL = "color_mm1z1pb2"
    REASON_277_COL = "text_mm1zsp2x"
    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(item_id: $itemId, board_id: $boardId,
                          column_id: $columnId, value: $value) { id }
    }
    """
    fmt = format_monday_value("status", status, STATUS_277_COL)
    if fmt:
        try:
            run_query(mutation, {"itemId": str(item_id), "boardId": str(CLAIMS_BOARD_ID),
                                 "columnId": STATUS_277_COL, "value": fmt})
            logger.info(f"[277] {item_id} → 277 Status = {status}")
        except Exception as e:
            logger.warning(f"[277] Failed to update 277 Status: {e}")

    if rejection_reason and "Rejected" in status:
        try:
            run_query(mutation, {"itemId": str(item_id), "boardId": str(CLAIMS_BOARD_ID),
                                 "columnId": REASON_277_COL, "value": json.dumps(rejection_reason)})
        except Exception as e:
            logger.warning(f"[277] Failed to update Rejected Reason: {e}")
    elif "Accepted" in status:
        try:
            run_query(mutation, {"itemId": str(item_id), "boardId": str(CLAIMS_BOARD_ID),
                                 "columnId": REASON_277_COL, "value": json.dumps("")})
        except Exception:
            pass


# =============================================================================
# WRITE CLAIM ID AFTER SUBMISSION  (PRD 13 / FR11 / FR12)
# =============================================================================

def write_claim_id_to_claims_board(item_id: str, claim_id: str) -> None:
    """PRD 13: Write Claim ID + Claim Sent Date. FR12: never touch Primary Status."""
    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(item_id: $itemId, board_id: $boardId,
                          column_id: $columnId, value: $value) { id }
    }
    """
    today = date.today().isoformat()
    writes = [
        (CLAIMS_PARENT_COL.get("claim_id"),       json.dumps(claim_id),           "Claim ID"),
        (CLAIMS_PARENT_COL.get("claim_sent_date"), json.dumps({"date": today}),   "Claim Sent Date"),
    ]
    for col_id, value, label in writes:
        if not col_id or not value:
            continue
        try:
            run_query(mutation, {"itemId": str(item_id), "boardId": str(CLAIMS_BOARD_ID),
                                 "columnId": col_id, "value": value})
            logger.info(f"[SUBMIT] Wrote {label} to {item_id}")
        except Exception as e:
            logger.warning(f"[SUBMIT] Failed to write {label}: {e}")