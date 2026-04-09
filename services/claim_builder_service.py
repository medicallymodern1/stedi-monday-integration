"""
services/claim_builder_service.py
Converts Monday API order data into Stedi claim JSON payloads.
"""

import logging
import sys
import os
from copy import deepcopy
from services.stedi_service import lookup_payer_name, lookup_payer_name_by_internal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from claim_infrastructure import (
    build_normalized_order_template,
    group_normalized_orders_into_claims,
    build_stedi_claim_json,
    parse_address,
    normalize_date,
    normalize_gender,
    split_full_name,
    safe_str,
)

logger = logging.getLogger(__name__)

# ── Parent item column IDs ──────────────────────────────────────────────────────
COLUMN_MAP = {
    "status":               "claim_status",
    "text_mm18zjmz":        "gender",
    "text_mm187t6a":        "dob",
    "phone_mm18rr9v":       "phone",
    "location_mm187v29":    "patient_address",
    "color_mm189t0b":       "diagnosis_code",
    "color_mm18ds28":       "cgm_coverage",
    "text_mm18w2y4":        "doctor_name",
    "text_mm18x1kj":        "doctor_npi",
    "location_mm18qfed":    "doctor_address",
    "phone_mm18t5ct":       "doctor_phone",
    "color_mm18jhq5":       "primary_insurance",
    "text_mm18s3fe":        "member_id",
    "color_mm18h6yn":       "pr_payor",
    "text_mm18c6z4":        "secondary_id",
    "color_mm18h05q":       "subscription_type",
    "color_mm1bx9az":       "status_277",
    "text_mm1b56xa":        "rejected_reason_277",
}

# ── Subitem column IDs ──────────────────────────────────────────────────────────
SUBITEM_COLUMN_MAP = {
    "status":               "order_status",
    "date0":                "order_date",
    "color_mm18p9f4":       "primary_insurance",
    "text_mm18k1x8":        "plan_name",
    "text_mm18zcs4":        "member_id",
    "color_mm18fzt5":       "secondary_payor",
    "text_mm18qg5j":        "secondary_id",
    "numeric_mm18mwna":     "coinsurance_pct",
    "numeric_mm18mdhg":     "deductible",
    "numeric_mm18bvg0":     "deductible_remaining",
    "numeric_mm1879ha":     "oop_max",
    "numeric_mm18c79g":     "oop_max_remaining",
    "numeric_mm18t2q9":     "quantity",
    "color_mm185yjy":       "cgm_type",
    "color_mm18e5yq":       "pump_type",
    "color_mm18pj26":       "infusion_set",
    "text_mm18dsxx":        "auth_id",
}


def extract_columns(column_values: list) -> dict:
    """Convert Monday column_values list into a simple dict"""
    result = {}
    for col in column_values:
        col_id = col.get("id", "")
        field_name = COLUMN_MAP.get(col_id, col_id)
        result[field_name] = col.get("text", "") or ""
    return result


def extract_subitem_columns(column_values: list) -> dict:
    """Convert subitem column_values into a simple dict"""
    result = {}
    for col in column_values:
        col_id = col.get("id", "")
        field_name = SUBITEM_COLUMN_MAP.get(col_id, col_id)
        result[field_name] = col.get("text", "") or ""
    return result


def monday_item_to_normalized_orders(monday_item: dict) -> list[dict]:
    """
    Convert Monday order item into normalized order dicts
    for Brandon's pipeline. One order per subitem.
    """
    parent_cols = extract_columns(monday_item.get("column_values", []))

    patient_full_name = monday_item.get("name", "")
    patient_first, patient_last = split_full_name(patient_full_name)

    patient_addr = parse_address(parent_cols.get("patient_address", ""))
    doctor_full_name = parent_cols.get("doctor_name", "")
    doctor_first, doctor_last = split_full_name(doctor_full_name)
    doctor_addr = parse_address(parent_cols.get("doctor_address", ""))

    subitems = monday_item.get("subitems", [])
    if not subitems:
        logger.warning(f"No subitems for item {monday_item.get('id')} — cannot build claim")
        return []

    normalized_orders = []

    for subitem in subitems:
        sub_cols = extract_subitem_columns(subitem.get("column_values", []))

        order = build_normalized_order_template()

        # ── Patient ────────────────────────────────────────────
        order["source_parent_name"]     = patient_full_name
        order["source_child_name"]      = subitem.get("name", "")
        order["patient_full_name"]      = patient_full_name
        order["patient_first_name"]     = patient_first
        order["patient_last_name"]      = patient_last
        order["patient_dob"]            = normalize_date(parent_cols.get("dob", ""))
        order["patient_gender"]         = normalize_gender(parent_cols.get("gender", ""))
        order["patient_phone"]          = parent_cols.get("phone", "")
        order["patient_address_1"]      = patient_addr.get("address1", "")
        order["patient_address_2"]      = patient_addr.get("address2", "")
        order["patient_city"]           = patient_addr.get("city", "")
        order["patient_state"]          = patient_addr.get("state", "")
        order["patient_postal_code"]    = patient_addr.get("postal_code", "")

        # ── Insurance ──────────────────────────────────────────
        # Use subitem values first, fall back to parent

        # order["primary_insurance_name"] = (
        #     sub_cols.get("primary_insurance") or
        #     parent_cols.get("primary_insurance", "")
        # )

        subitem_payer_id = sub_cols.get("payer_id", "")
        subitem_insurance = (
                sub_cols.get("primary_insurance") or
                parent_cols.get("primary_insurance", "")
        )

        official_payer_name = ""
        if subitem_payer_id:
            official_payer_name = lookup_payer_name(subitem_payer_id)

        order["primary_insurance_name"] = (
                official_payer_name or
                subitem_insurance or
                ""
        )

        order["member_id"] = (
                sub_cols.get("member_id") or
                parent_cols.get("member_id", "")
        )

        order["secondary_member_id"]    = (
            sub_cols.get("secondary_id") or
            parent_cols.get("secondary_id", "")
        )
        order["subscription_type"]      = parent_cols.get("subscription_type", "")
        order["diagnosis_code"]         = parent_cols.get("diagnosis_code", "")
        order["cgm_coverage"]           = parent_cols.get("cgm_coverage", "")

        order["group_number"] = ""
        order["subscriber_group_name"] = ""

        # ── Doctor ─────────────────────────────────────────────
        order["doctor_name"]            = doctor_full_name
        order["doctor_first_name"]      = doctor_first
        order["doctor_last_name"]       = doctor_last
        order["doctor_npi"]             = parent_cols.get("doctor_npi", "")
        order["doctor_address_1"]       = doctor_addr.get("address1", "")
        order["doctor_address_2"]       = doctor_addr.get("address2", "")
        order["doctor_city"]            = doctor_addr.get("city", "")
        order["doctor_state"]           = doctor_addr.get("state", "")
        order["doctor_postal_code"]     = doctor_addr.get("postal_code", "")
        order["doctor_phone"]           = parent_cols.get("doctor_phone", "")

        # ── Service Line ───────────────────────────────────────
        order["order_status"]           = sub_cols.get("order_status", "")
        order["order_date"]             = normalize_date(sub_cols.get("order_date", ""))
        order["service_date"]           = normalize_date(sub_cols.get("order_date", ""))
        order["quantity"]               = sub_cols.get("quantity", "")
        order["auth_id"]                = sub_cols.get("auth_id", "")
        order["item"]                   = subitem.get("name", "")

        # ── Product variant ────────────────────────────────────
        cgm_type     = sub_cols.get("cgm_type", "")
        pump_type    = sub_cols.get("pump_type", "")
        infusion_set = sub_cols.get("infusion_set", "")
        order["variant"] = cgm_type or pump_type or infusion_set or ""

        logger.info(
            f"Normalized: {patient_full_name} | "
            f"subitem={subitem.get('name')} | "
            f"insurance={order['primary_insurance_name']} | "
            f"member_id={order['member_id']} | "
            f"service_date={order['service_date']} | "
            f"quantity={order['quantity']}"
        )

        normalized_orders.append(order)

    return normalized_orders

def inject_ordering_provider(payload: dict, claim: dict) -> dict:
    """
    Add orderingProvider to every service line.

    For DME, Stedi/X12 837P guidance says to report the ordering provider
    at the line level (Loop 2420E), not as a referring provider (DN).
    """
    doctor_npi   = claim.get("doctor_npi", "").strip()
    doctor_first = claim.get("doctor_first_name", "").strip()
    doctor_last  = claim.get("doctor_last_name", "").strip()

    if not doctor_npi:
        logger.warning("No doctor NPI found — orderingProvider skipped for this claim")
        return payload

    ordering = {"npi": doctor_npi}
    if doctor_first:
        ordering["firstName"] = doctor_first
    if doctor_last:
        ordering["lastName"] = doctor_last

    claim_info = payload.get("claimInformation", {})
    for line in claim_info.get("serviceLines", []):
        line["orderingProvider"] = ordering

        # Defensive cleanup in case referringProvider was already added earlier
        if "referringProvider" in line:
            del line["referringProvider"]

    logger.info(
        f"orderingProvider injected | "
        f"npi={doctor_npi} name={doctor_first} {doctor_last} | "
        f"lines={len(claim_info.get('serviceLines', []))}"
    )
    return payload


def build_claims_from_monday_item(monday_item: dict) -> list[dict]:
    """Main entry point. Monday item → Stedi claim JSON payloads."""
    item_id = monday_item.get("id")
    patient_name = monday_item.get("name")

    logger.info(f"Building claims for: {patient_name} (id={item_id})")

    normalized_orders = monday_item_to_normalized_orders(monday_item)
    if not normalized_orders:
        logger.warning(f"No normalized orders for item {item_id}")
        return []

    logger.info(f"Normalized {len(normalized_orders)} service lines")

    grouped_claims = group_normalized_orders_into_claims(normalized_orders)
    logger.info(f"Grouped into {len(grouped_claims)} claim(s)")

    stedi_payloads = []
    for claim in grouped_claims:
        try:
            payload = build_stedi_claim_json(claim)

            # Remove groupNumber and subscriberGroupName
            subscriber = payload.get("subscriber", {})
            subscriber.pop("groupNumber", None)
            subscriber.pop("subscriberGroupName", None)

            # Format all charge amounts to 2 decimal places
            payload = format_charge_amounts(payload)

            # Inject ordering doctor as referringProvider on every service line
            payload = inject_ordering_provider(payload, claim)

            # Replace tradingPartnerName with official Stedi name
            # using the hardcoded mapping from claim_assumptions.py
            payer_id = payload.get("tradingPartnerServiceId", "")
            official_name = get_official_payer_name(payer_id)
            if official_name:
                payload["tradingPartnerName"] = official_name
                payload["receiver"] = {"organizationName": official_name}
                logger.info(f"tradingPartnerName: payer_id={payer_id} → '{official_name}'")

            stedi_payloads.append(payload)
            logger.info(f"Built payload: {claim.get('claim_key')}")

        except Exception as e:
            logger.error(f"Failed to build Stedi JSON: {e}", exc_info=True)

    logger.info(f"Total payloads: {len(stedi_payloads)}")
    return stedi_payloads


def get_official_payer_name(payer_id: str) -> str:
    """
    Get official Stedi tradingPartnerName from payer ID.
    Uses hardcoded mapping from claim_assumptions.py.
    """
    try:
        from claim_assumptions import STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID
        name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "")
        if not name:
            logger.warning(f"No official name for payer_id={payer_id} — using internal name")
        return name
    except Exception as e:
        logger.warning(f"Could not load STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID: {e}")
        return ""



def format_charge_amounts(payload: dict) -> dict:
    """Ensure all charge amounts are formatted with 2 decimal places."""
    claim_info = payload.get("claimInformation", {})
    if "claimChargeAmount" in claim_info:
        try:
            claim_info["claimChargeAmount"] = f"{float(claim_info['claimChargeAmount']):.2f}"
        except (ValueError, TypeError):
            pass
    for line in claim_info.get("serviceLines", []):
        svc = line.get("professionalService", {})
        if "lineItemChargeAmount" in svc:
            try:
                svc["lineItemChargeAmount"] = f"{float(svc['lineItemChargeAmount']):.2f}"
            except (ValueError, TypeError):
                pass
    return payload

# inject_referring_provider defined above
