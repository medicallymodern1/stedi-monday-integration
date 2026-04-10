DRY_RUN = True

"""
services/claims_submission_service.py
======================================
Stage B (PRD Section 5): Reads Claims Board parent + subitems →
builds Stedi payload → submits to Stedi → writes back Claim ID.

Source of truth is the Claims Board, not the Order Board (PRD 6.5).
"""

import os
import json
import logging
import uuid
from datetime import date
from services.monday_service import run_query
from services.stedi_service import submit_claim

logger = logging.getLogger(__name__)
CLAIMS_BOARD_ID = os.getenv("MONDAY_CLAIMS_BOARD_ID")

# ── Claims Board column IDs (same as claim_board_service) ────────────────────
CLAIMS_PARENT_COL = {
    "name": None,  # item.name
    "dob": "text_mkp3y5ax",
    "gender": "color_mm1zy5f2",
    "patient_phone": "phone_mm1znnww",
    "address": "location_mkxxpesw",
    "member_id": "text_mktat89m",
    "diagnosis": "color_mky2gpz5",
    "doctor": "text_mkxrh4a4",
    "npi": "text_mkxr2r9b",
    "doctor_address": "location_mkxr251b",
    "dr_phone": "phone_mm1zy789",
    "secondary_payer": "color_mkxq1a2p",
    "secondary_id": "text_mkxwcqfy",
    "primary_payor": "color_mkxmhypt",
    "pr_payor_id": "text_mm1gcz3y",
    "dos": "date_mkwr7spz",
    "auth": "text_mkwrb2t9",
    "claim_id": "text_mm1zpzrs",
    "claim_sent_date": "date_mm14rk8d",
    "pcn": "text_mkwzbcme",   # Patient Control Number — used by 277 webhook to find this item
    "status_277": "color_mm1z1pb2",
    "reason_277": "text_mm1zsp2x",
    # Test field — read to determine usageIndicator (PRD 12.1)
    # Update column ID once confirmed on Claims Board
    "is_test": "color_mm1z59nj",  # Stedi Test (status) — confirmed from board export
}

CLAIMS_SUBITEM_COL = {
    "hcpc_code": "color_mm1cdvq8",
    "modifiers": "dropdown_mm1z7je9",
    "claim_quantity": "numeric_mm20r76b",   # plain Numbers column — written by Python at claim creation
    "charge_amount": "numeric_mm1za8v5",    # plain Numbers column — used as lineItemChargeAmount in Stedi payload
    "est_pay": "numeric_mm1zspsy",          # plain Numbers column — same value as charge_amount (contracted rate)
    "auth_id": "text_mm1z8nks",             # Prior authorization / auth ID per service line
}


# ── Fetch ─────────────────────────────────────────────────────────────────────

def get_claims_item_with_subitems(item_id: str) -> dict:
    query = """
    query GetItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id name
        column_values { id text value }
        subitems {
          id name
          column_values { id text value }
        }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"No Claims Board item found for id={item_id}")
    return items[0]


# ── Extract ───────────────────────────────────────────────────────────────────

def _col_text(column_values: list, col_id: str) -> str:
    import json as _json
    for c in column_values:
        if c.get("id") == col_id:
            text = (c.get("text") or "").strip()
            if text:
                return text
            raw_value = c.get("value") or ""
            if raw_value:
                try:
                    parsed = _json.loads(raw_value)
                    if isinstance(parsed, dict):
                        addr = parsed.get("address", "")
                        if addr:
                            return str(addr).strip()
                except (ValueError, TypeError):
                    pass
            return ""
    return ""


def extract_parent_fields(item: dict) -> dict:
    cvs = item.get("column_values", [])

    def t(col_id):
        return _col_text(cvs, col_id) if col_id else ""

    doctor_full = t(CLAIMS_PARENT_COL["doctor"])
    doctor_parts = doctor_full.strip().split(None, 1)  # split on first whitespace
    doctor_first = doctor_parts[0] if len(doctor_parts) >= 1 else ""
    doctor_last = doctor_parts[1] if len(doctor_parts) >= 2 else ""

    return {
        "name": item.get("name", ""),
        "item_id": item.get("id", ""),
        "dob": t(CLAIMS_PARENT_COL["dob"]),
        "gender": t(CLAIMS_PARENT_COL["gender"]),
        "address": t(CLAIMS_PARENT_COL["address"]),
        "member_id": t(CLAIMS_PARENT_COL["member_id"]),
        "diagnosis": t(CLAIMS_PARENT_COL["diagnosis"]),
        "doctor": doctor_full,
        "doctor_first": doctor_first,
        "doctor_last": doctor_last,
        "npi": t(CLAIMS_PARENT_COL["npi"]),
        "primary_payor": t(CLAIMS_PARENT_COL["primary_payor"]),
        "pr_payor_id": t(CLAIMS_PARENT_COL["pr_payor_id"]),
        "dos": t(CLAIMS_PARENT_COL["dos"]),
        "auth": t(CLAIMS_PARENT_COL["auth"]),
        # PRD 12.1: read test field to determine usageIndicator
        "is_test": t(CLAIMS_PARENT_COL["is_test"]).strip().lower() == "test",
    }


def extract_subitem_fields(subitem: dict) -> dict:
    cvs = subitem.get("column_values", [])

    def t(col_id):
        return _col_text(cvs, col_id) if col_id else ""

    return {
        "name": subitem.get("name", ""),
        "hcpc_code": t(CLAIMS_SUBITEM_COL["hcpc_code"]),
        "modifiers": t(CLAIMS_SUBITEM_COL["modifiers"]),
        "claim_quantity": t(CLAIMS_SUBITEM_COL["claim_quantity"]),
        "charge_amount": t(CLAIMS_SUBITEM_COL["charge_amount"]),
        "est_pay": t(CLAIMS_SUBITEM_COL["est_pay"]),
        "auth_id": t(CLAIMS_SUBITEM_COL["auth_id"]),
    }


# ── Payload builder ───────────────────────────────────────────────────────────

def build_payload_from_claims_board(parent: dict, subitems: list) -> tuple:
    """
    Build Stedi submission payload from Claims Board data (PRD Section 11).
    Returns (payload_dict, patient_control_number).

    PRD 12.2: Primary Payor = "Stedi" → route to STEDITEST regardless of test field.
    PRD 12.1: test field = "test" → usageIndicator = "T".
    """
    from claim_infrastructure import (
        parse_address, normalize_date, normalize_gender, split_full_name
    )
    from claim_assumptions import (
        BILLING_PROVIDER_NPI, BILLING_PROVIDER_EIN, BILLING_PROVIDER_TAXONOMY_CODE,
        BILLING_PROVIDER_ORGANIZATION_NAME, BILLING_PROVIDER_ADDRESS_1,
        BILLING_PROVIDER_CITY, BILLING_PROVIDER_STATE, BILLING_PROVIDER_POSTAL_CODE,
        BILLING_PROVIDER_CONTACT_NAME, BILLING_PROVIDER_CONTACT_PHONE_NUMBER,
        SUBMITTER_ORGANIZATION_NAME, SUBMITTER_IDENTIFICATION,
        SUBMITTER_CONTACT_NAME, SUBMITTER_PHONE_NUMBER,
        resolve_claim_filing_code, generate_patient_control_number,
        generate_provider_control_number,
    )

    payer_label = parent.get("primary_payor", "")
    payer_id = parent.get("pr_payor_id", "")
    is_test = parent.get("is_test", False)

    # PRD 12.3: independently resolve usageIndicator and trading partner
    usage_indicator = "T" if is_test else "P"

    # PRD 12.2: Stedi fake payer routing
    if payer_label == "Stedi":
        trading_partner_id = "STEDITEST"
        trading_partner_name = "Stedi Test Payer"
        receiver_name = "Stedi"
    else:
        trading_partner_id = payer_id or payer_label
        from claim_assumptions import STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID
        trading_partner_name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "") or payer_label
        receiver_name = trading_partner_name

    claim_filing_code = resolve_claim_filing_code(payer_label)

    # Patient name
    raw_name = parent.get("name", "")
    patient_name = raw_name.split(" - ")[0].strip() if " - " in raw_name else raw_name
    first, last = split_full_name(patient_name)

    # Patient address
    patient_addr = parse_address(parent.get("address", ""))

    # DOS → YYYYMMDD
    dos_raw = parent.get("dos", "")  # YYYY-MM-DD from Monday date column
    dos_stedi = normalize_date(dos_raw)

    # Validate DOS is not in the future — Stedi rejects future service dates (error code 33)
    if dos_stedi:
        from datetime import date as _date
        try:
            dos_date = _date.fromisoformat(dos_raw)
            if dos_date > _date.today():
                raise ValueError(
                    f"DOS date {dos_raw} is in the future. "
                    f"Service date cannot be later than today ({_date.today().isoformat()}). "
                    "Please correct the DOS on the Claims Board and resubmit."
                )
        except ValueError as ve:
            if "DOS date" in str(ve):
                raise
            # date parse failed — let Stedi catch it

    # Diagnosis — strip dots for Stedi
    diagnosis = (parent.get("diagnosis", "") or "").replace(".", "")

    # Build service lines from subitems
    service_lines = []
    total_charge = 0.0

    for sub in subitems:
        hcpc_code = sub.get("hcpc_code", "").strip()
        if not hcpc_code:
            logger.warning(f"[SUBMIT] Subitem '{sub.get('name')}' missing HCPC code — skipped")
            continue

        modifiers_raw = sub.get("modifiers", "") or ""
        modifiers = [m.strip() for m in modifiers_raw.split(",") if m.strip()]
        claim_qty = sub.get("claim_quantity", "1") or "1"
        charge_raw = sub.get("charge_amount", "0") or "0"

        try:
            charge = float(charge_raw)
            total_charge += charge
        except ValueError:
            charge = 0.0

        svc_line = {
            "serviceDate": dos_stedi,
            "professionalService": {
                "procedureIdentifier": "HC",
                "procedureCode": hcpc_code,
                "lineItemChargeAmount": f"{charge:.2f}",
                "measurementUnit": "UN",
                "serviceUnitCount": str(int(float(claim_qty))) if claim_qty else "1",
                "compositeDiagnosisCodePointers": {
                    "diagnosisCodePointers": ["1"]
                },
            },
            "orderingProvider": {
                "npi": parent.get("npi", ""),
                "firstName": parent.get("doctor_first", ""),
                "lastName": parent.get("doctor_last", ""),
            },
            "providerControlNumber": generate_provider_control_number(),
        }

        line_auth_id = (sub.get("auth_id", "") or "").strip()
        if line_auth_id:
            svc_line["additionalNotes"] = line_auth_id

        if modifiers:
            svc_line["professionalService"]["procedureModifiers"] = modifiers[:4]

        service_lines.append(svc_line)

        est_pay_raw = sub.get("est_pay", "") or ""
        try:
            est_pay_log = f"${float(est_pay_raw):.2f}" if est_pay_raw else "not set"
        except ValueError:
            est_pay_log = "not set"

        logger.info(
            f"[SUBMIT] Service line: {hcpc_code} qty={claim_qty} charge={charge:.2f} "
            f"est_pay={est_pay_log} mods={modifiers}"
        )

    if not service_lines:
        raise ValueError("No valid HCPC-coded service lines found on Claims Board subitems")

    pcn = generate_patient_control_number()

    payload = {
        "tradingPartnerName": trading_partner_name,
        "tradingPartnerServiceId": trading_partner_id,
        "usageIndicator": usage_indicator,
        "receiver": {
            "organizationName": receiver_name,
        },
        "submitter": {
            "organizationName": SUBMITTER_ORGANIZATION_NAME,
            "submitterIdentification": SUBMITTER_IDENTIFICATION,
            "contactInformation": {
                "name": SUBMITTER_CONTACT_NAME,
                "phoneNumber": SUBMITTER_PHONE_NUMBER,
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
                "city": BILLING_PROVIDER_CITY,
                "state": BILLING_PROVIDER_STATE,
                "postalCode": BILLING_PROVIDER_POSTAL_CODE,
            },
            "contactInformation": {
                "name": BILLING_PROVIDER_CONTACT_NAME,
                "phoneNumber": BILLING_PROVIDER_CONTACT_PHONE_NUMBER,
            },
        },
        "subscriber": {
            "memberId": parent.get("member_id", ""),
            "paymentResponsibilityLevelCode": "P",
            "firstName": first,
            "lastName": last,
            "gender": normalize_gender(parent.get("gender", "")),
            "dateOfBirth": normalize_date(parent.get("dob", "")),
            "address": {
                "address1": patient_addr.get("address1", ""),
                "city": patient_addr.get("city", ""),
                "state": patient_addr.get("state", ""),
                "postalCode": patient_addr.get("postal_code", ""),
            },
        },
        "claimInformation": {
            "claimFilingCode": claim_filing_code,
            "patientControlNumber": pcn,
            "claimChargeAmount": f"{total_charge:.2f}",
            "placeOfServiceCode": "12",
            "claimFrequencyCode": "1",
            "signatureIndicator": "Y",
            "planParticipationCode": "A",
            "benefitsAssignmentCertificationIndicator": "Y",
            "releaseInformationCode": "Y",
            "healthCareCodeInformation": [
                {
                    "diagnosisTypeCode": "ABK",
                    "diagnosisCode": diagnosis,
                }
            ],
            "serviceLines": service_lines,
        },
    }

    logger.info(
        f"[SUBMIT] Payload built: payer={trading_partner_name} "
        f"usageIndicator={usage_indicator} pcn={pcn} "
        f"lines={len(service_lines)} total=${total_charge:.2f}"
    )
    return payload, pcn


# ── Write back after submission ───────────────────────────────────────────────

def _write_column_safe(item_id, col_id, value, label):
    if not col_id or value is None:
        return

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(item_id: $itemId, board_id: $boardId, column_id: $columnId, value: $value) { id }
    }
    """
    try:
        run_query(
            mutation,
            {
                "itemId": str(item_id),
                "boardId": str(CLAIMS_BOARD_ID),
                "columnId": col_id,
                "value": value,
            },
        )
        logger.info(f"[SUBMIT] Wrote {label} to item {item_id}")
    except Exception as e:
        logger.warning(f"[SUBMIT] Failed to write {label}: {e}")


def _write_submission_outputs(item_id: str, claim_id: str, pcn: str = "") -> None:
    """
    Write submission outputs only:
    - Claim ID
    - PCN
    - Claim Sent Date

    IMPORTANT:
    Do NOT write 277 Status or 277 Rejected Reason here.
    Those fields must remain blank until the real 277 workflow updates them.
    """
    from datetime import date as _date

    today = _date.today().isoformat()

    _write_column_safe(
        item_id,
        CLAIMS_PARENT_COL.get("claim_id"),
        json.dumps(claim_id),
        f"Claim ID={claim_id}",
    )

    if pcn:
        _write_column_safe(
            item_id,
            CLAIMS_PARENT_COL.get("pcn"),
            json.dumps(pcn),
            f"PCN={pcn}",
        )

    _write_column_safe(
        item_id,
        CLAIMS_PARENT_COL.get("claim_sent_date"),
        json.dumps({"date": today}),
        f"Claim Sent Date={today}",
    )


# ── Main entry point ──────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────────────────
# SUBMISSION ERROR HELPERS
# ─────────────────────────────────────────────────────────────────────────────

# Claims Board primary status column
_PRIMARY_STATUS_COL = "color_mkxmywtb"   # Primary Status (submission status)
_STATUS_REQUEST_REJECTED_INDEX = 9       # "Request Rejected" label index


def _post_error_update(item_id: str, message: str) -> None:
    """
    Post a Monday Updates tab comment on the Claims Board item explaining the error.
    The team can read this to understand what needs to be fixed before resubmitting.
    """
    mutation = """
    mutation PostUpdate($itemId: ID!, $body: String!) {
      create_update(item_id: $itemId, body: $body) { id }
    }
    """
    try:
        run_query(mutation, {"itemId": str(item_id), "body": message})
        logger.info(f"[SUBMIT] Posted error update to item {item_id}: {message}")
    except Exception as e:
        logger.warning(f"[SUBMIT] Failed to post error update: {e}")


def _set_status_request_rejected(item_id: str) -> None:
    """
    Set the Claims Board primary status to "Request Rejected" (index=9).
    Used when submission fails due to a data error that needs team action.
    """
    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(item_id: $itemId, board_id: $boardId,
                          column_id: $columnId, value: $value) { id }
    }
    """
    try:
        run_query(
            mutation,
            {
                "itemId": str(item_id),
                "boardId": str(CLAIMS_BOARD_ID),
                "columnId": _PRIMARY_STATUS_COL,
                "value": json.dumps({"index": _STATUS_REQUEST_REJECTED_INDEX}),
            },
        )
        logger.info(f"[SUBMIT] Set item {item_id} → Request Rejected (index=9)")
    except Exception as e:
        logger.warning(f"[SUBMIT] Failed to set Request Rejected status: {e}")


def _extract_stedi_error(exception: Exception) -> str:
    """
    Extract the most useful error message from a Stedi HTTP exception.
    Tries to pull the first error description from the JSON response body.
    Falls back to the raw exception message.
    """
    try:
        import requests
        if isinstance(exception, requests.exceptions.HTTPError):
            body = exception.response.json()
            errors = body.get("errors", [])
            if errors:
                return errors[0].get("description", str(exception))
            return body.get("message", str(exception))
    except Exception:
        pass
    return str(exception)


async def submit_from_claims_board(item_id: str) -> None:
    """
    Stage B main entry point (PRD Section 5).
    Fetches Claims Board item + subitems → builds payload → submits → writes back.
    PRD FR12: Does NOT change Primary Status.
    """
    logger.info(f"[SUBMIT] Fetching Claims Board item {item_id}")
    claims_item = get_claims_item_with_subitems(item_id)

    parent = extract_parent_fields(claims_item)
    subitems = [extract_subitem_fields(s) for s in claims_item.get("subitems", [])]

    if not subitems:
        logger.warning(f"[SUBMIT] No subitems for item {item_id} — cannot submit")
        _post_error_update(item_id, "Submission failed: no service line subitems found on this Claims Board item.")
        return

    logger.info(f"[SUBMIT] Building payload from {len(subitems)} subitem(s)")

    try:
        payload, pcn = build_payload_from_claims_board(parent, subitems)
    except ValueError as e:
        # Catches DOS future-date error and any other validation errors
        error_msg = str(e)
        logger.error(f"[SUBMIT] Payload validation failed: {error_msg}")
        _post_error_update(item_id, f"Submission failed: {error_msg}")

        # If DOS is in the future, set primary status → "Request Rejected" (index=9)
        if "DOS date" in error_msg and "in the future" in error_msg:
            _set_status_request_rejected(item_id)
        return

    logger.info(f"[SUBMIT] Submitting to Stedi | payer={parent.get('primary_payor')} | pcn={pcn}")

    try:
        # HARD STOP VALIDATION:
        # Do not allow the claim to go out if any service line still has referringProvider
        service_lines = payload.get("claimInformation", {}).get("serviceLines", [])

        for i, line in enumerate(service_lines, start=1):
            if "referringProvider" in line:
                raise ValueError(
                    f"Service line {i} still contains referringProvider. "
                    "Blocking claim because this would map to DN instead of ordering provider."
                )

            if "orderingProvider" not in line:
                raise ValueError(
                    f"Service line {i} is missing orderingProvider. "
                    "Blocking claim because the doctor must be sent as orderingProvider."
                )

        logger.info("[SUBMIT] FINAL PAYLOAD TO STEDI:")
        logger.info(json.dumps(payload, indent=2))

        response = submit_claim(payload)

    except ValueError as e:
        error_msg = str(e)
        logger.error(f"[SUBMIT] Final payload validation failed: {error_msg}")
        _post_error_update(item_id, f"Submission blocked: {error_msg}")
        _set_status_request_rejected(item_id)
        return

    except Exception as e:
        # HTTP errors, network failures, Stedi rejections (400, 401, 500 etc.)
        error_msg = _extract_stedi_error(e)
        logger.error(f"[SUBMIT] Stedi submission failed: {error_msg}")
        _post_error_update(item_id, f"Stedi submission error: {error_msg}")
        _set_status_request_rejected(item_id)
        return

    claim_id = response.get("claim_id", "")
    logger.info(f"[SUBMIT] Submitted: claim_id={claim_id} | pcn={pcn}")

    # IMPORTANT:
    # Do NOT write any 277 status here.
    # Leave the 277 fields blank until the real 277 workflow updates them.
    _write_submission_outputs(item_id, claim_id, pcn=pcn)
