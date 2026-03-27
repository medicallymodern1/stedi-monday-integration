"""
services/era_parser_service.py
Parses Stedi ERA JSON into Monday Claims Board structure.
Follows Brandon's claimsvisualizer.py Phase 1 mapping exactly.
"""

import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def safe_float(value):
    try:
        if value in ("", None):
            return 0.0
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def format_amount(value):
    if value in ("", None):
        return None
    return round(float(value), 2)


def format_stedi_date(date_str: str) -> str:
    """Convert YYYYMMDD → YYYY-MM-DD for Monday date columns"""
    if not date_str or len(date_str) != 8:
        return date_str
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return date_str


def iter_adjustment_slots(adjustment_obj):
    """Iterate all adjustment slots (1-6) in a single adjustment object"""
    group_code = adjustment_obj.get("claimAdjustmentGroupCode", "")
    for i in range(1, 7):
        amount = adjustment_obj.get(f"adjustmentAmount{i}")
        reason_code = adjustment_obj.get(f"adjustmentReasonCode{i}", "")
        reason_text = adjustment_obj.get(f"adjustmentReason{i}", "")
        if amount not in (None, ""):
            yield {
                "group_code":  str(group_code).strip(),
                "reason_code": str(reason_code).strip(),
                "reason_text": str(reason_text).strip(),
                "amount":      safe_float(amount),
            }


def parse_service_adjustments(service_adjustments):
    """Parse all adjustment amounts by group code"""
    result = {
        "Parsed Adjustment Codes":   "",
        "Parsed Adjustment Reasons": "",
        "Parsed PR Amount":          0.0,
        "Parsed Deductible Amount":  0.0,
        "Parsed Coinsurance Amount": 0.0,
        "Parsed Copay Amount":       0.0,
        "Parsed Other PR Amount":    0.0,
        "Parsed CO Amount":          0.0,
        "Parsed CO-45 Amount":       0.0,
        "Parsed CO-253 Amount":      0.0,
        "Parsed Other CO Amount":    0.0,
        "Parsed OA Amount":          0.0,
        "Parsed PI Amount":          0.0,
    }

    codes = []
    reasons = []

    for adj in (service_adjustments or []):
        for slot in iter_adjustment_slots(adj):
            g = slot["group_code"]
            rc = slot["reason_code"]
            amt = slot["amount"]

            if g and rc:
                codes.append(f"{g}-{rc}")
            elif g:
                codes.append(g)
            if slot["reason_text"]:
                reasons.append(slot["reason_text"])

            if g == "PR":
                result["Parsed PR Amount"] += amt
                if rc == "1":
                    result["Parsed Deductible Amount"] += amt
                elif rc == "2":
                    result["Parsed Coinsurance Amount"] += amt
                elif rc == "3":
                    result["Parsed Copay Amount"] += amt
                else:
                    result["Parsed Other PR Amount"] += amt
            elif g == "CO":
                result["Parsed CO Amount"] += amt
                if rc == "45":
                    result["Parsed CO-45 Amount"] += amt
                elif rc == "253":
                    result["Parsed CO-253 Amount"] += amt
                else:
                    result["Parsed Other CO Amount"] += amt
            elif g == "OA":
                result["Parsed OA Amount"] += amt
            elif g == "PI":
                result["Parsed PI Amount"] += amt

    result["Parsed Adjustment Codes"] = "; ".join(codes)
    result["Parsed Adjustment Reasons"] = "; ".join(reasons)
    return result


def parse_remark_codes(remark_codes):
    codes = []
    texts = []
    for r in (remark_codes or []):
        code = str(r.get("remarkCode", "")).strip()
        text = str(r.get("remark", "")).strip()
        if code:
            codes.append(code)
        if text:
            texts.append(text)
    return {
        "Parsed Remark Codes": "; ".join(codes),
        "Parsed Remark Text":  "; ".join(texts),
    }


def parse_era_json(era_json: dict) -> dict:
    """
    Parse Stedi ERA JSON into parent + children structure.
    Follows Brandon's claimsvisualizer.py Phase 1 mapping.

    Returns:
    {
        "parent":   { ... claim level fields for Monday parent row ... },
        "children": [ { ... service line fields for Monday subitems ... } ]
    }
    """
    claim_info   = era_json.get("claimPaymentInfo", {})
    service_lines = era_json.get("serviceLines", [])

    patient_control = claim_info.get("patientControlNumber", "")
    claim_status    = claim_info.get("claimStatusCode", "")

    # ── Parent (claim level) — Phase 1 raw fields ─────────────────────────
    parent = {
        # Raw Stedi fields to populate now
        "primary_paid":            format_amount(claim_info.get("claimPaymentAmount")),
        "pr_amount":               format_amount(claim_info.get("patientResponsibilityAmount")),
        "primary_status":          claim_status,
        "raw_patient_control_num": patient_control,
        "raw_payer_claim_control": claim_info.get("payerClaimControlNumber", ""),
        "raw_claim_charge":        format_amount(claim_info.get("totalClaimChargeAmount")),
        # Dates come from ERA envelope — placeholder for now
        "paid_date":               "",
        "check_number":            "",
        "raw_remittance_trace":    "",
    }

    logger.info(
        f"ERA parent: pcn={patient_control} | "
        f"paid={parent['primary_paid']} | pr={parent['pr_amount']}"
    )

    # ── Children (service line level) ─────────────────────────────────────
    children = []

    for line in service_lines:
        svc_info    = line.get("servicePaymentInformation", {})
        svc_supp    = line.get("serviceSupplementalAmounts", {})
        adjustments = line.get("serviceAdjustments", [])
        remarks     = line.get("healthCareCheckRemarkCodes", [])

        parsed_adj     = parse_service_adjustments(adjustments)
        parsed_remarks = parse_remark_codes(remarks)

        procedure_code = svc_info.get("adjudicatedProcedureCode", "")
        service_date   = format_stedi_date(line.get("serviceDate", ""))

        child = {
            # Copied from parent
            "Claim Status":       claim_status,
            "Patient Control #":  patient_control,
            "Claim Status Code":  claim_status,
            # Raw line fields
            "Primary Paid":                 format_amount(svc_info.get("lineItemProviderPaymentAmount")),
            "Raw Line Item Control Number": line.get("lineItemControlNumber", ""),
            "Raw Service Date":             service_date,
            "Raw Line Item Charge Amount":  format_amount(svc_info.get("lineItemChargeAmount")),
            "Raw Allowed Actual":           format_amount(svc_supp.get("allowedActual")),
            "HCPC Code":                    procedure_code,
            # Parsed adjustments
            **parsed_adj,
            # Parsed remarks
            **parsed_remarks,
        }

        logger.info(
            f"  Line {procedure_code}: "
            f"paid={child['Primary Paid']} | "
            f"allowed={child['Raw Allowed Actual']} | "
            f"PR={parsed_adj['Parsed PR Amount']} | "
            f"CO={parsed_adj['Parsed CO Amount']} | "
            f"remarks={parsed_remarks['Parsed Remark Codes']}"
        )

        children.append(child)

    return {"parent": parent, "children": children}


# ── Functions called from stedi_webhook.py ────────────────────────────────────

def parse_era_from_string(era_content: str) -> list:
    import json
    if not era_content:
        return []
    try:
        era_json = json.loads(era_content)

        # ── Stedi API format ──────────────────────────────────────────
        # Structure: transactions[].detailInfo[].paymentInfo[]
        # financialInformation and paymentAndRemitReassociationDetails
        # are at the transaction level, not the root
        if "transactions" in era_json:
            results = []
            for txn in era_json.get("transactions", []):

                # Envelope fields are at transaction level
                financial_info = txn.get("financialInformation", {})
                remit_details  = txn.get("paymentAndRemitReassociationDetails", {})

                paid_date    = format_stedi_date(financial_info.get("checkIssueOrEFTEffectiveDate", ""))
                check_number = remit_details.get("checkOrEFTTraceNumber", "")

                for detail in txn.get("detailInfo", []):
                    for payment in detail.get("paymentInfo", []):
                        # Build flat dict that parse_era_json expects
                        flat = {
                            "claimPaymentInfo": payment.get("claimPaymentInfo", {}),
                            "serviceLines":     payment.get("serviceLines", []),
                            "patientName":      payment.get("patientName", {}),
                            # Inject envelope fields so parse_era_json picks them up
                            "financialInformation": {
                                "paymentDate": paid_date,
                            },
                            "reassociationTraceNumber": {
                                "checkOrEftNumber": check_number,
                            },
                        }
                        result = parse_era_json(flat)
                        results.append(result)

            logger.info(f"Parsed {len(results)} ERA row(s) from Stedi API format")
            return results

        # ── Flat format (single claim at root) ────────────────────────
        elif "claimPaymentInfo" in era_json:
            result = parse_era_json(era_json)
            return [result]

        else:
            logger.error(f"Unknown ERA JSON format. Top-level keys: {list(era_json.keys())}")
            return []

    except Exception as e:
        logger.error(f"parse_era_from_string failed: {e}", exc_info=True)
        return []


def match_era_rows_to_claim_item(era_rows: list, patient_control_number: str) -> list:
    """Filter ERA rows by patient control number"""
    if not patient_control_number:
        return era_rows
    matched = [
        row for row in era_rows
        if row.get("parent", {}).get("raw_patient_control_num") == patient_control_number
    ]
    logger.info(f"Matched {len(matched)} ERA rows for PCN={patient_control_number}")
    return matched


def summarize_era_row_for_monday(era_row: dict) -> dict:
    """
    Extract ERA fields for Monday Claims Board population.
    Returns flat dict for parent + children list.
    """
    parent = era_row.get("parent", {})
    return {
        # Parent fields
        "primary_paid":            parent.get("primary_paid", ""),
        "pr_amount":               parent.get("pr_amount", ""),
        "paid_date":               parent.get("paid_date", ""),
        "primary_status":          parent.get("primary_status", ""),
        "raw_patient_control_num": parent.get("raw_patient_control_num", ""),
        "raw_payer_claim_control": parent.get("raw_payer_claim_control", ""),
        "check_number":            parent.get("check_number", ""),
        # Children for subitem creation
        "children":                era_row.get("children", []),
    }

