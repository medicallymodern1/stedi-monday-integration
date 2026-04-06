"""
services/era_parser_service.py
Parses Stedi 835 ERA JSON → normalized parent + children structure.
Handles both Stedi API format (transactions[]) and flat single-claim format.
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


def safe_float(value):
    try:
        if value in ("", None):
            return 0.0
        return round(float(value), 2)
    except (TypeError, ValueError):
        return 0.0


def format_amount(value):
    if value in ("", None):
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def format_stedi_date(date_str: str) -> str:
    """Convert YYYYMMDD → YYYY-MM-DD for Monday date columns"""
    if not date_str:
        return ""
    s = str(date_str).strip()
    if len(s) == 8:
        try:
            return datetime.strptime(s, "%Y%m%d").strftime("%Y-%m-%d")
        except ValueError:
            pass
    if len(s) == 10 and "-" in s:
        return s
    return s


def iter_adjustment_slots(adjustment_obj):
    """Iterate all adjustment slots (1–6) within one adjustment object"""
    group_code = adjustment_obj.get("claimAdjustmentGroupCode", "")
    for i in range(1, 7):
        amount      = adjustment_obj.get(f"adjustmentAmount{i}")
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
    """
    Parse all adjustment amounts by group code.
    Returns both 'Raw' and 'Parsed' aliased fields for full Monday column coverage.
    """
    result = {
        "Parsed Adjustment Codes":   "",
        "Parsed Adjustment Reasons": "",
        "Parsed CARC Codes":         "",
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

    codes   = []
    reasons = []
    carc    = []

    for adj in (service_adjustments or []):
        for slot in iter_adjustment_slots(adj):
            g   = slot["group_code"]
            rc  = slot["reason_code"]
            amt = slot["amount"]

            code_str = f"{g}-{rc}" if (g and rc) else g
            if code_str:
                codes.append(code_str)
            if rc:
                carc.append(rc)
            if slot["reason_text"]:
                reasons.append(slot["reason_text"])

            if g == "PR":
                result["Parsed PR Amount"] += amt
                if rc == "1":
                    result["Parsed Deductible Amount"]  += amt
                elif rc == "2":
                    result["Parsed Coinsurance Amount"] += amt
                elif rc == "3":
                    result["Parsed Copay Amount"]       += amt
                else:
                    result["Parsed Other PR Amount"]    += amt
            elif g == "CO":
                result["Parsed CO Amount"] += amt
                if rc == "45":
                    result["Parsed CO-45 Amount"]    += amt
                elif rc == "253":
                    result["Parsed CO-253 Amount"]   += amt
                else:
                    result["Parsed Other CO Amount"] += amt
            elif g == "OA":
                result["Parsed OA Amount"] += amt
            elif g == "PI":
                result["Parsed PI Amount"] += amt

    result["Parsed Adjustment Codes"]   = "; ".join(codes)
    result["Parsed Adjustment Reasons"] = "; ".join(reasons)
    result["Parsed CARC Codes"]         = "; ".join(carc)
    return result


def parse_remark_codes(remark_codes):
    """Parse RARC remark codes and descriptions"""
    codes = []
    texts = []
    for r in (remark_codes or []):
        code = str(r.get("remarkCode", "")).strip()
        text = str(r.get("remark",     "")).strip()
        if code:
            codes.append(code)
        if text:
            texts.append(text)
    return {
        "Parsed Remark Codes": "; ".join(codes),
        "Parsed Remark Text":  "; ".join(texts),
        "Parsed RARC Codes":   "; ".join(codes),   # RARC = Remark Codes alias
    }


def _parse_single_payment(payment: dict, paid_date: str, remittance_trace: str, era_date: str) -> dict:
    """
    Parse one paymentInfo block → { parent, children }.
    paid_date, remittance_trace, era_date come from the transaction envelope.
    """
    claim_info    = payment.get("claimPaymentInfo", {})
    service_lines = payment.get("serviceLines", [])

    patient_control = claim_info.get("patientControlNumber", "")
    claim_status    = claim_info.get("claimStatusCode", "")

    # ── Parent (claim-level) fields ───────────────────────────────────────────
    parent = {
        # 7 client-required Raw columns
        "raw_patient_control_num":    patient_control,
        "raw_payer_claim_control":    claim_info.get("payerClaimControlNumber", ""),
        "raw_total_claim_charge":     format_amount(claim_info.get("totalClaimChargeAmount")),
        "raw_remittance_trace":       remittance_trace,
        "raw_patient_responsibility": format_amount(claim_info.get("patientResponsibilityAmount")),
        "raw_era_date":               era_date,
        "raw_era_claim_status":       claim_status,
        # Existing working parent columns
        "primary_paid":               format_amount(claim_info.get("claimPaymentAmount")),
        "pr_amount":                  format_amount(claim_info.get("patientResponsibilityAmount")),
        "paid_date":                  paid_date,
        "check_number":               remittance_trace,
        "primary_status":             claim_status,
    }

    logger.info(
        f"ERA parent: pcn={patient_control} | paid={parent['primary_paid']} | "
        f"pr={parent['pr_amount']} | trace={remittance_trace} | era_date={era_date}"
    )

    # ── Children (service-line level) ─────────────────────────────────────────
    children = []
    for line in service_lines:
        svc_info    = line.get("servicePaymentInformation", {})
        svc_supp    = line.get("serviceSupplementalAmounts", {})
        adjustments = line.get("serviceAdjustments", [])
        remarks     = line.get("healthCareCheckRemarkCodes", [])

        parsed_adj     = parse_service_adjustments(adjustments)
        parsed_remarks = parse_remark_codes(remarks)

        procedure_code = svc_info.get("adjudicatedProcedureCode", "")
        service_date   = format_stedi_date(
            line.get("serviceDate") or line.get("serviceStartDate", "")
        )

        child = {
            # ── Fields mapped to Monday subitem columns ──────────────────────
            # Identifiers
            "Raw Line Item Control Number": line.get("lineItemControlNumber", ""),
            "Patient Control #":            patient_control,
            "Claim Status Code":            claim_status,
            "HCPC Code":                    procedure_code,
            # Dates & codes
            "Raw Service Date":             service_date,
            # Amounts
            "Primary Paid":                 format_amount(svc_info.get("lineItemProviderPaymentAmount")),
            "Raw Line Item Charge Amount":  format_amount(svc_info.get("lineItemChargeAmount")),
            "Raw Allowed Actual":           format_amount(svc_supp.get("allowedActual")),
            # Adjustment breakdown (Parsed names — mapped to Monday columns)
            "Parsed PR Amount":             parsed_adj.get("Parsed PR Amount",          0.0),
            "Parsed Deductible Amount":     parsed_adj.get("Parsed Deductible Amount",  0.0),
            "Parsed Coinsurance Amount":    parsed_adj.get("Parsed Coinsurance Amount", 0.0),
            "Parsed Copay Amount":          parsed_adj.get("Parsed Copay Amount",       0.0),
            "Parsed Other PR Amount":       parsed_adj.get("Parsed Other PR Amount",    0.0),
            "Parsed CO Amount":             parsed_adj.get("Parsed CO Amount",          0.0),
            "Parsed CO-45 Amount":          parsed_adj.get("Parsed CO-45 Amount",       0.0),
            "Parsed CO-253 Amount":         parsed_adj.get("Parsed CO-253 Amount",      0.0),
            "Parsed Other CO Amount":       parsed_adj.get("Parsed Other CO Amount",    0.0),
            "Parsed OA Amount":             parsed_adj.get("Parsed OA Amount",          0.0),
            "Parsed PI Amount":             parsed_adj.get("Parsed PI Amount",          0.0),
            # Code strings
            "Parsed Adjustment Codes":      parsed_adj.get("Parsed Adjustment Codes",   ""),
            "Parsed CARC Codes":            parsed_adj.get("Parsed CARC Codes",         ""),
            "Parsed RARC Codes":            parsed_remarks.get("Parsed RARC Codes",     ""),
            "Parsed Remark Codes":          parsed_remarks.get("Parsed Remark Codes",   ""),
            "Parsed Remark Text":           parsed_remarks.get("Parsed Remark Text",    ""),
            "Parsed Adjustment Reasons":    parsed_adj.get("Parsed Adjustment Reasons", ""),
        }

        logger.info(
            f"  Line {procedure_code}: paid={child['Primary Paid']} | "
            f"allowed={child['Raw Allowed Actual']} | "
            f"PR={child['Parsed PR Amount']} | CO={child['Parsed CO Amount']} | "
            f"CARC={child['Parsed CARC Codes']} | RARC={child['Parsed RARC Codes']}"
        )
        children.append(child)

    return {"parent": parent, "children": children}


def parse_era_stedi_format(era_json: dict) -> list:
    """
    Parse real Stedi 835 API format.
    Structure: transactions[].detailInfo[].paymentInfo[]
    Envelope fields (paid_date, remittance_trace, era_date) from transaction level.
    """
    results = []

    for txn in era_json.get("transactions", []):
        fin_info = txn.get("financialInformation", {})
        remit    = txn.get("paymentAndRemitReassociationDetails", {})

        paid_date        = format_stedi_date(fin_info.get("checkIssueOrEFTEffectiveDate", ""))
        remittance_trace = remit.get("checkOrEFTTraceNumber", "")
        era_date         = format_stedi_date(txn.get("productionDate", ""))

        for detail in txn.get("detailInfo", []):
            for payment in detail.get("paymentInfo", []):
                result = _parse_single_payment(payment, paid_date, remittance_trace, era_date)
                results.append(result)

    logger.info(f"Parsed {len(results)} ERA row(s) from Stedi API format")
    return results


def parse_era_json(era_json: dict) -> dict:
    """
    Parse flat/legacy ERA format (claimPaymentInfo at root).
    Also handles injected envelope fields.
    """
    fin_info = era_json.get("financialInformation", {})
    remit    = era_json.get("reassociationTraceNumber", {})

    paid_date        = format_stedi_date(fin_info.get("checkIssueOrEFTEffectiveDate", "")
                        or fin_info.get("paymentDate", ""))
    remittance_trace = remit.get("checkOrEFTTraceNumber", "") or remit.get("checkOrEftNumber", "")
    era_date         = ""

    return _parse_single_payment(era_json, paid_date, remittance_trace, era_date)


def parse_era_from_string(era_content: str) -> list:
    """Parse ERA from raw string — handles both Stedi API and flat formats"""
    if not era_content:
        return []
    try:
        era_json = json.loads(era_content)
        if "transactions" in era_json:
            return parse_era_stedi_format(era_json)
        elif "claimPaymentInfo" in era_json:
            return [parse_era_json(era_json)]
        else:
            logger.error(f"Unknown ERA format. Top-level keys: {list(era_json.keys())}")
            return []
    except json.JSONDecodeError as e:
        logger.error(f"ERA JSON decode failed: {e}")
        return []
    except Exception as e:
        logger.error(f"ERA parse failed: {e}", exc_info=True)
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
    Return ALL parsed fields for Monday writeback — parent fields + children list.
    This is the dict passed directly to populate_era_data_on_claims_item().
    """
    parent = era_row.get("parent", {})
    return {
        # 7 client-required Raw parent columns
        "raw_patient_control_num":    parent.get("raw_patient_control_num",    ""),
        "raw_payer_claim_control":    parent.get("raw_payer_claim_control",    ""),
        "raw_total_claim_charge":     parent.get("raw_total_claim_charge",     ""),
        "raw_remittance_trace":       parent.get("raw_remittance_trace",       ""),
        "raw_patient_responsibility": parent.get("raw_patient_responsibility", ""),
        "raw_era_date":               parent.get("raw_era_date",               ""),
        "raw_era_claim_status":       parent.get("raw_era_claim_status",       ""),
        # Existing working parent columns
        "primary_paid":               parent.get("primary_paid",   ""),
        "pr_amount":                  parent.get("pr_amount",       ""),
        "paid_date":                  parent.get("paid_date",       ""),
        "check_number":               parent.get("check_number",   ""),
        "primary_status":             parent.get("primary_status", ""),
        # Service line children for subitem population
        "children":                   era_row.get("children", []),
    }