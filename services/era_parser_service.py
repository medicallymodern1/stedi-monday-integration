"""
services/era_parser_service.py
Parses Stedi 835 ERA JSON → normalized parent + children structure.
Handles both Stedi API format (transactions[]) and flat single-claim format.
"""

import json
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CARC / RARC description lookups
# ---------------------------------------------------------------------------
# Stedi's X12-typed JSON does not include human-readable descriptions for
# adjustment reason codes or remark codes. We keep hand-curated maps in the
# repo root (carc_map.json, rarc_map.json) so we can populate the
# "Parsed Adjustment Reasons" and "Parsed Remark Text" columns on the
# Claims Board with the text billing/support actually needs to see.
#
# If the files are missing or unreadable, we fail soft - codes still get
# parsed, just without their description text.

def _load_code_map(filename: str) -> dict:
    import os as _os
    base = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
    path = _os.path.join(base, filename)
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        logger.info(f"Loaded {len(data)} entries from {filename}")
        return data
    except FileNotFoundError:
        logger.warning(f"{filename} not found at {path} — descriptions disabled")
        return {}
    except Exception as e:
        logger.warning(f"Failed to load {filename}: {e}")
        return {}


_CARC_DESCRIPTIONS = _load_code_map("carc_map.json")
_RARC_DESCRIPTIONS = _load_code_map("rarc_map.json")


def _describe_carc(code: str) -> str:
    """Return the human-readable text for a CARC code, or '' if unknown."""
    if not code:
        return ""
    return _CARC_DESCRIPTIONS.get(str(code).strip(), "") or ""


def _describe_rarc(code: str) -> str:
    """Return the human-readable text for a RARC code, or '' if unknown."""
    if not code:
        return ""
    return _RARC_DESCRIPTIONS.get(str(code).strip(), "") or ""


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


# =============================================================================
# X12-TYPED ERA PARSER (heading / detail / summary format)
# =============================================================================
# Stedi's Change-Healthcare-backed endpoint
# (/change/medicalnetwork/reports/v2/{id}/835) returns ERAs in a typed X12
# JSON shape: top-level heading/detail/summary, snake_case field names with
# positional suffixes (_01, _02, _03...). This is NOT the same as the
# classic Stedi SDK format (transactions[].detailInfo[].paymentInfo[]) that
# parse_era_stedi_format expects.
#
# Everything below is the alternate parsing path for the typed X12 format.


def _iter_x12_cas_slots(cas: dict):
    """
    Yield each populated reason/amount slot from one CAS segment.

    X12 CAS positions:
      slot 1: CAS02 reason, CAS03 amount, CAS04 qty
      slot 2: CAS05 reason, CAS06 amount, CAS07 qty
      slot 3: CAS08, CAS09, CAS10
      slot 4: CAS11, CAS12, CAS13
      slot 5: CAS14, CAS15, CAS16
      slot 6: CAS17, CAS18, CAS19

    In Stedi's typed JSON these become adjustment_reason_code_02,
    adjustment_amount_03, ..., adjustment_reason_code_17, adjustment_amount_18.
    """
    for reason_pos, amount_pos in [
        ("02", "03"),
        ("05", "06"),
        ("08", "09"),
        ("11", "12"),
        ("14", "15"),
        ("17", "18"),
    ]:
        amt = cas.get(f"adjustment_amount_{amount_pos}")
        if amt in (None, ""):
            continue
        rc = str(cas.get(f"adjustment_reason_code_{reason_pos}", "") or "").strip()
        yield {"reason_code": rc, "amount": safe_float(amt)}


def _parse_x12_service_adjustments(cas_list: list) -> dict:
    """
    Aggregate all adjustments for one service line into the same-shaped dict
    parse_service_adjustments produces for the older Stedi SDK format.
    Keys match what populate_era_service_line_subitems expects.
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

    codes   = []  # "CO-45"
    carc    = []  # "45"
    reasons = []  # text descriptions if ever present (Stedi typed JSON rarely carries them)

    for cas in (cas_list or []):
        if not isinstance(cas, dict):
            continue
        group = str(cas.get("claim_adjustment_group_code_01", "") or "").strip().upper()

        for slot in _iter_x12_cas_slots(cas):
            rc  = slot["reason_code"]
            amt = slot["amount"]

            if group and rc:
                codes.append(f"{group}-{rc}")
            elif group:
                codes.append(group)
            if rc:
                carc.append(rc)
                # Look up the human-readable CARC text for this reason code;
                # prefixed with group-code so billing can see e.g.
                # "CO-50: These are non-covered services because this is not
                # deemed a medical necessity...".
                desc = _describe_carc(rc)
                if desc:
                    prefix = f"{group}-{rc}" if group else rc
                    reasons.append(f"{prefix}: {desc}")

            if group == "PR":
                result["Parsed PR Amount"] += amt
                if rc == "1":
                    result["Parsed Deductible Amount"]  += amt
                elif rc == "2":
                    result["Parsed Coinsurance Amount"] += amt
                elif rc == "3":
                    result["Parsed Copay Amount"]       += amt
                else:
                    result["Parsed Other PR Amount"]    += amt
            elif group == "CO":
                result["Parsed CO Amount"] += amt
                if rc == "45":
                    result["Parsed CO-45 Amount"]    += amt
                elif rc == "253":
                    result["Parsed CO-253 Amount"]   += amt
                else:
                    result["Parsed Other CO Amount"] += amt
            elif group == "OA":
                result["Parsed OA Amount"] += amt
            elif group == "PI":
                result["Parsed PI Amount"] += amt

    result["Parsed Adjustment Codes"]   = "; ".join(codes)
    result["Parsed Adjustment Reasons"] = "; ".join(reasons)
    result["Parsed CARC Codes"]         = "; ".join(carc)
    return result


def _parse_x12_remark_codes(lq_list: list) -> dict:
    """Extract remark codes from the health_care_remark_codes_LQ array."""
    codes = []
    texts = []
    for lq in (lq_list or []):
        if not isinstance(lq, dict):
            continue
        code = str(lq.get("remark_code_02", "") or "").strip()
        if code:
            codes.append(code)
            desc = _describe_rarc(code)
            if desc:
                texts.append(f"{code}: {desc}")
    return {
        "Parsed Remark Codes": "; ".join(codes),
        "Parsed Remark Text":  "; ".join(texts),
        "Parsed RARC Codes":   "; ".join(codes),
    }


# 835 Claim Status Code → human-readable label
CLAIM_STATUS_LABELS = {
    "1":  "Processed as Primary",
    "2":  "Processed as Secondary",
    "3":  "Processed as Tertiary",
    "4":  "Denied",
    "5":  "Payer would not provide information",
    "13": "Suspended",
    "15": "Processed as Primary, Forwarded to Secondary Payer",
    "16": "Processed as Secondary, Forwarded to Tertiary Payer",
    "17": "Payment Reversed",
    "19": "Processed as Primary, Forwarded to Secondary Payer",
    "20": "Processed as Secondary, Forwarded to Tertiary Payer",
    "21": "Information Only",
    "22": "Reversal of Previous Payment",
}

def claim_status_label(code: str) -> str:
    """Return human-readable text for 835 claim status codes"""
    return CLAIM_STATUS_LABELS.get(str(code).strip(), code)


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
        "raw_era_claim_status":       claim_status_label(claim_status),  # text not numeric code
        # Existing working parent columns
        "primary_paid":               format_amount(claim_info.get("claimPaymentAmount")),
        "pr_amount":                  format_amount(claim_info.get("patientResponsibilityAmount")),
        "paid_date":                  paid_date,
        "check_number":               remittance_trace,
        "primary_status":             claim_status_label(claim_status),
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

        line_item_paid = format_amount(svc_info.get("lineItemProviderPaymentAmount"))

        child = {
            # ── Fields mapped to Monday subitem columns ──────────────────────
            # Identifiers
            "Raw Line Item Control Number": line.get("lineItemControlNumber", ""),
            "Patient Control #":            patient_control,
            # "Claim Status Code":            claim_status,
            "HCPC Code":                    procedure_code,
            # Dates & codes
            "Raw Service Date":             service_date,
            # Amounts
            "Primary Paid":                 line_item_paid,   # → numeric_mm11v6th
            "Raw Line Item Paid Amount":    line_item_paid,   # → numeric_mm201t4y
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


def _parse_single_x12_claim(
    clp_obj: dict,
    paid_date: str,
    remittance_trace: str,
    era_date: str,
) -> dict:
    """
    Parse one claim_payment_information_CLP_loop entry into the
    {parent, children} dict shape the Monday writer expects.
    """
    clp = clp_obj.get("claim_payment_information_CLP") or {}

    patient_control = str(clp.get("patient_control_number_01", "") or "")
    claim_status    = str(clp.get("claim_status_code_02",    "") or "")

    parent = {
        # 7 client-required Raw columns
        "raw_patient_control_num":    patient_control,
        "raw_payer_claim_control":    clp.get("payer_claim_control_number_07", "") or "",
        "raw_total_claim_charge":     format_amount(clp.get("total_claim_charge_amount_03")),
        "raw_remittance_trace":       remittance_trace,
        "raw_patient_responsibility": format_amount(clp.get("patient_responsibility_amount_05")),
        "raw_era_date":               era_date,
        "raw_era_claim_status":       claim_status_label(claim_status),
        # Existing working parent columns
        "primary_paid":               format_amount(clp.get("claim_payment_amount_04")),
        "pr_amount":                  format_amount(clp.get("patient_responsibility_amount_05")),
        "paid_date":                  paid_date,
        "check_number":               remittance_trace,
        "primary_status":             claim_status_label(claim_status),
    }

    logger.info(
        f"ERA X12 parent: pcn={patient_control} | paid={parent['primary_paid']} | "
        f"pr={parent['pr_amount']} | trace={remittance_trace} | era_date={era_date}"
    )

    children = []
    for svc_loop in (clp_obj.get("service_payment_information_SVC_loop") or []):
        if not isinstance(svc_loop, dict):
            continue

        svc       = svc_loop.get("service_payment_information_SVC") or {}
        composite = svc.get("composite_medical_procedure_identifier_01") or {}

        procedure_code = str(composite.get("adjudicated_procedure_code_02", "") or "").strip()

        # Service date — DTM array; take first usable entry (qualifier 472 preferred).
        service_date = ""
        for dtm in (svc_loop.get("service_date_DTM") or []):
            if not isinstance(dtm, dict):
                continue
            raw = dtm.get("service_date_02") or dtm.get("claim_date_02") or ""
            if raw:
                service_date = format_stedi_date(raw)
                break

        line_item_paid   = format_amount(svc.get("line_item_provider_payment_amount_03"))
        line_item_charge = format_amount(svc.get("line_item_charge_amount_02"))

        # AMT loop — "B6" qualifier = Allowed amount (aka Raw Allowed Actual).
        # Default to 0 when B6 is absent (common on denied lines where the
        # payer writes off the full charge and doesn't emit a B6), rather
        # than leaving the Monday cell blank. Blank reads as "unknown" to
        # billing; $0 reads as "payer allowed nothing", which is the
        # accurate interpretation for a denied line.
        allowed_actual = 0.0
        for amt in (svc_loop.get("service_supplemental_amount_AMT") or []):
            if not isinstance(amt, dict):
                continue
            qual = str(amt.get("amount_qualifier_code_01", "") or "").strip().upper()
            if qual == "B6":
                allowed_actual = format_amount(amt.get("service_supplemental_amount_02"))
                break

        parsed_adj     = _parse_x12_service_adjustments(svc_loop.get("service_adjustment_CAS"))
        parsed_remarks = _parse_x12_remark_codes(svc_loop.get("health_care_remark_codes_LQ"))

        line_ref = svc_loop.get("line_item_control_number_REF") or {}
        line_control_number = (
            line_ref.get("line_item_control_number_02", "")
            if isinstance(line_ref, dict) else ""
        )

        child = {
            # Identifiers
            "Raw Line Item Control Number": line_control_number,
            "Patient Control #":            patient_control,
            "HCPC Code":                    procedure_code,
            # Dates
            "Raw Service Date":             service_date,
            # Amounts
            "Primary Paid":                 line_item_paid,
            "Raw Line Item Paid Amount":    line_item_paid,
            "Raw Line Item Charge Amount":  line_item_charge,
            "Raw Allowed Actual":           allowed_actual,
            # Adjustment breakdown
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
            f"  X12 Line {procedure_code}: paid={child['Primary Paid']} | "
            f"allowed={child['Raw Allowed Actual']} | "
            f"PR={child['Parsed PR Amount']} | CO={child['Parsed CO Amount']} | "
            f"CARC={child['Parsed CARC Codes']} | RARC={child['Parsed RARC Codes']}"
        )
        children.append(child)

    return {"parent": parent, "children": children}


def parse_era_x12_typed_format(era_json: dict) -> list:
    """
    Parse the X12-typed JSON 835 shape (heading / detail / summary with
    snake_case + positional suffixes). This is what Stedi returns from
    /change/medicalnetwork/reports/v2/{id}/835.

    Returns the same list-of-{parent, children} dicts the other branches
    produce, so downstream code (summarize_era_row_for_monday +
    populate_era_service_line_subitems) works without any changes.
    """
    results = []

    heading = era_json.get("heading") or {}
    detail  = era_json.get("detail")  or {}

    # Envelope fields — each is an object in typed JSON.
    fin_info  = heading.get("financial_information_BPR")      or {}
    trn       = heading.get("reassociation_trace_number_TRN") or {}
    prod_dtm  = heading.get("production_date_DTM")            or {}

    paid_date        = format_stedi_date(fin_info.get("check_issue_or_eft_effective_date_16", "") or "")
    remittance_trace = trn.get("check_or_eft_trace_number_02", "") or ""
    era_date         = format_stedi_date(prod_dtm.get("production_date_02", "") or "")

    # Walk LX → CLP loops. Each CLP is a claim; one ERA can carry many.
    for lx_loop in (detail.get("header_number_LX_loop") or []):
        if not isinstance(lx_loop, dict):
            continue
        for clp_obj in (lx_loop.get("claim_payment_information_CLP_loop") or []):
            if not isinstance(clp_obj, dict):
                continue
            results.append(
                _parse_single_x12_claim(clp_obj, paid_date, remittance_trace, era_date)
            )

    logger.info(f"Parsed {len(results)} ERA row(s) from X12-typed format")
    return results


def parse_era_from_string(era_content: str) -> list:
    """Parse ERA from raw string — handles Stedi X12-typed, Stedi SDK, and flat formats."""
    if not era_content:
        return []
    try:
        era_json = json.loads(era_content)
        # X12-typed JSON (current Stedi /change/medicalnetwork/reports/v2 output)
        if "heading" in era_json and "detail" in era_json:
            return parse_era_x12_typed_format(era_json)
        # Classic Stedi SDK format
        if "transactions" in era_json:
            return parse_era_stedi_format(era_json)
        # Flat legacy single-claim format
        if "claimPaymentInfo" in era_json:
            return [parse_era_json(era_json)]
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