"""
stedi_eligibility_parser.py
============================
Parses a raw Stedi eligibility JSON response into a flat output dict
that maps directly to the Monday Intake Board output columns.

Architecture (PRD Section 13):
  This is the Output Mapping Layer.
  - Reads the Stedi response dict
  - Extracts each required V1 output field according to PRD rules
  - Returns a normalised dict keyed by Monday column name
  - Never modifies the Stedi response; read-only

Edge-case coverage (per PRD):
  - Medicare A&B plain
  - Medicare Advantage (Humana, United dual-complete)
  - QMB / QM insurance type
  - Fidelis Medicaid — explicit medicaidRecipientIdNumber
  - Anthem JLJ / Medicaid managed care — "Not returned" secondary ID
  - United Dual Complete — "Not returned" secondary ID
  - Aetna / commercial
  - Missing or error responses

General selection rules (PRD "General Selection Logic"):
  1. Exact service type 12 rows preferred over type 30 / general
  2. For deductible/OOP: require exact code + coverageLevelCode + timeQualifierCode
  3. Prefer rows with a usable value
  4. Prefer in-network rows over out-of-network rows
  5. First qualifying row wins — never combine rows
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Helpers — benefit row selection
# ---------------------------------------------------------------------------

def _benefits(response: dict) -> list[dict]:
    """Return the top-level benefitsInformation list (safe default: [])."""
    return response.get("benefitsInformation", []) or []


def _plan_status_list(response: dict) -> list[dict]:
    """Return the planStatus list (safe default: [])."""
    return response.get("planStatus", []) or []


def _has_service_type_12(row: dict) -> bool:
    """True if this benefit row explicitly contains service type code '12'."""
    codes = row.get("serviceTypeCodes") or []
    return "12" in codes


def _is_in_network(row: dict) -> bool:
    return (row.get("inPlanNetworkIndicatorCode") or "").upper() == "Y"


def _is_out_of_network(row: dict) -> bool:
    return (row.get("inPlanNetworkIndicatorCode") or "").upper() == "N"


def _usable_amount(row: dict) -> str | None:
    """Return benefitAmount if present and non-empty, else None."""
    v = row.get("benefitAmount")
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    return None


def _usable_percent(row: dict) -> str | None:
    """Return benefitPercent if present and non-empty, else None."""
    v = row.get("benefitPercent")
    if v is not None and str(v).strip() != "":
        return str(v).strip()
    return None


def _select_benefit_row(
    benefits: list[dict],
    code: str,
    coverage_level: str | None = None,
    time_qualifier: str | None = None,
    value_fn=_usable_amount,
) -> dict | None:
    """
    Select the best matching benefit row per PRD General Selection Logic.

    Priority:
      1. service type 12 exact match
      2. usable value present
      3. in-network preferred over out-of-network
      4. first match wins
    """
    candidates = []
    for row in benefits:
        if row.get("code") != code:
            continue
        if coverage_level and row.get("coverageLevelCode") != coverage_level:
            continue
        if time_qualifier and row.get("timeQualifierCode") != time_qualifier:
            continue
        candidates.append(row)

    if not candidates:
        return None

    # Score: (has_svc12, has_value, is_in_network)
    def score(r: dict) -> tuple:
        return (
            _has_service_type_12(r),
            value_fn(r) is not None,
            _is_in_network(r),
            not _is_out_of_network(r),
        )

    candidates.sort(key=score, reverse=True)

    # Only return rows that actually have a usable value
    for row in candidates:
        if value_fn(row) is not None:
            return row
    return None


# ---------------------------------------------------------------------------
# Field parsers
# ---------------------------------------------------------------------------

def _parse_part_b_active(response: dict) -> str:
    """
    Stedi Part B Active? — Yes/No.
    Look for active coverage status with service type 12 in planStatus,
    then fall back to a benefit code "1" row with service type 12.
    """
    # Check planStatus first
    for ps in _plan_status_list(response):
        codes = ps.get("serviceTypeCodes") or []
        status_text = (ps.get("status") or "").lower()
        if "12" in codes and "active" in status_text:
            return "Yes"

    # Check benefitsInformation for code "1" (active coverage) + service type 12
    for row in _benefits(response):
        if row.get("code") == "1" and _has_service_type_12(row):
            return "Yes"

    # If we got any planStatus at all but none matched, it's No
    if _plan_status_list(response) or _benefits(response):
        return "No"

    return "No"


# ── Coverage type signals ────────────────────────────────────────────────────

_MA_INSURANCE_TYPES = {
    "health maintenance organization (hmo) - medicare risk",
    "medicare primary",
    "medicare risk",
    "preferred provider organization (ppo) - medicare risk",
    "exclusive provider organization (epo) - medicare risk",
    "point of service (pos) - medicare risk",
}

_MEDICAID_INSURANCE_TYPES = {
    "medicaid",
    "medicaid managed care",
}

_MEDICAID_PLAN_KEYWORDS = {
    "new york medicaid",
    "ny ssi harp",
    "ny chip",
    "medicaid",
    "harp",
}


def _is_medicare_advantage(response: dict) -> bool:
    """True when response signals managed Medicare coverage."""
    for row in _benefits(response):
        ins_type = (row.get("insuranceType") or "").lower().strip()
        if ins_type in _MA_INSURANCE_TYPES:
            return True
        plan_cov = (row.get("planCoverage") or "").lower()
        if "dual complete" in plan_cov or "medicare advantage" in plan_cov:
            return True
    return False


def _is_medicaid(response: dict) -> bool:
    for row in _benefits(response):
        ins_type = (row.get("insuranceType") or "").lower().strip()
        if ins_type in _MEDICAID_INSURANCE_TYPES:
            return True
        for kw in _MEDICAID_PLAN_KEYWORDS:
            plan = (row.get("planDetails") or row.get("planCoverage") or "").lower()
            group = (row.get("groupDescription") or "").lower()
            if kw in plan or kw in group:
                return True
    return False


def _is_plain_medicare(response: dict) -> bool:
    """True for standard Medicare A&B without managed care signals."""
    payer = response.get("payer", {})
    payer_name = (payer.get("name") or "").lower()
    if "medicare" in payer_name and not _is_medicare_advantage(response):
        return True
    for row in _benefits(response):
        if (row.get("insuranceType") or "").lower() == "medicare part b":
            return True
    return False


def _parse_coverage_type(response: dict) -> str:
    if _is_medicare_advantage(response):
        return "Medicare Advantage"
    if _is_plain_medicare(response):
        return "Medicare A&B"
    if _is_medicaid(response):
        return "Medicaid"

    # Commercial: non-government payer with a standard plan
    payer = response.get("payer", {})
    payer_name = (payer.get("name") or "").lower()
    if payer_name and "medicare" not in payer_name and "medicaid" not in payer_name:
        return "Commercial"

    return "Unknown"


def _parse_payer_name(response: dict, coverage_type: str) -> str:
    """
    Operational payer name for the intake team.
    - Medicare A&B → 'Medicare A&B'
    - MA / commercial / Medicaid → actual carrier from the response
    """
    if coverage_type == "Medicare A&B":
        return "Medicare A&B"

    # Try subscriber/entity information first (most reliable carrier name)
    for entity in response.get("subscriberInformation", {}).get("additionalInformation", {}).get("entityInformation", []):
        name = (entity.get("entityName") or "").strip()
        if name:
            return name

    # Try payer-level name fields
    payer = response.get("payer", {})
    for key in ("name", "payerName", "entityName"):
        name = (payer.get(key) or "").strip()
        if name:
            return name

    # Fall back to benefit row entity names
    for row in _benefits(response):
        for entity in (row.get("entityInformation") or []):
            name = (entity.get("entityName") or "").strip()
            if name:
                return name

    return ""


def _parse_plan_name(response: dict, coverage_type: str) -> str:
    """Clearest returned plan label per PRD rules."""
    if coverage_type == "Medicare A&B":
        return ""   # PRD: blank if no meaningful plan label

    for row in _benefits(response):
        # Prefer planNetworkDescription (MA carrier plan name)
        v = (row.get("planNetworkDescription") or "").strip()
        if v:
            return v
        # Then planCoverage (dual / managed care plan names)
        v = (row.get("planCoverage") or "").strip()
        if v:
            return v
        # Then planDetails (commercial, e.g. "Aetna Choice POS II")
        v = (row.get("planDetails") or "").strip()
        if v:
            return v

    return ""


def _parse_ma_flag(coverage_type: str) -> str:
    return "Yes" if coverage_type == "Medicare Advantage" else "No"


def _parse_ma_carrier(response: dict, coverage_type: str) -> str:
    """Populate only when MA; return actual carrier entity name."""
    if coverage_type != "Medicare Advantage":
        return ""

    # Look for entityInformation inside benefit rows
    for row in _benefits(response):
        for entity in (row.get("entityInformation") or []):
            name = (entity.get("entityName") or "").strip()
            if name:
                return name

    # Fall back to payer name
    payer = response.get("payer", {})
    return (payer.get("name") or payer.get("entityName") or "").strip()


def _parse_ma_member_id(response: dict, coverage_type: str) -> str:
    """
    Populate only if the response explicitly returns the MA member ID in a
    structured field. Do not infer. Leave blank if not returned.
    """
    if coverage_type != "Medicare Advantage":
        return ""

    # Stedi may return this in subscriberAdditionalIdentification
    for ident in response.get("subscriberAdditionalIdentification", []) or []:
        qualifier = (ident.get("qualifier") or "").upper()
        id_value = (ident.get("memberIdentificationNumber") or "").strip()
        # Qualifier 1L = Group or policy number; SY = Social Security; look for MA-specific
        if id_value and qualifier not in ("SY", ""):
            return id_value

    return ""


def _parse_qmb(response: dict) -> str:
    """Yes if response explicitly indicates Qualified Medicare Beneficiary."""
    for row in _benefits(response):
        ins_type_code = (row.get("insuranceTypeCode") or "").upper()
        if ins_type_code == "QM":
            return "Yes"
        plan_cov = (row.get("planCoverage") or "").lower()
        if "qmb" in plan_cov:
            return "Yes"
        plan_details = (row.get("planDetails") or "").lower()
        if "qmb" in plan_details:
            return "Yes"

    return "No"


# ── Secondary / Medicaid ID ─────────────────────────────────────────────────

_ANTHEM_JLJ_PLAN_CONTEXTS = {
    "new york medicaid",
    "ny ssi harp",
    "ny chip - state billing only risk",
}

_MEDICAID_ID_QUALIFIERS = {
    "NQ",   # Medicaid recipient ID (X12 REF*NQ)
    "1D",   # Medicaid provider ID (also sometimes used)
}


def _parse_secondary_medicaid_id(response: dict) -> str:
    """
    PRD rules:
    - Fidelis: if medicaidRecipientIdNumber is returned, use it.
    - Anthem JLJ: "Not returned" if plan context matches and no ID returned.
    - United dual: "Not returned" if planCoverage contains "Dual" and no ID returned.
    - All others: blank unless ID is explicitly returned.
    """
    # 1. Check for explicit medicaidRecipientIdNumber (Fidelis pattern)
    for row in _benefits(response):
        mid = (row.get("medicaidRecipientIdNumber") or "").strip()
        if mid:
            return mid

    # 2. Check for REF*NQ or similar structured secondary ID
    for ref in response.get("additionalInformation", {}).get("referenceIdentification", []) or []:
        qualifier = (ref.get("qualifier") or "").upper()
        value = (ref.get("referenceIdentification") or "").strip()
        if qualifier in _MEDICAID_ID_QUALIFIERS and value:
            return value

    # 3. Check for "Not returned" triggers
    for row in _benefits(response):
        # Anthem JLJ context check
        group_desc = (row.get("groupDescription") or "").lower().strip()
        plan_cov = (row.get("planCoverage") or "").lower().strip()
        plan_details = (row.get("planDetails") or "").lower().strip()

        for ctx in _ANTHEM_JLJ_PLAN_CONTEXTS:
            if ctx in group_desc or ctx in plan_cov or ctx in plan_details:
                return "Not returned"

        # United Dual pattern
        if "dual" in plan_cov and "not returned" not in plan_cov:
            return "Not returned"

    return ""


def _parse_in_network(response: dict) -> str:
    """Yes / No / Unknown — prefer service type 12 rows."""
    # First pass: exact svc 12 rows
    for row in _benefits(response):
        if not _has_service_type_12(row):
            continue
        indicator = (row.get("inPlanNetworkIndicatorCode") or "").upper()
        if indicator == "Y":
            return "Yes"
        if indicator == "N":
            return "No"

    # Second pass: any row with a network indicator
    for row in _benefits(response):
        indicator = (row.get("inPlanNetworkIndicatorCode") or "").upper()
        if indicator == "Y":
            return "Yes"
        if indicator == "N":
            return "No"

    return "Unknown"


def _parse_prior_auth(response: dict) -> str:
    """Yes / No / Unknown — prefer service type 12 in-network rows."""
    svc12_rows = [r for r in _benefits(response) if _has_service_type_12(r)]
    in_net = [r for r in svc12_rows if _is_in_network(r)]

    # Prefer in-network svc12, then all svc12, then first usable row
    for candidate_pool in (in_net, svc12_rows, _benefits(response)):
        for row in candidate_pool:
            indicator = (row.get("authOrCertIndicator") or "").upper()
            if indicator == "Y":
                return "Yes"
            if indicator == "N":
                return "No"
            if indicator == "U":
                return "Unknown"

    return "Unknown"


def _parse_copay(response: dict) -> str:
    """
    Most relevant DME copay for service type 12.
    Code=B, prefer in-network svc12 rows.
    """
    row = _select_benefit_row(_benefits(response), code="B")
    if row:
        v = _usable_amount(row)
        if v is not None:
            return v
    return ""


def _parse_coinsurance(response: dict) -> str:
    """
    Most relevant DME coinsurance % for service type 12.
    Code=A, prefer in-network svc12 rows.
    """
    row = _select_benefit_row(_benefits(response), code="A", value_fn=_usable_percent)
    if row:
        v = _usable_percent(row)
        if v is not None:
            return v
    return ""


def _parse_deductible(
    response: dict,
    coverage_level: str,
    time_qualifier: str,
) -> str:
    """Generic deductible/OOP helper: code=C for deductible, code=G for OOP."""
    return _parse_financial_field(response, "C", coverage_level, time_qualifier)


def _parse_oop(
    response: dict,
    coverage_level: str,
    time_qualifier: str,
) -> str:
    return _parse_financial_field(response, "G", coverage_level, time_qualifier)


def _parse_financial_field(
    response: dict,
    code: str,
    coverage_level: str,
    time_qualifier: str,
) -> str:
    row = _select_benefit_row(
        _benefits(response),
        code=code,
        coverage_level=coverage_level,
        time_qualifier=time_qualifier,
    )
    if row:
        v = _usable_amount(row)
        if v is not None:
            return v
    return ""


def _parse_plan_begin_date(response: dict) -> str:
    """Top-level planDateInformation.planBegin, formatted as YYYY-MM-DD if YYYYMMDD."""
    plan_dates = response.get("planDateInformation") or {}
    raw = (plan_dates.get("planBegin") or "").strip()
    if not raw:
        return ""
    # Convert YYYYMMDD → YYYY-MM-DD for readability
    if len(raw) == 8 and raw.isdigit():
        return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
    return raw


def _parse_error_description(response: dict) -> str:
    """First structured error description from the response, if any."""
    for error in response.get("errors", []) or []:
        desc = (error.get("description") or error.get("message") or "").strip()
        if desc:
            return desc

    # Some payers return errors in benefitsInformation with a specific code
    for row in _benefits(response):
        if row.get("code") == "R":   # R = Non-covered
            notes = row.get("benefitsAdditionalInformation", {})
            msg = (notes.get("messageText") or "").strip()
            if msg:
                return msg

    return ""


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_eligibility_response(response: dict) -> dict[str, Any]:
    """
    Parse a raw Stedi eligibility JSON response into a flat Monday writeback dict.

    Keys match the Monday Intake Board column names defined in the PRD.
    Values are strings or blank strings (never None) so callers can write
    directly without extra null-checks.

    Always returns a complete dict with all 23 required V1 fields.
    """
    coverage_type = _parse_coverage_type(response)
    payer_name    = _parse_payer_name(response, coverage_type)
    ma_flag       = _parse_ma_flag(coverage_type)

    result: dict[str, Any] = {
        # ── Core classification ───────────────────────────────────────────
        "Stedi Part B Active?":              _parse_part_b_active(response),
        "Stedi Coverage Type":               coverage_type,
        "Stedi Payer Name":                  payer_name,
        "Stedi Plan Name":                   _parse_plan_name(response, coverage_type),

        # ── Medicare Advantage ────────────────────────────────────────────
        "Stedi Medicare Advantage?":         ma_flag,
        "Stedi Medicare Advantage Carrier":  _parse_ma_carrier(response, coverage_type),
        "Stedi Medicare Advantage Member ID": _parse_ma_member_id(response, coverage_type),

        # ── QMB / Secondary ──────────────────────────────────────────────
        "Stedi QMB?":                        _parse_qmb(response),
        "Stedi Secondary / Medicaid ID":     _parse_secondary_medicaid_id(response),

        # ── Network & auth ────────────────────────────────────────────────
        "Stedi In Network?":                 _parse_in_network(response),
        "Stedi Prior Auth Required?":        _parse_prior_auth(response),

        # ── Copay / coinsurance ───────────────────────────────────────────
        "Stedi Copay":                       _parse_copay(response),
        "Stedi Coinsurance %":               _parse_coinsurance(response),

        # ── Individual deductible ─────────────────────────────────────────
        "Stedi Individual Deductible":           _parse_deductible(response, "IND", "23"),
        "Stedi Individual Deductible Remaining": _parse_deductible(response, "IND", "29"),

        # ── Family deductible ─────────────────────────────────────────────
        "Stedi Family Deductible":               _parse_deductible(response, "FAM", "23"),
        "Stedi Family Deductible Remaining":     _parse_deductible(response, "FAM", "29"),

        # ── Individual OOP ────────────────────────────────────────────────
        "Stedi Individual OOP Max":              _parse_oop(response, "IND", "23"),
        "Stedi Individual OOP Max Remaining":    _parse_oop(response, "IND", "29"),

        # ── Family OOP ───────────────────────────────────────────────────
        "Stedi Family OOP Max":                  _parse_oop(response, "FAM", "23"),
        "Stedi Family OOP Max Remaining":        _parse_oop(response, "FAM", "29"),

        # ── Plan dates & errors ───────────────────────────────────────────
        "Stedi Plan Begin Date":             _parse_plan_begin_date(response),
        "Stedi Eligibility Error Description": _parse_error_description(response),
    }

    logger.info(
        f"[ELG-PARSER] Parsed | coverage_type={coverage_type!r} "
        f"part_b_active={result['Stedi Part B Active?']!r} "
        f"ma={result['Stedi Medicare Advantage?']!r} "
        f"qmb={result['Stedi QMB?']!r}"
    )

    return result


def error_response(error_description: str) -> dict[str, Any]:
    """
    Return a minimal writeback dict with only the error field populated.
    All other columns are blank so existing Monday values are not overwritten.
    Used when validation or HTTP errors prevent a real Stedi response.
    """
    return {
        "Stedi Part B Active?":              "",
        "Stedi Coverage Type":               "",
        "Stedi Payer Name":                  "",
        "Stedi Plan Name":                   "",
        "Stedi Medicare Advantage?":         "",
        "Stedi Medicare Advantage Carrier":  "",
        "Stedi Medicare Advantage Member ID": "",
        "Stedi QMB?":                        "",
        "Stedi Secondary / Medicaid ID":     "",
        "Stedi In Network?":                 "",
        "Stedi Prior Auth Required?":        "",
        "Stedi Copay":                       "",
        "Stedi Coinsurance %":               "",
        "Stedi Individual Deductible":       "",
        "Stedi Individual Deductible Remaining": "",
        "Stedi Family Deductible":           "",
        "Stedi Family Deductible Remaining": "",
        "Stedi Individual OOP Max":          "",
        "Stedi Individual OOP Max Remaining": "",
        "Stedi Family OOP Max":              "",
        "Stedi Family OOP Max Remaining":    "",
        "Stedi Plan Begin Date":             "",
        "Stedi Eligibility Error Description": error_description,
    }