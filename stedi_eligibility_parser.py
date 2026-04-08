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
    # Check benefit rows for Medicaid insurance type or plan keywords
    for row in _benefits(response):
        ins_type = (row.get("insuranceType") or "").lower().strip()
        if ins_type in _MEDICAID_INSURANCE_TYPES:
            return True
        for kw in _MEDICAID_PLAN_KEYWORDS:
            plan = (row.get("planDetails") or row.get("planCoverage") or "").lower()
            group = (row.get("groupDescription") or "").lower()
            if kw in plan or kw in group:
                return True

    # Check planInformation (Fidelis returns medicaidRecipientIdNumber here)
    pi = response.get("planInformation") or {}
    if pi.get("medicaidRecipientIdNumber"):
        return True

    # Check planInformation.groupDescription / planDescription for Medicaid keywords
    for field in ("groupDescription", "planDescription"):
        val = (pi.get(field) or "").lower()
        for kw in _MEDICAID_PLAN_KEYWORDS:
            if kw in val:
                return True

    # Check payer name for Medicaid signals (e.g. NYSDOH, eMedNY)
    payer_name = (response.get("payer", {}).get("name") or "").lower()
    if any(kw in payer_name for kw in ("medicaid", "nysdoh", "emedny", "doh")):
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

    - Medicare A&B  → "Medicare A&B"
    - Medicare Advantage → MA carrier name (not the CMS routing payer)
    - Commercial / Medicaid → payer name from the response
    """
    if coverage_type == "Medicare A&B":
        return "Medicare A&B"

    # Medicare Advantage: use the same logic as _parse_ma_carrier so Stedi Payer Name
    # and Stedi Medicare Advantage Carrier always show the same value —
    # the most operationally useful MA plan identifier per client PRD feedback.
    if coverage_type == "Medicare Advantage":
        carrier = _parse_ma_carrier(response, coverage_type)
        if carrier:
            return carrier

    # Non-MA: payer-level name fields
    payer = response.get("payer", {})
    for key in ("name", "payerName", "entityName"):
        name = (payer.get(key) or "").strip()
        if name:
            return name

    return ""


def _parse_plan_name(response: dict, coverage_type: str) -> str:
    """
    Clearest returned plan label per PRD rules.

    For Medicare Advantage: use benefitsAdditionalInformation.planNetworkDescription
    from MA-signal rows (insuranceTypeCode HN, code R or U rows that carry MA plan info).
    This is the correct source — e.g. "Humana Gold Plus H4141-017".
    Do NOT use benefitsRelatedEntity.entityName for plan name (that is the carrier name).

    For United Dual / managed care: planCoverage on the MA row carries the full plan name,
    e.g. "NY UNITEDHEALTHCARE DUAL COMPLETE HMOPOS FULL H338".
    For commercial / Medicaid: planCoverage then planDetails.
    """
    if coverage_type == "Medicare A&B":
        return ""   # PRD: blank if no meaningful plan label

    # Pass 1 — MA plan name from benefitsAdditionalInformation.planNetworkDescription.
    # Only look at rows that are MA-carrier signal rows: insuranceTypeCode HN (Medicare Risk
    # HMO contact/referral rows) or code R/U (Other Payor / Contact Following Entity rows
    # that carry MA plan metadata alongside the carrier entity name).
    _ma_bai_codes = {"HN", "OT"}  # insuranceTypeCode values that carry MA carrier info
    for row in _benefits(response):
        ins_code = (row.get("insuranceTypeCode") or "").upper()
        row_code = (row.get("code") or "").upper()
        bai = row.get("benefitsAdditionalInformation") or {}
        pnd = (bai.get("planNetworkDescription") or "").strip()
        if pnd and (ins_code in _ma_bai_codes or row_code in ("R", "U")):
            return pnd

    # Pass 2 — planCoverage on any benefit row.
    # Catches United Dual Complete, Medicaid managed care (Fidelis HealthierLife Plan,
    # Anthem NY SSI HARP), and other managed plan names.
    for row in _benefits(response):
        v = (row.get("planCoverage") or "").strip()
        if v:
            return v

    # Pass 3 — planDetails (commercial, e.g. "Aetna Choice POS II")
    for row in _benefits(response):
        v = (row.get("planDetails") or "").strip()
        if v:
            return v

    return ""


def _parse_ma_flag(coverage_type: str) -> str:
    return "Yes" if coverage_type == "Medicare Advantage" else "No"


def _parse_ma_carrier(response: dict, coverage_type: str) -> str:
    """
    Populate only when MA; return the most operationally useful MA plan identifier.

    Per client PRD feedback, priority order is:
      1. benefitsAdditionalInformation.planNetworkDescription — the MA plan name
         (e.g. "Humana Gold Plus H4141-017"). Present on Humana/CMS responses.
      2. planCoverage on the MA active coverage row — the full plan label
         (e.g. "NY UNITEDHEALTHCARE DUAL COMPLETE HMOPOS FULL H338"). Present on
         United and other Direct responses where planNetworkDescription is absent.
      3. benefitsRelatedEntity/Entities entityName — the carrier org name.
         Fallback when neither plan label is returned.

    We intentionally skip payer.name for CMS-routed lookups (payer ID 16013)
    because it returns "CMS" rather than the actual MA carrier.
    """
    if coverage_type != "Medicare Advantage":
        return ""

    # Pass 1: planNetworkDescription from benefitsAdditionalInformation
    # on MA-signal rows (insuranceTypeCode HN/OT, or code R/U).
    # This is the clearest plan-level identifier (e.g. "Humana Gold Plus H4141-017").
    _ma_bai_codes = {"HN", "OT"}
    for row in _benefits(response):
        ins_code = (row.get("insuranceTypeCode") or "").upper()
        row_code = (row.get("code") or "").upper()
        is_ma_row = ins_code in _ma_bai_codes or row_code in ("R", "U")
        if not is_ma_row:
            continue
        bai = row.get("benefitsAdditionalInformation") or {}
        pnd = (bai.get("planNetworkDescription") or "").strip()
        if pnd:
            return pnd

    # Pass 2: planCoverage on any MA-typed active coverage row
    # (e.g. "NY UNITEDHEALTHCARE DUAL COMPLETE HMOPOS FULL H338" on MP rows).
    _ma_ins_types = {
        "medicare primary", "medicare risk",
        "health maintenance organization (hmo) - medicare risk",
        "preferred provider organization (ppo) - medicare risk",
        "exclusive provider organization (epo) - medicare risk",
        "point of service (pos) - medicare risk",
    }
    for row in _benefits(response):
        ins_type = (row.get("insuranceType") or "").lower().strip()
        if ins_type not in _ma_ins_types:
            continue
        pc = (row.get("planCoverage") or "").strip()
        if pc:
            return pc

    # Pass 3: carrier entity name from MA-signal rows (last resort)
    for row in _benefits(response):
        ins_code = (row.get("insuranceTypeCode") or "").upper()
        row_code = (row.get("code") or "").upper()
        is_ma_row = ins_code in _ma_bai_codes or row_code in ("R", "U")
        if not is_ma_row:
            continue
        for entity in (row.get("benefitsRelatedEntities") or []):
            name = (entity.get("entityName") or "").strip()
            if name:
                return name
        bre = row.get("benefitsRelatedEntity") or {}
        name = (bre.get("entityName") or "").strip()
        if name:
            return name

    # Final fallback: payer name only if not CMS-routed
    payer = response.get("payer", {})
    payer_name = (payer.get("name") or payer.get("entityName") or "").strip()
    if payer_name.upper() not in ("CMS", ""):
        return payer_name

    return ""


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
    # 1a. Check top-level planInformation.medicaidRecipientIdNumber (Fidelis pattern).
    #     This is the primary structured source per PRD — use it first.
    pi_mid = (response.get("planInformation") or {}).get("medicaidRecipientIdNumber") or ""
    if pi_mid.strip():
        return pi_mid.strip()

    # 1b. Check benefitsInformation rows for medicaidRecipientIdNumber (legacy fallback)
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

    # Check benefitsRelatedEntities for Medicare/Medicaid crossover entity names
    # (e.g. NYSDOH response lists "MEDICARE ABDQMB" as a prior insurance carrier)
    for row in _benefits(response):
        for entity in (row.get("benefitsRelatedEntities") or []):
            ename = (entity.get("entityName") or "").lower()
            if "medicare" in ename and "medicaid" in ename:
                return "Not returned"
            # "MEDICARE ABDQMB" pattern — ABD = Aged Blind Disabled (Medicare+Medicaid dual)
            if "abd" in ename and "medicare" in ename:
                return "Not returned"
            if "qmb" in ename:
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
    """
    Find the best matching deductible or OOP row.

    Pass 1: strict match — coverageLevelCode + timeQualifierCode required.
    Pass 2: fallback for plain Medicare/QMB responses where CMS omits
            coverageLevelCode entirely. Accept rows where coverageLevelCode
            is absent but code and timeQualifierCode match.
    """
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

    # Fallback: accept rows with no coverageLevelCode (CMS/Medicare omits it for Part B rows).
    # Constraints:
    #   - Only accept rows that lack a coverageLevelCode entirely (CMS omission pattern)
    #   - For FAM lookups: if row has no coverageLevelCode we cannot confirm it is FAM → skip
    #   - Only accept Part B (MB) or QMB (QM) rows — not Part A (MA) rows
    if coverage_level == "FAM":
        return ""  # Cannot infer FAM from rows that omit coverageLevelCode

    _mb_type_codes = {"MB", "QM", ""}  # Part B, QMB, or unspecified
    _exclude_type_codes = {"MA"}        # Part A — never use for deductible/OOP fallback

    for row in _benefits(response):
        if row.get("code") != code:
            continue
        if row.get("coverageLevelCode"):  # has a level code but wrong one — skip
            continue
        if row.get("timeQualifierCode") != time_qualifier:
            continue
        ins_type_code = (row.get("insuranceTypeCode") or "").upper()
        if ins_type_code in _exclude_type_codes:
            continue  # skip Part A rows
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