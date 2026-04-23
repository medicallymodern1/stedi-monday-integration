"""
services/subscription_eligibility_service.py
=============================================
Orchestrates the Stedi eligibility pipeline for the Subscription Board.

Mirrors services/eligibility_service.py but reads from the Subscription Board
(18407459988) instead of the Intake Board, and resolves the Stedi payer ID
from the **specific** Primary Insurance label (e.g. "Fidelis Low-Cost") via
``claim_assumptions.PAYER_ID_MAP`` — the same source-of-truth map the claims
submission pipeline uses.

Architecture (same four layers as the Intake flow):
  A. Input layer      — extract Subscription Board columns -> row dict
  B. Business logic   — validate + map payer + build payload (stedi_eligibility_builder)
  C. HTTP layer       — send to Stedi                        (stedi_eligibility_client)
  D. Output layer     — parse response                       (stedi_eligibility_parser)

Entry points:
  run_subscription_eligibility_check(monday_item)
  extract_subscription_eligibility_inputs(monday_item)

Subscription Board INPUT column IDs (verified against live board export
2026-04 — see `query { boards(ids: [18407459988]) { columns { id title type } } }`):
  color_mm254qxj   Primary Insurance   (status — specific label, e.g. "Fidelis Low-Cost")
  text_mkvp6zfg    Member ID 1         (text)
  text_mkvdefh1    DOB                 (text)
  color_mm2nnjam   Run Check           (trigger — status, label "Run")
  name             Item name           ("Firstname Lastname", split here)

This module is completely isolated from the Intake flow — importing it must
never change any Intake Board behaviour.
"""

from __future__ import annotations

import logging
from typing import Any

from claim_assumptions import (
    PAYER_ID_MAP,
    STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID,
)
from services.eligibility_service import _split_name  # reuse the exact same name parser
from stedi_eligibility_builder import build_eligibility_payload
from stedi_eligibility_client import send_eligibility_request
from stedi_eligibility_parser import parse_eligibility_response, error_response

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Subscription Board — INPUT column IDs (verified against live board export)
# ---------------------------------------------------------------------------
SUBSCRIPTION_COL = {
    "primary_insurance": "color_mm254qxj",  # "Primary Insurance" — status, .text = label
    "member_id":         "text_mkvp6zfg",   # "Member ID 1"
    "dob":               "text_mkvdefh1",   # "DOB"
    "run_check":         "color_mm2nnjam",  # "Run Check" — trigger column
}


# =============================================================================
# A. INPUT LAYER
# =============================================================================

def extract_subscription_eligibility_inputs(monday_item: dict) -> dict[str, Any]:
    """
    Map Subscription Board column_values -> normalised row dict.

    Keys match what stedi_eligibility_builder.build_eligibility_payload expects,
    with the addition of "Primary Insurance" (the specific label, for payer
    resolution via claim_assumptions.PAYER_ID_MAP).
    """
    cols: dict[str, str] = {
        c.get("id", ""): (c.get("text") or "").strip()
        for c in monday_item.get("column_values", [])
    }

    primary_insurance = cols.get(SUBSCRIPTION_COL["primary_insurance"], "").strip()
    member_id         = cols.get(SUBSCRIPTION_COL["member_id"], "").strip()
    dob               = cols.get(SUBSCRIPTION_COL["dob"], "").strip()

    # Subscription Board items are named "Firstname Lastname" (e.g. "Margaret Purifoy")
    first_name, last_name = _split_name(monday_item.get("name", ""))

    logger.debug(
        f"[SUB-ELG-INPUT] primary_insurance={primary_insurance!r} "
        f"member_id={member_id!r} dob={dob!r} "
        f"first={first_name!r} last={last_name!r}"
    )

    return {
        "Primary Insurance":     primary_insurance,
        "Member ID":             member_id,
        "First Name":            first_name,
        "Last Name":             last_name,
        "Patient Date of Birth": dob,
        # Identifiers for logging / controlNumber
        "Pulse ID": str(monday_item.get("id", "")),
        "Name":     monday_item.get("name", ""),
    }


# =============================================================================
# B. PAYER RESOLUTION
# =============================================================================

def _resolve_subscription_payer(primary_insurance: str) -> tuple[str, str]:
    """
    Resolve the specific Primary Insurance label -> (payer_id, partner_name)
    using ``claim_assumptions`` as the single source of truth.

    Raises ValueError with an actionable message if either lookup fails.
    """
    if not primary_insurance:
        raise ValueError("Missing required field: Primary Insurance")

    payer_id = PAYER_ID_MAP.get(primary_insurance, "")
    if not payer_id:
        raise ValueError(
            f"Unknown payer mapping for Primary Insurance: {primary_insurance!r}. "
            f"Add it to claim_assumptions.PAYER_ID_MAP."
        )

    partner_name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "")
    if not partner_name:
        raise ValueError(
            f"No Stedi trading partner name for payer ID: {payer_id!r} "
            f"(from Primary Insurance: {primary_insurance!r}). "
            f"Add it to claim_assumptions.STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID."
        )

    return payer_id, partner_name


# =============================================================================
# C + D. ORCHESTRATION
# =============================================================================


def _compute_subscription_active(raw_response: dict) -> str:
    """
    Compute the Subscription Board "Active?" flag from the raw Stedi response.

    The Intake parser's ``_parse_part_b_active`` is Medicare-specific — it
    requires service type 12 (DME) with active-coverage signals. Commercial
    plans (e.g. Anthem PPO) don't surface anything under service type 12
    even when the member is actively covered for the plan as a whole, so
    that parser returns "No" for a fully active commercial member.

    For the Subscription Board we want a broader "is this plan active?"
    signal, which maps cleanly to the X12 271 "Active Coverage" markers:

      - ``planStatus[*].statusCode == "1"``   (EB*1 on the plan overall), OR
      - ``benefitsInformation[*].code == "1"``  (any Active Coverage line).

    If we don't see either, call it Inactive.
    """
    plan_status = raw_response.get("planStatus") or []
    for ps in plan_status:
        if str(ps.get("statusCode", "")).strip() == "1":
            return "Yes"

    benefits = raw_response.get("benefitsInformation") or []
    for b in benefits:
        if str(b.get("code", "")).strip() == "1":
            return "Yes"

    return "No"


def _compute_subscription_ma(raw_response: dict) -> tuple[bool, str]:
    """
    Detect Medicare Advantage enrollment in a Medicare (CMS / payer 16013)
    eligibility response, and extract the MA plan's display name.

    When we run an eligibility check against Medicare for a patient who is
    actually enrolled in an MA plan, CMS still responds - it confirms the
    member's Medicare entitlement (planStatus.statusCode=1) - but it also
    includes an "EB*U" ("Contact Following Entity for Eligibility or
    Benefit Information") line that points at the administering MA plan.
    If we mark the row plain "Active", billing will send the claim to CMS
    16013 and it will deny ("benefits applicable to another payer").

    Instead we flag the row as "Medicare Advantage" and rewrite the payer
    name to the MA plan so the billing team sees who actually owns the
    member.

    MA-specific signals used (either alone is sufficient):

      1. A ``benefitsInformation`` entry with ``code == "U"`` whose
         ``benefitsAdditionalInformation.planNumber`` starts with "H".
         H-numbers are CMS-assigned MA contract IDs (e.g. H2001 =
         UnitedHealthcare Group Medicare Advantage). Non-MA plans on a
         Medicare 271 use S-numbers (Part D) or no plan number at all.
      2. An ``additionalInformation`` entry whose description contains
         ``"MA Bill Option Code"`` - CMS's explicit text flag that
         claims should be billed to the MA plan, not to Medicare.

    MA plan name resolution (first non-empty wins):

      1. ``benefitsAdditionalInformation.planNetworkDescription`` -
         branded MA plan name, e.g. "UnitedHealthcare Group Medicare
         Advantage". Preferred because it names the plan, not the
         claims administrator.
      2. ``benefitsRelatedEntity.entityName`` - the administering entity
         (e.g. "SIERRA HEALTH AND LIFE INSURANCE COMPANY, INC."). Used
         only when the plan description is missing.
      3. First entry of ``benefitsRelatedEntities[*].entityName``.

    Returns ``(is_ma, ma_plan_name)``. ``ma_plan_name`` may be empty
    even when ``is_ma`` is True if the response had no resolvable name,
    in which case the caller should leave the payer name column alone.
    """
    benefits = raw_response.get("benefitsInformation") or []

    for b in benefits:
        if not isinstance(b, dict):
            continue

        code      = str(b.get("code", "")).strip().upper()
        addl      = b.get("benefitsAdditionalInformation") or {}
        plan_num  = str(addl.get("planNumber", "")).strip().upper()
        info_list = b.get("additionalInformation") or []
        info_text = " ".join(
            str(x.get("description", ""))
            for x in info_list
            if isinstance(x, dict)
        ).upper()

        is_ma_hit = False
        # Signal 1: U code + H-number plan ID
        if code == "U" and plan_num.startswith("H") and len(plan_num) >= 4:
            is_ma_hit = True
        # Signal 2: explicit CMS MA Bill Option Code text
        if "MA BILL OPTION CODE" in info_text:
            is_ma_hit = True

        if not is_ma_hit:
            continue

        # Resolve plan name
        name = (addl.get("planNetworkDescription") or "").strip()
        if not name:
            rel = b.get("benefitsRelatedEntity") or {}
            if isinstance(rel, dict):
                name = (rel.get("entityName") or "").strip()
        if not name:
            rels = b.get("benefitsRelatedEntities") or []
            if rels and isinstance(rels[0], dict):
                name = (rels[0].get("entityName") or "").strip()

        return True, name

    return False, ""


def _compute_subscription_plan_begin(raw_response: dict) -> str:
    """
    Compute "Date Plan Begin" for the Subscription Board.

    The Intake parser's ``_parse_plan_begin_date`` only reads
    ``planDateInformation.planBegin`` — which is what Medicare A&B returns.
    Commercial payers (Anthem, BCBS, etc.) typically return a range under
    ``planDateInformation.plan`` or ``planDateInformation.benefit`` instead,
    e.g. "20241101-20260501", and no ``planBegin`` key at all.

    Resolution order:
      1. ``planDateInformation.planBegin``                        (Medicare)
      2. start half of ``planDateInformation.plan`` range         (commercial)
      3. start half of ``planDateInformation.benefit`` range      (fallback)
      4. start half of the first planStatus ``planDateInformation.plan``

    Returns ``""`` if nothing resolvable is present.
    Output format: ``YYYY-MM-DD``.
    """
    def _fmt(raw: str) -> str:
        raw = (raw or "").strip()
        if not raw:
            return ""
        # Range "YYYYMMDD-YYYYMMDD" -> take the start
        if "-" in raw and len(raw.split("-", 1)[0]) == 8:
            raw = raw.split("-", 1)[0]
        if len(raw) == 8 and raw.isdigit():
            return f"{raw[:4]}-{raw[4:6]}-{raw[6:]}"
        # Already YYYY-MM-DD? pass through
        if len(raw) == 10 and raw[4] == "-" and raw[7] == "-":
            return raw
        return ""

    pdi = raw_response.get("planDateInformation") or {}

    for key in ("planBegin", "plan", "benefit"):
        out = _fmt(pdi.get(key, ""))
        if out:
            return out

    # Per-planStatus fallback
    for ps in raw_response.get("planStatus") or []:
        inner = ps.get("planDateInformation") or {}
        for key in ("planBegin", "plan", "benefit"):
            out = _fmt(inner.get(key, ""))
            if out:
                return out

    return ""


# Sentinel key set on the writeback dict when we couldn't determine coverage
# and the Subscription flow should flip Run Check -> "Failed" instead of
# writing any of the 5 Stedi output columns.
SUB_FAILED_FLAG = "_subscription_failed"


def _is_coverage_unavailable(raw_response: dict) -> tuple[bool, str]:
    """
    Detect the "payer located the patient but did not return coverage"
    response — Stedi flags this explicitly via the top-level ``warnings``
    array with ``code = "COVERAGE_INFORMATION_UNAVAILABLE"``.

    When this fires, ``planStatus`` is typically empty and no
    ``benefitsInformation`` entry carries ``code == "1"`` (Active
    Coverage). The current Active? resolver would fall through to "No"
    and the board would show "Inactive" — which is misleading because
    the payer never said the patient is inactive, only that they
    couldn't tell us. In that case we flip Run Check to "Failed" and
    leave the rest of the Stedi columns untouched.

    Returns ``(is_unavailable, reason_description)``.
    """
    warnings = raw_response.get("warnings") or []
    for w in warnings:
        if not isinstance(w, dict):
            continue
        code = str(w.get("code", "")).strip().upper()
        if code == "COVERAGE_INFORMATION_UNAVAILABLE":
            desc = str(w.get("description", "")).strip()
            return True, desc or "Coverage information unavailable"
    return False, ""


def _failed_writeback(reason: str) -> dict:
    """
    Minimal writeback dict used when the eligibility check didn't yield
    a usable answer. The Monday writer branches on ``SUB_FAILED_FLAG``
    and writes only the Run Check column (-> "Failed"), leaving every
    other column untouched.
    """
    return {
        SUB_FAILED_FLAG: True,
        "_failure_reason": reason,
    }


def run_subscription_eligibility_check(monday_item: dict) -> dict[str, Any]:
    """
    Full pipeline: Subscription Board item -> eligibility writeback dict.

    Always returns a complete 23-field writeback dict (same shape as the
    Intake flow). The Monday writer filters this down to the 5 Subscription
    Board columns; keeping the dict full-shape means the parser and error
    handling are unchanged.
    """
    item_id   = str(monday_item.get("id", ""))
    item_name = monday_item.get("name", "")

    logger.info(
        f"[SUB-ELG] ── Subscription eligibility check start ── "
        f"item={item_id} name={item_name!r}"
    )

    try:
        # A. Extract inputs from Monday item
        row = extract_subscription_eligibility_inputs(monday_item)

        # B1. Resolve payer from claim_assumptions (NOT from GENERAL_PAYER_ID_MAP)
        payer_id, partner_name = _resolve_subscription_payer(row["Primary Insurance"])
        logger.info(
            f"[SUB-ELG] Payer resolved | item={item_id} "
            f"primary_insurance={row['Primary Insurance']!r} "
            f"payer_id={payer_id} partner={partner_name!r}"
        )

        # B2. Build + validate payload (builder skips GENERAL map since we passed payer_id)
        payload = build_eligibility_payload(
            row,
            payer_id=payer_id,
            partner_name=partner_name,
        )
        logger.info(
            f"[SUB-ELG] Sending | item={item_id} "
            f"tradingPartnerServiceId={payload.get('tradingPartnerServiceId')}"
        )

        # C. Send to Stedi (same client as Intake flow)
        raw_response = send_eligibility_request(payload)
        logger.info(f"[SUB-ELG] Response received | item={item_id}")

        # C2. Short-circuit on COVERAGE_INFORMATION_UNAVAILABLE.
        # When Stedi flags the response as coverage-unavailable, any
        # writeback we produce would be misleading (Active? would
        # default to "Inactive" even though the payer never said so).
        # Flip Run Check -> "Failed" and skip the Stedi column writes.
        is_unavail, unavail_reason = _is_coverage_unavailable(raw_response)
        if is_unavail:
            logger.warning(
                f"[SUB-ELG] ! Coverage unavailable | item={item_id} "
                f"reason={unavail_reason!r}"
            )
            return _failed_writeback(unavail_reason)

        # D. Parse response -> full writeback dict (same parser as Intake flow)
        writeback = parse_eligibility_response(raw_response)

        # D2. Compute Active? for the Subscription Board.
        #
        # First check for Medicare Advantage enrollment, because an MA
        # patient's CMS 271 ALSO contains planStatus.statusCode=1 (CMS
        # confirms Medicare entitlement). If we don't catch MA here, the
        # broader active check will write "Active" and billing will send
        # the claim to CMS 16013 and get denied ("benefits applicable to
        # another payer"). When MA is detected we write "Medicare
        # Advantage" instead, and override Stedi Payer Name with the MA
        # plan so the board shows who actually owns the member.
        #
        # For non-MA responses (commercial plans + traditional Medicare
        # A&B), use the broad active check from planStatus /
        # benefitsInformation code 1 - which handles commercial plans
        # that don't surface service type 12 the way the Intake Part B
        # parser requires.
        is_ma, ma_plan_name = _compute_subscription_ma(raw_response)
        if is_ma:
            sub_active = "Medicare Advantage"
            if ma_plan_name:
                writeback["Stedi Payer Name"] = ma_plan_name
        else:
            sub_active = _compute_subscription_active(raw_response)
        writeback["Sub Stedi Active?"] = sub_active

        # D3. Override Stedi Plan Begin Date with a broader lookup.
        # Intake parser only reads planDateInformation.planBegin (Medicare).
        # Commercial payers return a range under planDateInformation.plan
        # or .benefit; take the start of that range.
        sub_plan_begin = _compute_subscription_plan_begin(raw_response)
        if sub_plan_begin:
            writeback["Stedi Plan Begin Date"] = sub_plan_begin
        logger.info(
            f"[SUB-ELG] ✓ Done | item={item_id} "
            f"active={sub_active!r} "
            f"is_ma={is_ma} ma_plan={ma_plan_name!r} "
            f"part_b_active={writeback.get('Stedi Part B Active?')!r} "
            f"payer_name={writeback.get('Stedi Payer Name')!r} "
            f"plan_name={writeback.get('Stedi Plan Name')!r}"
        )
        return writeback

    except ValueError as e:
        msg = str(e)
        logger.warning(f"[SUB-ELG] ✗ Validation error | item={item_id}: {msg}")
        # Flip Run Check -> "Failed" so the board makes the error visible
        # without clobbering any of the existing Stedi output columns.
        return _failed_writeback(f"Validation error: {msg}")

    except Exception as e:
        msg = str(e)
        logger.error(
            f"[SUB-ELG] ✗ Unexpected error | item={item_id}: {msg}",
            exc_info=True,
        )
        return _failed_writeback(f"Unexpected error: {msg}")
