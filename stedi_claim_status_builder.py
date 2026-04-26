"""
stedi_claim_status_builder.py
==============================
Validates inputs and builds the Stedi real-time Claim Status (276/277)
request payload.

Mirrors the shape of ``stedi_eligibility_builder.build_eligibility_payload``
so the two flows feel symmetric. The 276 JSON endpoint is documented at:
https://www.stedi.com/docs/healthcare/api-reference/post-healthcare-claim-status

Key differences from eligibility:
  - Providers is an ARRAY (not a single object), each with ``providerType``
    plus ``taxId`` (the billing provider's TIN/EIN).
  - Required date range: ``beginningDateOfService`` / ``endDateOfService``.
    We widen DOS to a 60-days-before/60-days-after window because some
    payers reject pinpoint DOS matches on 276.
  - No encounter.serviceTypeCodes — 276 is claim-level, not benefit-level.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from stedi_eligibility_builder import (
    STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID,
    _normalize_dob,
    _resolve_payer_id,
    _resolve_trading_partner_name,
    _safe_str,
)

logger = logging.getLogger(__name__)


STEDI_CLAIM_STATUS_ENDPOINT = (
    "https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/claimstatus/v2"
)

# Per Stedi docs: "The date range should be at least plus or minus 7
# days from the date of the services" and "Keep the date range to 30
# days or less. Some payers may reject requests with a date range that
# is too wide." Settled on ±10 (20 day window total) — comfortably
# inside Stedi's safe band while accommodating slightly mis-recorded
# DOS values on our side.
CLAIM_STATUS_DATE_WINDOW_DAYS_BEFORE = 10
CLAIM_STATUS_DATE_WINDOW_DAYS_AFTER  = 10


def _validate_inputs(row: dict[str, Any], *, payer_id: str | None = None) -> None:
    """Raise ValueError with a clear message for any missing required field."""
    if payer_id is None:
        general_insurance = _safe_str(row.get("General Insurance"))
        if not general_insurance:
            raise ValueError("Missing required field: General Insurance")
        _resolve_payer_id(general_insurance)

    if not _safe_str(row.get("Member ID")):
        raise ValueError("Missing required field: Member ID")
    if not _safe_str(row.get("First Name")):
        raise ValueError("Missing required field: First Name")
    if not _safe_str(row.get("Last Name")):
        raise ValueError("Missing required field: Last Name")

    dob_raw = row.get("Patient Date of Birth")
    if not _normalize_dob(dob_raw):
        raise ValueError(
            f"Missing or unparseable Patient Date of Birth: {dob_raw!r}. "
            f"Expected formats: MM/DD/YYYY, MM/DD/YY, YYYY-MM-DD"
        )

    if not _safe_str(row.get("Date of Service")):
        raise ValueError("Missing required field: Date of Service (Monday DOS column)")

    try:
        from claim_assumptions import (
            BILLING_PROVIDER_NPI,
            BILLING_PROVIDER_ORGANIZATION_NAME,
            BILLING_PROVIDER_EIN,
        )
        if not _safe_str(BILLING_PROVIDER_NPI):
            raise ValueError("BILLING_PROVIDER_NPI is blank in claim_assumptions.py")
        if not _safe_str(BILLING_PROVIDER_ORGANIZATION_NAME):
            raise ValueError(
                "BILLING_PROVIDER_ORGANIZATION_NAME is blank in claim_assumptions.py"
            )
        if not _safe_str(BILLING_PROVIDER_EIN):
            raise ValueError("BILLING_PROVIDER_EIN is blank in claim_assumptions.py")
    except ImportError:
        raise ValueError(
            "claim_assumptions.py not found — cannot load billing provider defaults"
        )


def _dos_window(dos_raw: Any) -> tuple[str, str]:
    """
    Normalise a single Date of Service into a (begin, end) window in
    YYYYMMDD — widened by the before/after constants above.

    The end date is hard-capped at today so we never send future dates;
    Stedi's docs explicitly call this out as a payer-rejection cause.
    """
    dos = _normalize_dob(dos_raw)
    if not dos:
        raise ValueError(f"Could not normalise Date of Service: {dos_raw!r}")
    d = datetime.strptime(dos, "%Y%m%d")
    begin_d = d - timedelta(days=CLAIM_STATUS_DATE_WINDOW_DAYS_BEFORE)
    end_d   = d + timedelta(days=CLAIM_STATUS_DATE_WINDOW_DAYS_AFTER)

    # Hard cap end at today (UTC date is fine — we're working in days)
    today = datetime.utcnow().date()
    if end_d.date() > today:
        end_d = datetime.combine(today, datetime.min.time())

    return begin_d.strftime("%Y%m%d"), end_d.strftime("%Y%m%d")


def build_claim_status_payload(
    row: dict[str, Any],
    *,
    payer_id: str | None = None,
    partner_name: str | None = None,
    fallback_mode: bool = False,
) -> dict[str, Any]:
    """
    Validate inputs and return a Stedi-compatible 276 claim-status payload.

    Row keys (from services.claim_status_service.extract_claim_status_inputs):
      "General Insurance"     — generic payer label (e.g. "Cigna")
      "Member ID"             — subscriber's member ID
      "First Name"            — split from the Claims Board item name
      "Last Name"
      "Patient Date of Birth" — free-text DOB
      "Date of Service"       — DOS (single date; widened to window)
      "Pulse ID", "Name"      — for controlNumber + logs
    """
    _validate_inputs(row, payer_id=payer_id)

    from claim_assumptions import (
        BILLING_PROVIDER_NPI,
        BILLING_PROVIDER_ORGANIZATION_NAME,
        BILLING_PROVIDER_EIN,
    )

    if payer_id is None:
        general_insurance = _safe_str(row["General Insurance"])
        payer_id          = _resolve_payer_id(general_insurance)
        partner_name      = _resolve_trading_partner_name(payer_id)
    else:
        general_insurance = _safe_str(
            row.get("General Insurance") or row.get("Primary Insurance") or ""
        )
        if not _safe_str(partner_name):
            partner_name = STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID.get(payer_id, "")
        if not _safe_str(partner_name):
            raise ValueError(
                f"No Stedi trading partner name for payer ID: {payer_id!r}. "
                f"Pass partner_name explicitly, or add it to "
                f"STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID."
            )

    dob         = _normalize_dob(row.get("Patient Date of Birth"))
    member_id   = _safe_str(row["Member ID"])
    begin, end  = _dos_window(row.get("Date of Service"))

    # Stedi's claim-status docs do NOT document controlNumber as a
    # request field — it appears only in responses (echoes of X12 ISA13
    # generated by Stedi's own envelope). Including it in the request
    # is at best ignored, at worst confuses payer-side parsing. Drop.

    # Per Stedi's "Check claim status" guide, the recommended base
    # request is intentionally minimal — but empirical testing showed
    # several payers (Fidelis confirmed) need encounter.submittedAmount
    # as a search criterion to find the claim. Stedi's docs don't list
    # it as a request field; Stedi's UI does, and it works. We always
    # send it when the Claims Board row carries a charge.
    #
    # Fallback mode adds Stedi's documented "if base returns no data,
    # try these" fields on top of the base:
    #   - encounter.tradingPartnerClaimNumber  (the payer's claim ID/ICN)
    #   - providers[0].taxId                   (billing provider TIN/EIN)
    # See ``fallback_mode`` parameter; the orchestrator flips it after
    # a D0 ("Data Search Unsuccessful") response from the base call.
    encounter: dict[str, Any] = {
        "beginningDateOfService": begin,
        "endDateOfService":       end,
    }
    raw_amt = _safe_str(row.get("Claim Charge Amount"))
    if raw_amt:
        try:
            amt = float(raw_amt.replace("$", "").replace(",", ""))
            # Stedi 276 expects submittedAmount as a STRING (HTTP 400
            # 'Encounter.submittedAmount: invalid type: number, expected
            # a string' otherwise). Format whole dollars without decimals,
            # fractional amounts with two decimals — matches the shape
            # Stedi echoes back in 277 responses.
            if amt == int(amt):
                encounter["submittedAmount"] = str(int(amt))
            else:
                encounter["submittedAmount"] = f"{amt:.2f}"
        except ValueError:
            pass  # silently skip — non-fatal
    if fallback_mode:
        tp_claim_num = _safe_str(row.get("Tradingpartner Claim Number"))
        if tp_claim_num:
            encounter["tradingPartnerClaimNumber"] = tp_claim_num[:50]

    # Subscriber gender is recommended in the docs and improves match
    # rate. Read from the row when the caller passed it; otherwise omit.
    subscriber: dict[str, Any] = {
        "memberId":    member_id,
        "firstName":   _safe_str(row["First Name"]),
        "lastName":    _safe_str(row["Last Name"]),
        "dateOfBirth": dob,
    }
    gender = _safe_str(row.get("Gender")).strip().upper()
    if gender in ("M", "F"):
        subscriber["gender"] = gender

    # If the caller indicated the patient is a dependent and supplied
    # policyholder details, swap subscriber→dependent and put the
    # policyholder in subscriber. Today the Claims Board doesn't carry
    # policyholder fields, so this code path is dormant and exists only
    # so we can flip it on without further refactor.
    pholder = row.get("Policyholder") or {}
    if isinstance(pholder, dict) and pholder.get("memberId"):
        dependent = {
            "firstName":   subscriber["firstName"],
            "lastName":    subscriber["lastName"],
            "dateOfBirth": subscriber["dateOfBirth"],
        }
        if "gender" in subscriber:
            dependent["gender"] = subscriber["gender"]
        subscriber = {
            "memberId":    _safe_str(pholder.get("memberId")),
            "firstName":   _safe_str(pholder.get("firstName")),
            "lastName":    _safe_str(pholder.get("lastName")),
            "dateOfBirth": _safe_str(pholder.get("dateOfBirth")),
        }
        ph_gender = _safe_str(pholder.get("gender")).strip().upper()
        if ph_gender in ("M", "F"):
            subscriber["gender"] = ph_gender
    else:
        dependent = None

    payload: dict[str, Any] = {
        "tradingPartnerServiceId": payer_id,
        "providers": [
            (lambda _b: (
                {**_b, "taxId": BILLING_PROVIDER_EIN}
                if fallback_mode else _b
            ))({
                "organizationName": BILLING_PROVIDER_ORGANIZATION_NAME,
                "npi":              BILLING_PROVIDER_NPI,
                "providerType":     "BillingProvider",
            })
        ],
        "subscriber": subscriber,
        "encounter":  encounter,
        "_meta": {
            "generalInsurance":   general_insurance,
            "tradingPartnerName": partner_name,
            "pulseId":            _safe_str(row.get("Pulse ID")),
            "itemName":           _safe_str(row.get("Name")),
            "dosWindow":          f"{begin}..{end}",
            "fallbackMode":       fallback_mode,
            "submittedAmount":    raw_amt,
        },
    }
    if dependent:
        payload["dependent"] = dependent

    logger.info(
        f"[CS-BUILDER] Payload ready | "
        f"general_insurance={general_insurance!r} -> payer_id={payer_id} | "
        f"partner={partner_name!r} | member_id={member_id!r} | "
        f"dos_window={begin}..{end} | submitted_amount={raw_amt!r} | "
        f"is_dependent={'yes' if dependent else 'no'} | "
        f"fallback={'yes' if fallback_mode else 'no'}"
    )
    # And dump the actual JSON we are about to send so we can verify
    # the exact request shape from Railway logs alone, no separate
    # test endpoint round-trip needed.
    import json as _json
    _send = {k: v for k, v in payload.items() if k != "_meta"}
    logger.info(
        f"[CS-BUILDER] Sending body: {_json.dumps(_send, default=str)[:1500]}"
    )

    return payload
