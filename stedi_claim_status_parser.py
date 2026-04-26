"""
stedi_claim_status_parser.py
=============================
Parses Stedi's JSON 277 response into a flat writeback dict keyed by
Monday-facing field names. The Monday writer
(services/claim_status_monday_service.py) picks the six fields it
actually writes; the full dict shape is kept complete so this module
can also be used for unit tests + debugging endpoints.

Category-code mapping (277's ``statusCategoryCode``) → Monday label:

  A  Acknowledgement             → "Acknowledged"
  P  Pending                     → "Pending"   (P0/P1/P2/P3/P5 default)
  F  Finalized                   → "Paid"     (F1) or "Denied" (F2/F4)
                                   or "In Process" (F3 — revised)
  R  Requests for more info      → "Requests Info"
  E  Error / no match            → "Error"
  no claims                      → "No Match"

If multiple claims come back (rare but possible when the DOS window
overlaps adjacent claims), we pick the first "actionable" one by priority:
Paid > Denied > Requests Info > Pending > In Process > Acknowledged > Error.
This surfaces the most useful status for billing, even if an older
duplicate is also present.
"""

from __future__ import annotations

import datetime as _dt
import logging
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Category → Monday label mapping
# ---------------------------------------------------------------------------

def _category_to_monday_label(category: str, status_code: str) -> str:
    """
    Map (statusCategoryCode, statusCode) to a Monday label.

    Stedi returns the X12 STC01-1 ``statusCategoryCode`` as a 2-char
    value (letter + digit), e.g. "F0", "F1", "F2", "F3", "F4", "A1", "A2",
    "A3", "P1", "P3", "R0", "E0" - NOT the bare letter. We key off
    the FIRST character for the broad bucket and use the second
    character (or the separate ``statusCode``) for the F-subcategory
    split that matters most for the Monday board (Paid vs Denied vs
    In Process).

      F0 plain Finalized                 → In Process
      F1 Finalized/Payment               → Paid
      F2 Finalized/Denial                → Denied
      F3 Finalized/Revised               → In Process
      F4 Finalized/Adjudication-no-pay   → Denied
      P* Pending                         → Pending
      A* Acknowledgement                 → Acknowledged
      R* Requests for additional info    → Requests Info
      E* System / processing error       → Error
      anything else (incl. blanks)       → No Match
    """
    cat  = (category or "").strip().upper()
    code = (status_code or "").strip().upper()
    head = cat[:1]                              # broad bucket letter
    sub  = cat[1:2] if len(cat) > 1 else ""     # F-subcat for category-encoded values

    if head == "F":
        # Subcat may live in either char-2 of the category ("F1") or as a
        # separate statusCode (some payers split them differently).
        f_sub = sub or code[:1]
        if code.startswith("F") and len(code) >= 2:
            f_sub = code[1]
        if f_sub == "1":
            return "Paid"
        if f_sub in ("2", "4"):
            return "Denied"
        if f_sub == "3":
            return "In Process"
        # F0 / unknown F-subcode — treat as In Process so billing still sees activity
        return "In Process"

    if head == "P":
        return "Pending"
    if head == "A":
        return "Acknowledged"
    if head == "R":
        return "Requests Info"
    if head == "E":
        return "Error"
    return "No Match"


# Priority order when the 277 returns multiple claims — higher index wins.
_LABEL_PRIORITY = {
    "Error":         1,
    "Acknowledged":  2,
    "In Process":    3,
    "Pending":       4,
    "Requests Info": 5,
    "Denied":        6,
    "Paid":          7,
    "No Match":      0,
}


# ---------------------------------------------------------------------------
# Response shape helpers
# ---------------------------------------------------------------------------

def _walk_claims(raw: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Extract the list of claim-level records out of a Stedi 277 JSON
    response. The JSON endpoint nests them under various keys depending
    on what the payer returned; we probe the most common ones and
    flatten.

    Observed shapes across Stedi 277 responses:
      - {"claims": [...]}
      - {"informationReceiverLevel": {..., "claims": [...]}}
      - {"providerLevel": {..., "claims": [...]}}
      - {"informationReceiverLevel": [{"providerLevel": [{"claims": [...]}]}]}
    """
    claims: list[dict[str, Any]] = []

    def _collect(obj: Any) -> None:
        if isinstance(obj, dict):
            maybe = obj.get("claims")
            if isinstance(maybe, list):
                claims.extend(m for m in maybe if isinstance(m, dict))
            for v in obj.values():
                _collect(v)
        elif isinstance(obj, list):
            for item in obj:
                _collect(item)

    _collect(raw)
    return claims


def _pick_first(d: dict[str, Any], *keys: str, default: Any = "") -> Any:
    """Return the first non-empty value for any of the given keys."""
    for k in keys:
        v = d.get(k)
        if v not in (None, "", []):
            return v
    return default


def _money(val: Any) -> float:
    """Best-effort numeric coercion; returns 0.0 for blanks/non-numerics."""
    if val in (None, "", "0", 0):
        return 0.0
    try:
        return float(str(val).replace(",", "").replace("$", "").strip())
    except (TypeError, ValueError):
        return 0.0


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def parse_claim_status_response(raw: dict[str, Any]) -> dict[str, Any]:
    """
    Turn a Stedi 277 JSON response into a flat writeback dict.

    Shape:
        {
            "Claim Status Category": "Paid",          # one of the 8 labels
            "Claim Status Detail":   "F1 · 65 · Paid as billed — $...",
            "277 ICN":               "ABC123456",
            "277 Paid Amount":       1284.0,
            "Last Claim Status Check": "2026-04-24",
            "_category_code":        "F",             # raw
            "_status_code":          "65",            # raw
            "_n_claims_returned":    1,               # for log/debug
        }
    """
    claims = _walk_claims(raw)
    today  = _dt.date.today().isoformat()

    if not claims:
        # Stedi-level error or empty response — surface the raw 277 so
        # we can see exactly what came back without needing a second
        # round-trip to debug.
        import json as _json
        try:
            raw_excerpt = _json.dumps(raw, default=str)[:2000]
        except Exception:
            raw_excerpt = repr(raw)[:2000]
        logger.warning(f"[CS-PARSER] RAW 277 (no claims envelope): {raw_excerpt}")

        return {
            "Claim Status Category":   "No Match",
            "Claim Status Detail":     _error_detail(raw) or "No claims returned in 277 response.",
            "277 ICN":                 "",
            "277 Paid Amount":         0.0,
            "Last Claim Status Check": today,
            "_category_code":          "",
            "_status_code":            "",
            "_n_claims_returned":      0,
        }

    # Pick the highest-priority claim among the returned matches.
    decorated: list[tuple[int, dict[str, Any]]] = []
    for c in claims:
        cs = c.get("claimStatus") or c.get("status") or {}
        if isinstance(cs, list):
            cs = cs[0] if cs else {}
        cat  = _pick_first(cs, "statusCategoryCode", "category")
        code = _pick_first(cs, "statusCode", "code")
        label = _category_to_monday_label(str(cat), str(code))
        priority = _LABEL_PRIORITY.get(label, 0)
        decorated.append((priority, {"claim": c, "claim_status": cs, "label": label,
                                     "category": str(cat), "code": str(code)}))

    decorated.sort(key=lambda x: -x[0])
    top = decorated[0][1]
    claim  = top["claim"]
    cs     = top["claim_status"]
    label  = top["label"]
    cat    = top["category"]
    code   = top["code"]

    # Stedi exposes two rich text values in 277 responses:
    #   statusCategoryCodeValue — the full sentence for the category
    #     code (e.g. "Finalized/Payment - The claim/line has been paid.")
    #   statusCodeValue          — the full sentence for the status code
    #     (e.g. "Claim/line has been paid.")
    # Older field names (statusDescription / description / text) are kept
    # in the probe order as a fallback so this code still works against
    # mocks and edge-case payer payloads.
    cat_text  = _pick_first(cs, "statusCategoryCodeValue") or ""
    code_text = _pick_first(cs, "statusCodeValue") or ""
    fallback  = _pick_first(cs, "statusDescription", "description", "text") or ""

    # Build a human-readable detail string. Format:
    #   "[F1] Finalized/Payment - The claim/line has been paid. · [65] Claim/line has been paid."
    # If the rich values are missing we fall back to "[F1] · [65]" plus
    # whatever generic description we got, so the cell is never blank
    # when the parser actually saw a claim.
    bits: list[str] = []
    if cat:
        bits.append(f"[{cat}] {cat_text}".rstrip())
    if code:
        bits.append(f"[{code}] {code_text}".rstrip())
    if fallback and fallback not in (cat_text, code_text):
        bits.append(fallback)
    detail = " · ".join(bits) if bits else "(no description)"

    # Payer's claim control number (ICN). Stedi puts it inside
    # claimStatus as ``tradingPartnerClaimNumber``. Older fallback
    # names probed against the outer claim envelope so legacy mocks
    # still work.
    icn = (
        _pick_first(
            cs,
            "tradingPartnerClaimNumber",
            "payerClaimControlNumber",
            "claimControlNumber",
        )
        or _pick_first(
            claim,
            "tradingPartnerClaimNumber",
            "claimControlNumber",
            "payerClaimControlNumber",
            "payerClaimIdentifier",
            "claimId",
        )
    )

    # Payment amount. Stedi names it ``amountPaid`` (string) inside
    # claimStatus; older mocks used ``claimPaymentAmount`` at the
    # outer level. _money() handles string-or-number cleanly.
    paid = _money(
        _pick_first(
            cs,
            "amountPaid", "paymentAmount", "claimPaymentAmount",
        )
        or _pick_first(
            claim,
            "claimPaymentAmount", "amountPaid",
            "paymentAmount", "totalClaimPaidAmount",
        )
    )

    # Bonus metadata for the Notes & Activity log line.
    check_no   = _pick_first(cs, "checkNumber") or ""
    paid_date  = _pick_first(cs, "paidDate", "effectiveDate") or ""
    pcn_echo   = _pick_first(cs, "patientAccountNumber") or ""

    # F0 = "Finalized" without an explicit Paid/Denied/Revised subcategory
    # — the X12 spec leaves it ambiguous, so we resolve via the dollar
    # amount: anything paid > 0 is "Paid", $0 is effectively "Denied".
    # Other F-subcodes (F1/F2/F3/F4) are unambiguous and stay as-is.
    if cat.upper().startswith("F") and (cat[1:2] == "0" or cat.upper() == "F"):
        label = "Paid" if paid > 0 else "Denied"

    writeback: dict[str, Any] = {
        "Claim Status Category":    label,
        "Claim Status Detail":      (detail[:500] if detail else "(no description)"),
        "277 ICN":                  str(icn) if icn else "",
        "277 Paid Amount":          paid,
        "Last Claim Status Check":  today,
        "_category_code":           cat,
        "_status_code":             code,
        "_n_claims_returned":       len(claims),
        # Bonus fields surfaced for the Monday Notes & Activity formatter
        "_check_number":            str(check_no) if check_no else "",
        "_paid_date":               str(paid_date) if paid_date else "",
        "_patient_account_number":  str(pcn_echo) if pcn_echo else "",
    }

    logger.info(
        f"[CS-PARSER] claims={len(claims)} label={label!r} "
        f"cat={cat!r} code={code!r} paid={paid} icn={icn!r}"
    )
    if label in ("Error", "No Match") or not icn:
        # Surface the raw response so we can see exactly what Stedi/payer
        # said. Truncate so logs don't blow up; full response is usually
        # under 4 KB anyway.
        import json as _json
        try:
            raw_excerpt = _json.dumps(raw, default=str)[:2000]
        except Exception:
            raw_excerpt = repr(raw)[:2000]
        logger.warning(f"[CS-PARSER] RAW 277 (excerpt): {raw_excerpt}")
    return writeback


# ---------------------------------------------------------------------------
# Error helper — used for both failed parses and "no claims" bodies.
# ---------------------------------------------------------------------------

def _error_detail(raw: dict[str, Any]) -> str:
    """
    Reach into a Stedi error response and produce a human-readable
    one-liner for Monday's Claim Status Detail cell.
    """
    errors = raw.get("errors") or []
    if errors and isinstance(errors[0], dict):
        return (
            errors[0].get("description")
            or errors[0].get("message")
            or str(errors[0])
        )[:500]
    return ""


def error_writeback(reason: str) -> dict[str, Any]:
    """
    Produce the writeback used when we can't even talk to Stedi
    (validation errors, HTTP timeouts, etc).
    """
    return {
        "Claim Status Category":    "Error",
        "Claim Status Detail":      (reason or "Unknown error")[:500],
        "277 ICN":                  "",
        "277 Paid Amount":          0.0,
        "Last Claim Status Check":  _dt.date.today().isoformat(),
        "_category_code":           "",
        "_status_code":             "",
        "_n_claims_returned":       0,
    }
