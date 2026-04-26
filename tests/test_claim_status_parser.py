"""
Unit tests for stedi_claim_status_parser.

Runs without hitting Stedi or Monday. Exercises the category/code
translation logic with minimal fabricated 277 response shapes that match
what Stedi returns in practice.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Make the repo root importable when running `python -m tests.test_claim_status_parser`
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from stedi_claim_status_parser import (  # noqa: E402
    parse_claim_status_response,
    error_writeback,
    _category_to_monday_label,
)


def _fake_claim(category: str, code: str, *, paid: float = 0.0,
                icn: str = "", description: str = "",
                cat_text: str = "", code_text: str = "") -> dict:
    """
    Build a minimal 277 claim envelope. Mirrors Stedi's real shape:
    money + ICN live INSIDE claimStatus (not at the outer level), and
    descriptive text uses statusCategoryCodeValue / statusCodeValue.
    The outer fallback fields are still populated so tests cover the
    legacy probe path in the parser too.
    """
    return {
        "claimStatus": {
            "statusCategoryCode":      category,
            "statusCategoryCodeValue": cat_text,
            "statusCode":              code,
            "statusCodeValue":         code_text,
            "statusDescription":       description,
            "amountPaid":              str(paid),
            "tradingPartnerClaimNumber": icn,
        },
        # Legacy/outer-level fields kept to confirm the fallback chain.
        "claimControlNumber":  icn,
        "claimPaymentAmount":  paid,
    }


def test_category_code_to_label():
    # Single-letter category (legacy/X12-stripped)
    assert _category_to_monday_label("F", "1") == "Paid"
    assert _category_to_monday_label("F", "F1") == "Paid"
    assert _category_to_monday_label("F", "2") == "Denied"
    assert _category_to_monday_label("F", "F2") == "Denied"
    assert _category_to_monday_label("F", "4") == "Denied"
    assert _category_to_monday_label("F", "3") == "In Process"
    assert _category_to_monday_label("P", "0") == "Pending"
    assert _category_to_monday_label("A", "1") == "Acknowledged"
    assert _category_to_monday_label("R", "0") == "Requests Info"
    assert _category_to_monday_label("E", "0") == "Error"
    assert _category_to_monday_label("", "") == "No Match"
    # 2-char X12 category codes (the format Stedi actually returns)
    assert _category_to_monday_label("F0", "")  == "In Process"
    assert _category_to_monday_label("F1", "")  == "Paid"
    assert _category_to_monday_label("F2", "")  == "Denied"
    assert _category_to_monday_label("F3", "")  == "In Process"
    assert _category_to_monday_label("F4", "")  == "Denied"
    assert _category_to_monday_label("A1", "20") == "Acknowledged"
    assert _category_to_monday_label("P1", "0")  == "Pending"
    assert _category_to_monday_label("R0", "0")  == "Requests Info"
    assert _category_to_monday_label("E0", "21") == "Error"
    # Lowercase category should still work
    assert _category_to_monday_label("e0", "21") == "Error"


def test_parse_paid_claim():
    raw = {"claims": [_fake_claim(
        "F1", "65",
        paid=1284.00,
        icn="ABC123",
        cat_text="Finalized/Payment - The claim/line has been paid.",
        code_text="Claim/line has been paid.",
    )]}
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "Paid"
    assert out["277 Paid Amount"]       == 1284.00
    assert out["277 ICN"]               == "ABC123"
    # Rich detail format: "[F1] Finalized/Payment - ... · [65] Claim/line has been paid."
    assert "[F1]"         in out["Claim Status Detail"]
    assert "[65]"         in out["Claim Status Detail"]
    assert "Finalized/Payment" in out["Claim Status Detail"]
    assert "has been paid"     in out["Claim Status Detail"]
    assert out["_n_claims_returned"]    == 1


def test_parse_denied_claim():
    raw = {"claims": [_fake_claim(
        "F2", "65",
        paid=0.0,
        icn="DENY-9",
        cat_text="Finalized/Denial - The claim/line has been denied.",
        code_text="Claim/line lacks information.",
    )]}
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "Denied"
    assert out["277 Paid Amount"] == 0.0
    assert out["277 ICN"] == "DENY-9"
    assert "Finalized/Denial" in out["Claim Status Detail"]


def test_parse_no_claims_returned():
    out = parse_claim_status_response({})
    assert out["Claim Status Category"] == "No Match"
    assert out["277 ICN"] == ""
    assert out["_n_claims_returned"] == 0


def test_parse_stedi_error_body():
    raw = {"errors": [{"description": "Payer offline"}]}
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "No Match"
    assert "Payer offline" in out["Claim Status Detail"]


def test_multiple_claims_picks_highest_priority():
    raw = {"claims": [
        _fake_claim("A1", "20", icn="OLD1",  cat_text="Acknowledgement"),
        _fake_claim("F1", "65", paid=500.0, icn="PAID1", cat_text="Finalized/Payment"),
        _fake_claim("P0", "247",icn="PEND1", cat_text="Pending"),
    ]}
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "Paid"
    assert out["277 ICN"]               == "PAID1"
    assert out["277 Paid Amount"]       == 500.0
    assert out["_n_claims_returned"]    == 3


def test_nested_claims_under_provider_level():
    raw = {
        "informationReceiverLevel": {
            "providerLevel": {
                "claims": [_fake_claim("P1", "247", icn="NESTED1",
                                       cat_text="Pending/In Process")]
            }
        }
    }
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "Pending"
    assert out["277 ICN"] == "NESTED1"


def test_error_writeback():
    out = error_writeback("timeout after 60s")
    assert out["Claim Status Category"] == "Error"
    assert "timeout" in out["Claim Status Detail"]
    assert out["277 Paid Amount"] == 0.0



def test_parse_real_anthem_response_shape():
    """Regression: actual Stedi 277 for Anthem (Seven Hanley-Creary)."""
    raw = {
        "claims": [{
            "claimStatus": {
                "amountPaid": "1125",
                "checkIssueDate": "2026-04-21",
                "checkNumber": "7706295087",
                "claimServiceDate": "20260407-20260407",
                "effectiveDate": "2026-04-11",
                "paidDate": "2026-04-11",
                "patientAccountNumber": "4O89ZYUC76T8IOINZ",
                "statusCategoryCode": "F1",
                "statusCategoryCodeValue": "Finalized/Payment - The claim/line has been paid.",
                "statusCode": "65",
                "statusCodeValue": "Claim/line has been paid.",
                "submittedAmount": "1125",
                "trackingNumber": "01KQ5CASRCN0XCCDX997JQ067C",
                "tradingPartnerClaimNumber": "2026100ET8632",
            }
        }],
    }
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "Paid"
    assert out["277 Paid Amount"]       == 1125.0
    assert out["277 ICN"]               == "2026100ET8632"
    assert "Finalized/Payment"          in out["Claim Status Detail"]
    assert "has been paid"              in out["Claim Status Detail"]
    assert out["_check_number"]         == "7706295087"
    assert out["_paid_date"]            == "2026-04-11"

if __name__ == "__main__":
    test_category_code_to_label()
    test_parse_paid_claim()
    test_parse_denied_claim()
    test_parse_no_claims_returned()
    test_parse_stedi_error_body()
    test_multiple_claims_picks_highest_priority()
    test_nested_claims_under_provider_level()
    test_error_writeback()
    test_parse_real_anthem_response_shape()
    print("all parser tests passed")
