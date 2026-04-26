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
                icn: str = "", description: str = "") -> dict:
    return {
        "claimStatus": {
            "statusCategoryCode": category,
            "statusCode":         code,
            "statusDescription":  description,
        },
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
        "F", "1",
        paid=1284.00,
        icn="ABC123",
        description="Paid as billed",
    )]}
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "Paid"
    assert out["277 Paid Amount"]       == 1284.00
    assert out["277 ICN"]               == "ABC123"
    assert "F1"          in out["Claim Status Detail"]
    assert "Paid"        in out["Claim Status Detail"]
    assert out["_n_claims_returned"]    == 1


def test_parse_denied_claim():
    raw = {"claims": [_fake_claim(
        "F", "2",
        paid=0.0,
        icn="DENY-9",
        description="Claim lacks information",
    )]}
    out = parse_claim_status_response(raw)
    assert out["Claim Status Category"] == "Denied"
    assert out["277 Paid Amount"] == 0.0
    assert out["277 ICN"] == "DENY-9"


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
        _fake_claim("A", "20", icn="OLD1",  description="Received"),
        _fake_claim("F", "1",  paid=500.0, icn="PAID1", description="Paid"),
        _fake_claim("P", "0",  icn="PEND1", description="Pending"),
    ]}
    out = parse_claim_status_response(raw)
    # Paid has the highest priority
    assert out["Claim Status Category"] == "Paid"
    assert out["277 ICN"]               == "PAID1"
    assert out["277 Paid Amount"]       == 500.0
    assert out["_n_claims_returned"]    == 3


def test_nested_claims_under_provider_level():
    raw = {
        "informationReceiverLevel": {
            "providerLevel": {
                "claims": [_fake_claim("P", "0", icn="NESTED1")]
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


if __name__ == "__main__":
    test_category_code_to_label()
    test_parse_paid_claim()
    test_parse_denied_claim()
    test_parse_no_claims_returned()
    test_parse_stedi_error_body()
    test_multiple_claims_picks_highest_priority()
    test_nested_claims_under_provider_level()
    test_error_writeback()
    print("all parser tests passed")
