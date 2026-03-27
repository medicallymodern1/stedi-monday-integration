"""
main.py - Medically Modern Stedi Integration
=============================================

FastAPI entry point for the entire integration.

FLOW:
  Monday (Order Board)
    → Webhook trigger (status → "Submit Claim" or "Test Claim Submitted")
    → Fetch order data from Monday API
    → Run through Brandon's claim builder (claim_infrastructure.py)
    → POST to Stedi API
    → Store claim_id + post comment back in Monday Order Board
    → Stedi webhook fires when 277CA is ready → update 277 Status on Order Board
    → Stedi webhook fires when 835 ERA is ready
    → Parse ERA with era_parser_service.py
    → Populate Monday Claims Board parent + service line subitems

HOW TO RUN:
  uvicorn main:app --reload --port 5000

ENVIRONMENT VARIABLES (set in .env):
  MONDAY_API_TOKEN       = your Monday.com API token
  MONDAY_ORDER_BOARD_ID  = New Order Board ID
  MONDAY_CLAIMS_BOARD_ID = Claims Board ID
  STEDI_API_KEY          = your Stedi API key
  STEDI_CLAIM_ENDPOINT   = Stedi claim submission URL
  WEBHOOK_SECRET         = Monday webhook signing secret
  PORT                   = port to listen on (default: 5000)
"""

import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, Request

from routes.monday_webhook import router as monday_router
from routes.stedi_webhook import router as stedi_router

# ─── Load env ─────────────────────────────────────────────────────────────────
load_dotenv()

# ─── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# ─── App ──────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="Medically Modern – Stedi Integration",
    description="Automates the full insurance claims lifecycle: Monday → Stedi → Monday",
    version="1.0.0",
)

# ─── Routers ──────────────────────────────────────────────────────────────────
app.include_router(monday_router, prefix="/monday", tags=["Monday"])
app.include_router(stedi_router, prefix="/stedi", tags=["Stedi"])


# ─── Health check ─────────────────────────────────────────────────────────────
@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok"}


# ─── ERA test endpoints ───────────────────────────────────────────────────────
from pydantic import BaseModel
from typing import Any

class EraTestBody(BaseModel):
    claimPaymentInfo: Any
    patientName: Any
    serviceLines: Any

@app.post("/test/era", tags=["Testing"])
async def test_era_parse(body: EraTestBody):
    """
    Test ERA parsing locally with a sample JSON.
    Paste a single-claim ERA body here to test parsing.
    """
    from services.era_parser_service import parse_era_json, summarize_era_row_for_monday
    era_json = body.dict()
    result = parse_era_json(era_json)
    summary = summarize_era_row_for_monday(result)
    return {
        "parent": result["parent"],
        "children_count": len(result["children"]),
        "children": result["children"],
        "monday_summary": summary,
    }


@app.post("/test/era-to-monday/{claims_item_id}", tags=["Testing"])
async def test_era_to_monday(claims_item_id: str, request: Request):
    from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday
    from services.monday_service import populate_era_data_on_claims_item
    import json

    body = await request.body()
    era_rows = parse_era_from_string(body.decode())

    if not era_rows:
        return {"error": "No rows parsed — check JSON format"}

    # Write first row to the specified claims item
    # (use all rows if you want to write multiple)
    era_row = era_rows[0]
    summary = summarize_era_row_for_monday(era_row)
    populate_era_data_on_claims_item(claims_item_id, summary)

    return {
        "status": "written",
        "claims_item_id": claims_item_id,
        "rows_parsed": len(era_rows),
        "fields_written": {k: v for k, v in summary.items() if k != "children"},
        "service_lines": len(summary.get("children", [])),
    }


@app.post("/test/835-sample", tags=["Testing"])
async def test_835_sample(request: Request):
    """
    Test 835 parsing with a manually pasted Stedi sample JSON.

    Use this to verify the parser works BEFORE a live 835 arrives.
    Paste the raw Stedi 835 JSON from their docs as the request body.

    Handles both formats:
      - Flat single-claim:  { "claimPaymentInfo": {...}, "serviceLines": [...] }
      - Full 835 envelope:  { "financialInformation": {...}, "claimPayments": [...] }

    Does NOT write to Monday — just returns parsed output for inspection.
    Usage: POST /test/835-sample  with raw Stedi 835 JSON as body
    """
    from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday

    body = await request.body()
    era_content = body.decode()

    era_rows = parse_era_from_string(era_content)

    if not era_rows:
        return {
            "error": "No rows parsed",
            "hint": "Check JSON format — must be valid Stedi 835 JSON",
            "raw_preview": era_content[:500],
        }

    results = []
    for era_row in era_rows:
        parent = era_row.get("parent", {})
        summary = summarize_era_row_for_monday(era_row)
        results.append({
            "parent_summary": {k: v for k, v in summary.items() if k != "children"},
            "children_count": len(era_row.get("children", [])),
            "children": era_row.get("children", []),
        })

    return {
        "parsed_rows": len(results),
        "results": results,
    }


@app.post("/test/835/{transaction_id}", tags=["Testing"])
async def test_835_manual(transaction_id: str):
    """
    Manually trigger 835 ERA processing for a given Stedi transaction ID.

    Use this when:
    - You want to test without waiting for the webhook to fire
    - The webhook fired but you want to re-run the ERA processing
    - You're debugging why the Claims Board wasn't populated

    Steps:
    1. Go to Stedi dashboard → Transactions
    2. Find the 835 transaction → copy the transaction ID
    3. POST to /test/835/{transaction_id}

    This fetches the ERA, parses it, finds the Claims Board item by PCN,
    and writes all fields — exactly the same as the live webhook flow.

    Usage: POST /test/835/YOUR_TRANSACTION_ID
    """
    from services.stedi_service import get_era_as_835_file
    from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday
    from routes.stedi_webhook import _find_claims_item_by_pcn, _find_claims_item_by_correlation_id
    from services.monday_service import populate_era_data_on_claims_item

    # Step 1: Fetch
    era_content = get_era_as_835_file(transaction_id)
    if not era_content:
        return {"error": "Empty ERA response from Stedi — check transaction_id"}

    # Step 2: Parse
    era_rows = parse_era_from_string(era_content)
    if not era_rows:
        return {
            "error": "No rows parsed from ERA",
            "raw_preview": era_content[:1000],
        }

    results = []
    for era_row in era_rows:
        parent = era_row.get("parent", {})
        pcn    = parent.get("raw_patient_control_num", "")
        summary = summarize_era_row_for_monday(era_row)

        # Step 3: Find Claims Board item (same logic as webhook)
        claims_item_id = _find_claims_item_by_correlation_id(transaction_id)
        if not claims_item_id:
            claims_item_id = _find_claims_item_by_pcn(pcn)

        # Step 4: Write to Monday
        if claims_item_id:
            populate_era_data_on_claims_item(claims_item_id, summary)

        results.append({
            "pcn":               pcn,
            "claims_item_id":    claims_item_id or "NOT FOUND — check PCN matches Claims Board",
            "primary_paid":      parent.get("primary_paid"),
            "pr_amount":         parent.get("pr_amount"),
            "paid_date":         parent.get("paid_date"),
            "check_number":      parent.get("check_number"),
            "children_count":    len(era_row.get("children", [])),
            "written_to_monday": bool(claims_item_id),
        })

    return {
        "transaction_id": transaction_id,
        "era_rows_found": len(results),
        "results": results,
    }


# ─── Debug / utility endpoints ────────────────────────────────────────────────

@app.get("/test/claims-subitem-columns", tags=["Debug"])
async def get_claims_subitem_columns():
    """Get subitem column IDs from Claims Board"""
    from services.monday_service import run_query
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    query = """
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        items_page(limit: 1) {
          items {
            subitems {
              id
              column_values {
                id
                title: id
                type
              }
            }
          }
        }
      }
    }
    """
    result = run_query(query, {"boardId": claims_board_id})
    return result


@app.post("/submit-test-claim", tags=["Testing"])
async def submit_test_claim():
    """
    Submits a hardcoded test claim directly to Stedi Test Payer.
    Bypasses Monday entirely — useful for testing the Stedi connection.
    Triggers full flow: 277CA + 835 ERA via webhook within 2-3 minutes.
    """
    from services.stedi_service import submit_claim
    import uuid

    pcn = str(uuid.uuid4()).replace("-", "")[:20].upper()

    payload = {
        "tradingPartnerName":      "Stedi Test Payer",
        "tradingPartnerServiceId": "STEDITEST",
        "usageIndicator":          "T",
        "submitter": {
            "organizationName":        "Mid-Island Medical Supply Company",
            "submitterIdentification": "113254896",
            "contactInformation": {
                "name":        "Billing Department",
                "phoneNumber": "3475037148"
            }
        },
        "receiver": {
            "organizationName": "Stedi"
        },
        "billing": {
            "providerType":     "BillingProvider",
            "npi":              "1023042348",
            "employerId":       "113254896",
            "taxonomyCode":     "332B00000X",
            "organizationName": "Mid-Island Medical Supply Company",
            "address": {
                "address1":   "2093 Wantagh Ave",
                "city":       "Wantagh",
                "state":      "NY",
                "postalCode": "117930000"
            },
            "contactInformation": {
                "name":        "Billing Department",
                "phoneNumber": "3475037148"
            }
        },
        "subscriber": {
            "memberId":                       "TEST123456",
            "paymentResponsibilityLevelCode": "P",
            "firstName":                      "John",
            "lastName":                       "TestPatient",
            "gender":                         "M",
            "dateOfBirth":                    "19800101",
            "address": {
                "address1":   "123 Test St",
                "city":       "Brooklyn",
                "state":      "NY",
                "postalCode": "112210000"
            }
        },
        "claimInformation": {
            "claimFilingCode":                          "CI",
            "patientControlNumber":                     pcn,
            "claimChargeAmount":                        "500.00",
            "placeOfServiceCode":                       "12",
            "claimFrequencyCode":                       "1",
            "signatureIndicator":                       "Y",
            "planParticipationCode":                    "A",
            "benefitsAssignmentCertificationIndicator": "Y",
            "releaseInformationCode":                   "Y",
            "healthCareCodeInformation": [
                {
                    "diagnosisTypeCode": "ABK",
                    "diagnosisCode":     "E10.65"
                }
            ],
            "serviceLines": [
                {
                    "serviceDate": "20260101",
                    "professionalService": {
                        "procedureIdentifier":  "HC",
                        "procedureCode":        "A4239",
                        "lineItemChargeAmount": "500.00",
                        "measurementUnit":      "UN",
                        "serviceUnitCount":     "1",
                        "compositeDiagnosisCodePointers": {
                            "diagnosisCodePointers": ["1"]
                        }
                    },
                    "providerControlNumber": "TESTLINE001"
                }
            ]
        }
    }

    result = submit_claim(payload)
    return {
        "status":         "submitted",
        "pcn":            pcn,
        "claim_id":       result.get("claim_id"),
        "transaction_id": result.get("transaction_id"),
        "note":           "Check Railway logs in 2-3 minutes for 277CA and 835 webhook"
    }


@app.get("/test/subitem-titles/{item_id}", tags=["Debug"])
async def get_subitem_titles(item_id: str):
    """Get subitem column titles for a specific item"""
    from services.monday_service import run_query
    query = """
    query ($itemId: ID!) {
      items(ids: [$itemId]) {
        subitems {
          id
          name
          column_values {
            id
            text
            value
          }
        }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    return result


@app.get("/test/order-board-columns", tags=["Debug"])
async def get_order_board_columns():
    """Get all column IDs and types from the Order Board"""
    from services.monday_service import run_query
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
    query = """
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns { id title type settings_str }
      }
    }
    """
    result = run_query(query, {"boardId": board_id})
    cols = result.get("data", {}).get("boards", [{}])[0].get("columns", [])
    return [{"id": c["id"], "title": c["title"], "type": c["type"]} for c in cols]


@app.get("/test/order-status-settings", tags=["Debug"])
async def get_order_status_settings():
    """
    Returns the Claim Status column settings including all label indexes.
    Use this to confirm CLAIM_STATUS_TO_INDEX values in monday_service.py.
    """
    from services.monday_service import get_column_settings
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
    result = get_column_settings(board_id, "status")
    return result