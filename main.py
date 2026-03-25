"""
main.py - Medically Modern Stedi Integration
=============================================

FastAPI entry point for the entire integration.

FLOW:
  Monday (Order Board)
    → Webhook trigger (status → "Submit Claim")
    → Fetch order data from Monday API
    → Run through Brandon's claim builder (claim_infrastructure.py)
    → POST to Stedi API
    → Store 277 acknowledgement back in Monday Order Board
    → Stedi webhook fires when 835 ERA is ready
    → Parse ERA with EraParser.py
    → Populate Monday Claims Board

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

from pydantic import BaseModel
from typing import Any

class EraTestBody(BaseModel):
    claimPaymentInfo: Any
    patientName: Any
    serviceLines: Any

@app.post("/test/era")
async def test_era_parse(body: EraTestBody):
    """
    Test ERA parsing locally with a sample JSON.
    Paste RobertaDaley.json body here to test parsing.
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
    """
    Test writing ERA data to a specific Claims Board item.
    POST RobertaDaley.json body to this endpoint.
    Usage: POST /test/era-to-monday/YOUR_CLAIMS_ITEM_ID
    """
    from services.era_parser_service import parse_era_json, summarize_era_row_for_monday
    from services.monday_service import populate_era_data_on_claims_item

    body = await request.json()
    result = parse_era_json(body)
    summary = summarize_era_row_for_monday(result)
    populate_era_data_on_claims_item(claims_item_id, summary)

    return {
        "status": "written",
        "claims_item_id": claims_item_id,
        "fields_written": {k: v for k, v in summary.items() if k != "children"},
        "service_lines": len(summary.get("children", [])),
    }

@app.get("/test/claims-subitem-columns")
async def get_claims_subitem_columns():
    """Get subitem column IDs from Claims Board"""
    from services.monday_service import run_query
    import os

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
    Submits a real test claim to Stedi Test Payer.
    Triggers full flow: 277CA + 835 ERA via webhook.
    """
    from services.stedi_service import submit_claim
    import uuid

    pcn = str(uuid.uuid4()).replace("-", "")[:20].upper()

    payload = {
        "tradingPartnerName":      "Stedi Test Payer",
        "tradingPartnerServiceId": "STEDITEST",
        "usageIndicator":          "T",
        "submitter": {
            "organizationName":      "Mid-Island Medical Supply Company",
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
                "address1":    "2093 Wantagh Ave",
                "city":        "Wantagh",
                "state":       "NY",
                "postalCode":  "117930000"
            },
            "contactInformation": {
                "name":        "Billing Department",
                "phoneNumber": "3475037148"
            }
        },
        "subscriber": {
            "memberId":                        "TEST123456",
            "paymentResponsibilityLevelCode":  "P",
            "firstName":                       "John",
            "lastName":                        "TestPatient",
            "gender":                          "M",
            "dateOfBirth":                     "19800101",
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
        "note":           "Check logs in 2-3 minutes for 277CA and 835 webhook"
    }


@app.get("/test/subitem-titles/{item_id}")
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

@app.get("/test/order-board-columns")
async def get_order_board_columns():
    from services.monday_service import run_query
    import os
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
    query = """
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns { id title type }
      }
    }
    """
    result = run_query(query, {"boardId": board_id})
    cols = result.get("data", {}).get("boards", [{}])[0].get("columns", [])
    return [{"id": c["id"], "title": c["title"], "type": c["type"]} for c in cols]