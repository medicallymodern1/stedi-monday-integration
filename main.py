"""
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
import json

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from routes.monday_webhook import router as monday_router
from routes.stedi_webhook import router as stedi_router
from routes.eligibility_webhook import router as eligibility_router
from routes.order_webhook import router as order_router
from routes.claims_webhook import router as claims_router
from routes.intake_insurance_webhook import router as intake_insurance_router
from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday
from services.monday_service import populate_era_data_on_claims_item


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
app.include_router(order_router,  prefix="/order",  tags=["Order"])
app.include_router(claims_router, prefix="/claims", tags=["Claims"])
app.include_router(intake_insurance_router, prefix="/intake-insurance", tags=["Intake Insurance"])
app.include_router(eligibility_router, prefix="/eligibility", tags=["Eligibility"])


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
