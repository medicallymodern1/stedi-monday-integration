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
  MONDAY_INTAKE_BOARD_ID = Intake Board ID (for Stedi eligibility on intake)
  MONDAY_SUBSCRIPTION_BOARD_ID = Subscription Board ID (for Stedi eligibility on subscriptions; defaults to 18407459988)
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
from routes.subscription_eligibility_webhook import router as subscription_eligibility_router
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
app.include_router(subscription_eligibility_router, prefix="/subscription-eligibility", tags=["Subscription Eligibility"])


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

    return {"parsed": True, "summary": summary}


# ─── Reprocess ERA endpoint ───────────────────────────────────────────────────
# Accepts a raw X12-typed Stedi 835 JSON body (the kind you can copy-paste out
# of Stedi's dashboard or logs) and runs it through the exact same parse +
# PCN-match + populate_era_data_on_claims_item pipeline that handle_835_event
# runs when a real webhook fires. Use to replay ERAs that didn't land the
# first time because of parser/writer bugs now fixed.
from fastapi import Request, BackgroundTasks
import logging as _reprocess_logging

_reprocess_logger = _reprocess_logging.getLogger("reprocess_era")


def _reprocess_era_rows_background(era_rows: list) -> None:
    """
    Run the PCN-match + Monday writeback for each parsed ERA row.
    Invoked from BackgroundTasks so the webhook can ACK in <1s; all the
    heavy Monday API calls happen after the HTTP response is sent.
    Logs each outcome (watch Railway logs for the [REPROCESS] prefix).
    """
    from services.era_parser_service import summarize_era_row_for_monday
    from services.monday_service import populate_era_data_on_claims_item
    from routes.stedi_webhook import _find_claims_item_by_pcn

    for i, row in enumerate(era_rows, 1):
        parent = row.get("parent", {}) or {}
        pcn = parent.get("raw_patient_control_num", "") or ""
        paid = parent.get("primary_paid")

        try:
            claims_item_id = _find_claims_item_by_pcn(pcn) if pcn else ""
            if not claims_item_id:
                _reprocess_logger.warning(
                    f"[REPROCESS] ({i}/{len(era_rows)}) NO MATCH | "
                    f"pcn={pcn!r} paid={paid}"
                )
                continue

            summary = summarize_era_row_for_monday(row)
            populate_era_data_on_claims_item(claims_item_id, summary)

            carc = [c.get("Parsed CARC Codes", "") for c in row.get("children", [])]
            rarc = [c.get("Parsed RARC Codes", "") for c in row.get("children", [])]
            _reprocess_logger.info(
                f"[REPROCESS] ({i}/{len(era_rows)}) WROTE | "
                f"pcn={pcn!r} item={claims_item_id} paid={paid} "
                f"carc={carc} rarc={rarc}"
            )
        except Exception as e:
            _reprocess_logger.error(
                f"[REPROCESS] ({i}/{len(era_rows)}) ERROR | "
                f"pcn={pcn!r}: {e}",
                exc_info=True,
            )


@app.post("/test/reprocess-era", tags=["Testing"])
async def reprocess_era_json(request: Request, background_tasks: BackgroundTasks):
    """
    POST the raw 835 ERA JSON as the request body. We parse it synchronously
    (fast), then kick off the PCN-match + Monday writeback as a background
    task so the response returns in ~100ms instead of blocking for ~1 min
    while we hammer the Monday API with 80+ sequential writes.

    Response tells you what was queued; watch Railway logs with
    [REPROCESS] prefix for per-claim match/write outcomes.
    """
    raw = await request.body()
    if not raw:
        return {"status": "error", "message": "empty body"}

    try:
        era_content = raw.decode("utf-8")
    except UnicodeDecodeError as e:
        return {"status": "error", "message": f"body is not UTF-8: {e}"}

    from services.era_parser_service import parse_era_from_string

    era_rows = parse_era_from_string(era_content)
    if not era_rows:
        return {
            "status": "parse_empty",
            "message": "parser returned 0 rows — check format",
        }

    # Synchronous preview so the caller gets an immediate confirmation
    # of what will be processed. No Monday calls here; zero-latency.
    preview = []
    for row in era_rows:
        parent = row.get("parent", {}) or {}
        preview.append({
            "pcn":  parent.get("raw_patient_control_num", ""),
            "paid": parent.get("primary_paid"),
            "lines": [
                {
                    "hcpc":          c.get("HCPC Code", ""),
                    "carc":          c.get("Parsed CARC Codes", ""),
                    "rarc":          c.get("Parsed RARC Codes", ""),
                    "adj_codes":     c.get("Parsed Adjustment Codes", ""),
                    "adj_reasons":   c.get("Parsed Adjustment Reasons", ""),
                    "remark_text":   c.get("Parsed Remark Text", ""),
                    "allowed":       c.get("Raw Allowed Actual"),
                }
                for c in row.get("children", [])
            ],
        })

    # Fire-and-forget the actual Monday writes.
    background_tasks.add_task(_reprocess_era_rows_background, era_rows)

    return {
        "status": "queued",
        "claims_queued": len(era_rows),
        "preview": preview,
        "message": (
            "Parsing succeeded; Monday writes happening asynchronously. "
            "Watch Railway logs for [REPROCESS] entries to see each per-claim "
            "outcome. Check the Claims Board in ~60-120s."
        ),
    }
