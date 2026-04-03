"""
routes/claims_webhook.py
Handles Claims Board webhook → submits claim to Stedi
Trigger: Claims Board submission status = "Submitted"
"""

import logging
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from services.monday_service import run_query
from services.claims_submission_service import submit_from_claims_board

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/webhook")
async def claims_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    if "challenge" in body:
        return JSONResponse({"challenge": body["challenge"]})

    background_tasks.add_task(handle_claims_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


async def handle_claims_event(body: dict):
    event     = body.get("event", {})
    item_id   = str(event.get("pulseId") or event.get("itemId") or "")
    new_label = event.get("value", {}).get("label", {}).get("text", "")

    logger.info(f"[CLAIMS] Status: '{new_label}' | item: {item_id}")

    if new_label != "Submitted":
        logger.info(f"[CLAIMS] Ignored — status is '{new_label}'")
        return

    try:
        await submit_from_claims_board(item_id)
    except Exception as e:
        logger.error(f"[CLAIMS] Submission failed: {e}", exc_info=True)