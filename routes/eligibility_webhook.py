"""
routes/eligibility_webhook.py
Handles eligibility check requests triggered from Monday.
"""

import logging
import os
from fastapi import APIRouter, BackgroundTasks, Request
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)
router = APIRouter(tags=["Eligibility"])


@router.post("/check/{item_id}")
async def trigger_eligibility_check(item_id: str, background_tasks: BackgroundTasks):
    """
    Trigger a real-time eligibility check for a Monday Onboarding Board item.
    Returns 200 immediately; processes asynchronously.
    """
    logger.info(f"Eligibility check triggered for item_id={item_id}")
    background_tasks.add_task(run_eligibility_for_item, item_id)
    return JSONResponse({"status": "queued", "item_id": item_id}, status_code=200)


@router.post("/webhook")
async def eligibility_monday_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Receive Monday webhook trigger for eligibility check.
    Fires when status changes on the New Onboarding Board.
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    if "challenge" in body:
        return JSONResponse({"challenge": body["challenge"]})

    event = body.get("event", {})
    item_id = str(event.get("pulseId") or event.get("itemId") or "")
    new_label = event.get("value", {}).get("label", {}).get("text", "")

    logger.info(f"Eligibility webhook: label='{new_label}' | item={item_id}")

    # Trigger on "Check Eligibility" status — confirm label with Brandon
    if new_label in ("Check Eligibility", "Run Eligibility"):
        background_tasks.add_task(run_eligibility_for_item, item_id)
    else:
        logger.info(f"Ignored status: {new_label}")

    return JSONResponse({"status": "received"}, status_code=200)


async def run_eligibility_for_item(item_id: str) -> None:
    """
    Full eligibility pipeline for one Monday item:
    1. Fetch item from Monday Onboarding Board
    2. Extract required fields
    3. Build + send Stedi eligibility request
    4. Parse response
    5. Write results back to Monday
    """
    from services.monday_service import run_query
    from stedi_eligibility import run_parse_and_build_monday_writeback
    from stedi_eligibility_monday_mapping import extract_eligibility_inputs_from_monday_item
    from services.eligibility_monday_service import write_eligibility_to_monday

    logger.info(f"[ELG] Starting eligibility for item_id={item_id}")

    # Step 1: Fetch item from Monday Onboarding Board
    try:
        row = fetch_onboarding_item(item_id)
    except Exception as e:
        logger.error(f"[ELG] Failed to fetch Monday item: {e}")
        return

    logger.info(f"[ELG] Fetched item: {row.get('Name', item_id)}")

    # Step 2: Run eligibility + parse
    try:
        writeback_payload = run_parse_and_build_monday_writeback(
            row,
            api_key=os.getenv("STEDI_API_KEY"),
        )
        logger.info(f"[ELG] Eligibility completed: active={writeback_payload.get('Stedi Eligibility Active?')}")
    except ValueError as e:
        # Validation error — write error back to Monday
        logger.warning(f"[ELG] Validation error: {e}")
        writeback_payload = {
            "Stedi Eligibility Error Description": str(e)
        }
    except Exception as e:
        logger.error(f"[ELG] Eligibility failed: {e}", exc_info=True)
        writeback_payload = {
            "Stedi Eligibility Error Description": f"Eligibility check failed: {str(e)}"
        }

    # Step 3: Write results back to Monday
    try:
        write_eligibility_to_monday(item_id, writeback_payload)
        logger.info(f"[ELG] Results written to Monday item {item_id}")
    except Exception as e:
        logger.error(f"[ELG] Failed to write results to Monday: {e}", exc_info=True)


def fetch_onboarding_item(item_id: str) -> dict:
    """
    Fetch a New Onboarding Board item from Monday.
    Returns a flat dict matching Brandon's expected row format.
    """
    from services.monday_service import run_query

    query = """
    query GetItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values {
          id
          text
          value
        }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"No item found for item_id={item_id}")

    item = items[0]
    col_values = {col.get("id"): col.get("text", "") or "" for col in item.get("column_values", [])}

    # Map Monday column IDs → Brandon's expected field names
    # UPDATE THESE IDs once Brandon confirms them from the Onboarding Board
    ONBOARDING_COLUMN_MAP = {
        os.getenv("ELIG_COL_INSURANCE",  "color_insurance"):   "Primary Insurance Final",
        os.getenv("ELIG_COL_MEMBER_ID",  "text_member_id"):    "Member ID",
        os.getenv("ELIG_COL_FIRST_NAME", "text_first_name"):   "First Name",
        os.getenv("ELIG_COL_LAST_NAME",  "text_last_name"):    "Last Name",
        os.getenv("ELIG_COL_DOB",        "text_dob"):          "Patient Date of Birth",
    }

    row = {
        "Name":     item.get("name", ""),
        "Pulse ID": item_id,
    }
    for col_id, field_name in ONBOARDING_COLUMN_MAP.items():
        row[field_name] = col_values.get(col_id, "")

    logger.info(
        f"[ELG] Row extracted: "
        f"payer={row.get('Primary Insurance Final')} | "
        f"member={row.get('Member ID')} | "
        f"name={row.get('First Name')} {row.get('Last Name')}"
    )

    return row


