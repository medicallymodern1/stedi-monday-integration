import logging
import json
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from services.monday_service import get_order_item, update_277_status, create_claims_board_item, update_claim_status, post_claim_update_to_monday, store_claim_pcn

from services.claim_builder_service import build_claims_from_monday_item
from services.stedi_service import submit_claim, get_277_acknowledgement

logger = logging.getLogger(__name__)
router = APIRouter()

COLUMN_MAP = {
    "status":               "Claim Status",
    "text_mm18zjmz":        "Gender",
    "text_mm187t6a":        "DOB",
    "phone_mm18rr9v":       "Phone",
    "location_mm187v29":    "Patient Address",
    "color_mm189t0b":       "Diagnosis Code",
    "color_mm18ds28":       "CGM Coverage",
    "text_mm18w2y4":        "Doctor Name",
    "text_mm18x1kj":        "Doctor NPI",
    "location_mm18qfed":    "Doctor Address",
    "phone_mm18t5ct":       "Doctor Phone",
    "color_mm18jhq5":       "Primary Insurance",
    "text_mm18s3fe":        "Member ID",
    "color_mm18h6yn":       "PR Payor",
    "text_mm18c6z4":        "Secondary ID",
    "color_mm18h05q":       "Subscription Type",
    "color_mm1bx9az":       "277 Status",
    "text_mm1b56xa":        "277 Rejected Reason",
}


@router.post("/webhook")
async def monday_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    if "challenge" in body:
        logger.info("Monday challenge received")
        return JSONResponse({"challenge": body["challenge"]})

    background_tasks.add_task(handle_event, body)
    return JSONResponse({"status": "received"}, status_code=200)

@router.get("/test-payer/{name}")
async def test_payer_lookup(name: str):
    """Test payer name lookup from Stedi directory"""
    from services.stedi_service import lookup_payer_name_by_internal
    result = lookup_payer_name_by_internal(name)
    return {"internal": name, "official": result}


async def handle_event(body: dict):
    event = body.get("event", {})
    item_id = str(event.get("pulseId") or event.get("itemId") or "")
    new_label = event.get("value", {}).get("label", {}).get("text", "")

    logger.info(f"Status: '{new_label}' | item: {item_id}")

    # if new_label != "Submit Claim":
    #     logger.info(f"Ignored — status is '{new_label}'")
    #     return

    # Handle both real and test claim submission
    if new_label == "Submit Claim":
        is_test = False
    elif new_label == "Test Claim Submitted":
        print("Test Claim Submitted")
        is_test = True
    else:
        logger.info(f"Ignored — status is '{new_label}'")
        return

    # Step 1: Fetch order
    logger.info(f"Step 1: Fetching order {item_id} | test={is_test}")
    try:
        order_data = get_order_item(item_id)
        log_order_data(order_data)
    except Exception as e:
        logger.error(f"Failed to fetch order: {e}", exc_info=True)
        return

    # Step 2: Build Stedi claim JSON
    logger.info("Step 2: Building Stedi claim JSON")
    try:
        stedi_payloads = build_claims_from_monday_item(order_data)
        if not stedi_payloads:
            logger.warning("No payloads generated")
            return
        logger.info(f"Built {len(stedi_payloads)} payload(s)")
    except Exception as e:
        logger.error(f"Failed to build claim: {e}", exc_info=True)
        return

    # Step 3: Submit each payload
    for i, payload in enumerate(stedi_payloads, 1):

        # ── If test mode: override payer to Stedi Test Payer ──────────
        if is_test:
            payload["tradingPartnerServiceId"] = "STEDITEST"
            payload["tradingPartnerName"] = "Stedi Test Payer"
            payload["receiver"] = {"organizationName": "Stedi"}
            payload["usageIndicator"] = "T"
            logger.info(f"TEST MODE: payload overridden to Stedi Test Payer")
        # ──────────────────────────────────────────────────────────────

        payer = payload.get("tradingPartnerName", "Unknown")
        logger.info(f"Step 3: Submitting payload #{i} | payer={payer}")

        try:
            stedi_response = submit_claim(payload)
            claim_id = stedi_response.get("claim_id", "")
            transaction_id = stedi_response.get("transaction_id", "")
            patient_control_number = stedi_response.get("patient_control_number", "")

            logger.info(f"Submitted: claim_id={claim_id} | pcn={patient_control_number}")

            # Update Order Board → Submitted
            try:
                update_claim_status(item_id=item_id, status="Submitted")
                logger.info("Order status → Submitted")
            except Exception as e:
                logger.warning(f"Status update failed: {e}")

            # ── ADD THIS: Store PCN + claim_id on Order Board ──────────────
            try:
                store_claim_pcn(
                    item_id=item_id,
                    pcn=patient_control_number,
                    claim_id=claim_id,
                )
            except Exception as e:
                logger.warning(f"PCN store failed: {e}")
            # ────────────────────────────────────────────────────────────────

            # ── ADD THIS: Post update to Monday item timeline ───────────────
            try:
                post_claim_update_to_monday(
                    item_id=item_id,
                    claim_id=claim_id,
                    payer=payer,
                    pcn=patient_control_number,
                )
            except Exception as e:
                logger.warning(f"Monday update post failed: {e}")
            # ────────────────────────────────────────────────────────────────

            # Parse inline 277 status
            inline_277_status = stedi_response.get("inline_277_status", "Pending")
            if inline_277_status != "Pending":
                try:
                    update_277_status(
                        item_id=item_id,
                        status=inline_277_status,
                        rejection_reason="",
                    )
                    logger.info(f"277 status → {inline_277_status}")
                except Exception as e:
                    logger.warning(f"277 update failed: {e}")

            # Create Claims Board item
            try:
                claims_item_id = create_claims_board_item(
                    order_item=order_data,
                    claim_id=claim_id,
                    payer_name=payer,
                )
                logger.info(f"Claims Board item created: {claims_item_id}")
            except Exception as e:
                logger.warning(f"Claims Board creation failed: {e}")

        except Exception as e:
            logger.error(f"Failed on payload #{i}: {e}", exc_info=True)


def log_order_data(order: dict):
    logger.info("=" * 60)
    logger.info("FULL ORDER DATA FROM MONDAY")
    logger.info(f"Item ID   : {order.get('id')}")
    logger.info(f"Item Name : {order.get('name')}")
    logger.info("--- Column Values ---")
    for col in order.get("column_values", []):
        col_id = col.get("id")
        col_name = COLUMN_MAP.get(col_id, col_id)
        col_value = col.get("text") or "empty"
        # logger.info(f"  {col_name:30} | {col_value}")
    logger.info("--- Sub Items ---")
    for sub in order.get("subitems", []):
        logger.info(f"  Subitem: {sub.get('name')}")
        for col in sub.get("column_values", []):
            if col.get("text"):
                logger.info(f"    {col.get('id'):30} | {col.get('text')}")
    logger.info("=" * 60)
