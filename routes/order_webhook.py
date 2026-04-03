"""
routes/order_webhook.py
Handles Order Board webhook → creates Claims Board items + subitems
Trigger: Order Status = "Process Claim" on New Order Board (no sub-items)
"""

import logging
from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse
from services.monday_service import run_query
from services.claim_board_service import create_claims_board_items_from_order

logger = logging.getLogger(__name__)
router = APIRouter()


@router.post("/webhook")
async def order_webhook(request: Request, background_tasks: BackgroundTasks):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"error": "invalid json"}, status_code=400)

    if "challenge" in body:
        return JSONResponse({"challenge": body["challenge"]})

    background_tasks.add_task(handle_order_event, body)
    return JSONResponse({"status": "received"}, status_code=200)


async def handle_order_event(body: dict):
    event     = body.get("event", {})
    item_id   = str(event.get("pulseId") or event.get("itemId") or "")
    new_label = event.get("value", {}).get("label", {}).get("text", "")

    logger.info(f"[ORDER] Status: '{new_label}' | item: {item_id}")

    if new_label != "Process Claim":
        logger.info(f"[ORDER] Ignored — status is '{new_label}'")
        return

    # Fetch order item from Monday
    try:
        order_item = get_order_item(item_id)
        logger.info(f"[ORDER] Fetched: {order_item.get('name')}")
    except Exception as e:
        logger.error(f"[ORDER] Failed to fetch order: {e}", exc_info=True)
        return

    # Create Claims Board items + subitems
    try:
        created = create_claims_board_items_from_order(order_item)
        logger.info(f"[ORDER] Created {len(created)} Claims Board item(s)")
    except Exception as e:
        logger.error(f"[ORDER] Failed to create Claims Board items: {e}", exc_info=True)


def get_order_item(item_id: str) -> dict:
    query = """
    query GetItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values { id text value }
      }
    }
    """
    result = run_query(query, {"itemId": item_id})
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"No item found for id={item_id}")
    return items[0]