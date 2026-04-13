"""
routes/intake_insurance_webhook.py — Intake Board insurance resolution webhook.
================================================================================

Receives Monday.com webhooks when Insurance Plan, Primary Insurance, or Serving
changes on the Intake Board, then runs the full insurance resolution pipeline
and writes derived values back.

Loop prevention:
  • Skips events where the changed column is one of our OUTPUT columns
  • Tracks in-memory set of item IDs currently being processed
  • Responds 200 immediately and processes in background
"""

import json
import logging
import os
import time

from fastapi import APIRouter, Request, BackgroundTasks
from fastapi.responses import JSONResponse

from services.monday_service import run_query
from intake_insurance_classifier import classify_primary_insurance
from intake_insurance_resolver import (
    resolve_intake_fields,
    ALL_OUTPUT_COLUMN_IDS,
    TRIGGER_COLUMN_IDS,
    COL_PRIMARY_INSURANCE,
    COL_STEDI_COVERAGE_TYPE,
    COL_STEDI_PAYER_NAME,
    COL_STEDI_PLAN_NAME,
    COL_MEMBER_ID_1,
)

logger = logging.getLogger(__name__)
router = APIRouter()

# In-memory set of item IDs currently being processed (loop prevention)
_processing_items = set()


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _get_item_fields(item_id):
    """Fetch all column values for an Intake Board item. Returns {col_id: text}."""
    query = """
    query ($itemId: [ID!]!) {
        items(ids: $itemId) {
            id
            name
            column_values { id text value type }
        }
    }
    """
    result = run_query(query, {"itemId": [str(item_id)]})
    items = result.get("data", {}).get("items", [])
    if not items:
        logger.warning("[INTAKE-INS] No item found for ID %s", item_id)
        return {}

    item = items[0]
    fields = {"_name": item.get("name", "")}
    for col in item.get("column_values", []):
        fields[col["id"]] = col.get("text") or ""
    return fields


def _write_columns(board_id, item_id, column_values):
    """Write multiple column values to a board item."""
    if not column_values:
        return

    # Build the column_values dict for the mutation.
    # Status columns (color_*) need {"label": "value"} format.
    # Numeric columns (numeric_*) need just the number string.
    # Text columns (text_*) need just the string.
    formatted = {}
    for col_id, value in column_values.items():
        if value == "" or value is None:
            formatted[col_id] = ""
        elif col_id.startswith("color_"):
            formatted[col_id] = {"label": value}
        elif col_id.startswith("numeric_"):
            # Strip non-numeric chars for safety, but keep decimals
            clean = value.replace("$", "").replace(",", "").replace("%", "").strip()
            try:
                formatted[col_id] = str(float(clean)) if clean else ""
            except ValueError:
                formatted[col_id] = ""
                logger.warning("[INTAKE-INS] Cannot convert '%s' to number for %s", value, col_id)
        else:
            formatted[col_id] = value

    col_json = json.dumps(formatted)

    mutation = """
    mutation ($boardId: ID!, $itemId: ID!, $columnValues: JSON!) {
        change_multiple_column_values(
            board_id: $boardId,
            item_id: $itemId,
            column_values: $columnValues
        ) { id }
    }
    """
    try:
        run_query(mutation, {
            "boardId": str(board_id),
            "itemId": str(item_id),
            "columnValues": col_json,
        })
        logger.info("[INTAKE-INS] Wrote %d columns to item %s", len(column_values), item_id)
    except Exception:
        logger.exception("[INTAKE-INS] Failed to write columns to item %s", item_id)


# ═══════════════════════════════════════════════════════════════════════════════
# Core processing
# ═══════════════════════════════════════════════════════════════════════════════

def _process_intake_insurance(item_id, board_id, column_id=""):
    """Run the full insurance resolution pipeline for a single item."""
    if item_id in _processing_items:
        logger.debug("[INTAKE-INS] Skipping item %s — already processing", item_id)
        return
    _processing_items.add(item_id)
    start = time.time()

    try:
        # Read all fields
        fields = _get_item_fields(item_id)
        if not fields:
            logger.warning("[INTAKE-INS] Could not read item %s", item_id)
            return

        logger.info("[INTAKE-INS] Processing item %s (%s) trigger=%s",
                     item_id, fields.get("_name", ""), column_id or "manual")

        # Run LLM classifier if Primary Insurance is empty
        primary_ins = (fields.get(COL_PRIMARY_INSURANCE) or "").strip()
        if not primary_ins:
            logger.info("[INTAKE-INS] Primary Insurance empty — running classifier")
            classified = classify_primary_insurance(
                stedi_coverage_type=fields.get(COL_STEDI_COVERAGE_TYPE, ""),
                stedi_payer_name=fields.get(COL_STEDI_PAYER_NAME, ""),
                stedi_plan_name=fields.get(COL_STEDI_PLAN_NAME, ""),
                member_id=fields.get(COL_MEMBER_ID_1, ""),
            )
            if classified:
                logger.info("[INTAKE-INS] Classified as: %s", classified)
                fields[COL_PRIMARY_INSURANCE] = classified
                _write_columns(board_id, item_id, {COL_PRIMARY_INSURANCE: classified})
            else:
                logger.info("[INTAKE-INS] Could not classify — skipping resolution")
                return

        # Resolve all derived fields
        output, log_lines = resolve_intake_fields(fields)

        for line in log_lines:
            logger.info("[INTAKE-INS] %s", line)

        if not output:
            logger.info("[INTAKE-INS] No output produced for item %s", item_id)
            return

        # Write back to Monday
        _write_columns(board_id, item_id, output)

        elapsed = time.time() - start
        logger.info("[INTAKE-INS] Item %s resolved in %.2fs — %d columns written",
                     item_id, elapsed, len(output))

    except Exception:
        logger.exception("[INTAKE-INS] Error processing item %s", item_id)
    finally:
        _processing_items.discard(item_id)


# ═══════════════════════════════════════════════════════════════════════════════
# Routes
# ═══════════════════════════════════════════════════════════════════════════════

@router.post("/webhook")
async def intake_insurance_webhook(request: Request, background_tasks: BackgroundTasks):
    """
    Monday.com webhook endpoint for the Intake Board.
    Triggers when Insurance Plan, Primary Insurance, or Serving changes.
    """
    body = await request.json()

    # Challenge handshake
    if "challenge" in body:
        logger.info("[INTAKE-INS] Responding to challenge")
        return JSONResponse({"challenge": body["challenge"]})

    event = body.get("event", body)
    item_id   = str(event.get("pulseId") or event.get("itemId") or "")
    board_id  = str(event.get("boardId") or os.getenv("MONDAY_INTAKE_BOARD_ID", "18406352652"))
    column_id = event.get("columnId", "")

    if not item_id:
        return JSONResponse({"status": "skipped", "reason": "no item ID"})

    # Loop prevention: skip our own writes
    if column_id and column_id in ALL_OUTPUT_COLUMN_IDS:
        logger.debug("[INTAKE-INS] Skipping output column %s", column_id)
        return JSONResponse({"status": "skipped", "reason": "output column"})

    # Only trigger on relevant columns (if columnId is provided)
    if column_id and column_id not in TRIGGER_COLUMN_IDS:
        logger.debug("[INTAKE-INS] Ignoring non-trigger column %s", column_id)
        return JSONResponse({"status": "skipped", "reason": "non-trigger column"})

    logger.info("[INTAKE-INS] Webhook received: item=%s column=%s", item_id, column_id)
    background_tasks.add_task(_process_intake_insurance, item_id, board_id, column_id)
    return JSONResponse({"status": "received"})


@router.post("/run/{item_id}")
async def intake_insurance_manual_run(item_id: str):
    """
    Manual trigger: run insurance resolution for a specific Intake Board item.
    Synchronous — returns the full result for debugging.
    """
    board_id = os.getenv("MONDAY_INTAKE_BOARD_ID", "18406352652")

    fields = _get_item_fields(item_id)
    if not fields:
        return JSONResponse({"status": "error", "reason": "item not found"})

    # Run classifier if needed
    primary_ins = (fields.get(COL_PRIMARY_INSURANCE) or "").strip()
    if not primary_ins:
        classified = classify_primary_insurance(
            stedi_coverage_type=fields.get(COL_STEDI_COVERAGE_TYPE, ""),
            stedi_payer_name=fields.get(COL_STEDI_PAYER_NAME, ""),
            stedi_plan_name=fields.get(COL_STEDI_PLAN_NAME, ""),
            member_id=fields.get(COL_MEMBER_ID_1, ""),
        )
        if classified:
            fields[COL_PRIMARY_INSURANCE] = classified
            _write_columns(board_id, item_id, {COL_PRIMARY_INSURANCE: classified})

    output, log_lines = resolve_intake_fields(fields)

    if output:
        _write_columns(board_id, item_id, output)

    return {
        "status": "success",
        "item_id": item_id,
        "item_name": fields.get("_name", ""),
        "primary_insurance": fields.get(COL_PRIMARY_INSURANCE, ""),
        "columns_written": len(output),
        "log": log_lines,
        "output": output,
    }


@router.post("/test/{item_id}")
async def intake_insurance_dry_run(item_id: str):
    """
    Dry run: compute insurance resolution but do NOT write to Monday.
    Use for debugging before going live.
    """
    fields = _get_item_fields(item_id)
    if not fields:
        return JSONResponse({"status": "error", "reason": "item not found"})

    primary_ins = (fields.get(COL_PRIMARY_INSURANCE) or "").strip()
    classified_as = None
    if not primary_ins:
        classified_as = classify_primary_insurance(
            stedi_coverage_type=fields.get(COL_STEDI_COVERAGE_TYPE, ""),
            stedi_payer_name=fields.get(COL_STEDI_PAYER_NAME, ""),
            stedi_plan_name=fields.get(COL_STEDI_PLAN_NAME, ""),
            member_id=fields.get(COL_MEMBER_ID_1, ""),
        )
        if classified_as:
            fields[COL_PRIMARY_INSURANCE] = classified_as

    output, log_lines = resolve_intake_fields(fields)

    return {
        "status": "dry_run",
        "item_id": item_id,
        "item_name": fields.get("_name", ""),
        "primary_insurance": fields.get(COL_PRIMARY_INSURANCE, ""),
        "classified_by_llm": classified_as,
        "columns_would_write": len(output),
        "log": log_lines,
        "output": output,
        "note": "DRY RUN — nothing was written to Monday.",
    }
