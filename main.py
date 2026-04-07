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
import json

from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from routes.monday_webhook import router as monday_router
from routes.stedi_webhook import router as stedi_router
from routes.eligibility_webhook import router as eligibility_router
from routes.order_webhook import router as order_router
from routes.claims_webhook import router as claims_router
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
    print(era_rows)

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

@app.post("/test/create-claims-from-order/{item_id}", tags=["Testing"])
async def test_create_claims_from_order(item_id: str):
    """
    Stage A test: reads Order Board item → creates Claims Board items + subitems
    without needing the Monday webhook trigger.
    """
    from routes.order_webhook import get_order_item
    from services.claim_board_service import create_claims_board_items_from_order

    order_item = get_order_item(item_id)
    created = create_claims_board_items_from_order(order_item)
    return {"created_claims_board_items": created}


@app.post("/test/submit-from-claims/{item_id}", tags=["Testing"])
async def test_submit_from_claims(item_id: str):
    """
    Stage B test: reads Claims Board item + subitems → builds payload → submits to Stedi.
    Use this before setting up the Monday webhook trigger.
    """
    from services.claims_submission_service import submit_from_claims_board
    await submit_from_claims_board(item_id)
    return {"status": "submitted", "claims_item_id": item_id}


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

@app.post("/era/{claims_item_id}")
async def test_era_to_monday(claims_item_id: str, request: Request):
    """
    Paste a Stedi 835 JSON body and it will:
    1. Parse it using your existing parse_era_from_string()
    2. Write parent + subitem columns using populate_era_data_on_claims_item()
    3. Return what was parsed so you can verify every field
    """

    # Step 1: Read body
    try:
        raw = await request.body()
        era_json = json.loads(raw)
    except Exception as e:
        return JSONResponse({"error": f"Invalid JSON: {e}"}, status_code=400)

    # Step 2: Parse using your existing parser
    era_rows = parse_era_from_string(json.dumps(era_json))

    if not era_rows:
        return JSONResponse({
            "error": "Parsing returned no rows. Check JSON format.",
            "hint": "Expected 'transactions' key (Stedi API) or 'claimPaymentInfo' key (flat)"
        }, status_code=422)

    logger.info(f"[ERA TEST] Parsed {len(era_rows)} row(s) — writing to item {claims_item_id}")

    results = []

    # Step 3: Write each row to Monday using your existing function
    for i, era_row in enumerate(era_rows, 1):
        summary = summarize_era_row_for_monday(era_row)
        try:
            populate_era_data_on_claims_item(claims_item_id, summary)
            logger.info(f"[ERA TEST] Row {i} written to Monday item {claims_item_id}")
            status = "written_to_monday"
        except Exception as e:
            logger.error(f"[ERA TEST] Row {i} failed: {e}", exc_info=True)
            status = f"error: {e}"

        results.append({
            "row":    i,
            "status": status,
            # Show exactly what was parsed so you can verify each field
            "parent_fields_written": {
                "raw_patient_control_num":    summary.get("raw_patient_control_num"),
                "raw_payer_claim_control":    summary.get("raw_payer_claim_control"),
                "raw_total_claim_charge":     summary.get("raw_total_claim_charge"),
                "raw_remittance_trace":       summary.get("raw_remittance_trace"),
                "raw_patient_responsibility": summary.get("raw_patient_responsibility"),
                "raw_era_date":               summary.get("raw_era_date"),
                "raw_era_claim_status":       summary.get("raw_era_claim_status"),
                "primary_paid":               summary.get("primary_paid"),
                "pr_amount":                  summary.get("pr_amount"),
                "primary_status":             summary.get("primary_status"),
                "paid_date":                  summary.get("paid_date"),
                "check_number":               summary.get("check_number"),
            },
            "service_lines_count": len(summary.get("children", [])),
            "service_lines": summary.get("children", []),
        })

    return JSONResponse({
        "claims_item_id": claims_item_id,
        "rows_parsed":    len(era_rows),
        "results":        results,
    })

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

@app.get("/test/claims-board-columns", tags=["Debug"])
async def get_claims_board_columns():
    """Get all parent and subitem column IDs and titles from Claims Board"""
    from services.monday_service import run_query
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    # Step 1: Get parent columns
    parent_query = """
    query GetParentColumns($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns {
          id
          title
          type
        }
      }
    }
    """
    result = run_query(parent_query, {"boardId": claims_board_id})
    parent_columns = (
        result.get("data", {})
        .get("boards", [{}])[0]
        .get("columns", [])
    )

    # Step 2: Find any item that has subitems, then read subitem board columns
    items_query = """
    query GetItemsWithSubitems($boardId: ID!) {
      boards(ids: [$boardId]) {
        items_page(limit: 100) {
          items {
            id
            name
            subitems {
              id
              board {
                columns {
                  id
                  title
                  type
                }
              }
            }
          }
        }
      }
    }
    """
    items_result = run_query(items_query, {"boardId": claims_board_id})
    items = (
        items_result.get("data", {})
        .get("boards", [{}])[0]
        .get("items_page", {})
        .get("items", [])
    )

    subitem_columns = []
    source_item = None
    for item in items:
        if item.get("subitems"):
            subitem_columns = (
                item["subitems"][0]
                .get("board", {})
                .get("columns", [])
            )
            source_item = {"id": item["id"], "name": item["name"]}
            break

    return {
        "parent_columns": [
            {"id": c["id"], "title": c["title"], "type": c["type"]}
            for c in parent_columns
        ],
        "subitem_columns": [
            {"id": c["id"], "title": c["title"], "type": c["type"]}
            for c in subitem_columns
        ],
        "subitem_columns_sourced_from_item": source_item,
        "note": "No subitems found on any item — subitem_columns will be empty" if not subitem_columns else None,
    }

@app.get("/test/claims-columns", tags=["Debug"])
async def get_claims_columns():
    from services.monday_service import run_query
    import os

    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    query = """
    query ($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns {
          id
          title
          type
        }
        items_page(limit: 5) {
          items {
            subitems {
              column_values {
                id
                type
              }
            }
          }
        }
      }
    }
    """

    result = run_query(query, {"boardId": claims_board_id})

    boards = result.get("data", {}).get("boards", [])
    if not boards:
        return {"error": "Board not found"}

    board = boards[0]

    # ✅ Parent columns (correct)
    parent_columns = board.get("columns", [])

    # ✅ Extract subitem columns dynamically
    subitem_column_map = {}

    items = board.get("items_page", {}).get("items", [])
    for item in items:
        for sub in item.get("subitems", []):
            for col in sub.get("column_values", []):
                col_id = col.get("id")
                col_type = col.get("type")

                if col_id and col_id not in subitem_column_map:
                    subitem_column_map[col_id] = {
                        "id": col_id,
                        "title": col_id,  # fallback (real title not available here)
                        "type": col_type
                    }

    subitem_columns = list(subitem_column_map.values())

    return {
        "parent_columns": parent_columns,
        "subitem_columns": subitem_columns
    }


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


@app.post("/test/era-full-flow", tags=["Testing"])
async def test_era_full_flow(request: Request):
    """
    FULL ERA TEST — paste any Stedi 835 JSON as the request body.

    What this does:
      1. Parses the ERA JSON (both Stedi API format and flat format supported)
      2. For each ERA row, looks up the Claims Board item by PCN
      3. Writes ALL Raw + Parsed fields to parent columns + service line subitems
      4. Returns a complete breakdown of every field parsed and written

    How to use:
      - Submit a test claim → note the PCN from the Stedi response
      - Paste the Stedi sample 835 JSON but change
        claimPaymentInfo.patientControlNumber to your PCN
      - POST to /test/era-full-flow with that JSON as the body
      - Check Claims Board — all Raw/Parsed columns should populate

    Alternatively use /test/835/{transaction_id} to pull live from Stedi.
    """
    from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday
    from routes.stedi_webhook import _find_claims_item_by_pcn
    from services.monday_service import populate_era_data_on_claims_item

    body        = await request.body()
    era_content = body.decode()

    era_rows = parse_era_from_string(era_content)
    if not era_rows:
        return {
            "status": "error",
            "error":  "No ERA rows parsed — check JSON format",
            "hint":   "Must be valid Stedi 835 JSON with 'transactions' key (API format) "
                      "or 'claimPaymentInfo' key (flat format)",
        }

    results = []
    for i, era_row in enumerate(era_rows):
        parent  = era_row.get("parent", {})
        pcn     = parent.get("raw_patient_control_num", "")
        summary = summarize_era_row_for_monday(era_row)

        claims_item_id = _find_claims_item_by_pcn(pcn)

        row_result = {
            "row":                   i,
            "pcn":                   pcn,
            "claims_item_id":        claims_item_id or "NOT FOUND",
            "written":               False,
            "error":                 None,
            # Complete parent field breakdown
            "parent_fields_parsed": {k: v for k, v in summary.items() if k != "children"},
            "service_lines_count":  len(summary.get("children", [])),
            "service_line_detail":  summary.get("children", []),
        }

        if claims_item_id:
            try:
                populate_era_data_on_claims_item(claims_item_id, summary)
                row_result["written"] = True
                logger.info(f"[ERA FULL FLOW] Written to Claims Board item {claims_item_id}")
            except Exception as e:
                row_result["error"] = str(e)
                logger.error(f"[ERA FULL FLOW] Write failed for {claims_item_id}: {e}", exc_info=True)
        else:
            row_result["error"] = (
                f"No Claims Board item found for PCN='{pcn}'. "
                f"Submit a claim first, then use that PCN in your test 835 JSON."
            )

        results.append(row_result)

    written = sum(1 for r in results if r["written"])
    return {
        "status":       "done",
        "rows_parsed":  len(results),
        "rows_written": written,
        "rows_failed":  len(results) - written,
        "results":      results,
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

@app.get("/test/onboarding-board-columns", tags=["Testing"])
async def get_onboarding_board_columns():
    """Get all column IDs from the New Onboarding Board"""
    from services.monday_service import run_query
    board_id = os.getenv("MONDAY_ONBOARDING_BOARD_ID")
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


@app.post("/test/eligibility/{item_id}", tags=["Testing"])
async def test_eligibility_dry_run(item_id: str):
    """
    Dry run: fetch item, build payload, run eligibility, return results.
    Does NOT write to Monday — for testing only.
    """
    from routes.eligibility_webhook import fetch_onboarding_item
    from stedi_eligibility import run_parse_and_build_monday_writeback

    try:
        row = fetch_onboarding_item(item_id)
        writeback = run_parse_and_build_monday_writeback(
            row,
            api_key=os.getenv("STEDI_API_KEY"),
        )
        return {
            "status": "success",
            "item_id": item_id,
            "input_row": row,
            "eligibility_results": writeback,
        }
    except Exception as e:
        return {
            "status": "error",
            "item_id": item_id,
            "error": str(e),
        }


@app.post("/test/eligibility-payload/{item_id}", tags=["Testing"])
async def test_eligibility_payload_only(item_id: str):
    """
    Build and return Stedi payload without sending it.
    Use to verify payload is correct before making a real API call.
    """
    from routes.eligibility_webhook import fetch_onboarding_item
    from stedi_eligibility import build_eligibility_payload_from_monday_row

    try:
        row = fetch_onboarding_item(item_id)
        print("row:", row)
        payload = build_eligibility_payload_from_monday_row(row)
        return {"status": "ok", "payload": payload}
    except Exception as e:
        return {"status": "error", "error": str(e)}

@app.post("/test/full-claim-flow/{item_id}", tags=["Testing"])
async def test_full_claim_flow(item_id: str):
    """
    FULL FLOW:
    1. Submit claim
    2. Wait
    3. Fetch ERA manually
    4. Parse
    5. Write to Monday
    """

    import time

    from services.monday_service import (
        get_order_item,
        update_claim_status,
        populate_era_data_on_claims_item,
    )
    from services.claim_builder_service import build_claims_from_monday_item
    from services.stedi_service import submit_claim, get_era_as_835_file
    from services.era_parser_service import parse_era_from_string, summarize_era_row_for_monday

    try:
        # Step 1: Get item
        order_data = get_order_item(item_id)

        payloads = build_claims_from_monday_item(order_data)

        results = []

        for payload in payloads:
            # Step 2: Submit claim
            result = submit_claim(payload)

            claim_id = result.get("claim_id")
            transaction_id = result.get("transaction_id")

            update_claim_status(item_id, claim_id=claim_id, status="Submitted")

            # add_update(item_id, f"✅ Claim Submitted\nTxn: {transaction_id}")

            # Step 3: WAIT (VERY IMPORTANT)
            time.sleep(5)  # give Stedi time

            # Step 4: Fetch ERA manually
            era_content = get_era_as_835_file(transaction_id)

            if not era_content:
                # add_update(item_id, "⚠️ No ERA returned yet")
                continue

            # Step 5: Parse
            era_rows = parse_era_from_string(era_content)

            if not era_rows:
                # add_update(item_id, "❌ ERA parsing failed")
                continue

            # Step 6: Write to Monday
            for era_row in era_rows:
                summary = summarize_era_row_for_monday(era_row)

                populate_era_data_on_claims_item(item_id, summary)

            # add_update(item_id, "💰 ERA processed successfully")

            results.append({
                "claim_id": claim_id,
                "transaction_id": transaction_id,
                "era_rows": len(era_rows)
            })

        return {
            "status": "success",
            "results": results
        }

    except Exception as e:
        logger.error(f"Full flow failed: {e}", exc_info=True)

        return {
            "status": "error",
            "error": str(e)
        }