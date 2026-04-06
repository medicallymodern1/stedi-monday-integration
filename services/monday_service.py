import os
import logging
import requests
from dotenv import load_dotenv

load_dotenv()
logger = logging.getLogger(__name__)

MONDAY_API_URL = "https://api.monday.com/v2"


def get_headers() -> dict:
    token = os.getenv("MONDAY_API_TOKEN")
    if not token:
        raise ValueError("MONDAY_API_TOKEN not set in .env")
    return {
        "Authorization": token,
        "Content-Type": "application/json",
        "API-Version": "2024-01",
    }


def run_query(query: str, variables: dict = None) -> dict:
    payload = {"query": query}
    if variables:
        payload["variables"] = variables

    response = requests.post(
        MONDAY_API_URL,
        json=payload,
        headers=get_headers(),
        timeout=30,
    )
    response.raise_for_status()
    result = response.json()

    if "errors" in result:
        raise ValueError(f"Monday API error: {result['errors']}")

    return result

def get_order_item(item_id: str) -> dict:
    """Fetch order item with all column values"""
    query = """
    query GetOrderItem($itemId: ID!) {
      items(ids: [$itemId]) {
        id
        name
        column_values {
          id
          text
          value
        }
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
    items = result.get("data", {}).get("items", [])
    if not items:
        raise ValueError(f"No item found for item_id={item_id}")
    logger.info(f"Fetched item: {items[0].get('name')}")
    return items[0]

STATUS_TO_INDEX = {
    "Accepted":       "1",
    "Rejected":       "0",   # Payer Rejected
    "Stedi Rejected": "2",
}

def update_277_status(item_id: str, status: str, rejection_reason: str = "") -> None:
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    # Update 277 status with correct index
    label_index = STATUS_TO_INDEX.get(status, "1")
    status_value = '{"index": ' + label_index + '}'

    try:
        run_query(mutation, {
            "itemId": str(item_id),
            "boardId": str(board_id),
            "columnId": "color_mm1bx9az",
            "value": status_value,
        })
        logger.info(f"Updated 277 status: {status}")
    except Exception as e:
        logger.warning(f"Failed to update 277 status: {e}")

    # Only store rejection reason when actually rejected
    if status != "Accepted" and rejection_reason:
        try:
            run_query(mutation, {
                "itemId": str(item_id),
                "boardId": str(board_id),
                "columnId": "text_mm1b56xa",
                "value": f'"{rejection_reason}"',
            })
        except Exception as e:
            logger.warning(f"Failed to store rejection reason: {e}")

CLAIM_STATUS_TO_INDEX = {
    "Submit Claim": "0",
    "Submitted":    "1",
    "Rejected":     "2",
    "Test Claim Submitted":  "3",
}

def update_claim_status(item_id: str, status: str) -> None:
    board_id     = os.getenv("MONDAY_ORDER_BOARD_ID")
    label_index  = CLAIM_STATUS_TO_INDEX.get(status, "1")
    status_value = '{"index": ' + label_index + '}'

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    try:
        run_query(mutation, {
            "itemId":   str(item_id),
            "boardId":  str(board_id),
            "columnId": "status",
            "value":    status_value,
        })
        logger.info(f"Claim Status column → {status}")
    except Exception as e:
        logger.warning(f"Failed to update Claim Status column: {e}")
        raise

# def create_claims_board_item(order_item: dict, claim_id: str) -> str:
#     """Create new item in Claims Board when claim is accepted"""
#     claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
#
#     if not claims_board_id:
#         logger.warning("MONDAY_CLAIMS_BOARD_ID not set — skipping claims board creation")
#         return ""
#
#     patient_name = order_item.get("name", "Unknown")
#
#     mutation = """
#     mutation CreateItem($boardId: ID!, $itemName: String!) {
#       create_item(board_id: $boardId, item_name: $itemName) { id }
#     }
#     """
#     result = run_query(mutation, {
#         "boardId": claims_board_id,
#         "itemName": patient_name,
#     })
#
#     new_item_id = result.get("data", {}).get("create_item", {}).get("id", "")
#     logger.info(f"Created Claims Board item {new_item_id} for {patient_name}")
#     return new_item_id

def create_claims_board_item(order_item: dict, claim_id: str, payer_name: str = "") -> str:
    """
    Create new item in Claims Board after claim is submitted.
    Populates as many fields as possible from the order data.
    """
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")
    if not claims_board_id:
        logger.warning("MONDAY_CLAIMS_BOARD_ID not set — skipping")
        return ""

    patient_name = order_item.get("name", "Unknown")
    item_name = f"[TEST] {patient_name} - {payer_name}" if payer_name else f"[TEST] {patient_name}"

    # Step 1: Create the item
    mutation = """
    mutation CreateItem($boardId: ID!, $itemName: String!) {
      create_item(board_id: $boardId, item_name: $itemName) { id }
    }
    """
    result = run_query(mutation, {
        "boardId": claims_board_id,
        "itemName": item_name,
    })
    new_item_id = result.get("data", {}).get("create_item", {}).get("id", "")
    if not new_item_id:
        logger.warning("Failed to create Claims Board item")
        return ""

    logger.info(f"Created Claims Board item {new_item_id}: {item_name}")

    # Step 2: Populate columns from order data
    col_values = {col.get("id"): col.get("text", "") for col in order_item.get("column_values", [])}

    # Map of column_id → value to set
    # Based on Claims Board columns logged earlier
    fields_to_set = {
        "text_mktat89m":   col_values.get("text_mm18s3fe", ""),    # Member ID
        "text_mkp3y5ax":   col_values.get("text_mm187t6a", ""),    # DOB
        "text_mkxr2r9b":   col_values.get("text_mm18x1kj", ""),    # NPI
        "text_mkxrh4a4":   col_values.get("text_mm18w2y4", ""),    # Doctor
        "text_mkwzbcme":   claim_id,                                # Stedi Correlation ID / Customer Order ref
    }

    # DOS from subitem order_date
    subitems = order_item.get("subitems", [])
    if subitems:
        sub_cols = {c.get("id"): c.get("text", "") for c in subitems[0].get("column_values", [])}
        dos_raw = sub_cols.get("date0", "")
        if dos_raw:
            fields_to_set["date_mkwr7spz"] = dos_raw  # DOS

    # Claim Sent Date = today
    from datetime import date
    today = date.today().isoformat()
    fields_to_set["date_mm14rk8d"] = today  # Claim Sent Date

    update_mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    for col_id, value in fields_to_set.items():
        if not value:
            continue
        try:
            # Date columns need JSON format
            if col_id.startswith("date_"):
                formatted = '{"date": "' + str(value) + '"}'
            else:
                formatted = f'"{value}"'

            run_query(update_mutation, {
                "itemId":   str(new_item_id),
                "boardId":  str(claims_board_id),
                "columnId": col_id,
                "value":    formatted,
            })
            logger.info(f"Claims Board: set {col_id} = {value}")
        except Exception as e:
            logger.warning(f"Claims Board: failed to set {col_id}: {e}")

    return new_item_id

def update_eligibility_data(item_id: str, data: dict) -> None:
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    # 👉 Map your fields → Monday column IDs
    field_map = {
        "eligibility_active": "text_elig_active",     # UPDATE THIS
        "eligibility_plan_name": "text_elig_plan",    # UPDATE THIS
        "eligibility_error_description": "text_elig_error",  # UPDATE THIS
    }

    for field, column_id in field_map.items():
        value = data.get(field, "")

        if value is None:
            continue

        try:
            run_query(mutation, {
                "itemId": str(item_id),
                "boardId": str(board_id),
                "columnId": column_id,
                "value": f'"{value}"',
            })
            logger.info(f"Eligibility: set {field} = {value}")
        except Exception as e:
            logger.warning(f"Failed to update {field}: {e}")

# ── ERA Parent column map — confirmed from GET /test/claims-columns ───────────
# Column IDs verified against Claims Board column export (document index 2).
ERA_PARENT_COLUMN_MAP = {
    # field key                      column_id            type      # Board title
    # 7 client-required "Raw" columns (confirmed IDs)
    "raw_patient_control_num":    ("text_mm1gkf40",    "text"),    # Raw Patient Control Number
    "raw_payer_claim_control":    ("text_mm1gefbz",    "text"),    # Raw Payer Claim Control Number
    "raw_total_claim_charge":     ("numeric_mm1ghydj", "number"),  # Raw Claim Charge Amount
    "raw_remittance_trace":       ("text_mm1gz8ss",    "text"),    # Raw Remittance Trace Number
    "raw_patient_responsibility": ("numeric_mm1gdpjq", "number"),  # Raw Patient Responsibility Amount
    "raw_era_date":               ("text_mm2047g9",    "text"),    # Raw ERA Date
    "raw_era_claim_status":       ("text_mm20k1zv",    "text"),    # Raw ERA Claim Status
    # Existing working columns (confirmed IDs)
    "primary_paid":               ("numeric_mm115q76", "number"),  # Primary Paid (A)
    "pr_amount":                  ("numeric_mkxmc2rh", "number"),  # PR Amount (C)
    "paid_date":                  ("date_mm11zg2f",    "date"),    # Primary Paid Date (D)
    "check_number":               ("text_mm11m3fh",    "text"),    # Check #
    "primary_status":             ("text_mkzck8tw",    "text"),    # Primary -->
}

# ── ERA Subitem column map — confirmed from GET /test/subitem-titles ──────────
# Column IDs verified against subitem column export (document index 2).
# Keys must match field names produced by era_parser_service.py children dicts.
SUBITEM_ERA_COLUMN_MAP = {
    # field key in child dict          column_id               type      # Board title
    # ── Identifiers ────────────────────────────────────────────────────────────
    "Raw Line Item Control Number": ("text_mm1ge9yn",       "text"),    # Raw Line Item Control Number
    "Patient Control #":            ("text_mm16qhea",       "text"),    # Patient Control #
    "Claim Status Code":            ("text_mm1gat8c",       "text"),    # Claim Status Code
    # ── Dates ──────────────────────────────────────────────────────────────────
    "Raw Service Date":             ("date_mm11hscn",       "date"),    # Raw Service Date
    # ── Amounts ────────────────────────────────────────────────────────────────
    "Primary Paid":                 ("numeric_mm1czbyg",    "number"),  # Primary Paid (line item paid)
    "Raw Line Item Charge Amount":  ("numeric_mm11v6th",    "number"),  # Raw Line Item Charge Amount
    "Raw Allowed Actual":           ("numeric_mm1gg3pj",    "number"),  # Raw Allowed Amount
    # ── PR Adjustment Breakdown ────────────────────────────────────────────────
    "Parsed PR Amount":             ("numeric_mm1gtdts",    "number"),  # Raw PR Amount (total patient resp)
    "Parsed Deductible Amount":     ("numeric_mm1gredn",    "number"),  # Raw Deductible Amount (PR-1)
    "Parsed Coinsurance Amount":    ("numeric_mm1g3nvh",    "number"),  # Raw Coinsurance Amount (PR-2)
    "Parsed Copay Amount":          ("numeric_mm11aqr1",    "number"),  # Raw Copay Amount (PR-3)
    "Parsed Other PR Amount":       ("numeric_mm1gtd3e",    "number"),  # Raw Other PR Amount
    # ── CO Adjustment Breakdown ────────────────────────────────────────────────
    "Parsed CO Amount":             ("numeric_mm1g48c",     "number"),  # Raw CO Amount (total contractual)
    "Parsed CO-45 Amount":          ("numeric_mm1gken",     "number"),  # Raw CO-45 Amount
    "Parsed CO-253 Amount":         ("numeric_mm1gt3ky",    "number"),  # Raw CO-253 Amount
    "Parsed Other CO Amount":       ("numeric_mm1g3vgp",    "number"),  # Raw Other CO Amount
    # ── OA / PI Adjustments ───────────────────────────────────────────────────
    "Parsed OA Amount":             ("numeric_mm1grbc3",    "number"),  # Raw OA Amount
    "Parsed PI Amount":             ("numeric_mm1gh22d",    "number"),  # Raw PI Amount
    # ── Code Strings ──────────────────────────────────────────────────────────
    "Parsed Adjustment Codes":      ("text_mm1gt1dh",       "text"),    # Raw Adjustment Codes (e.g. CO-45; PR-1)
    "Parsed CARC Codes":            ("text_mm20ke2s",       "text"),    # Raw CARC Codes
    "Parsed RARC Codes":            ("text_mm20brp",        "text"),    # Raw RARC Codes
    "Parsed Remark Codes":          ("text_mm1g6tw3",       "text"),    # Raw Remark Codes
    "Parsed Remark Text":           ("long_text_mm1ggyz6",  "long_text"), # Raw Remark Text
    "Parsed Adjustment Reasons":    ("long_text_mm1g7xmy",  "long_text"), # Raw Adjustment Reasons
}


def populate_era_data_on_claims_item(claims_item_id: str, era_data: dict) -> None:
    """
    Write ALL ERA parent fields + service line subitems to a Claims Board item.
    Covers every Raw/Parsed column the client defined.
    """
    claims_board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """

    # Write all parent fields
    for field, (column_id, col_type) in ERA_PARENT_COLUMN_MAP.items():
        value = era_data.get(field)
        if value is None or value == "":
            continue
        try:
            if col_type == "number":
                formatted = str(value)
            elif col_type == "date":
                formatted = '{"date": "' + str(value) + '"}'
            else:
                formatted = f'"{str(value)}"'

            run_query(mutation, {
                "itemId":   str(claims_item_id),
                "boardId":  str(claims_board_id),
                "columnId": column_id,
                "value":    formatted,
            })
            logger.info(f"ERA parent: {field} → {column_id} = {value}")
        except Exception as e:
            logger.warning(f"ERA parent: failed {field} ({column_id}): {e}")

    # Write service line subitems
    children = era_data.get("children", [])
    if children:
        populate_era_service_line_subitems(claims_item_id, children)

#
# def store_claim_pcn(item_id: str, pcn: str, claim_id: str) -> None:
#     """
#     Store patientControlNumber and claim_id on Order Board item.
#     Used to match 277/835 responses back to the correct order.
#     Requires 'Claim ID' text column added by Brandon.
#     """
#     board_id = os.getenv("MONDAY_ORDER_BOARD_ID")
#
#     mutation = """
#     mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
#       change_column_value(
#         item_id: $itemId,
#         board_id: $boardId,
#         column_id: $columnId,
#         value: $value
#       ) { id }
#     }
#     """
#
#     # Store claim_id in the new Claim ID column Brandon added
#     # Update column ID once confirmed from board
#     fields = {
#         "text_mm1ra2v1": pcn,   # Claim ID column — update ID if different
#     }
#
#     for col_id, value in fields.items():
#         if not value:
#             continue
#         try:
#             run_query(mutation, {
#                 "itemId":   str(item_id),
#                 "boardId":  str(board_id),
#                 "columnId": col_id,
#                 "value":    f'"{value}"',
#             })
#             logger.info(f"Stored claim_id={claim_id} on order item {item_id}")
#         except Exception as e:
#             logger.warning(f"Failed to store claim_id: {e}")

def update_277_on_claims_board(item_id: str, status: str, rejection_reason: str = "") -> None:
    """
    Update 277 Status and 277 Rejected Reason on Claims Board item.
    PRD Section 14 status values:
      Stedi Accepted, Stedi Rejected, Payer Accepted, Payer Rejected
    """
    board_id = os.getenv("MONDAY_CLAIMS_BOARD_ID")

    # Confirmed column IDs from Claims Board export
    STATUS_277_COL = "color_mm1z1pb2"   # 277 Status  (status)
    REASON_277_COL = "text_mm1zsp2x"    # 277 Rejected Reason  (text)

    STATUS_INDEX = {
        "Stedi Accepted": 0,
        "Stedi Rejected": 1,
        "Payer Accepted": 2,
        "Payer Rejected": 3,
    }

    import json as _json
    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId, board_id: $boardId,
        column_id: $columnId, value: $value
      ) { id }
    }
    """

    idx = STATUS_INDEX.get(status)
    if idx is not None:
        try:
            run_query(mutation, {
                "itemId":   str(item_id),
                "boardId":  str(board_id),
                "columnId": STATUS_277_COL,
                "value":    _json.dumps({"index": idx}),
            })
            logger.info(f"[277] Updated 277 Status={status} on item {item_id}")
        except Exception as e:
            logger.warning(f"[277] Failed to update 277 Status: {e}")
    else:
        logger.warning(f"[277] Unknown status value: {status!r} — skipped")

    # Write rejection reason on reject, clear it on accept
    reason_value = rejection_reason if "Rejected" in status else ""
    try:
        run_query(mutation, {
            "itemId":   str(item_id),
            "boardId":  str(board_id),
            "columnId": REASON_277_COL,
            "value":    _json.dumps(reason_value),
        })
    except Exception as e:
        logger.warning(f"[277] Failed to update 277 Rejected Reason: {e}")

def _get_column_value(item_id: str, column_id: str) -> str:
    """Read a single column value from an Order Board item"""
    query = """
    query GetItem($itemId: ID!) {
      items(ids: [$itemId]) {
        column_values { id text }
      }
    }
    """
    try:
        result = run_query(query, {"itemId": item_id})
        cols = result.get("data", {}).get("items", [{}])[0].get("column_values", [])
        for col in cols:
            if col.get("id") == column_id:
                return col.get("text", "") or ""
    except Exception:
        pass
    return ""

def store_claim_pcn(item_id: str, pcn: str, claim_id: str) -> None:
    board_id = os.getenv("MONDAY_ORDER_BOARD_ID")

    # Read existing value first, then append
    existing = _get_column_value(item_id, "text_mm1ra2v1")
    if existing and pcn not in existing:
        new_value = f"{existing},{pcn}"
    else:
        new_value = pcn

    mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """
    try:
        run_query(mutation, {
            "itemId":   str(item_id),
            "boardId":  str(board_id),
            "columnId": "text_mm1ra2v1",
            "value":    f'"{new_value}"',
        })
        logger.info(f"Stored pcn={pcn} on order item {item_id} (full: {new_value})")
    except Exception as e:
        logger.warning(f"Failed to store pcn: {e}")

def post_claim_update_to_monday(
    item_id: str,
    submitted_claims: list,
    is_test: bool = False,
) -> None:
    import json
    from datetime import datetime
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M UTC")
    mode_tag = "🧪 TEST CLAIM" if is_test else "✅ LIVE CLAIM"

    lines = [f"{mode_tag} submitted to Stedi — {now}\n"]

    for i, c in enumerate(submitted_claims, 1):
        payload_json = json.dumps(c["payload"], indent=2)
        lines.append(
            f"-- Claim #{i} --\n"
            f"Payer: {c['payer']}\n"
            f"Claim ID: {c['claim_id']}\n"
            f"Patient Control #: {c['pcn']}\n"
            f"Payload:\n{payload_json}\n"
        )

    message = "\n".join(lines)

    mutation = """
    mutation PostUpdate($itemId: ID!, $body: String!) {
      create_update(item_id: $itemId, body: $body) { id }
    }
    """
    try:
        run_query(mutation, {"itemId": str(item_id), "body": message})
        logger.info(f"Posted combined claim update to Monday item {item_id}")
    except Exception as e:
        logger.warning(f"Failed to post Monday update: {e}")


def _get_existing_subitems(claims_item_id: str) -> dict:
    """
    Fetch all existing subitems for a Claims Board item.
    Returns dict keyed by HCPC code → {subitem_id, subitem_board_id}
    HCPC code is read from color_mm1cdvq8 (the HCPCS status column on the subitem).
    """
    query = """
    query GetSubitems($itemId: ID!) {
      items(ids: [$itemId]) {
        subitems {
          id
          name
          board { id }
          column_values { id text }
        }
      }
    }
    """
    try:
        result = run_query(query, {"itemId": str(claims_item_id)})
        subitems = (
            result.get("data", {})
            .get("items", [{}])[0]
            .get("subitems", [])
        )
        existing = {}
        for sub in subitems:
            subitem_id       = sub.get("id", "")
            subitem_board_id = sub.get("board", {}).get("id", "")
            hcpc = ""
            for col in sub.get("column_values", []):
                if col.get("id") == "color_mm1cdvq8":
                    hcpc = (col.get("text") or "").strip()
                    break
            if hcpc and subitem_id:
                existing[hcpc] = {
                    "subitem_id":       subitem_id,
                    "subitem_board_id": subitem_board_id,
                }
                logger.info(f"  Existing subitem: HCPC={hcpc} → id={subitem_id}")
        return existing
    except Exception as e:
        logger.warning(f"Failed to fetch existing subitems: {e}")
        return {}


def populate_era_service_line_subitems(claims_item_id: str, children: list) -> None:
    """
    Match ERA service lines to existing subitems by HCPC code, then write ERA fields.
    - Match found → UPDATE existing subitem in place (no new subitem created)
    - No match    → CREATE new subitem named after the HCPC code
    """
    update_mutation = """
    mutation UpdateColumn($itemId: ID!, $boardId: ID!, $columnId: String!, $value: JSON!) {
      change_column_value(
        item_id: $itemId,
        board_id: $boardId,
        column_id: $columnId,
        value: $value
      ) { id }
    }
    """
    create_mutation = """
    mutation CreateSubitem($parentId: ID!, $itemName: String!) {
      create_subitem(parent_item_id: $parentId, item_name: $itemName) {
        id
        board { id }
      }
    }
    """

    # Fetch all existing subitems once, keyed by HCPC code
    existing_subitems = _get_existing_subitems(claims_item_id)

    for child in children:
        hcpc_code = (child.get("HCPC Code") or "").strip()
        item_name = hcpc_code or "ERA Line"

        try:
            # Match by name — same key used when the subitem was first created
            if item_name in existing_subitems:
                subitem_id = existing_subitems[item_name]["subitem_id"]
                subitem_board_id = existing_subitems[item_name]["subitem_board_id"]
                logger.info(f"Matched existing subitem name={item_name!r} → id={subitem_id} (updating in place)")
            else:
                # No match — create new subitem
                result = run_query(create_mutation, {
                    "parentId": str(claims_item_id),
                    "itemName": item_name,
                })
                sub_data = result.get("data", {}).get("create_subitem", {})
                subitem_id = sub_data.get("id", "")
                subitem_board_id = sub_data.get("board", {}).get("id", "")
                if not subitem_id:
                    logger.warning(f"Failed to create subitem name={item_name!r}")
                    continue
                logger.info(f"Created new subitem name={item_name!r} → id={subitem_id}")

            # Write all ERA fields into the subitem using SUBITEM_ERA_COLUMN_MAP
            for field_name, (col_id, col_type) in SUBITEM_ERA_COLUMN_MAP.items():
                value = child.get(field_name)
                if value is None or value == "":
                    continue
                try:
                    if col_type == "number":
                        formatted = str(value)
                    elif col_type == "date":
                        formatted = '{"date": "' + str(value) + '"}'
                    elif col_type == "long_text":
                        formatted = '{"text": "' + str(value).replace('"', "'") + '"}'
                    else:
                        formatted = f'"{str(value)}"'

                    run_query(update_mutation, {
                        "itemId": str(subitem_id),
                        "boardId": str(subitem_board_id),
                        "columnId": col_id,
                        "value": formatted,
                    })
                    logger.info(f"  Subitem name={item_name!r}: {field_name} = {value}")
                except Exception as e:
                    logger.warning(f"  Subitem name={item_name!r}: failed {field_name}: {e}")

        except Exception as e:
            logger.warning(f"Failed to process subitem name={item_name!r}: {e}")

def get_column_settings(board_id: str, column_id: str) -> dict:
    """Debug: Get column settings to find valid status labels"""
    query = """
    query GetColumns($boardId: ID!) {
      boards(ids: [$boardId]) {
        columns {
          id
          title
          type
          settings_str
        }
      }
    }
    """
    result = run_query(query, {"boardId": board_id})
    columns = (
        result.get("data", {})
        .get("boards", [{}])[0]
        .get("columns", [])
    )
    for col in columns:
        if col.get("id") == column_id:
            return col
    return {}