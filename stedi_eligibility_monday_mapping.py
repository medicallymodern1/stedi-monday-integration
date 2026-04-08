from __future__ import annotations

from typing import Any


# ============================================================
# MONDAY INPUT / OUTPUT MAPPING FOR STEDI ELIGIBILITY (V1)
# ============================================================
# This file isolates all Monday board column-name mappings.
# Developers can later swap this to use Monday column IDs if needed.
# ============================================================

# Onboarding board input columns -> normalized input keys
MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP = {
    "payer_label": "Primary Insurance Final",
    "member_id": "Member ID",
    "first_name": "First Name",
    "last_name": "Last Name",
    "date_of_birth": "Patient Date of Birth",
}

# Normalized parser output keys -> new Monday output columns
MONDAY_ELIGIBILITY_OUTPUT_COLUMN_MAP = {
    "eligibility_active": "Stedi Eligibility Active?",
    "eligibility_in_network": "Stedi In Network?",
    "eligibility_plan_name": "Stedi Plan Name",
    "eligibility_prior_auth_required": "Stedi Prior Auth Required?",
    "eligibility_copay": "Stedi Copay",
    "eligibility_coinsurance_percent": "Stedi Coinsurance %",
    "eligibility_individual_deductible": "Stedi Individual Deductible",
    "eligibility_individual_deductible_remaining": "Stedi Individual Deductible Remaining",
    "eligibility_family_deductible": "Stedi Family Deductible",
    "eligibility_family_deductible_remaining": "Stedi Family Deductible Remaining",
    "eligibility_individual_oop_max": "Stedi Individual OOP Max",
    "eligibility_individual_oop_max_remaining": "Stedi Individual OOP Max Remaining",
    "eligibility_family_oop_max": "Stedi Family OOP Max",
    "eligibility_family_oop_max_remaining": "Stedi Family OOP Max Remaining",
    "eligibility_plan_begin_date": "Stedi Plan Begin Date",
    "eligibility_error_description": "Stedi Eligibility Error Description",
}


# ============================================================
# HELPERS
# ============================================================

def safe_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()



def get_required_input_fields() -> list[str]:
    return list(MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP.values())



def get_output_column_names() -> list[str]:
    return list(MONDAY_ELIGIBILITY_OUTPUT_COLUMN_MAP.values())



def extract_eligibility_inputs_from_monday_item(
    monday_item_fields: dict[str, Any],
) -> dict[str, Any]:
    """
    Return a row-style dictionary shaped exactly how stedi_eligibility.py expects.

    This function does not transform values. It only pulls the expected
    Monday column names into a simpler dict that the payload builder uses.
    """
    return {
        "Primary Insurance Final": monday_item_fields.get(MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP["payer_label"]),
        "Member ID": monday_item_fields.get(MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP["member_id"]),
        "First Name": monday_item_fields.get(MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP["first_name"]),
        "Last Name": monday_item_fields.get(MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP["last_name"]),
        "Patient Date of Birth": monday_item_fields.get(MONDAY_ELIGIBILITY_INPUT_COLUMN_MAP["date_of_birth"]),
        # Optional helper identifiers if present on the board payload.
        "Pulse ID": monday_item_fields.get("Pulse ID") or monday_item_fields.get("pulse_id"),
        "Name": monday_item_fields.get("Name") or monday_item_fields.get("name"),
    }



def build_monday_writeback_payload(
    normalized_output: dict[str, Any],
) -> dict[str, Any]:
    """
    Convert normalized parser output into a Monday column-name -> value mapping.

    This is intentionally generic so developers can later convert this dict into
    whatever Monday API mutation format they use.
    """
    writeback: dict[str, Any] = {}

    for normalized_key, monday_column_name in MONDAY_ELIGIBILITY_OUTPUT_COLUMN_MAP.items():
        writeback[monday_column_name] = normalized_output.get(normalized_key, "")

    return writeback
