"""
services/eligibility_monday_service.py
Writes eligibility check results back to Monday Onboarding Board.
"""

import os
import logging
from services.monday_service import run_query

logger = logging.getLogger(__name__)


# Eligibility output column name → Monday column ID
# UPDATE THESE once Brandon creates the 16 columns on the Onboarding Board
# Run GET /test/onboarding-board-columns to find the real IDs
ELIGIBILITY_OUTPUT_COLUMN_IDS = {
    "Stedi Eligibility Active?":               os.getenv("ELIG_OUT_ACTIVE",            "text_elig_active"),
    "Stedi In Network?":                       os.getenv("ELIG_OUT_IN_NETWORK",         "text_elig_in_network"),
    "Stedi Plan Name":                         os.getenv("ELIG_OUT_PLAN_NAME",          "text_elig_plan_name"),
    "Stedi Prior Auth Required?":              os.getenv("ELIG_OUT_PRIOR_AUTH",         "text_elig_prior_auth"),
    "Stedi Copay":                             os.getenv("ELIG_OUT_COPAY",              "text_elig_copay"),
    "Stedi Coinsurance %":                     os.getenv("ELIG_OUT_COINSURANCE",        "text_elig_coinsurance"),
    "Stedi Individual Deductible":             os.getenv("ELIG_OUT_IND_DED",            "text_elig_ind_ded"),
    "Stedi Individual Deductible Remaining":   os.getenv("ELIG_OUT_IND_DED_REM",        "text_elig_ind_ded_rem"),
    "Stedi Family Deductible":                 os.getenv("ELIG_OUT_FAM_DED",            "text_elig_fam_ded"),
    "Stedi Family Deductible Remaining":       os.getenv("ELIG_OUT_FAM_DED_REM",        "text_elig_fam_ded_rem"),
    "Stedi Individual OOP Max":                os.getenv("ELIG_OUT_IND_OOP",            "text_elig_ind_oop"),
    "Stedi Individual OOP Max Remaining":      os.getenv("ELIG_OUT_IND_OOP_REM",        "text_elig_ind_oop_rem"),
    "Stedi Family OOP Max":                    os.getenv("ELIG_OUT_FAM_OOP",            "text_elig_fam_oop"),
    "Stedi Family OOP Max Remaining":          os.getenv("ELIG_OUT_FAM_OOP_REM",        "text_elig_fam_oop_rem"),
    "Stedi Plan Begin Date":                   os.getenv("ELIG_OUT_PLAN_BEGIN",         "text_elig_plan_begin"),
    "Stedi Eligibility Error Description":     os.getenv("ELIG_OUT_ERROR",              "text_elig_error"),
}


def write_eligibility_to_monday(item_id: str, writeback_payload: dict) -> None:
    """
    Write eligibility results to Monday Onboarding Board item.
    writeback_payload keys are Monday column names from MONDAY_ELIGIBILITY_OUTPUT_COLUMN_MAP.
    """
    board_id = os.getenv("MONDAY_ONBOARDING_BOARD_ID")
    if not board_id:
        logger.warning("MONDAY_ONBOARDING_BOARD_ID not set — skipping eligibility writeback")
        return

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

    for column_name, value in writeback_payload.items():
        if value is None or value == "":
            continue

        col_id = ELIGIBILITY_OUTPUT_COLUMN_IDS.get(column_name)
        if not col_id:
            logger.warning(f"No column ID for: {column_name}")
            continue

        try:
            formatted = f'"{value}"'
            run_query(mutation, {
                "itemId":   str(item_id),
                "boardId":  str(board_id),
                "columnId": col_id,
                "value":    formatted,
            })
            logger.info(f"[ELG] Wrote {column_name} = {value}")
        except Exception as e:
            logger.warning(f"[ELG] Failed to write {column_name}: {e}")