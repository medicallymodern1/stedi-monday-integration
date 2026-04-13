"""
intake_insurance_resolver.py — Main resolution engine for the Intake Board.
===========================================================================

Given an Intake Board item's column values, computes ALL derived output fields:
  • Stedi copy            (PRD §8)
  • Insurance bucket split (PRD §9)
  • Product serving logic  (PRD §10)
  • HCPC mapping           (PRD §11)
  • Auth requirements      (PRD §12)
  • Network status         (PRD §13)
  • Co-insurance carveouts (PRD §14)

Returns a dict of { monday_column_id: value_to_write }.
"""

import logging

from intake_insurance_rules import (
    ALLOWED_LABELS_SET,
    SUPPLIES_ROUTE_TO_MEDICAID,
    MONITOR_HCPC, SENSORS_HCPC, INSULIN_PUMP_HCPC,
    SUPPLY_HCPC_MAP,
    get_auth_requirement,
    get_network_status,
    get_coinsurance,
)

logger = logging.getLogger(__name__)

NOT_SERVING = "Not Serving"
EVALUATE    = "Evaluate"

# ═══════════════════════════════════════════════════════════════════════════════
# Monday Column IDs  (actual IDs from board 18406352652)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Input columns ────────────────────────────────────────────────────────────
COL_PRIMARY_INSURANCE           = "color_mm1xg10n"
COL_INSURANCE_PLAN              = "dropdown_mm1y2x75"
COL_MEMBER_ID_1                 = "text_mm1x2qk2"
COL_MEMBER_ID_2                 = "text_mm1xaccx"
COL_SERVING                     = "color_mm1w1cm9"

# ── Stedi input columns ───────────────────────────────────────────────────────
COL_STEDI_COVERAGE_TYPE         = "text_mm25pxed"
COL_STEDI_PAYER_NAME            = "text_mm25wrxw"
COL_STEDI_PLAN_NAME             = "text_mm1xdcet"
COL_STEDI_ELIGIBILITY_ACTIVE    = "text_mm1xpgy2"
COL_STEDI_COINSURANCE           = "text_mm1xssyw"
COL_STEDI_INDIVIDUAL_DED        = "text_mm1x46kd"
COL_STEDI_INDIVIDUAL_DED_REM    = "text_mm1xyga2"
COL_STEDI_INDIVIDUAL_OOP        = "text_mm1xdtxq"
COL_STEDI_INDIVIDUAL_OOP_REM    = "text_mm1x32jw"
COL_STEDI_SECONDARY_MEDICAID_ID = "text_mm25bjz7"

# ── Benefits output columns ───────────────────────────────────────────────────
COL_DEDUCTIBLE                  = "numeric_mm1ztdz4"
COL_DEDUCTIBLE_REMAINING        = "numeric_mm1zv64b"
COL_OOP_MAX                     = "numeric_mm1zfv02"
COL_OOP_MAX_REMAINING           = "numeric_mm1zxktp"

# ── Insurance buckets ────────────────────────────────────────────────────────
COL_CGM_PUMP_INSURANCE          = "color_mm2bgjxp"
COL_CGM_PUMP_MEMBER_ID          = "text_mm2b5bf5"
COL_CGM_PUMP_INS_ACTIVE         = "color_mm2bg0rs"
COL_SUPPLIES_INSURANCE          = "color_mm2b5e6a"
COL_SUPPLIES_MEMBER_ID          = "text_mm2bs9y4"
COL_SUPPLIES_INS_ACTIVE         = "color_mm2bvmw7"

# ── Monitor ──────────────────────────────────────────────────────────────────
COL_MONITOR_AUTH_REQ            = "color_mm2bpw7z"
COL_MONITOR_NETWORK_STATUS      = "color_mm2b7c1z"
COL_MONITOR_COINSURANCE         = "numeric_mm2bffek"
COL_MONITOR_HCPC                = "color_mm2b1zgq"

# ── Sensors ──────────────────────────────────────────────────────────────────
COL_SENSORS_AUTH_REQ            = "color_mm2bscj"
COL_SENSORS_NETWORK_STATUS      = "color_mm2brh0x"
COL_SENSORS_COINSURANCE         = "numeric_mm2byn1k"
COL_SENSORS_HCPC                = "color_mm2b6t98"

# ── Insulin Pump ─────────────────────────────────────────────────────────────
COL_INSULIN_PUMP_AUTH_REQ       = "color_mm2bx2ys"
COL_INSULIN_PUMP_NETWORK_STATUS = "color_mm2b91nc"
COL_INSULIN_PUMP_COINSURANCE    = "numeric_mm2b4nzx"
COL_INSULIN_PUMP_HCPC           = "color_mm2bjwvx"

# ── Infusion Set ─────────────────────────────────────────────────────────────
COL_INFUSION_SET_AUTH_REQ       = "color_mm2btvq0"
COL_INFUSION_SET_NETWORK_STATUS = "color_mm2b1ver"
COL_INFUSION_SET_COINSURANCE    = "numeric_mm2bksj8"
COL_INFUSION_SET_HCPC           = "color_mm2bpvvy"

# ── Cartridge ────────────────────────────────────────────────────────────────
COL_CARTRIDGE_AUTH_REQ          = "color_mm2bd0q0"
COL_CARTRIDGE_NETWORK_STATUS    = "color_mm2bm7g8"
COL_CARTRIDGE_COINSURANCE       = "numeric_mm2bhdrr"
COL_CARTRIDGE_HCPC              = "color_mm2bxxz2"

# All output column IDs (used for loop prevention)
ALL_OUTPUT_COLUMN_IDS = {
    COL_DEDUCTIBLE, COL_DEDUCTIBLE_REMAINING, COL_OOP_MAX, COL_OOP_MAX_REMAINING,
    COL_CGM_PUMP_INSURANCE, COL_CGM_PUMP_MEMBER_ID, COL_CGM_PUMP_INS_ACTIVE,
    COL_SUPPLIES_INSURANCE, COL_SUPPLIES_MEMBER_ID, COL_SUPPLIES_INS_ACTIVE,
    COL_MONITOR_AUTH_REQ, COL_MONITOR_NETWORK_STATUS, COL_MONITOR_COINSURANCE, COL_MONITOR_HCPC,
    COL_SENSORS_AUTH_REQ, COL_SENSORS_NETWORK_STATUS, COL_SENSORS_COINSURANCE, COL_SENSORS_HCPC,
    COL_INSULIN_PUMP_AUTH_REQ, COL_INSULIN_PUMP_NETWORK_STATUS, COL_INSULIN_PUMP_COINSURANCE, COL_INSULIN_PUMP_HCPC,
    COL_INFUSION_SET_AUTH_REQ, COL_INFUSION_SET_NETWORK_STATUS, COL_INFUSION_SET_COINSURANCE, COL_INFUSION_SET_HCPC,
    COL_CARTRIDGE_AUTH_REQ, COL_CARTRIDGE_NETWORK_STATUS, COL_CARTRIDGE_COINSURANCE, COL_CARTRIDGE_HCPC,
}

# Columns that trigger resolution
TRIGGER_COLUMN_IDS = {COL_INSURANCE_PLAN, COL_PRIMARY_INSURANCE, COL_SERVING}


# ═══════════════════════════════════════════════════════════════════════════════
# Serving logic  (PRD §10)
# ═══════════════════════════════════════════════════════════════════════════════

_SERVING_MAP = {
    "Supplies Only":        {"cgm_pump_bucket": False, "monitor": False, "sensors": False,
                             "insulin_pump": False, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
    "CGM":                  {"cgm_pump_bucket": True, "monitor": True, "sensors": True,
                             "insulin_pump": False, "supplies_bucket": False,
                             "infusion_set": False, "cartridge": False},
    "Insulin Pump":         {"cgm_pump_bucket": True, "monitor": False, "sensors": False,
                             "insulin_pump": True, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
    "Supplies + CGM":       {"cgm_pump_bucket": True, "monitor": True, "sensors": True,
                             "insulin_pump": False, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
    "Insulin Pump + CGM":   {"cgm_pump_bucket": True, "monitor": True, "sensors": True,
                             "insulin_pump": True, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
}


def _is_serving(serving_value, group):
    m = _SERVING_MAP.get(serving_value)
    return m.get(group, True) if m else True


# ═══════════════════════════════════════════════════════════════════════════════
# Helper to read a column value from the item dict
# ═══════════════════════════════════════════════════════════════════════════════

def _val(item_fields, col_id):
    """Get the text value of a column, stripped.  Returns '' if missing."""
    return (item_fields.get(col_id) or "").strip()

def _stedi_active_to_status(stedi_val):
    """Map Stedi eligibility Yes/No to Monday status labels Active/Not Active."""
    v = (stedi_val or "").strip().lower()
    if v in ("yes", "y", "true", "1", "active"):
        return "Active"
    elif v in ("no", "n", "false", "0", "not active"):
        return "Not Active"
    return "Evaluate"


# ═══════════════════════════════════════════════════════════════════════════════
# Main resolution
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_intake_fields(item_fields):
    """
    Given { column_id: text_value } for an Intake Board item,
    compute all derived output fields.

    Returns (output_dict, log_lines) where output_dict is
    { column_id: value_to_write } and log_lines is a list of
    human-readable strings describing what was set and why.
    """
    out = {}
    log = []

    primary_ins     = _val(item_fields, COL_PRIMARY_INSURANCE)
    insurance_plan  = _val(item_fields, COL_INSURANCE_PLAN)
    member_id_1     = _val(item_fields, COL_MEMBER_ID_1)
    member_id_2     = _val(item_fields, COL_MEMBER_ID_2)
    serving         = _val(item_fields, COL_SERVING)
    stedi_active    = _val(item_fields, COL_STEDI_ELIGIBILITY_ACTIVE)
    stedi_coins     = _val(item_fields, COL_STEDI_COINSURANCE)

    log.append(f"primary_ins={primary_ins!r}  plan={insurance_plan!r}  "
               f"serving={serving!r}  stedi_active={stedi_active!r}")

    # ── Step 1: Stedi copy (PRD §8) ────────────────────────────────────────
    for src, dst in [
        (COL_STEDI_INDIVIDUAL_DED,     COL_DEDUCTIBLE),
        (COL_STEDI_INDIVIDUAL_DED_REM, COL_DEDUCTIBLE_REMAINING),
        (COL_STEDI_INDIVIDUAL_OOP,     COL_OOP_MAX),
        (COL_STEDI_INDIVIDUAL_OOP_REM, COL_OOP_MAX_REMAINING),
    ]:
        v = _val(item_fields, src)
        if v:
            out[dst] = v
            log.append(f"  {dst} = {v!r}  (copied from Stedi)")

    med_id = _val(item_fields, COL_STEDI_SECONDARY_MEDICAID_ID)
    if med_id:
        out[COL_MEMBER_ID_2] = med_id
        log.append(f"  Member ID 2 = {med_id!r}  (Stedi Secondary / Medicaid ID)")
        member_id_2 = med_id  # use for downstream

    # ── Step 2: bail out if no primary insurance ─────────────────────────
    if not primary_ins:
        log.append("Primary Insurance empty — cannot resolve further.")
        return out, log

    if primary_ins not in ALLOWED_LABELS_SET:
        log.append(f"Primary Insurance '{primary_ins}' not in allowed labels.")

    # ── Step 3: insurance bucket logic (PRD §9) ─────────────────────────────
    supplies_to_medicaid = primary_ins in SUPPLIES_ROUTE_TO_MEDICAID

    # CGM & Pump bucket
    if _is_serving(serving, "cgm_pump_bucket"):
        out[COL_CGM_PUMP_INSURANCE] = primary_ins
        out[COL_CGM_PUMP_MEMBER_ID] = member_id_1
        out[COL_CGM_PUMP_INS_ACTIVE] = _stedi_active_to_status(stedi_active)
        log.append(f"  CGM & Pump bucket → {primary_ins}")
    else:
        out[COL_CGM_PUMP_INSURANCE] = NOT_SERVING
        out[COL_CGM_PUMP_MEMBER_ID] = ""
        out[COL_CGM_PUMP_INS_ACTIVE] = NOT_SERVING
        log.append("  CGM & Pump bucket → Not Serving")

    # Supplies bucket
    if _is_serving(serving, "supplies_bucket"):
        if supplies_to_medicaid:
            out[COL_SUPPLIES_INSURANCE] = "Medicaid"
            out[COL_SUPPLIES_MEMBER_ID] = member_id_2
            out[COL_SUPPLIES_INS_ACTIVE] = "Active"
            log.append("  Supplies bucket → Medicaid (route override)")
        else:
            out[COL_SUPPLIES_INSURANCE] = primary_ins
            out[COL_SUPPLIES_MEMBER_ID] = member_id_1
            out[COL_SUPPLIES_INS_ACTIVE] = _stedi_active_to_status(stedi_active)
            log.append(f"  Supplies bucket → {primary_ins}")
    else:
        out[COL_SUPPLIES_INSURANCE] = NOT_SERVING
        out[COL_SUPPLIES_MEMBER_ID] = ""
        out[COL_SUPPLIES_INS_ACTIVE] = NOT_SERVING
        log.append("  Supplies bucket → Not Serving")

    # Determine resolved insurance for supply-side products
    supplies_ins = (
        "Medicaid"
        if supplies_to_medicaid and _is_serving(serving, "supplies_bucket")
        else primary_ins
    )

    # ── Step 4: per-product fields ────────────────────────────────────────

    # Fixed HCPC products (CGM & Pump side)
    _resolve_product(out, log, "monitor",      primary_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_MONITOR_AUTH_REQ, COL_MONITOR_NETWORK_STATUS,
                     COL_MONITOR_COINSURANCE, COL_MONITOR_HCPC, MONITOR_HCPC)

    _resolve_product(out, log, "sensors",      primary_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_SENSORS_AUTH_REQ, COL_SENSORS_NETWORK_STATUS,
                     COL_SENSORS_COINSURANCE, COL_SENSORS_HCPC, SENSORS_HCPC)

    _resolve_product(out, log, "insulin_pump", primary_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_INSULIN_PUMP_AUTH_REQ, COL_INSULIN_PUMP_NETWORK_STATUS,
                     COL_INSULIN_PUMP_COINSURANCE, COL_INSULIN_PUMP_HCPC, INSULIN_PUMP_HCPC)

    # Variable HCPC products (Supplies side)
    supply_hcpc = SUPPLY_HCPC_MAP.get(supplies_ins)
    inf_hcpc  = supply_hcpc[0] if supply_hcpc else EVALUATE
    cart_hcpc = supply_hcpc[1] if supply_hcpc else EVALUATE

    _resolve_product(out, log, "infusion_set", supplies_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_INFUSION_SET_AUTH_REQ, COL_INFUSION_SET_NETWORK_STATUS,
                     COL_INFUSION_SET_COINSURANCE, COL_INFUSION_SET_HCPC, inf_hcpc)

    _resolve_product(out, log, "cartridge",    supplies_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_CARTRIDGE_AUTH_REQ, COL_CARTRIDGE_NETWORK_STATUS,
                     COL_CARTRIDGE_COINSURANCE, COL_CARTRIDGE_HCPC, cart_hcpc)

    return out, log


# ─── Per-product helper ────────────────────────────────────────────────────────

_PRODUCT_NAME = {
    "monitor":      "Monitor",
    "sensors":      "Sensors",
    "insulin_pump": "Insulin Pump",
    "infusion_set": "Infusion Set",
    "cartridge":    "Cartridge",
}

def _resolve_product(out, log, product_key, insurance_label, insurance_plan,
                     stedi_coins, serving,
                     auth_col, network_col, coins_col, hcpc_col, hcpc_value):
    product_name = _PRODUCT_NAME[product_key]

    if not _is_serving(serving, product_key):
        out[auth_col]    = NOT_SERVING
        out[network_col] = NOT_SERVING
        out[coins_col]   = ""
        out[hcpc_col]    = NOT_SERVING
        log.append(f"  {product_name}: Not Serving")
        return

    auth    = get_auth_requirement(insurance_label, product_name, insurance_plan)
    network = get_network_status(insurance_label, product_name, insurance_plan)
    coins   = get_coinsurance(insurance_label, stedi_coins, insurance_plan)

    out[auth_col]    = auth
    out[network_col] = network
    out[coins_col]   = coins
    out[hcpc_col]    = hcpc_value

    log.append(f"  {product_name}: auth={auth}  network={network}  "
               f"coins={coins!r}  hcpc={hcpc_value}")
"""
intake_insurance_resolver.py — Main resolution engine for the Intake Board.
===========================================================================

Given an Intake Board item's column values, computes ALL derived output fields:
  • Stedi copy            (PRD §8)
  • Insurance bucket split (PRD §9)
  • Product serving logic  (PRD §10)
  • HCPC mapping           (PRD §11)
  • Auth requirements      (PRD §12)
  • Network status         (PRD §13)
  • Co-insurance carveouts (PRD §14)

Returns a dict of { monday_column_id: value_to_write }.
"""

import logging

from intake_insurance_rules import (
    ALLOWED_LABELS_SET,
    SUPPLIES_ROUTE_TO_MEDICAID,
    MONITOR_HCPC, SENSORS_HCPC, INSULIN_PUMP_HCPC,
    SUPPLY_HCPC_MAP,
    get_auth_requirement,
    get_network_status,
    get_coinsurance,
)

logger = logging.getLogger(__name__)

NOT_SERVING = "Not Serving"
EVALUATE    = "Evaluate"

# ═══════════════════════════════════════════════════════════════════════════════
# Monday Column IDs  (actual IDs from board 18406352652)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Input columns ────────────────────────────────────────────────────────────
COL_PRIMARY_INSURANCE           = "color_mm1xg10n"
COL_INSURANCE_PLAN              = "dropdown_mm1y2x75"
COL_MEMBER_ID_1                 = "text_mm1x2qk2"
COL_MEMBER_ID_2                 = "text_mm1xaccx"
COL_SERVING                     = "color_mm1w1cm9"

# ── Stedi input columns ───────────────────────────────────────────────────────
COL_STEDI_COVERAGE_TYPE         = "text_mm25pxed"
COL_STEDI_PAYER_NAME            = "text_mm25wrxw"
COL_STEDI_PLAN_NAME             = "text_mm1xdcet"
COL_STEDI_ELIGIBILITY_ACTIVE    = "text_mm1xpgy2"
COL_STEDI_COINSURANCE           = "text_mm1xssyw"
COL_STEDI_INDIVIDUAL_DED        = "text_mm1x46kd"
COL_STEDI_INDIVIDUAL_DED_REM    = "text_mm1xyga2"
COL_STEDI_INDIVIDUAL_OOP        = "text_mm1xdtxq"
COL_STEDI_INDIVIDUAL_OOP_REM    = "text_mm1x32jw"
COL_STEDI_SECONDARY_MEDICAID_ID = "text_mm25bjz7"

# ── Benefits output columns ───────────────────────────────────────────────────
COL_DEDUCTIBLE                  = "numeric_mm1ztdz4"
COL_DEDUCTIBLE_REMAINING        = "numeric_mm1zv64b"
COL_OOP_MAX                     = "numeric_mm1zfv02"
COL_OOP_MAX_REMAINING           = "numeric_mm1zxktp"

# ── Insurance buckets ────────────────────────────────────────────────────────
COL_CGM_PUMP_INSURANCE          = "color_mm2bgjxp"
COL_CGM_PUMP_MEMBER_ID          = "text_mm2b5bf5"
COL_CGM_PUMP_INS_ACTIVE         = "color_mm2bg0rs"
COL_SUPPLIES_INSURANCE          = "color_mm2b5e6a"
COL_SUPPLIES_MEMBER_ID          = "text_mm2bs9y4"
COL_SUPPLIES_INS_ACTIVE         = "color_mm2bvmw7"

# ── Monitor ──────────────────────────────────────────────────────────────────
COL_MONITOR_AUTH_REQ            = "color_mm2bpw7z"
COL_MONITOR_NETWORK_STATUS      = "color_mm2b7c1z"
COL_MONITOR_COINSURANCE         = "numeric_mm2bffek"
COL_MONITOR_HCPC                = "color_mm2b1zgq"

# ── Sensors ──────────────────────────────────────────────────────────────────
COL_SENSORS_AUTH_REQ            = "color_mm2bscj"
COL_SENSORS_NETWORK_STATUS      = "color_mm2brh0x"
COL_SENSORS_COINSURANCE         = "numeric_mm2byn1k"
COL_SENSORS_HCPC                = "color_mm2b6t98"

# ── Insulin Pump ─────────────────────────────────────────────────────────────
COL_INSULIN_PUMP_AUTH_REQ       = "color_mm2bx2ys"
COL_INSULIN_PUMP_NETWORK_STATUS = "color_mm2b91nc"
COL_INSULIN_PUMP_COINSURANCE    = "numeric_mm2b4nzx"
COL_INSULIN_PUMP_HCPC           = "color_mm2bjwvx"

# ── Infusion Set ─────────────────────────────────────────────────────────────
COL_INFUSION_SET_AUTH_REQ       = "color_mm2btvq0"
COL_INFUSION_SET_NETWORK_STATUS = "color_mm2b1ver"
COL_INFUSION_SET_COINSURANCE    = "numeric_mm2bksj8"
COL_INFUSION_SET_HCPC           = "color_mm2bpvvy"

# ── Cartridge ────────────────────────────────────────────────────────────────
COL_CARTRIDGE_AUTH_REQ          = "color_mm2bd0q0"
COL_CARTRIDGE_NETWORK_STATUS    = "color_mm2bm7g8"
COL_CARTRIDGE_COINSURANCE       = "numeric_mm2bhdrr"
COL_CARTRIDGE_HCPC              = "color_mm2bxxz2"

# All output column IDs (used for loop prevention)
ALL_OUTPUT_COLUMN_IDS = {
    COL_DEDUCTIBLE, COL_DEDUCTIBLE_REMAINING, COL_OOP_MAX, COL_OOP_MAX_REMAINING,
    COL_CGM_PUMP_INSURANCE, COL_CGM_PUMP_MEMBER_ID, COL_CGM_PUMP_INS_ACTIVE,
    COL_SUPPLIES_INSURANCE, COL_SUPPLIES_MEMBER_ID, COL_SUPPLIES_INS_ACTIVE,
    COL_MONITOR_AUTH_REQ, COL_MONITOR_NETWORK_STATUS, COL_MONITOR_COINSURANCE, COL_MONITOR_HCPC,
    COL_SENSORS_AUTH_REQ, COL_SENSORS_NETWORK_STATUS, COL_SENSORS_COINSURANCE, COL_SENSORS_HCPC,
    COL_INSULIN_PUMP_AUTH_REQ, COL_INSULIN_PUMP_NETWORK_STATUS, COL_INSULIN_PUMP_COINSURANCE, COL_INSULIN_PUMP_HCPC,
    COL_INFUSION_SET_AUTH_REQ, COL_INFUSION_SET_NETWORK_STATUS, COL_INFUSION_SET_COINSURANCE, COL_INFUSION_SET_HCPC,
    COL_CARTRIDGE_AUTH_REQ, COL_CARTRIDGE_NETWORK_STATUS, COL_CARTRIDGE_COINSURANCE, COL_CARTRIDGE_HCPC,
}

# Columns that trigger resolution
TRIGGER_COLUMN_IDS = {COL_INSURANCE_PLAN, COL_PRIMARY_INSURANCE, COL_SERVING}


# ═══════════════════════════════════════════════════════════════════════════════
# Serving logic  (PRD §10)
# ═══════════════════════════════════════════════════════════════════════════════

_SERVING_MAP = {
    "Supplies Only":        {"cgm_pump_bucket": False, "monitor": False, "sensors": False,
                             "insulin_pump": False, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
    "CGM":                  {"cgm_pump_bucket": True, "monitor": True, "sensors": True,
                             "insulin_pump": False, "supplies_bucket": False,
                             "infusion_set": False, "cartridge": False},
    "Insulin Pump":         {"cgm_pump_bucket": True, "monitor": False, "sensors": False,
                             "insulin_pump": True, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
    "Supplies + CGM":       {"cgm_pump_bucket": True, "monitor": True, "sensors": True,
                             "insulin_pump": False, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
    "Insulin Pump + CGM":   {"cgm_pump_bucket": True, "monitor": True, "sensors": True,
                             "insulin_pump": True, "supplies_bucket": True,
                             "infusion_set": True, "cartridge": True},
}


def _is_serving(serving_value, group):
    m = _SERVING_MAP.get(serving_value)
    return m.get(group, True) if m else True


# ═══════════════════════════════════════════════════════════════════════════════
# Helper to read a column value from the item dict
# ═══════════════════════════════════════════════════════════════════════════════

def _val(item_fields, col_id):
    """Get the text value of a column, stripped.  Returns '' if missing."""
    return (item_fields.get(col_id) or "").strip()


# ═══════════════════════════════════════════════════════════════════════════════
# Main resolution
# ═══════════════════════════════════════════════════════════════════════════════

def resolve_intake_fields(item_fields):
    """
    Given { column_id: text_value } for an Intake Board item,
    compute all derived output fields.

    Returns (output_dict, log_lines) where output_dict is
    { column_id: value_to_write } and log_lines is a list of
    human-readable strings describing what was set and why.
    """
    out = {}
    log = []

    primary_ins     = _val(item_fields, COL_PRIMARY_INSURANCE)
    insurance_plan  = _val(item_fields, COL_INSURANCE_PLAN)
    member_id_1     = _val(item_fields, COL_MEMBER_ID_1)
    member_id_2     = _val(item_fields, COL_MEMBER_ID_2)
    serving         = _val(item_fields, COL_SERVING)
    stedi_active    = _val(item_fields, COL_STEDI_ELIGIBILITY_ACTIVE)
    stedi_coins     = _val(item_fields, COL_STEDI_COINSURANCE)

    log.append(f"primary_ins={primary_ins!r}  plan={insurance_plan!r}  "
               f"serving={serving!r}  stedi_active={stedi_active!r}")

    # ── Step 1: Stedi copy (PRD §8) ────────────────────────────────────────
    for src, dst in [
        (COL_STEDI_INDIVIDUAL_DED,     COL_DEDUCTIBLE),
        (COL_STEDI_INDIVIDUAL_DED_REM, COL_DEDUCTIBLE_REMAINING),
        (COL_STEDI_INDIVIDUAL_OOP,     COL_OOP_MAX),
        (COL_STEDI_INDIVIDUAL_OOP_REM, COL_OOP_MAX_REMAINING),
    ]:
        v = _val(item_fields, src)
        if v:
            out[dst] = v
            log.append(f"  {dst} = {v!r}  (copied from Stedi)")

    med_id = _val(item_fields, COL_STEDI_SECONDARY_MEDICAID_ID)
    if med_id:
        out[COL_MEMBER_ID_2] = med_id
        log.append(f"  Member ID 2 = {med_id!r}  (Stedi Secondary / Medicaid ID)")
        member_id_2 = med_id  # use for downstream

    # ── Step 2: bail out if no primary insurance ─────────────────────────
    if not primary_ins:
        log.append("Primary Insurance empty — cannot resolve further.")
        return out, log

    if primary_ins not in ALLOWED_LABELS_SET:
        log.append(f"Primary Insurance '{primary_ins}' not in allowed labels.")

    # ── Step 3: insurance bucket logic (PRD §9) ─────────────────────────────
    supplies_to_medicaid = primary_ins in SUPPLIES_ROUTE_TO_MEDICAID

    # CGM & Pump bucket
    if _is_serving(serving, "cgm_pump_bucket"):
        out[COL_CGM_PUMP_INSURANCE] = primary_ins
        out[COL_CGM_PUMP_MEMBER_ID] = member_id_1
        out[COL_CGM_PUMP_INS_ACTIVE] = stedi_active or ""
        log.append(f"  CGM & Pump bucket → {primary_ins}")
    else:
        out[COL_CGM_PUMP_INSURANCE] = NOT_SERVING
        out[COL_CGM_PUMP_MEMBER_ID] = ""
        out[COL_CGM_PUMP_INS_ACTIVE] = NOT_SERVING
        log.append("  CGM & Pump bucket → Not Serving")

    # Supplies bucket
    if _is_serving(serving, "supplies_bucket"):
        if supplies_to_medicaid:
            out[COL_SUPPLIES_INSURANCE] = "Medicaid"
            out[COL_SUPPLIES_MEMBER_ID] = member_id_2
            out[COL_SUPPLIES_INS_ACTIVE] = "Yes"
            log.append("  Supplies bucket → Medicaid (route override)")
        else:
            out[COL_SUPPLIES_INSURANCE] = primary_ins
            out[COL_SUPPLIES_MEMBER_ID] = member_id_1
            out[COL_SUPPLIES_INS_ACTIVE] = stedi_active or ""
            log.append(f"  Supplies bucket → {primary_ins}")
    else:
        out[COL_SUPPLIES_INSURANCE] = NOT_SERVING
        out[COL_SUPPLIES_MEMBER_ID] = ""
        out[COL_SUPPLIES_INS_ACTIVE] = NOT_SERVING
        log.append("  Supplies bucket → Not Serving")

    # Determine resolved insurance for supply-side products
    supplies_ins = (
        "Medicaid"
        if supplies_to_medicaid and _is_serving(serving, "supplies_bucket")
        else primary_ins
    )

    # ── Step 4: per-product fields ────────────────────────────────────────

    # Fixed HCPC products (CGM & Pump side)
    _resolve_product(out, log, "monitor",      primary_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_MONITOR_AUTH_REQ, COL_MONITOR_NETWORK_STATUS,
                     COL_MONITOR_COINSURANCE, COL_MONITOR_HCPC, MONITOR_HCPC)

    _resolve_product(out, log, "sensors",      primary_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_SENSORS_AUTH_REQ, COL_SENSORS_NETWORK_STATUS,
                     COL_SENSORS_COINSURANCE, COL_SENSORS_HCPC, SENSORS_HCPC)

    _resolve_product(out, log, "insulin_pump", primary_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_INSULIN_PUMP_AUTH_REQ, COL_INSULIN_PUMP_NETWORK_STATUS,
                     COL_INSULIN_PUMP_COINSURANCE, COL_INSULIN_PUMP_HCPC, INSULIN_PUMP_HCPC)

    # Variable HCPC products (Supplies side)
    supply_hcpc = SUPPLY_HCPC_MAP.get(supplies_ins)
    inf_hcpc  = supply_hcpc[0] if supply_hcpc else EVALUATE
    cart_hcpc = supply_hcpc[1] if supply_hcpc else EVALUATE

    _resolve_product(out, log, "infusion_set", supplies_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_INFUSION_SET_AUTH_REQ, COL_INFUSION_SET_NETWORK_STATUS,
                     COL_INFUSION_SET_COINSURANCE, COL_INFUSION_SET_HCPC, inf_hcpc)

    _resolve_product(out, log, "cartridge",    supplies_ins, insurance_plan,
                     stedi_coins, serving,
                     COL_CARTRIDGE_AUTH_REQ, COL_CARTRIDGE_NETWORK_STATUS,
                     COL_CARTRIDGE_COINSURANCE, COL_CARTRIDGE_HCPC, cart_hcpc)

    return out, log


# ─── Per-product helper ────────────────────────────────────────────────────────

_PRODUCT_NAME = {
    "monitor":      "Monitor",
    "sensors":      "Sensors",
    "insulin_pump": "Insulin Pump",
    "infusion_set": "Infusion Set",
    "cartridge":    "Cartridge",
}

def _resolve_product(out, log, product_key, insurance_label, insurance_plan,
                     stedi_coins, serving,
                     auth_col, network_col, coins_col, hcpc_col, hcpc_value):
    product_name = _PRODUCT_NAME[product_key]

    if not _is_serving(serving, product_key):
        out[auth_col]    = NOT_SERVING
        out[network_col] = NOT_SERVING
        out[coins_col]   = ""
        out[hcpc_col]    = NOT_SERVING
        log.append(f"  {product_name}: Not Serving")
        return

    auth    = get_auth_requirement(insurance_label, product_name, insurance_plan)
    network = get_network_status(insurance_label, product_name, insurance_plan)
    coins   = get_coinsurance(insurance_label, stedi_coins, insurance_plan)

    out[auth_col]    = auth
    out[network_col] = network
    out[coins_col]   = coins
    out[hcpc_col]    = hcpc_value

    log.append(f"  {product_name}: auth={auth}  network={network}  "
               f"coins={coins!r}  hcpc={hcpc_value}")
