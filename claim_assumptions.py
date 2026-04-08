from __future__ import annotations

import secrets
import string
from datetime import datetime, timedelta
from typing import Any, Optional

# ============================================================
# SOURCE OF TRUTH: BILLING / CLAIM ASSUMPTIONS
# ============================================================
# This file should contain the business assumptions that determine
# how a claim is built.
#
# If something bills wrong, this is the first file to inspect.
# ============================================================


# ============================================================
# BILLING PROVIDER / SUBMITTER DEFAULTS
# ============================================================

USAGE_INDICATOR = "P"  # "P" = Production, "T" = Test

BILLING_PROVIDER_ORGANIZATION_NAME = "Mid-Island Medical Supply Company"
BILLING_PROVIDER_ADDRESS_1 = "2093 Wantagh Ave"
BILLING_PROVIDER_ADDRESS_2 = ""
BILLING_PROVIDER_CITY = "Wantagh"
BILLING_PROVIDER_STATE = "NY"
BILLING_PROVIDER_POSTAL_CODE = "11793"
BILLING_PROVIDER_NPI = "1023042348"
BILLING_PROVIDER_EIN = "113254896"
BILLING_PROVIDER_TAXONOMY_CODE = "332B00000X"

BILLING_PROVIDER_CONTACT_NAME = "Billing Department"
BILLING_PROVIDER_CONTACT_PHONE_NUMBER = "347-503-7148"

# For now, submitter is treated as the same entity as the billing provider.
SUBMITTER_ORGANIZATION_NAME = BILLING_PROVIDER_ORGANIZATION_NAME
SUBMITTER_IDENTIFICATION = BILLING_PROVIDER_EIN
SUBMITTER_CONTACT_NAME = BILLING_PROVIDER_CONTACT_NAME
SUBMITTER_PHONE_NUMBER = BILLING_PROVIDER_CONTACT_PHONE_NUMBER

# ============================================================
# CLAIM-LEVEL DEFAULTS
# ============================================================

DEFAULT_CLAIM_FREQUENCY_CODE = "1"
DEFAULT_SIGNATURE_INDICATOR = "Y"
DEFAULT_PLAN_PARTICIPATION_CODE = "A"
DEFAULT_BENEFITS_ASSIGNMENT_CERTIFICATION_INDICATOR = "Y"
DEFAULT_RELEASE_INFORMATION_CODE = "Y"

# ============================================================
# PATIENT CONTROL NUMBER RULES
# ============================================================

PCN_ALPHABET = string.ascii_uppercase + string.digits


def generate_patient_control_number(length: int = 17) -> str:
    """Generate a random patient control number."""
    return "".join(secrets.choice(PCN_ALPHABET) for _ in range(length))


PROVIDER_CONTROL_NUMBER_ALPHABET = string.ascii_uppercase + string.digits


def generate_provider_control_number(length: int = 12) -> str:
    """Generate a random provider control number for one service line."""
    return "".join(secrets.choice(PROVIDER_CONTROL_NUMBER_ALPHABET) for _ in range(length))


# ============================================================
# PAYER MAPPINGS
# ============================================================

PAYER_ID_MAP = {
    "Anthem BCBS Commercial": "803",
    "Anthem BCBS Medicare": "803",
    "Anthem BCBS Medicaid (JLJ)": "803",
    "Fidelis Commercial": "11315",
    "Fidelis Medicaid": "11315",
    "Medicare A&B": "16013",
    "NYSHIP": "87726",
    "United Commercial": "87726",
    "United Medicare": "87726",
    "Aetna Medicare": "60054",
    "Aetna Commercial": "60054",
    "Wellcare": "14163",
    "Humana": "61101",
    "Cigna": "62308",
    "Medicaid": "MCDNY",
    "Midlands Choice": "47080",
    "Horizon BCBS": "22099",
    "BCBS TN": "SB890",
    "BCBS FL": "BCBSF",
}

CLAIM_FILING_CODE_MAP = {
    "Anthem BCBS Commercial": "CI",
    "Anthem BCBS Medicare": "MB",
    "Anthem BCBS Medicaid (JLJ)": "MC",
    "Fidelis Commercial": "CI",
    "Fidelis Medicaid": "MC",
    "Medicare A&B": "MB",
    "NYSHIP": "CI",
    "United Commercial": "CI",
    "United Medicare": "MB",
    "Aetna Commercial": "CI",
    "Aetna Medicare": "MB",
    "Wellcare": "MC",
    "Humana": "CI",
    "Cigna": "CI",
    "Medicaid": "MC",
    "Midlands Choice": "CI",
    "Horizon BCBS": "CI",
    "BCBS TN": "CI",
    "BCBS FL": "CI",
}

DEFAULT_CLAIM_FILING_CODE = "CI"

# ============================================================
# BCBS / ANTHEM ROUTING + PLACE OF SERVICE RULES
# ============================================================

DEFAULT_PLACE_OF_SERVICE_CODE = "12"
OUT_OF_STATE_BCBS_PLACE_OF_SERVICE_CODE = "11"

# Any of these payer names should trigger BCBS home-state routing logic.
BCBS_FAMILY_PAYER_NAMES = {
    "Anthem BCBS Commercial",
    "Anthem BCBS Medicare",
    "Anthem BCBS Medicaid (JLJ)",
    "Horizon BCBS",
    "BCBS TN",
    "BCBS FL",
}

# Route BCBS-family claims based on the patient's home state.
# If the patient is in one of these states, route to that local BCBS payer and keep POS 12.
BCBS_HOME_STATE_PAYER_MAP = {
    "NJ": "Horizon BCBS",
    "TN": "BCBS TN",
    "FL": "BCBS FL",
    "NY": "Anthem BCBS Commercial",
}

# For any other patient home state, default to Anthem Commercial
# but use POS 11 instead of POS 12.
DEFAULT_OUT_OF_STATE_BCBS_PAYER_NAME = "Anthem BCBS Commercial"

# ============================================================
# PROCEDURE CODE MAPPINGS
# ============================================================

FIXED_PROCEDURE_CODE_MAP = {
    "Insulin Pump": "E0784",
    "CGM Monitor": "E2103",
    "CGM Sensors": "A4239",
}

INFUSION_SET_ITEM_NAMES = {
    "Infusion Set 1",
    "Infusion Set 2",
}

CARTRIDGE_ITEM_NAMES = {
    "Cartridge",
    "Cartridges",
}

PAYER_SUPPLY_PROCEDURE_CODE_MAP = {
    "Anthem BCBS Commercial": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "Anthem BCBS Medicare": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "Anthem BCBS Medicaid (JLJ)": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "Fidelis Commercial": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "Fidelis Medicaid": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "Medicare A&B": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "NYSHIP": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "United Commercial": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "United Medicare": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "Aetna Commercial": {
        "infusion_set": "A4231",
        "cartridge": "A4232",
    },
    "Aetna Medicare": {
        "infusion_set": "A4231",
        "cartridge": "A4232",
    },
    "Wellcare": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "Humana": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "Cigna": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "Medicaid": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "Midlands Choice": {
        "infusion_set": "A4224",
        "cartridge": "A4225",
    },
    "Horizon BCBS": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "BCBS TN": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
    "BCBS FL": {
        "infusion_set": "A4230",
        "cartridge": "A4232",
    },
}

# ============================================================
# SERVICE UNIT COUNT RULES
# ============================================================

CGM_UNITS_DIVISOR_MAP = {
    "Freestyle Libre 2 Plus": 2,
    "FreeStyle Libre 3 Plus": 2,
    "FreeStyle Libre 14-Day": 2,
    "Dexcom G7 15-Day": 2,
    "Instinct": 2,
    "Dexcom G7": 3,
    "Dexcom G6": 3,
    "Guardian 4": 4,
}

STEDI_TRADING_PARTNER_NAME_BY_PAYER_ID = {
    "803": "Anthem Blue Cross Blue Shield of New York",
    "11315": "Fidelis Care",
    "87726": "UnitedHealthcare",
    "60054": "Aetna",
    "14163": "Wellcare",
    "61101": "Humana",
    "62308": "Cigna",
    "MCDNY": "Medicaid New York",
    "47080": "Midlands Choice",
    "22099": "Horizon Blue Cross and Blue Shield of New Jersey",
    "SB890": "BlueCross BlueShield of Tennessee",
    "BCBSF": "Florida Blue",
    "STEDITEST": "Stedi Test Payer",
}

FIXED_SERVICE_UNIT_COUNT_BY_PROCEDURE_AND_PAYER = {
    "A4224": {
        "__default__": "14",
    },
    "A4225": {
        "Medicare A&B": "40",
        "NYSHIP": "40",
        "Anthem BCBS Medicare": "30",
        "Humana": "30",
        "Cigna": "30",
        "Midlands Choice": "30",
    },
}

DEFAULT_A4225_SERVICE_UNIT_COUNT = "30"

QUANTITY_BASED_PROCEDURE_CODES = {
    "A4230",
    "A4231",
    "A4232",
}

# ============================================================
# PAYER-SPECIFIC RATE ASSUMPTIONS
# ============================================================

PAYER_RATE_SCHEDULE = {

    "NYSHIP": dict(
        pump_rate=4326.7,
        infusion_rate=24.64,
        cartridge_rate=3.30,
        monitor_rate=298.7,
        sensor_rate=310.0,
    ),
    "Anthem BCBS Commercial": dict(
        pump_rate=4200.0,
        infusion_rate=8.75,
        cartridge_rate=2.95,
        monitor_rate=400.0,
        sensor_rate=375.0,
    ),
    "Anthem BCBS Medicare": dict(
        pump_rate=4200.0,
        infusion_rate=25.19,
        cartridge_rate=3.38,
        monitor_rate=267.92,
        sensor_rate=255.0,
    ),
    "Anthem BCBS Medicaid (JLJ)": dict(
        pump_rate=4200.0,
        infusion_rate=8.75,
        cartridge_rate=2.95,
        monitor_rate=None,
        sensor_rate=None,
    ),
    "Fidelis Commercial": dict(
        pump_rate=4000,
        infusion_rate=11.17,
        cartridge_rate=2.65,
        monitor_rate=193.97,
        sensor_rate=216.67,
    ),
    "Fidelis Medicaid": dict(
        pump_rate=4000,
        infusion_rate=None,
        cartridge_rate=None,
        monitor_rate=None,
        sensor_rate=None,
    ),
    "Medicare A&B": dict(
        pump_rate=576.0,
        infusion_rate=29.07,
        cartridge_rate=3.62,
        monitor_rate=322.63,
        sensor_rate=318.00,
    ),
    "Medicaid": dict(
        pump_rate=4440.0,
        infusion_rate=15.2,
        cartridge_rate=3.61,
        monitor_rate=None,
        sensor_rate=None,
    ),
    "United Commercial": dict(
        pump_rate=None,
        infusion_rate=6.97,
        cartridge_rate=1.83,
        monitor_rate=167.27,
        sensor_rate=176.55,
    ),
    "United Medicare": dict(
        pump_rate=None,
        infusion_rate=None,
        cartridge_rate=None,
        monitor_rate=167.27,
        sensor_rate=176.55,
    ),
    "Aetna Commercial": dict(
        pump_rate=1597.0,
        infusion_rate=23.51,
        cartridge_rate=0.92,
        monitor_rate=191.17,
        sensor_rate=201.77,
    ),
    "Aetna Medicare": dict(
        pump_rate=1597.0,
        infusion_rate=23.51,
        cartridge_rate=0.92,
        monitor_rate=191.17,
        sensor_rate=201.77,
    ),
    "Wellcare": dict(
        pump_rate=None,
        infusion_rate=None,
        cartridge_rate=None,
        monitor_rate=241.97,
        sensor_rate=229.13,
    ),
    "Humana": dict(
        pump_rate=5431.0,
        infusion_rate=25.19,
        cartridge_rate=3.38,
        monitor_rate=295.36,
        sensor_rate=267.92,
    ),
    "Cigna": dict(
        pump_rate=4200.0,
        infusion_rate=17.75,
        cartridge_rate=2.36,
        monitor_rate=214.05,
        sensor_rate=170.42,
    ),
    "Midlands Choice": dict(
        pump_rate=5644.0,
        infusion_rate=31.68,
        cartridge_rate=3.96,
        monitor_rate=331.40,
        sensor_rate=349.77,
    ),
    "Horizon BCBS": dict(
        pump_rate=4300.0,
        infusion_rate=10.90,
        cartridge_rate=3.10,
        monitor_rate=480.0,
        sensor_rate=445.0,
    ),
    "BCBS TN": dict(
        pump_rate=None,
        infusion_rate=None,
        cartridge_rate=None,
        monitor_rate=None,
        sensor_rate=None,
    ),
    "BCBS FL": dict(
        pump_rate=None,
        infusion_rate=None,
        cartridge_rate=None,
        monitor_rate=None,
        sensor_rate=None,
    ),
}

LEGACY_PROCEDURE_CODE_CHARGE_MAP = {
    "E0784": "6000",
    "A4224": "1000",
    "A4225": "1000",
    "A4230": "1000",
    "A4231": "1000",
    "A4232": "1000",
    "E2103": "500",
    "A4239": "1500",
}

UNIT_BASED_PROCEDURE_CODES_FOR_RATE_PRICING = {
    "A4224",
    "A4225",
    "A4230",
    "A4231",
    "A4232",
    "A4239",
}

# ============================================================
# MODIFIER RULES
# ============================================================

CGM_COVERAGE_MODIFIER_PREFIX_MAP = {
    "Insulin": "KX",
    "Hypo": "KS",
}

DEFAULT_CGM_MODIFIERS = ["KF", "CG"]
E2103_BASE_MODIFIERS = ["KF", "CG", "NU"]

E0784_MODIFIERS = ["NU", "KX"]
A4224_MODIFIERS = ["KX"]
A4225_MODIFIERS = ["KX"]
A4230_MODIFIERS = ["KX"]
A4231_MODIFIERS = ["KX"]
A4232_MODIFIERS = ["KX"]

UNITED_PAYER_NAMES = {"United Commercial", "United Medicare"}


# ============================================================
# SHARED LOW-LEVEL HELPERS
# ============================================================

def safe_str(value: Any) -> str:
    """Convert a value to a clean string. Returns '' for None."""
    if value is None:
        return ""
    return str(value).strip()


def normalize_spaces(text: str) -> str:
    """Collapse multiple spaces into a single space."""
    return " ".join(safe_str(text).split())


def parse_int(value: Any) -> int:
    """Safely parse an integer. Returns 0 if blank or invalid."""
    value = safe_str(value)
    if not value:
        return 0

    try:
        return int(float(value))
    except ValueError:
        return 0


def parse_yyyymmdd(date_str: str) -> Optional[datetime]:
    """Parse a YYYYMMDD string into a datetime object."""
    date_str = safe_str(date_str)
    if not date_str:
        return None

    try:
        return datetime.strptime(date_str, "%Y%m%d")
    except ValueError:
        return None


def add_days_to_yyyymmdd(date_str: str, days: int) -> str:
    """Add days to a YYYYMMDD string and return YYYYMMDD."""
    dt = parse_yyyymmdd(date_str)
    if dt is None:
        return ""
    return (dt + timedelta(days=days)).strftime("%Y%m%d")


def normalize_item_name(item_name: str) -> str:
    """Normalize item/subitem names for matching."""
    return normalize_spaces(item_name)


# ============================================================
# ASSUMPTION RESOLVERS
# ============================================================

def safe_rate(value: Any) -> float:
    """Treat None as missing/zero-safe for rate math."""
    return 0.0 if value is None else float(value)


def resolve_rate_category_for_procedure_code(payer_name: str, procedure_code: str) -> str:
    """
    Determine which payer rate bucket applies to the resolved HCPCS code.

    This uses the existing payer-specific HCPCS mapping logic so we do not
    need to duplicate both A4224 and A4230/A4231/A4232 in the rate schedule.
    """
    payer_name = safe_str(payer_name)
    procedure_code = safe_str(procedure_code)

    if procedure_code == "E0784":
        return "pump_rate"

    if procedure_code == "E2103":
        return "monitor_rate"

    if procedure_code == "A4239":
        return "sensor_rate"

    payer_supply_map = PAYER_SUPPLY_PROCEDURE_CODE_MAP.get(payer_name, {})

    if procedure_code == safe_str(payer_supply_map.get("infusion_set", "")):
        return "infusion_rate"

    if procedure_code == safe_str(payer_supply_map.get("cartridge", "")):
        return "cartridge_rate"

    return ""


def resolve_bcbs_routed_payer_name_and_pos(original_payer_name: str, patient_state: str) -> tuple[str, str]:
    """
    Route BCBS / Anthem family payers based on the patient's home state.

    Rules:
    - NJ -> Horizon BCBS, POS 12
    - TN -> BCBS TN, POS 12
    - FL -> BCBS FL, POS 12
    - NY -> Anthem BCBS Commercial, POS 12
    - Any other state -> Anthem BCBS Commercial, POS 11

    If the original payer is not part of the BCBS family, leave payer unchanged and use POS 12.
    """
    original_payer_name = safe_str(original_payer_name)
    patient_state = safe_str(patient_state).upper()

    if original_payer_name not in BCBS_FAMILY_PAYER_NAMES:
        return original_payer_name, DEFAULT_PLACE_OF_SERVICE_CODE

    if patient_state in BCBS_HOME_STATE_PAYER_MAP:
        return BCBS_HOME_STATE_PAYER_MAP[patient_state], DEFAULT_PLACE_OF_SERVICE_CODE

    return DEFAULT_OUT_OF_STATE_BCBS_PAYER_NAME, OUT_OF_STATE_BCBS_PLACE_OF_SERVICE_CODE


def resolve_payer_name(normalized_order: dict) -> str:
    """
    Resolve the final payer name used for mappings.

    Steps:
    1. Start with payer_name if present, otherwise primary_insurance_name.
    2. If payer belongs to the BCBS family, reroute based on patient home state.
    """
    original_payer_name = safe_str(normalized_order.get("payer_name", ""))
    if not original_payer_name:
        original_payer_name = safe_str(normalized_order.get("primary_insurance_name", ""))

    patient_state = safe_str(normalized_order.get("patient_state", ""))
    routed_payer_name, _ = resolve_bcbs_routed_payer_name_and_pos(
        original_payer_name=original_payer_name,
        patient_state=patient_state,
    )
    return routed_payer_name


def resolve_payer_id(payer_name: str) -> str:
    """Return the payer ID from the payer mapping table."""
    return safe_str(PAYER_ID_MAP.get(payer_name, ""))


def resolve_claim_filing_code(payer_name: str) -> str:
    """Return the claim filing code for the payer."""
    return safe_str(CLAIM_FILING_CODE_MAP.get(payer_name, DEFAULT_CLAIM_FILING_CODE))


def resolve_place_of_service_code(payer_name: str, patient_state: str) -> str:
    """
    Resolve final place of service code.

    For BCBS-family payers, POS depends on the patient's home state routing.
    For all other payers, default to POS 12.
    """
    _, pos_code = resolve_bcbs_routed_payer_name_and_pos(
        original_payer_name=payer_name,
        patient_state=patient_state,
    )
    return pos_code


def resolve_procedure_code(payer_name: str, item_name: str) -> str:
    """
    Resolve procedure code from payer + item name.
    """
    item_name = normalize_item_name(item_name)

    if item_name in FIXED_PROCEDURE_CODE_MAP:
        return FIXED_PROCEDURE_CODE_MAP[item_name]

    payer_supply_map = PAYER_SUPPLY_PROCEDURE_CODE_MAP.get(payer_name, {})

    if item_name in INFUSION_SET_ITEM_NAMES:
        return safe_str(payer_supply_map.get("infusion_set", ""))

    if item_name in CARTRIDGE_ITEM_NAMES:
        return safe_str(payer_supply_map.get("cartridge", ""))

    return ""


def resolve_cgm_service_unit_count(variant: str, quantity: Any,
                                   order_frequency: str = "") -> str:
    """
    Resolve service unit count for CGM Sensors.

    Primary rule (matches Monday Claim Quantity Formula exactly):
      90-Day(s) order → 3 billing units
      60-Day(s) order → 2 billing units

    Fallback (when frequency unknown): order_qty / variant_divisor.
    """
    freq = normalize_spaces(order_frequency).rstrip("s")  # "90-Days" -> "90-Day"

    if freq == "90-Day":
        return "3"
    if freq == "60-Day":
        return "2"

    # Fallback: physical box count via variant divisor
    variant = normalize_spaces(variant)
    qty = parse_int(quantity)

    if qty <= 0:
        return ""

    divisor = CGM_UNITS_DIVISOR_MAP.get(variant)
    if not divisor:
        return str(qty) if qty > 0 else ""

    units = qty / divisor
    return str(int(units)) if units.is_integer() else str(units)


def resolve_supply_service_unit_count(procedure_code: str, payer_name: str, quantity: Any) -> str:
    """
    Resolve service unit count for infusion sets / cartridges.
    """
    procedure_code = safe_str(procedure_code)
    qty = parse_int(quantity)

    if procedure_code in QUANTITY_BASED_PROCEDURE_CODES:
        return str(qty * 10) if qty > 0 else ""

    if procedure_code == "A4224":
        return FIXED_SERVICE_UNIT_COUNT_BY_PROCEDURE_AND_PAYER["A4224"]["__default__"]

    if procedure_code == "A4225":
        payer_rules = FIXED_SERVICE_UNIT_COUNT_BY_PROCEDURE_AND_PAYER["A4225"]
        return safe_str(payer_rules.get(payer_name, DEFAULT_A4225_SERVICE_UNIT_COUNT))

    return ""


def resolve_service_unit_count(
        payer_name: str,
        item_name: str,
        variant: str,
        quantity: Any,
        procedure_code: str,
        order_frequency: str = "",
) -> str:
    """
    Resolve service unit count based on item type and procedure code.
    order_frequency is used for CGM Sensors to match the Monday formula.
    """
    item_name = normalize_item_name(item_name)

    if item_name == "Insulin Pump":
        return "1"

    if item_name == "CGM Monitor":
        return "1"

    if item_name == "CGM Sensors":
        return resolve_cgm_service_unit_count(variant, quantity, order_frequency)

    if item_name in INFUSION_SET_ITEM_NAMES or item_name in CARTRIDGE_ITEM_NAMES:
        return resolve_supply_service_unit_count(procedure_code, payer_name, quantity)

    return ""


def resolve_procedure_modifiers(
        payer_name: str,
        procedure_code: str,
        cgm_coverage: str,
) -> list[str]:
    """
    Resolve procedure modifiers based on procedure code, payer, and CGM coverage.
    """
    procedure_code = safe_str(procedure_code)
    cgm_coverage = normalize_spaces(cgm_coverage)

    if procedure_code == "E0784":
        return E0784_MODIFIERS.copy()

    if procedure_code == "A4224":
        return A4224_MODIFIERS.copy()

    if procedure_code == "A4225":
        return A4225_MODIFIERS.copy()

    if procedure_code == "A4230":
        return A4230_MODIFIERS.copy()

    if procedure_code == "A4231":
        return A4231_MODIFIERS.copy()

    if procedure_code == "A4232":
        return A4232_MODIFIERS.copy()

    if procedure_code == "A4239":
        coverage_prefix = CGM_COVERAGE_MODIFIER_PREFIX_MAP.get(cgm_coverage)
        modifiers = DEFAULT_CGM_MODIFIERS.copy()

        if coverage_prefix:
            modifiers.insert(0, coverage_prefix)

        if payer_name in UNITED_PAYER_NAMES:
            modifiers.append("NU")

        return modifiers

    if procedure_code == "E2103":
        coverage_prefix = CGM_COVERAGE_MODIFIER_PREFIX_MAP.get(cgm_coverage)
        modifiers = E2103_BASE_MODIFIERS.copy()

        if coverage_prefix:
            modifiers.insert(0, coverage_prefix)

        return modifiers

    return []


def resolve_line_item_charge_amount(
        payer_name: str,
        procedure_code: str,
        service_unit_count: Any,
) -> str:
    """
    Resolve line item charge amount.

    Smarter rule:
    1. Use the existing payer-specific HCPCS mapping logic to determine
       whether the resolved procedure code should use infusion_rate,
       cartridge_rate, pump_rate, monitor_rate, or sensor_rate.
    2. If that payer has a configured rate for that category, use it.
    3. Multiply by service units for unit-based HCPCS codes.
    4. If no configured payer/category rate exists, fall back to the
       legacy hardcoded amount.
    """
    payer_name = safe_str(payer_name)
    procedure_code = safe_str(procedure_code)
    units = parse_int(service_unit_count)

    payer_rates = PAYER_RATE_SCHEDULE.get(payer_name, {})
    rate_category = resolve_rate_category_for_procedure_code(payer_name, procedure_code)

    if rate_category:
        rate_value = payer_rates.get(rate_category)

        if rate_value is not None:
            amount = safe_rate(rate_value)

            if procedure_code in UNIT_BASED_PROCEDURE_CODES_FOR_RATE_PRICING:
                amount *= units

            # if amount.is_integer():
            #     return str(int(amount))
            return f"{amount:.2f}"

    return safe_str(LEGACY_PROCEDURE_CODE_CHARGE_MAP.get(procedure_code, ""))


def sum_claim_charge_amount(service_lines: list[dict]) -> str:
    """
    Sum all line-item charges into one claim-level charge amount string.
    """
    total = 0.0

    for line in service_lines:
        amount_str = safe_str(line.get("line_item_charge_amount", ""))
        if not amount_str:
            continue

        try:
            total += float(amount_str)
        except ValueError:
            continue

    # if total.is_integer():
    #     return str(int(total))

    return f"{total:.2f}"
