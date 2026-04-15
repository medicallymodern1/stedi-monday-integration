"""
intake_insurance_rules.py — Source-of-truth rule tables for Intake Board insurance logic.
=========================================================================================

This file mirrors the business-team’s Excel workbook and serves as the single
place in code where payer rules are maintained.  When the workbook is updated,
update the corresponding dict here and redeploy.

Sections
--------
1.  Allowed Primary Insurance Labels
2.  Supplies → Medicaid routing set
3.  HCPC mapping
4.  Auth-requirement rules  (workbook: Auth Requirements sheet)
5.  Network-status rules    (workbook: Network Status sheet)
6.  Co-insurance carveouts  (workbook: Co-Insurance Carveouts sheet)
"""

import logging

logger = logging.getLogger(__name__)

# ─── 1. Allowed Primary Insurance Labels (PRD §5) ──────────────────────

ALLOWED_PRIMARY_INSURANCE_LABELS = [
    # Fidelis
    "Fidelis Medicaid",
    "Fidelis Low-Cost",
    "Fidelis Commercial",
    "Fidelis Medicare",
    # Anthem / BCBS
    "Anthem BCBS Medicare",
    "Anthem BCBS Commercial",
    "Anthem BCBS Medicaid (JLJ)",
    "Anthem BCBS Low-Cost (JLJ)",
    "Horizon BCBS",
    "BCBS TN",
    "BCBS FL",
    "BCBS WY",
    # United
    "United Medicare",
    "United Medicaid",
    "United Commercial",
    "United Low-Cost",
    # Aetna
    "Aetna Medicare",
    "Aetna Commercial",
    # Government / Public
    "Medicare A&B",
    "Medicaid",
    "NYSHIP",
    # Other Commercial / Regional
    "Cigna",
    "Humana",
    "Wellcare",
    "Midlands Choice",
    "MagnaCare",
    "UMR",
    "Oregon Care",
]

ALLOWED_LABELS_SET = set(ALLOWED_PRIMARY_INSURANCE_LABELS)

# ─── 2. Supplies → Medicaid routing (PRD §9.2) ─────────────────────

SUPPLIES_ROUTE_TO_MEDICAID = {
    "Fidelis Medicaid",
    "Anthem BCBS Medicaid (JLJ)",
    "Medicaid",
}

# ─── 3. HCPC mapping (PRD §11) ───────────────────────────────

MONITOR_HCPC   = "E2103"
SENSORS_HCPC   = "A4239"
INSULIN_PUMP_HCPC = "E0784"

_GA = ("A4230", "A4232")   # Group A
_GB = ("A4224", "A4225")   # Group B
_GC = ("A4231", "A4232")   # Group C (Aetna)

SUPPLY_HCPC_MAP = {
    "Fidelis Medicaid":             _GA,
    "Fidelis Low-Cost":             _GA,
    "Fidelis Commercial":           _GA,
    "Anthem BCBS Commercial":       _GA,
    "Anthem BCBS Medicaid (JLJ)":   _GA,
    "Anthem BCBS Low-Cost (JLJ)":   _GA,
    "United Medicaid":              _GA,
    "United Commercial":            _GA,
    "United Low-Cost":              _GA,
    "Horizon BCBS":                 _GA,
    "BCBS TN":                      _GA,
    "BCBS FL":                      _GA,
    "BCBS WY":                      _GA,
    "Medicaid":                     _GA,
    "Oregon Care":                  _GA,
    "MagnaCare":                    _GA,
    "UMR":                          _GA,
    "Anthem BCBS Medicare":         _GB,
    "Fidelis Medicare":             _GB,
    "Medicare A&B":                 _GB,
    "NYSHIP":                       _GB,
    "United Medicare":              _GB,
    "Wellcare":                     _GB,
    "Humana":                       _GB,
    "Cigna":                        _GB,
    "Midlands Choice":              _GB,
    "Aetna Commercial":             _GC,
    "Aetna Medicare":               _GC,
}

# ─── 4. Auth-requirement rules (workbook: Auth Requirements) ────────────────

AUTH_RULES = {
    ("Fidelis Medicaid", "Monitor"):       "Not Serving",
    ("Fidelis Medicaid", "Sensors"):       "Not Serving",
    ("Fidelis Medicaid", "Insulin Pump"):  "Yes",
    ("Fidelis Medicaid", "Infusion Set"):  "Not Serving",
    ("Fidelis Medicaid", "Cartridge"):     "Not Serving",

    ("Fidelis Low-Cost", "Monitor"):       "Yes",
    ("Fidelis Low-Cost", "Sensors"):       "Evaluate",
    ("Fidelis Low-Cost", "Insulin Pump"):  "Yes",
    ("Fidelis Low-Cost", "Infusion Set"):  "Evaluate",
    ("Fidelis Low-Cost", "Cartridge"):     "Evaluate",

    ("Fidelis Commercial", "Monitor"):       "Yes",
    ("Fidelis Commercial", "Sensors"):       "No",
    ("Fidelis Commercial", "Insulin Pump"):  "Yes",
    ("Fidelis Commercial", "Infusion Set"):  "No",
    ("Fidelis Commercial", "Cartridge"):     "No",

    ("Fidelis Medicare", "Monitor"):       "Yes",
    ("Fidelis Medicare", "Sensors"):       "No",
    ("Fidelis Medicare", "Insulin Pump"):  "Yes",
    ("Fidelis Medicare", "Infusion Set"):  "No",
    ("Fidelis Medicare", "Cartridge"):     "No",

    ("Anthem BCBS Medicare", "Monitor"):       "Yes",
    ("Anthem BCBS Medicare", "Sensors"):       "Yes",
    ("Anthem BCBS Medicare", "Insulin Pump"):  "Evaluate",
    ("Anthem BCBS Medicare", "Infusion Set"):  "Evaluate",
    ("Anthem BCBS Medicare", "Cartridge"):     "Evaluate",

    ("Anthem BCBS Commercial", "Monitor"):       "No",
    ("Anthem BCBS Commercial", "Sensors"):       "No",
    ("Anthem BCBS Commercial", "Insulin Pump"):  "No",
    ("Anthem BCBS Commercial", "Infusion Set"):  "No",
    ("Anthem BCBS Commercial", "Cartridge"):     "No",

    ("Anthem BCBS Medicaid (JLJ)", "Monitor"):       "Not Serving",
    ("Anthem BCBS Medicaid (JLJ)", "Sensors"):       "Not Serving",
    ("Anthem BCBS Medicaid (JLJ)", "Insulin Pump"):  "Yes",
    ("Anthem BCBS Medicaid (JLJ)", "Infusion Set"):  "Evaluate",
    ("Anthem BCBS Medicaid (JLJ)", "Cartridge"):     "Evaluate",

    ("Anthem BCBS Low-Cost (JLJ)", "Monitor"):       "Not Serving",
    ("Anthem BCBS Low-Cost (JLJ)", "Sensors"):       "Not Serving",
    ("Anthem BCBS Low-Cost (JLJ)", "Insulin Pump"):  "Evaluate",
    ("Anthem BCBS Low-Cost (JLJ)", "Infusion Set"):  "No",
    ("Anthem BCBS Low-Cost (JLJ)", "Cartridge"):     "No",

    ("Horizon BCBS", "Monitor"):       "Yes",
    ("Horizon BCBS", "Sensors"):       "Yes",
    ("Horizon BCBS", "Insulin Pump"):  "Yes",
    ("Horizon BCBS", "Infusion Set"):  "Yes",
    ("Horizon BCBS", "Cartridge"):     "Yes",

    ("Aetna Medicare", "Monitor"):       "No",
    ("Aetna Medicare", "Sensors"):       "No",
    ("Aetna Medicare", "Insulin Pump"):  "No",
    ("Aetna Medicare", "Infusion Set"):  "No",
    ("Aetna Medicare", "Cartridge"):     "No",

    ("Aetna Commercial", "Monitor"):       "No",
    ("Aetna Commercial", "Sensors"):       "No",
    ("Aetna Commercial", "Insulin Pump"):  "No",
    ("Aetna Commercial", "Infusion Set"):  "No",
    ("Aetna Commercial", "Cartridge"):     "No",

    ("Medicare A&B", "Monitor"):       "No",
    ("Medicare A&B", "Sensors"):       "No",
    ("Medicare A&B", "Insulin Pump"):  "No",
    ("Medicare A&B", "Infusion Set"):  "No",
    ("Medicare A&B", "Cartridge"):     "No",

    ("Medicaid", "Monitor"):       "Not Serving",
    ("Medicaid", "Sensors"):       "Not Serving",
    ("Medicaid", "Insulin Pump"):  "Evaluate",
    ("Medicaid", "Infusion Set"):  "Evaluate",
    ("Medicaid", "Cartridge"):     "Evaluate",
}

# Everything else (BCBS TN/FL/WY, United *, Cigna, Humana, Wellcare,
# Midlands Choice, MagnaCare, UMR, Oregon Care, NYSHIP) → Evaluate
_EVALUATE_CARRIERS = [
    "BCBS TN", "BCBS FL", "BCBS WY",
    "United Medicare", "United Medicaid", "United Commercial", "United Low-Cost",
    "NYSHIP", "Cigna", "Humana", "Wellcare",
    "Midlands Choice", "MagnaCare", "UMR", "Oregon Care",
]
for _carrier in _EVALUATE_CARRIERS:
    for _prod in ("Monitor", "Sensors", "Insulin Pump", "Infusion Set", "Cartridge"):
        AUTH_RULES.setdefault((_carrier, _prod), "Evaluate")

# Plan-level overrides
AUTH_PLAN_OVERRIDES = {
    ("Anthem BCBS Medicaid (JLJ)", "MLTC", "Insulin Pump"):     "Yes",
    ("Anthem BCBS Low-Cost (JLJ)", "MLTC", "Insulin Pump"):     "Evaluate",
}


def get_auth_requirement(primary_insurance, product, insurance_plan=""):
    plan_upper = (insurance_plan or "").upper()
    for (ins, plan_kw, prod), value in AUTH_PLAN_OVERRIDES.items():
        if ins == primary_insurance and prod == product and plan_kw.upper() in plan_upper:
            return value
    return AUTH_RULES.get((primary_insurance, product), "Evaluate")


# ─── 5. Network-status rules (workbook: Network Status) ──────────────────

NETWORK_RULES = {
    "Fidelis Medicaid":             {"All Products": "INN"},
    "Fidelis Low-Cost":             {"All Products": "INN"},
    "Fidelis Commercial":           {"All Products": "INN"},
    "Fidelis Medicare":             {"All Products": "INN"},
    "Anthem BCBS Medicare":         {"All Products": "INN"},
    "Anthem BCBS Commercial":       {"All Products": "Evaluate"},
    "Anthem BCBS Medicaid (JLJ)":   {"All Products": "INN"},
    "Anthem BCBS Low-Cost (JLJ)":   {"All Products": "INN"},
    "Horizon BCBS":                 {"All Products": "INN"},
    "BCBS TN":                      {"All Products": "INN"},
    "BCBS FL":                      {"All Products": "INN"},
    "BCBS WY":                      {"All Products": "INN"},
    "United Medicare":              {"All Products": "Evaluate"},
    "United Medicaid":              {"All Products": "Evaluate"},
    "United Commercial":            {"All Products": "Evaluate"},
    "United Low-Cost":              {"All Products": "Evaluate"},
    "Aetna Medicare":               {"All Products": "INN"},
    "Aetna Commercial":             {"All Products": "INN"},
    "Medicare A&B":                 {"All Products": "INN"},
    "Medicaid":                     {"All Products": "INN"},
    "NYSHIP":                       {"All Products": "INN"},
    "Cigna":                        {"CGM": "Evaluate", "Pump/Supplies": "INN"},
    "Humana":                       {"All Products": "INN"},
    "Wellcare":                     {"All Products": "INN"},
    "Midlands Choice":              {"All Products": "INN"},
    "MagnaCare":                    {"All Products": "INN"},
    "UMR":                          {"All Products": "INN"},
    "Oregon Care":                  {"All Products": "INN"},
}

_CGM_PRODUCTS = {"Monitor", "Sensors"}
_PUMP_SUPPLY_PRODUCTS = {"Insulin Pump", "Infusion Set", "Cartridge"}


def get_network_status(primary_insurance, product, insurance_plan=""):
    rules = NETWORK_RULES.get(primary_insurance)
    if not rules:
        return "Evaluate"
    if "All Products" in rules:
        return rules["All Products"]
    if product in _CGM_PRODUCTS and "CGM" in rules:
        return rules["CGM"]
    if product in _PUMP_SUPPLY_PRODUCTS and "Pump/Supplies" in rules:
        return rules["Pump/Supplies"]
    return "Evaluate"


# ─── 6. Co-insurance carveouts (workbook: Co-Insurance Carveouts) ───────────

COINSURANCE_OVERRIDES = {
    "United Medicare":  0,
    "Medicare A&B":     0,
    "Humana":           0,
}

COINSURANCE_PLAN_OVERRIDES = {}


def get_coinsurance(primary_insurance, stedi_coinsurance, insurance_plan=""):
    plan_upper = (insurance_plan or "").upper()
    for (ins, plan_kw), value in COINSURANCE_PLAN_OVERRIDES.items():
        if ins == primary_insurance and plan_kw.upper() in plan_upper:
            return str(int(value)) if value == 0 else str(value)
    override = COINSURANCE_OVERRIDES.get(primary_insurance)
    if override is not None:
        return str(int(override)) if override == 0 else str(override)
    # Stedi returns coinsurance as a decimal (e.g. 0.05 = 5%).
    # Convert to whole-number percentage to match Monday numeric columns.
    if stedi_coinsurance:
        try:
            val = float(stedi_coinsurance)
            if val < 1:
                val = val * 100
            return str(int(val)) if val == int(val) else str(val)
        except (ValueError, TypeError):
            pass
    return stedi_coinsurance or ""
