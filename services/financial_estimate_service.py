"""
services/financial_estimate_service.py
=======================================
Pure math for the Subscription Board "Calculate Financials" feature.

For one fill (one shipment cycle), compute:
  - Sensors Revenue / Cost / GP
  - Supplies Revenue / Cost / GP

Inputs (per row):
  - Primary Insurance label (from the Subscription board status column)
  - Number of infusion sets shipped this fill (Inf Qty 1 + Inf Qty 2)

Outputs are returned as plain dicts; the Monday-writer layer is the
only thing that knows about column IDs.

Sources of truth:
  - PAYER_RATE_SCHEDULE          — per-payer per-HCPC unit rates
  - PAYER_SUPPLY_PROCEDURE_CODE_MAP — which supply HCPCs each payer uses

Calculation rules (per Brandon's spec):

  Sensors:
    - Always 3 HCPC units per fill (A4239)
    - Revenue = sensor_rate * 3
    - Cost    = $500 (flat)
    - GP      = Revenue - Cost

  Supplies:
    - Apply Medicaid-supplies-split first: if Primary Insurance is
      Fidelis Medicaid, Anthem BCBS Medicaid (JLJ), or generic
      Medicaid, the supplies side is paid by Medicaid (mirrors
      intake_insurance_resolver). Use Medicaid rates for supplies.
    - HCPC group depends on payer:
        Medicare-style (A4224 + A4225) — fixed 13 infusion units +
          30 cartridge units, regardless of physical sets shipped.
        Commercial-style (A4230 + A4232) — both lines billed at
          (sets * 10) units.
        Aetna-style (A4231 + A4232) — same as commercial.
    - Revenue = (infusion_units * infusion_rate)
              + (cartridge_units * cartridge_rate)
    - Cost    = $314 * (sets / 3)  — $314 is per-3-set baseline
    - GP      = Revenue - Cost

Failure modes (return ``ok=False`` with a ``reason`` string):
  - Missing primary insurance
  - Primary insurance not in PAYER_RATE_SCHEDULE
  - Required rate is None
  - Sets <= 0 (for supplies)

The webhook layer flips the trigger column to "Failed" when any
side fails, so the operator can see at a glance which rows need
rates added.
"""

from __future__ import annotations

from typing import Any

from claim_assumptions import (
    PAYER_RATE_SCHEDULE,
    PAYER_SUPPLY_PROCEDURE_CODE_MAP,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SENSORS_HCPC_UNITS_PER_FILL = 3   # All CGM bills 3 units of A4239 per fill
SENSORS_COST_PER_FILL       = 500.0
SUPPLIES_COST_PER_3_SETS    = 314.0  # baseline cost for 3 infusion sets

# Medicaid-supplies-split set, mirrored from intake_insurance_resolver.
# When Primary Insurance is one of these, the SUPPLIES side bills
# Medicaid (and uses Medicaid rates), even though the patient's
# overall primary remains the original label.
SUPPLIES_ROUTE_TO_MEDICAID = {
    "Fidelis Medicaid",
    "Anthem BCBS Medicaid (JLJ)",
    "Medicaid",
}

# Board-label -> code-label aliases. The Subscription board status
# column uses casings that don't always match claim_assumptions keys.
PRIMARY_INSURANCE_ALIASES = {
    "Magnacare":     "MagnaCare",
    "BCBS Wyoming":  "BCBS WY",
    # Brandon's note (2026-04-29): rates for these don't exist yet, but
    # keep the alias here so adding rates later "just works".
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _canonical(label: str) -> str:
    """Normalize a board status label to the codebase's preferred spelling."""
    return PRIMARY_INSURANCE_ALIASES.get(label, label)


def _supplies_payer(primary: str) -> str:
    """Apply Medicaid-supplies-split rule. Mirrors intake_insurance_resolver."""
    if primary in SUPPLIES_ROUTE_TO_MEDICAID:
        return "Medicaid"
    return primary


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def estimate_sensors(primary_insurance: str) -> dict[str, Any]:
    """
    Compute sensors revenue/cost/GP for one fill.

    Returns:
      {"ok": True,  "revenue": float, "cost": float, "gp": float}
      {"ok": False, "reason": str}
    """
    primary = _canonical((primary_insurance or "").strip())
    if not primary:
        return {"ok": False, "reason": "Missing Primary Insurance"}

    rates = PAYER_RATE_SCHEDULE.get(primary)
    if not rates:
        return {"ok": False, "reason": f"No rate schedule for {primary!r}"}

    sensor_rate = rates.get("sensor_rate")
    if sensor_rate is None:
        return {"ok": False, "reason": f"No sensor_rate for {primary!r}"}

    revenue = round(float(sensor_rate) * SENSORS_HCPC_UNITS_PER_FILL, 2)
    cost    = round(SENSORS_COST_PER_FILL, 2)
    gp      = round(revenue - cost, 2)
    return {"ok": True, "revenue": revenue, "cost": cost, "gp": gp}


def estimate_supplies(primary_insurance: str, sets: int) -> dict[str, Any]:
    """
    Compute supplies revenue/cost/GP for one fill, given the number
    of infusion sets shipped (Inf Qty 1 + Inf Qty 2 on the board).

    Returns:
      {"ok": True,  "revenue": float, "cost": float, "gp": float,
       "supplies_payer": str, "infusion_units": int, "cartridge_units": int}
      {"ok": False, "reason": str}
    """
    primary = _canonical((primary_insurance or "").strip())
    if not primary:
        return {"ok": False, "reason": "Missing Primary Insurance"}

    if not isinstance(sets, int) or sets <= 0:
        return {"ok": False, "reason": "Inf Qty 1 + Inf Qty 2 must be > 0"}

    supplies_payer = _supplies_payer(primary)

    rates        = PAYER_RATE_SCHEDULE.get(supplies_payer)
    supply_codes = PAYER_SUPPLY_PROCEDURE_CODE_MAP.get(supplies_payer)
    if not rates:
        return {"ok": False, "reason": f"No rate schedule for {supplies_payer!r}"}
    if not supply_codes:
        return {"ok": False, "reason": f"No supply HCPC mapping for {supplies_payer!r}"}

    infusion_rate  = rates.get("infusion_rate")
    cartridge_rate = rates.get("cartridge_rate")
    if infusion_rate is None or cartridge_rate is None:
        return {"ok": False,
                "reason": f"Missing infusion_rate / cartridge_rate for {supplies_payer!r}"}

    infusion_code = supply_codes.get("infusion_set", "")

    # Determine billed unit counts based on which HCPC group the payer uses.
    # A4224 is the Medicare-style infusion code; everything else (A4230 or
    # A4231) is unit-based at sets * 10.
    if infusion_code == "A4224":
        infusion_units  = 13
        cartridge_units = 30
    else:
        infusion_units  = sets * 10
        cartridge_units = sets * 10

    revenue = round(
        infusion_units  * float(infusion_rate)
        + cartridge_units * float(cartridge_rate),
        2,
    )
    cost = round(SUPPLIES_COST_PER_3_SETS * (sets / 3.0), 2)
    gp   = round(revenue - cost, 2)
    return {
        "ok": True,
        "revenue": revenue,
        "cost": cost,
        "gp": gp,
        "supplies_payer": supplies_payer,
        "infusion_units": infusion_units,
        "cartridge_units": cartridge_units,
    }
