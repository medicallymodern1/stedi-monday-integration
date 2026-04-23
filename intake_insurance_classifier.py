"""
intake_insurance_classifier.py — LLM-based Primary Insurance classifier (PRD §15).
==================================================================================

Two-stage classification:
  1. Deterministic rule-based matching (handles ~90 % of cases, zero latency)
  2. Anthropic Claude fallback for ambiguous inputs

The prompt and allowed-label set are kept here in one obvious place
so they are easy to maintain.
"""

import logging
import os
import re
import json

import requests

from insurance_rules import ALLOWED_PRIMARY_INSURANCE_LABELS

logger = logging.getLogger(__name__)

# ─── Deterministic rules ────────────────────────────────────────────

_DIRECT_MATCH_RULES = [
    (["cigna"],                          "Cigna"),
    (["magna"],                          "MagnaCare"),
    (["horizon"],                        "Horizon BCBS"),
    (["oregon"],                         "Oregon Care"),
    (["bcbs tn", "tennessee"],           "BCBS TN"),
    (["umr"],                            "UMR"),
    (["wellcare"],                       "Wellcare"),
    (["florida blue", "bcbs fl"],        "BCBS FL"),
    (["bcbs wy", "wyoming"],             "BCBS WY"),
    (["nyship", "empire plan"],          "NYSHIP"),
    (["humana"],                         "Humana"),
    (["midlands"],                       "Midlands Choice"),
]

_LOW_COST_KEYWORDS      = {"child health plus", "chp", "essential"}
_ANTHEM_LOW_COST_KW     = {"child health plus", "chp", "cdhp", "essential", "ep"}


def _deterministic_classify(payer, plan, coverage, member_id):
    """Return an allowed label or None if no confident deterministic match."""
    payer_lower    = payer.lower()
    plan_lower     = plan.lower()
    coverage_lower = coverage.lower()
    combined       = f"{payer_lower} {plan_lower}"

    # Direct-match carrier rules
    for keywords, label in _DIRECT_MATCH_RULES:
        if any(kw in combined for kw in keywords):
            return label

    # Aetna
    if "aetna" in combined:
        return "Aetna Medicare" if "medicare" in coverage_lower else "Aetna Commercial"

    # Medicare A&B
    if "medicare" in combined:
        if any(x in combined for x in ["a&b", "part a", "part b"]):
            return "Medicare A&B"
        if member_id and re.match(r"^[A-Za-z]\d[A-Za-z0-9]{9}$", member_id.strip()):
            return "Medicare A&B"

    # Generic Medicaid (not Fidelis/Anthem/United)
    if "medicaid" in coverage_lower:
        if not any(x in payer_lower for x in ["fidelis", "anthem", "empire", "united", "uhc"]):
            return "Medicaid"

    # Fidelis
    if "fidelis" in payer_lower:
        if "medicare" in coverage_lower:
            return "Fidelis Medicare"
        if "commercial" in coverage_lower:
            return "Fidelis Commercial"
        if "medicaid" in coverage_lower:
            if any(kw in plan_lower for kw in _LOW_COST_KEYWORDS):
                return "Fidelis Low-Cost"
            return "Fidelis Medicaid"
        if member_id and re.match(r"^7\d{8}$", member_id.strip()):
            return "Fidelis Medicaid"

    # Anthem / Empire
    if any(x in payer_lower for x in ["anthem", "empire"]):
        if "medicare" in coverage_lower:
            return "Anthem BCBS Medicare"
        if "commercial" in coverage_lower:
            return "Anthem BCBS Commercial"
        if "medicaid" in coverage_lower:
            if any(kw in plan_lower for kw in _ANTHEM_LOW_COST_KW):
                return "Anthem BCBS Low-Cost (JLJ)"
            return "Anthem BCBS Medicaid (JLJ)"
        if member_id and member_id.strip().upper().startswith("JLJ"):
            return "Anthem BCBS Medicaid (JLJ)"

    # United
    if any(x in payer_lower for x in ["united", "uhc"]):
        if "medicare" in coverage_lower:
            return "United Medicare"
        if "commercial" in coverage_lower:
            return "United Commercial"
        if "medicaid" in coverage_lower:
            if any(kw in plan_lower for kw in _ANTHEM_LOW_COST_KW):
                return "United Low-Cost"
            return "United Medicaid"

    return None


# ─── LLM fallback ─────────────────────────────────────────────────

_LABELS_BLOCK = "\n".join(f"- {l}" for l in ALLOWED_PRIMARY_INSURANCE_LABELS)

_SYSTEM_PROMPT = f"""You are an insurance classification engine. Return exactly one value
from the allowed list, or return BLANK if there is truly no reasonable match.

Allowed values:
{_LABELS_BLOCK}

Rules:
- Return ONLY the final board value from the allowed list, nothing else.
- Use partial keyword matching on the payer name and plan name.
- Fidelis CHP should map to Fidelis Low-Cost.
- If there is truly no reasonable match, return exactly the word BLANK.
- Do not explain. Output only the value."""


def classify_primary_insurance(stedi_coverage_type, stedi_payer_name,
                               stedi_plan_name, member_id):
    """
    Classify primary insurance.  Returns an allowed label or "" if unknown.
    Synchronous — safe to call from a background task.
    """
    # Stage 1: deterministic
    det = _deterministic_classify(
        stedi_payer_name or "", stedi_plan_name or "",
        stedi_coverage_type or "", member_id or "",
    )
    if det:
        logger.info("[CLASSIFIER] Deterministic → %s  (payer=%s, plan=%s, coverage=%s)",
                     det, stedi_payer_name, stedi_plan_name, stedi_coverage_type)
        return det

    # Stage 2: LLM
    api_key = os.getenv("ANTHROPIC_API_KEY", "")
    if not api_key:
        logger.warning("[CLASSIFIER] No ANTHROPIC_API_KEY — cannot run LLM fallback")
        return ""

    user_msg = (
        f"Stedi Coverage Type: {stedi_coverage_type}\n"
        f"Stedi Payer Name: {stedi_payer_name}\n"
        f"Stedi Plan Name: {stedi_plan_name}\n"
        f"Member ID: {member_id}"
    )

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 50,
                "temperature": 0,
                "system": _SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_msg}],
            },
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()
        raw = data["content"][0]["text"].strip()
        logger.info("[CLASSIFIER] LLM raw output: '%s'", raw)

        if raw.upper() == "BLANK" or not raw:
            return ""

        if raw in ALLOWED_PRIMARY_INSURANCE_LABELS:
            return raw

        # Case-insensitive fallback
        label_map = {l.lower(): l for l in ALLOWED_PRIMARY_INSURANCE_LABELS}
        return label_map.get(raw.lower(), "")

    except Exception:
        logger.exception("[CLASSIFIER] LLM call failed")
        return ""
