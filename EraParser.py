#!/usr/bin/env python3
"""
EraParser.py (X12 835 only)

Outputs ONE ROW PER SERVICE LINE (SVC).

Key behaviors (facts from ERA):
- Parses 835 ERA files into a flat CSV with one row per SVC (service line).
- Collects CAS (adjustments) + LQ(HE) remark codes at the correct scope.
- Computes PR buckets (deductible / coinsurance / PR total), allowed, paid, etc.

Triage layer (meaning/action from rulebook.json):
- For each row, svc_codes_actions includes actionable codes (action_required=True) + unknown codes.
- Claim-level columns include ONLY claim-level codes (CAS/LQ that occur BEFORE the first SVC within a CLP).

IMPORTANT SCOPING FIX (your request):
- Codes DO NOT leak across CLPs.
- claim_codes_* columns are populated per-claim (only for the rows belonging to that CLP).
- claim_codes_* contain ONLY claim-level codes (CAS/LQ before first SVC).

Column rules (Option A):
- claim_codes_all / claim_codes_actions: claim-level only (pre-first SVC within CLP)
- svc_codes_all / svc_codes_actions: service-line only (codes tied to that SVC row)
- claim_codes_actions is populated ONLY when claim_denied == TRUE (otherwise blank)

Denied reason rule (your request):
- denied_reason shows either:
    - "Denial codes: CO151, N790" (if present on the service line and actionable/unknown focus)
    - OR "Paid $0" (only if no denial codes above and paid == 0 with charge > 0)
    - OR blank
It will NOT show both.
"""

import csv
import json
import os
import re
from typing import Tuple, List, Dict, Optional, Set, Any


# -----------------------------
# CONFIG
# -----------------------------

# CLP02 status codes (claim-level)
CLP02_STATUS_MAP = {
    "1": "Processed as primary",
    "2": "Processed as secondary",
    "3": "Processed as tertiary",
    "4": "Denied",
    "19": "Processed as primary, forwarded",
    "20": "Processed as secondary, forwarded",
    "21": "Processed as tertiary, forwarded",
    "22": "Reversal of previous payment",
    "23": "Not our claim, forwarded",
}


# -----------------------------
# Helpers
# -----------------------------

def _safe_float(v) -> Optional[float]:
    try:
        if v is None:
            return None
        s = str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _fmt2(v: Optional[float]) -> str:
    if v is None:
        return ""
    try:
        return f"{float(v):.2f}"
    except Exception:
        return ""


def _fmt_mmddyyyy_from_ccyymmdd(s: str) -> str:
    s = (s or "").strip()
    if len(s) != 8 or not s.isdigit():
        return ""
    return f"{s[4:6]}/{s[6:8]}/{s[0:4]}"


def _format_patient_name(last: str, first: str) -> str:
    last = (last or "").strip()
    first = (first or "").strip()
    if last and first:
        return f"{last}, {first}"
    return last or first


def detect_delimiters(data: str) -> Tuple[str, str, str]:
    """
    Detect element, segment, and component delimiters.
    Typical X12: element='*', segment='~', component=':'
    """
    if data.startswith("ISA") and len(data) > 106:
        elem = data[3]
        comp = data[104]
        seg = data[data.find("ISA") + 105]  # best effort
        if seg in ["~", "\n", "\r"]:
            return elem, seg, comp
    return "*", "~", ":"


def parse_cas_reason_amount_pairs(parts: List[str]) -> List[Tuple[str, float]]:
    """
    CAS*<grp>*<reason1>*<amt1>*<qty1>*<reason2>*<amt2>*<qty2>*...
    Returns list of (reason_code, amount)
    """
    out: List[Tuple[str, float]] = []
    i = 2
    while i + 1 < len(parts):
        reason = (parts[i] or "").strip()
        amt = _safe_float(parts[i + 1] if i + 1 < len(parts) else None)
        if reason != "" and amt is not None:
            out.append((reason, amt))
        i += 3
    return out


def parse_cas_adjustments(parts: List[str]) -> List[dict]:
    """
    Parse CAS into structured adjustments.
    Returns list: [{"group":"CO","reason":"45","amount":300.0,"quantity":None}, ...]
    """
    group = (parts[1] or "").strip() if len(parts) > 1 else ""
    out: List[dict] = []
    i = 2
    while i + 1 < len(parts):
        reason = (parts[i] or "").strip()
        amt = _safe_float(parts[i + 1] if i + 1 < len(parts) else None)
        qty = _safe_float(parts[i + 2] if i + 2 < len(parts) else None) if i + 2 < len(parts) else None
        if group and reason and amt is not None:
            out.append({"group": group, "reason": reason, "amount": amt, "quantity": qty})
        i += 3
    return out


def is_carc_combo(code: str) -> bool:
    return re.fullmatch(r"(CO|PR|OA|PI|CR)\d+", code or "") is not None


def is_rarc_code(code: str) -> bool:
    return re.fullmatch(r"(?:MA\d+|[NM]\d+)", code or "") is not None


def code_description(code: str, carc_map: Dict[str, str], rarc_map: Dict[str, str]) -> str:
    if is_carc_combo(code):
        num = re.sub(r"^(CO|PR|OA|PI|CR)", "", code)
        desc = carc_map.get(num)
        return f"{code}: {desc}" if desc else ""
    if is_rarc_code(code):
        desc = rarc_map.get(code)
        return f"{code}: {desc}" if desc else ""
    return ""


def build_codes_commentary(codes: Set[str], carc_map: Dict[str, str], rarc_map: Dict[str, str]) -> str:
    pieces: List[str] = []
    for code in sorted(codes):
        s = code_description(code, carc_map, rarc_map)
        if s:
            pieces.append(s)
    return "; ".join(pieces)


def get_focus_codes(codes: Set[str], rulebook: Dict) -> Set[str]:
    """
    Focus/action codes:
    - codes where rulebook[code].action_required == True
    - plus unknown codes (so you see them and can map them)
    """
    out: Set[str] = set()
    rb_codes = (rulebook or {}).get("codes") or {}
    for c in codes:
        info = rb_codes.get(c)
        if not isinstance(info, dict):
            out.add(c)
            continue
        if info.get("action_required") is True:
            out.add(c)
    return out


def has_medicaid_as_secondary(codes: Set[str], rulebook: Dict) -> bool:
    """
    Returns True if any code in `codes` has rulebook entry with medicaid_as_secondary == true.
    Missing field is treated as False (default).
    """
    rb_codes = (rulebook or {}).get("codes") or {}
    for c in codes or set():
        info = rb_codes.get(c)
        if isinstance(info, dict) and info.get("medicaid_as_secondary") is True:
            return True
    return False



def build_claim_commentary_all(codes: Set[str], carc_map: Dict[str, str], rarc_map: Dict[str, str]) -> str:
    return build_codes_commentary(codes, carc_map, rarc_map)


# -----------------------------
# Rulebook
# -----------------------------

def load_rulebook_json() -> Dict:
    base = os.path.dirname(__file__)
    rulebook_path = os.path.join(base, "rulebook.json")
    if not os.path.exists(rulebook_path):
        raise FileNotFoundError(f"Missing {rulebook_path}. Put rulebook.json next to EraParser.py")
    with open(rulebook_path, "r", encoding="utf-8") as f:
        return json.load(f)


def reconcile_paid_to_allowed(
    allowed: Optional[float],
    paid: Optional[float],
    pr_total: Optional[float],
    sequestration: float,
    pct_tol: float
) -> bool:
    if allowed is None or allowed <= 0.005:
        return False
    paid_v = paid or 0.0
    pr_v = pr_total or 0.0
    target = paid_v + pr_v + sequestration
    return abs(target - allowed) / allowed <= pct_tol


def build_adjustment_reasons_from_codes(svc_codes_set: Set[str]) -> List[str]:
    out: List[str] = []
    for c in sorted(svc_codes_set):
        if re.fullmatch(r"(CO|PR|OA|PI|CR)\d+", c):
            out.append(c)
    return out


def build_rarc_list_from_codes(svc_codes_set: Set[str]) -> List[str]:
    out: List[str] = []
    for c in sorted(svc_codes_set):
        if re.fullmatch(r"[NM]\d+", c):
            out.append(c)
    return out


def compute_sequestration_amount(adjustments: List[dict]) -> float:
    total = 0.0
    for adj in adjustments or []:
        g = (adj.get("group") or "").strip()
        r = (adj.get("reason") or "").strip()
        amt = adj.get("amount")
        if g == "CO" and r == "253" and amt is not None:
            try:
                total += float(amt)
            except Exception:
                pass
    return round(total, 2)


def build_raw_cas_summary(adjustments: List[dict]) -> str:
    parts: List[str] = []
    for adj in adjustments or []:
        g = (adj.get("group") or "").strip()
        r = (adj.get("reason") or "").strip()
        amt = adj.get("amount")
        if g and r and amt is not None:
            try:
                parts.append(f"{g}{r}:{float(amt):.2f}")
            except Exception:
                parts.append(f"{g}{r}:{amt}")
    return "|".join(parts)


def build_flat_adjustment_fields(row: Dict[str, str]) -> None:
    """
    Populate flat, analyzable fields using the already-collected data on the row.
    """
    svc_codes_raw = (row.get("svc_codes_all") or "").strip()
    svc_codes_set = set([c.strip() for c in svc_codes_raw.split(";") if c.strip()]) if svc_codes_raw else set()

    adjustments_list = row.get("_adjustments")
    if not adjustments_list:
        adj_json = row.get("adjustments")
        if isinstance(adj_json, str) and adj_json.strip():
            try:
                adjustments_list = json.loads(adj_json)
            except Exception:
                adjustments_list = []
        else:
            adjustments_list = []
    if not isinstance(adjustments_list, list):
        adjustments_list = []

    carc_combos = build_adjustment_reasons_from_codes(svc_codes_set)
    rarc_codes = build_rarc_list_from_codes(svc_codes_set)

    # Include claim-level CARC combos (claim_codes_all is claim-level only now)
    claim_codes_raw = (row.get("claim_codes_all") or "").strip()
    if claim_codes_raw:
        for token in [t.strip() for t in claim_codes_raw.split(";") if t.strip()]:
            if re.fullmatch(r"(CO|PR|OA|PI|CR)\d+", token) and token not in carc_combos:
                carc_combos.append(token)

    sequestration = compute_sequestration_amount(adjustments_list)

    carc_nums: List[str] = []
    for c in carc_combos:
        m = re.fullmatch(r"(CO|PR|OA|PI|CR)(\d+)", c)
        if m:
            carc_nums.append(m.group(2))

    carc_nums_unique = sorted(set(carc_nums), key=lambda s: int(s)) if carc_nums else []

    row["adjustment_reasons"] = "; ".join(carc_combos)
    row["carc_codes"] = ", ".join(carc_nums_unique)
    row["rarc_codes"] = ", ".join(rarc_codes)
    row["sequestration_amount"] = _fmt2(sequestration) if sequestration else "0"
    row["raw_cas_summary"] = build_raw_cas_summary(adjustments_list)


def evaluate_triage(row: Dict[str, Any], rulebook: Dict[str, Any]) -> None:
    """Populate triage fields used by the user-facing workflow."""
    codes_map: Dict[str, Any] = (rulebook or {}).get("codes", {}) if isinstance(rulebook, dict) else {}

    def normalize_bucket(bucket: str) -> str:
        mapping = {
            "PolicyReview": "Resubmit",
            "AdminFix": "Resubmit",
            "CoverageDenial": "Resubmit",
            "PayerReview": "Payer Issue - contact payer",
            "AutoClose": "No action",
            "No action": "No action",
            "Resubmit": "Resubmit",
            "Payer Issue - contact payer": "Payer Issue - contact payer",
        }
        return mapping.get(bucket or "", bucket or "")

    def parse_codes(value: Any) -> List[str]:
        if value is None:
            return []
        text = str(value).strip()
        if not text:
            return []
        parts = [p.strip() for p in text.split(";")]
        return [p for p in parts if p]

    def code_confidence(entry: Dict[str, Any]) -> str:
        if not isinstance(entry, dict):
            return "ai_guess"
        if entry.get("confidence"):
            return str(entry.get("confidence"))
        if entry.get("definition_unknown") is True:
            return "ai_guess"
        return "vetted"

    raw = parse_codes(row.get("claim_codes_actions")) + parse_codes(row.get("svc_codes_actions"))
    seen: Set[str] = set()
    action_codes: List[str] = []
    for c in raw:
        if c not in seen:
            seen.add(c)
            action_codes.append(c)

    per_code_bucket: List[Tuple[str, str]] = []
    per_code_action: List[Tuple[str, str]] = []
    per_code_conf: List[Tuple[str, str]] = []

    action_required_any = False
    for code in action_codes:
        entry = codes_map.get(code, {}) if isinstance(codes_map, dict) else {}
        bucket_raw = entry.get("triage_bucket") or entry.get("triage") or ""
        bucket = normalize_bucket(str(bucket_raw))
        needs_action = bool(entry.get("action_required")) if isinstance(entry, dict) else False
        rec_action = (entry.get("recommended_action") if isinstance(entry, dict) else "") or "Review code meaning and determine appropriate correction or appeal"
        if needs_action:
            action_required_any = True
            per_code_bucket.append((code, bucket or "Resubmit"))
            per_code_action.append((code, str(rec_action)))
            per_code_conf.append((code, code_confidence(entry)))

    denied_flag = row.get("denied")
    denied_bool = (str(denied_flag).strip().upper() == "TRUE") if isinstance(denied_flag, str) else bool(denied_flag)
    if denied_bool and not action_required_any:
        action_required_any = True

    severity = {"No action": 0, "Payer Issue - contact payer": 1, "Resubmit": 2}
    if not action_required_any:
        overall_bucket = "No action"
    else:
        overall_bucket = max([b for _, b in per_code_bucket], key=lambda b: severity.get(b, 1)) if per_code_bucket else "Resubmit"

    row["triage_bucket"] = overall_bucket
    row["action_required"] = action_required_any
    row["triage_reason"] = ", ".join([f"{bucket} ({code})" for code, bucket in per_code_bucket])
    row["recommended_action"] = ", ".join([f"{act} ({code})" for code, act in per_code_action])
    row["confidence"] = ", ".join([f"{conf} ({code})" for code, conf in per_code_conf])


def load_code_maps_from_json() -> Tuple[Dict[str, str], Dict[str, str]]:
    base = os.path.dirname(__file__)
    carc_path = os.path.join(base, "carc_map.json")
    rarc_path = os.path.join(base, "rarc_map.json")

    if not os.path.exists(carc_path):
        raise FileNotFoundError(f"Missing {carc_path}. Run build_denial_maps.py first.")
    if not os.path.exists(rarc_path):
        raise FileNotFoundError(f"Missing {rarc_path}. Run build_denial_maps.py first.")

    with open(carc_path, "r", encoding="utf-8") as f:
        carc_map = json.load(f)

    with open(rarc_path, "r", encoding="utf-8") as f:
        rarc_map = json.load(f)

    return carc_map, rarc_map


# -----------------------------
# Main parser
# -----------------------------

def parse_835_file(path: str, carc_map: Dict[str, str], rarc_map: Dict[str, str], rulebook: Dict) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []

    try:
        data = open(path, "r", encoding="utf-8", errors="ignore").read().strip()
    except Exception:
        return rows

    if not data:
        return rows

    elem, seg, comp = detect_delimiters(data)
    segments = data.split(seg)

    inside_st = False

    # Transaction-level
    paid_date = ""
    check_number = ""

    # Claim-level (current CLP)
    patient_name = ""
    patient_control_number = ""
    payer_claim_control_number = ""
    claim_status_code = ""
    claim_status = ""
    claim_forwarded_to_secondary = False
    claim_denied = False

    # Claim-level codes for current CLP (ONLY pre-first-SVC)
    claim_level_codes: Set[str] = set()
    before_first_lx_in_clp: bool = False
    current_claim_row_indices: List[int] = []

    # Service helpers
    last_open_svc_index: Optional[int] = None
    pending_dos_for_next_svc = ""
    pending_allowed_for_next_svc: Optional[float] = None

    def apply_allowed_to_last_or_pending(allowed: Optional[float]) -> None:
        nonlocal pending_allowed_for_next_svc, last_open_svc_index
        if allowed is None:
            return
        if last_open_svc_index is not None and 0 <= last_open_svc_index < len(rows):
            rows[last_open_svc_index]["allowed_amount"] = _fmt2(allowed)
        else:
            pending_allowed_for_next_svc = allowed

    def increment_row_amount(idx: int, field: str, amt: float) -> None:
        if not (0 <= idx < len(rows)):
            return
        cur = _safe_float(rows[idx].get(field, "0")) or 0.0
        rows[idx][field] = _fmt2(cur + amt)

    def increment_adjustment_bucket(idx: Optional[int], bucket_key: str, amt: float) -> None:
        if idx is None:
            return
        increment_row_amount(idx, bucket_key, amt)
        increment_row_amount(idx, "adjustment_total", amt)

    def set_row_denial_fields(idx: int) -> None:
        """
        Per-row denial fields + service-line code fields.

        denied_reason rule:
          - Prefer denial/action codes CO151 and/or N790 (if present on that row)
          - Else if paid == 0 and charge > 0 -> "Paid $0"
          - Else blank
        """
        if not (0 <= idx < len(rows)):
            return
        r = rows[idx]

        paid = _safe_float(r.get("paid_amount")) or 0.0
        charge = _safe_float(r.get("charge_amount")) or 0.0

        svc_codes_raw = (r.get("svc_codes_all") or "").strip()
        svc_codes_list = [c.strip() for c in svc_codes_raw.split(";") if c.strip()] if svc_codes_raw else []
        svc_codes_set = set(svc_codes_list)

        # Medicaid as Secondary flag (does NOT affect triage / action_required)
        r["medicaid_as_secondary"] = "TRUE" if has_medicaid_as_secondary(svc_codes_set, rulebook) else "FALSE"

        focus_set = get_focus_codes(svc_codes_set, rulebook)
        focus_list = sorted(focus_set)

        r["svc_codes_all_commentary"] = build_codes_commentary(svc_codes_set, carc_map, rarc_map) if svc_codes_set else ""
        r["svc_codes_actions"] = "; ".join(focus_list) if focus_list else ""
        r["svc_codes_actions_commentary"] = build_codes_commentary(focus_set, carc_map, rarc_map) if focus_set else ""

        zero_paid = abs(paid) < 0.005
        positive_charge = charge > 0.005

        denied_bool = ((zero_paid and positive_charge) or (len(focus_set) > 0))
        r["denied"] = "TRUE" if denied_bool else "FALSE"

        # Denied reason: ONLY one of the two
        denial_code_whitelist = {"CO151", "N790"}
        denial_codes_present = sorted([c for c in focus_set if c in denial_code_whitelist])

        if denial_codes_present:
            r["denied_reason"] = f"Denial codes: {', '.join(denial_codes_present)}"
        elif zero_paid and positive_charge:
            r["denied_reason"] = "Paid $0"
        else:
            r["denied_reason"] = ""

    def finalize_current_claim() -> None:
        """
        Backfill claim_codes_* fields for ONLY the rows in current_claim_row_indices.
        IMPORTANT: claim_codes_* are claim-level only (pre-first SVC).
        claim_codes_actions is shown ONLY when claim_denied == TRUE.
        """
        nonlocal claim_level_codes, current_claim_row_indices, claim_denied

        if not current_claim_row_indices:
            claim_level_codes = set()
            return

        claim_codes_all_str = "; ".join(sorted(claim_level_codes)) if claim_level_codes else ""
        claim_codes_all_commentary = build_claim_commentary_all(claim_level_codes, carc_map, rarc_map) if claim_level_codes else ""

        claim_focus_set = get_focus_codes(claim_level_codes, rulebook) if claim_level_codes else set()
        claim_codes_actions_str = "; ".join(sorted(claim_focus_set)) if claim_focus_set else ""
        claim_codes_actions_commentary = build_codes_commentary(claim_focus_set, carc_map, rarc_map) if claim_focus_set else ""

        for idx in current_claim_row_indices:
            if not (0 <= idx < len(rows)):
                continue
            r = rows[idx]
            r["claim_codes_all"] = claim_codes_all_str
            r["claim_codes_all_commentary"] = claim_codes_all_commentary

            # Update Medicaid-as-Secondary flag using BOTH claim-level + service-line codes  # medicaid_as_secondary_union
            # (still does not affect triage/action_required)
            svc_set = set([c.strip() for c in (r.get("svc_codes_all") or "").split(";") if c.strip()])
            claim_set = set([c.strip() for c in (r.get("claim_codes_all") or "").split(";") if c.strip()])
            combined = svc_set.union(claim_set)
            r["medicaid_as_secondary"] = "TRUE" if has_medicaid_as_secondary(combined, rulebook) else "FALSE"

            # Only show claim_codes_actions when claim_denied == TRUE
            if str(r.get("claim_denied") or "").strip().upper() == "TRUE":
                r["claim_codes_actions"] = claim_codes_actions_str
                r["claim_codes_actions_commentary"] = claim_codes_actions_commentary
            else:
                r["claim_codes_actions"] = ""
                r["claim_codes_actions_commentary"] = ""

        # Reset for next CLP
        claim_level_codes = set()
        current_claim_row_indices = []


    for raw in segments:
        s = raw.strip()
        if not s:
            continue

        p = s.split(elem)
        tag = p[0].upper()

        if tag == "ST":
            inside_st = True

            paid_date = ""
            check_number = ""

            # reset claim-level state
            patient_name = ""
            patient_control_number = ""
            payer_claim_control_number = ""
            claim_status_code = ""
            claim_status = ""
            claim_forwarded_to_secondary = False
            claim_denied = False

            claim_level_codes = set()
            before_first_lx_in_clp = False
            current_claim_row_indices = []

            last_open_svc_index = None
            pending_dos_for_next_svc = ""
            pending_allowed_for_next_svc = None
            continue

        if tag == "SE":
            # finalize the last CLP within this ST
            finalize_current_claim()

            inside_st = False
            last_open_svc_index = None
            pending_dos_for_next_svc = ""
            pending_allowed_for_next_svc = None
            continue

        if not inside_st:
            continue

        # Header / payment info
        if tag == "BPR" and len(p) >= 16:
            paid_date = _fmt_mmddyyyy_from_ccyymmdd(p[15])

        elif tag == "DTM" and len(p) >= 3 and p[1] == "405":
            if not paid_date:
                paid_date = _fmt_mmddyyyy_from_ccyymmdd(p[2])

        elif tag == "TRN" and len(p) >= 3:
            if p[1] == "1" and p[2]:
                check_number = p[2]

        elif tag == "NM1" and len(p) >= 5 and p[1] == "QC":
            patient_name = _format_patient_name(
                p[3] if len(p) > 3 else "",
                p[4] if len(p) > 4 else "",
            )

        # New claim (CLP)
        elif tag == "CLP":
            # finalize previous claim BEFORE starting a new one
            finalize_current_claim()

            last_open_svc_index = None
            pending_dos_for_next_svc = ""
            pending_allowed_for_next_svc = None

            claim_level_codes = set()
            before_first_lx_in_clp = True
            current_claim_row_indices = []

            patient_control_number = p[1].strip() if len(p) > 1 else ""
            claim_status_code = p[2].strip() if len(p) > 2 else ""
            claim_status = CLP02_STATUS_MAP.get(
                claim_status_code,
                f"Unknown ({claim_status_code})" if claim_status_code else "Unknown",
            )
            payer_claim_control_number = p[7].strip() if len(p) > 7 else ""

            claim_forwarded_to_secondary = claim_status_code in {"19", "20"}
            claim_denied = (claim_status_code == "4")

        # DOS
        elif tag == "DTM" and len(p) >= 3 and p[1] == "472":
            dos = _fmt_mmddyyyy_from_ccyymmdd(p[2])
            if last_open_svc_index is not None and 0 <= last_open_svc_index < len(rows):
                if not rows[last_open_svc_index].get("dos"):
                    rows[last_open_svc_index]["dos"] = dos
            else:
                pending_dos_for_next_svc = dos

        # Allowed amount
        elif tag == "AMT" and len(p) >= 3 and (p[1] or "").strip() == "B6":
            apply_allowed_to_last_or_pending(_safe_float(p[2] if len(p) > 2 else None))


        # Service line counter (LX) - marks start of service-line loops within CLP
        elif tag == "LX":
            # Once we hit the first LX in a CLP, any subsequent CAS/LQ are assumed service-line scoped
            # unless they appear before the first LX.
            before_first_lx_in_clp = False

        # Service line
        elif tag == "SVC":
            before_first_lx_in_clp = False

            hcpcs_code = ""
            if len(p) >= 2:
                svc01 = p[1]
                if comp in svc01:
                    parts = svc01.split(comp)
                    hcpcs_code = parts[1] if len(parts) > 1 else svc01
                else:
                    hcpcs_code = svc01

            charge_amount = _safe_float(p[2] if len(p) > 2 else None)
            paid_amount = _safe_float(p[3] if len(p) > 3 else None)

            row: Dict[str, str] = {
                "patient_name": patient_name,
                "patient_control_number": patient_control_number,
                "payer_claim_control_number": payer_claim_control_number,
                "check_number": check_number,
                "paid_date": paid_date,

                "hcpcs_code": hcpcs_code,
                "charge_amount": _fmt2(charge_amount),
                "paid_amount": _fmt2(paid_amount),
                "allowed_amount": _fmt2(pending_allowed_for_next_svc) if pending_allowed_for_next_svc is not None else "",
                "dos": pending_dos_for_next_svc or "",

                "deductible_amount": "0.00",
                "coinsurance_amount": "0.00",
                "patient_responsibility_total": "0.00",

                "claim_status_code": claim_status_code,
                "claim_status": claim_status,
                "forwarded_to_secondary": "TRUE" if claim_forwarded_to_secondary else "FALSE",
                "claim_denied": "TRUE" if claim_denied else "FALSE",

                "denied": "FALSE",
                "denied_reason": "",

                # claim-level fields (backfilled in finalize_current_claim)
                "claim_codes_all": "",
                "claim_codes_all_commentary": "",
                "claim_codes_actions": "",
                "claim_codes_actions_commentary": "",

                # service-line fields
                "svc_codes_all": "",
                "svc_codes_all_commentary": "",
                "svc_codes_actions": "",
                "svc_codes_actions_commentary": "",

                # flat + triage
                "adjustment_reasons": "",
                "carc_codes": "",
                "rarc_codes": "",
                "sequestration_amount": "0",
                "raw_cas_summary": "",
                "triage_bucket": "",
                "action_required": "",
                "recommended_action": "",
                "triage_reason": "",
                "confidence": "",

                "adjustment_groups": "",
                "adjustment_total": "0.00",
                "adjustment_co_amount": "0.00",
                "adjustment_pr_amount": "0.00",
                "adjustment_oa_amount": "0.00",
                "adjustment_pi_amount": "0.00",
                "adjustment_cr_amount": "0.00",

                "adjustments": "",
                "_adjustments": [],

                "source_file": os.path.basename(path),
            }

            rows.append(row)
            last_open_svc_index = len(rows) - 1
            current_claim_row_indices.append(last_open_svc_index)

            pending_dos_for_next_svc = ""
            pending_allowed_for_next_svc = None

            set_row_denial_fields(last_open_svc_index)

        # CAS adjustments
        elif tag == "CAS" and len(p) >= 3:
            group_code = (p[1] or "").strip()
            pairs = parse_cas_reason_amount_pairs(p)

            # Scope: claim-level only if before first SVC in the CLP
            if before_first_lx_in_clp:
                for reason, _amt in pairs:
                    claim_level_codes.add(f"{group_code}{reason}")

            adjustments = parse_cas_adjustments(p)

            # Service-line: attach to current open SVC if any
            if last_open_svc_index is not None and 0 <= last_open_svc_index < len(rows):
                r = rows[last_open_svc_index]

                # Append structured adjustments onto this service line
                adj_list = r.get("_adjustments") or []
                for adj in adjustments:
                    reason = adj.get("reason", "")
                    if reason:
                        desc = carc_map.get(str(reason).strip())
                        if desc:
                            adj["reason_description"] = desc
                    adj_list.append(adj)
                r["_adjustments"] = adj_list

                # Keep svc_codes in sync
                svc_codes = set(r["svc_codes_all"].split("; ")) if r.get("svc_codes_all") else set()
                for reason, _amt in pairs:
                    svc_codes.add(f"{group_code}{reason}")
                r["svc_codes_all"] = "; ".join(sorted(c for c in svc_codes if c))

                # Bucket totals by group code
                groups_seen = set()
                for adj in adjustments:
                    g = (adj.get("group") or "").strip()
                    amt = adj.get("amount")
                    if not g or amt is None:
                        continue
                    groups_seen.add(g)

                    if g == "CO":
                        increment_adjustment_bucket(last_open_svc_index, "adjustment_co_amount", float(amt))
                    elif g == "PR":
                        increment_adjustment_bucket(last_open_svc_index, "adjustment_pr_amount", float(amt))
                    elif g == "OA":
                        increment_adjustment_bucket(last_open_svc_index, "adjustment_oa_amount", float(amt))
                    elif g == "PI":
                        increment_adjustment_bucket(last_open_svc_index, "adjustment_pi_amount", float(amt))
                    elif g == "CR":
                        increment_adjustment_bucket(last_open_svc_index, "adjustment_cr_amount", float(amt))
                    else:
                        increment_row_amount(last_open_svc_index, "adjustment_total", float(amt))

                # Update adjustment_groups
                existing_groups = set(r["adjustment_groups"].split("; ")) if r.get("adjustment_groups") else set()
                for g in groups_seen:
                    existing_groups.add(g)
                r["adjustment_groups"] = "; ".join(sorted(g for g in existing_groups if g))

                # PR breakdown
                if group_code == "PR":
                    for reason, amt in pairs:
                        if amt is None:
                            continue
                        increment_row_amount(last_open_svc_index, "patient_responsibility_total", amt)
                        if reason == "1":
                            increment_row_amount(last_open_svc_index, "deductible_amount", amt)
                        elif reason == "2":
                            increment_row_amount(last_open_svc_index, "coinsurance_amount", amt)

                set_row_denial_fields(last_open_svc_index)

        # LQ remarks (RARCs)
        elif tag == "LQ" and len(p) >= 3 and p[1] == "HE":
            remark_code = (p[2] or "").strip().replace(" ", "")
            if remark_code:
                # Scope: claim-level only if before first SVC in the CLP
                if before_first_lx_in_clp:
                    claim_level_codes.add(remark_code)

                # Service-line: attach to current open SVC if any
                if last_open_svc_index is not None and 0 <= last_open_svc_index < len(rows):
                    r = rows[last_open_svc_index]
                    svc_codes = set(r["svc_codes_all"].split("; ")) if r.get("svc_codes_all") else set()
                    svc_codes.add(remark_code)
                    r["svc_codes_all"] = "; ".join(sorted(c for c in svc_codes if c))
                    set_row_denial_fields(last_open_svc_index)

    # Finalize last claim if file ends without SE (defensive)
    finalize_current_claim()

    # Finalize service-line code fields / denial fields
    for i in range(len(rows)):
        set_row_denial_fields(i)

    # Serialize adjustments + build flat fields + triage
    for r in rows:
        adj_list = r.get("_adjustments") or []
        try:
            r["adjustments"] = json.dumps(adj_list, ensure_ascii=False)
        except Exception:
            r["adjustments"] = "[]"
        if "_adjustments" in r:
            del r["_adjustments"]

        build_flat_adjustment_fields(r)
        evaluate_triage(r, rulebook)

    return rows


def main() -> None:
    carc_map, rarc_map = load_code_maps_from_json()
    rulebook = load_rulebook_json()
    print(f"Loaded {len(carc_map)} CARC descriptions and {len(rarc_map)} RARC descriptions from JSON files")
    print(f"Loaded rulebook with {len((rulebook.get('codes') or {}))} code entries")

    base = os.path.expanduser("~/Desktop/MedicallyModern/ERAparser/ERAs_new")
    out_csv = os.path.expanduser("~/Desktop/MedicallyModern/ERAparser/output.csv")

    all_rows: List[Dict[str, str]] = []

    for root, _, files in os.walk(base):
        for name in files:
            if os.path.splitext(name)[1].lower() in [".835", ".x12", ".txt"]:
                fullpath = os.path.join(root, name)
                try:
                    all_rows.extend(parse_835_file(fullpath, carc_map, rarc_map, rulebook))
                except Exception as e:
                    print(f"Warning parsing {name}: {e}")

    if not all_rows:
        print("No rows parsed. Ensure ERAs are unzipped .835/.x12/.txt files in ERAs_new/")
        return

    fieldnames = [
        "patient_name",
        "patient_control_number",
        "payer_claim_control_number",
        "check_number",
        "paid_date",
        "hcpcs_code",
        "charge_amount",
        "paid_amount",
        "allowed_amount",
        "dos",
        "deductible_amount",
        "coinsurance_amount",
        "patient_responsibility_total",
        "claim_status_code",
        "claim_status",
        "forwarded_to_secondary",
        "claim_denied",
        "denied",
        "denied_reason",
        "medicaid_as_secondary",
        "claim_codes_all",
        "claim_codes_all_commentary",
        "claim_codes_actions",
        "claim_codes_actions_commentary",
        "svc_codes_all",
        "svc_codes_all_commentary",
        "svc_codes_actions",
        "svc_codes_actions_commentary",

        # flat + triage
        "adjustment_reasons",
        "carc_codes",
        "rarc_codes",
        "sequestration_amount",
        "raw_cas_summary",
        "triage_bucket",
        "action_required",
        "recommended_action",
        "confidence",
        "triage_reason",

        # buckets
        "adjustment_groups",
        "adjustment_total",
        "adjustment_co_amount",
        "adjustment_pr_amount",
        "adjustment_oa_amount",
        "adjustment_pi_amount",
        "adjustment_cr_amount",
        "adjustments",
        "source_file",
    ]

    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(all_rows)

    print(f"Done. Wrote {len(all_rows)} rows to {out_csv}")


if __name__ == "__main__":
    main()
