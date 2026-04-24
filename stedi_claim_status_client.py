"""
stedi_claim_status_client.py
=============================
Thin HTTP layer for Stedi's real-time Claim Status (276/277) endpoint.

Mirrors ``stedi_eligibility_client.send_eligibility_request`` — same auth,
same concurrency semaphore, same 429 backoff. Separated so the business
layer (services/claim_status_service.py) stays focused on orchestration.
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import requests

try:
    from services.eligibility_worker_pool import stedi_concurrency
except Exception:  # pragma: no cover
    import threading
    stedi_concurrency = threading.BoundedSemaphore(
        int(os.getenv('STEDI_MAX_CONCURRENT', '5'))
    )

from stedi_claim_status_builder import STEDI_CLAIM_STATUS_ENDPOINT

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 60


def _get_api_key() -> str:
    key = os.getenv("STEDI_API_KEY", "").strip()
    if not key:
        raise ValueError(
            "Missing Stedi API key. Set STEDI_API_KEY environment variable."
        )
    return key


def send_claim_status_request(
    payload: dict[str, Any],
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    POST payload to Stedi Claim Status endpoint. Strips ``_meta`` before sending.
    Returns parsed JSON dict. Raises ValueError on HTTP failure so the
    orchestrator can surface a structured error to Monday.
    """
    api_key = _get_api_key()
    send_payload = {k: v for k, v in payload.items() if k != "_meta"}

    headers = {
        "Authorization": api_key,
        "Content-Type":  "application/json",
    }

    meta = payload.get("_meta", {})
    logger.info(
        f"[CS-CLIENT] POST {STEDI_CLAIM_STATUS_ENDPOINT} | "
        f"payer={payload.get('tradingPartnerServiceId')} "
        f"partner={meta.get('tradingPartnerName')!r} "
        f"dos_window={meta.get('dosWindow')!r}"
    )

    response = None
    for attempt in range(4):
        try:
            with stedi_concurrency:
                response = requests.post(
                    STEDI_CLAIM_STATUS_ENDPOINT,
                    headers=headers,
                    json=send_payload,
                    timeout=timeout,
                )
        except requests.exceptions.Timeout:
            raise ValueError(f"Stedi claim-status request timed out after {timeout}s")
        except requests.exceptions.ConnectionError as e:
            raise ValueError(f"Stedi claim-status connection error: {e}")

        if response.status_code != 429:
            break

        retry_after = response.headers.get("Retry-After")
        try:
            sleep_s = float(retry_after) if retry_after else (2 ** attempt)
        except ValueError:
            sleep_s = 2 ** attempt
        logger.warning(
            f"[CS-CLIENT] 429 from Stedi | "
            f"payer={payload.get('tradingPartnerServiceId')} "
            f"attempt={attempt + 1}/4 sleep={sleep_s}s"
        )
        time.sleep(sleep_s)
    else:
        raise ValueError(
            "Stedi rate-limited the claim-status request (HTTP 429) after 4 "
            "attempts. Consider lowering STEDI_MAX_CONCURRENT or asking "
            "Stedi support to raise your account concurrency cap."
        )

    try:
        response_json = response.json()
    except ValueError:
        response.raise_for_status()
        raise ValueError("Stedi returned a non-JSON response for claim-status")

    if not response.ok:
        errors = response_json.get("errors", [])
        first_error = ""
        if errors:
            first_error = errors[0].get("description") or errors[0].get("message") or ""
        raise ValueError(
            first_error
            or f"Stedi claim-status HTTP {response.status_code}: "
            f"{json.dumps(response_json)[:300]}"
        )

    logger.info(
        f"[CS-CLIENT] Response OK | "
        f"payer={payload.get('tradingPartnerServiceId')} "
        f"status={response.status_code}"
    )

    return response_json
