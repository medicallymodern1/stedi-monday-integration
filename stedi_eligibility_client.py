"""
stedi_eligibility_client.py
============================
Thin HTTP layer: sends one real-time eligibility request to Stedi
and returns the parsed JSON response.

Separated from the builder and parser so it can be swapped or mocked
in tests without changing business logic.

Endpoint confirmed from Stedi docs:
  POST https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/eligibility/v3
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any

import requests

# Concurrency semaphore lives in the worker pool module so the webhook
# route and this HTTP client share one bound (default 5, matches Stedi's
# default account cap). Import is delayed until we actually need it so
# this module is still usable outside a FastAPI process (e.g. test
# scripts) without spinning up the worker pool.
try:
    from services.eligibility_worker_pool import stedi_concurrency
except Exception:  # pragma: no cover — fallback for standalone imports
    import threading
    stedi_concurrency = threading.BoundedSemaphore(int(os.getenv('STEDI_MAX_CONCURRENT', '5')))

logger = logging.getLogger(__name__)

# Confirmed from https://www.stedi.com/docs/healthcare/api-reference/post-healthcare-eligibility
STEDI_ELIGIBILITY_URL = (
    "https://healthcare.us.stedi.com/2024-04-01/change/medicalnetwork/eligibility/v3"
)
DEFAULT_TIMEOUT = 60


def _get_api_key() -> str:
    key = os.getenv("STEDI_API_KEY", "").strip()
    if not key:
        raise ValueError(
            "Missing Stedi API key. Set STEDI_API_KEY environment variable."
        )
    return key


def send_eligibility_request(
    payload: dict[str, Any],
    *,
    timeout: int = DEFAULT_TIMEOUT,
) -> dict[str, Any]:
    """
    POST payload to Stedi real-time eligibility endpoint.

    Strips the internal _meta key before sending — it is for logging only.

    Returns parsed JSON response dict.
    Raises ValueError on HTTP errors so the orchestrator can write
    a structured error back to Monday.
    """
    api_key = _get_api_key()

    # Remove internal metadata before sending
    send_payload = {k: v for k, v in payload.items() if k != "_meta"}

    headers = {
        "Authorization": api_key,   # Stedi uses raw key (not Bearer prefix)
        "Content-Type":  "application/json",
    }

    meta = payload.get("_meta", {})
    logger.info(
        f"[ELG-CLIENT] POST {STEDI_ELIGIBILITY_URL} | "
        f"payer={payload.get('tradingPartnerServiceId')} "
        f"partner={meta.get('tradingPartnerName')!r}"
    )

    # Stedi's default account cap is 5 concurrent in-flight eligibility calls.
    # The semaphore bounds us across all threads in this process so a batch
    # of 100 webhooks queues gracefully instead of hitting 429s. We also
    # handle 429 defensively with exponential backoff in case our local cap
    # is looser than Stedi's actual cap.
    response = None
    for attempt in range(4):
        try:
            with stedi_concurrency:
                response = requests.post(
                    STEDI_ELIGIBILITY_URL,
                    headers=headers,
                    json=send_payload,
                    timeout=timeout,
                )
        except requests.exceptions.Timeout:
            raise ValueError(f"Stedi eligibility request timed out after {timeout}s")
        except requests.exceptions.ConnectionError as e:
            raise ValueError(f"Stedi eligibility connection error: {e}")

        if response.status_code != 429:
            break

        retry_after = response.headers.get("Retry-After")
        try:
            sleep_s = float(retry_after) if retry_after else (2 ** attempt)
        except ValueError:
            sleep_s = 2 ** attempt
        logger.warning(
            f"[ELG-CLIENT] 429 from Stedi | payer={payload.get('tradingPartnerServiceId')} "
            f"attempt={attempt + 1}/4 sleep={sleep_s}s"
        )
        time.sleep(sleep_s)
    else:
        # Exhausted retries — surface the last 429 as a ValueError
        raise ValueError(
            "Stedi rate-limited the request (HTTP 429) after 4 attempts. "
            "Consider lowering STEDI_MAX_CONCURRENT or asking Stedi support "
            "to raise your account concurrency cap."
        )

    try:
        response_json = response.json()
    except ValueError:
        response.raise_for_status()
        raise ValueError("Stedi returned a non-JSON response")

    if not response.ok:
        # Surface the first meaningful error from Stedi's response body
        errors = response_json.get("errors", [])
        first_error = ""
        if errors:
            first_error = errors[0].get("description") or errors[0].get("message") or ""
        raise ValueError(
            first_error
            or f"Stedi HTTP {response.status_code}: {json.dumps(response_json)[:300]}"
        )

    logger.info(
        f"[ELG-CLIENT] Response OK | "
        f"payer={payload.get('tradingPartnerServiceId')} "
        f"status={response.status_code}"
    )

    return response_json