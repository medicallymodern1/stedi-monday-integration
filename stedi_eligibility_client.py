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
from typing import Any

import requests

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

    try:
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