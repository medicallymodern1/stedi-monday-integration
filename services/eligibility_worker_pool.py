"""
services/eligibility_worker_pool.py
===================================
Process-wide thread pool + Stedi concurrency semaphore shared by the
Subscription (and optionally Intake) eligibility flows.

Why a thread pool?
------------------
Monday's webhook requires an ACK within ~5 seconds. Our per-request pipeline
does three network calls (Monday fetch + Stedi POST + Monday writeback) and
takes 5-7 seconds end-to-end. If we process inline on the async event loop,
only one request runs at a time and Monday times out the ACK long before
the pipeline finishes.

The fix: the webhook handler enqueues a job into this pool and returns the
ACK immediately. Multiple jobs run in parallel on background threads,
bounded by:
  - ELIGIBILITY_POOL_MAX_WORKERS  thread pool size (default 8)
  - STEDI_MAX_CONCURRENT          Stedi in-flight requests (default 5)

The Stedi semaphore is exposed for stedi_eligibility_client.py to acquire
around each HTTP call, so Stedi's default 5-concurrent account cap is
respected even when the pool is bigger than that.
"""

from __future__ import annotations

import concurrent.futures
import logging
import os
import threading
from typing import Callable

logger = logging.getLogger(__name__)

MAX_WORKERS = int(os.getenv("ELIGIBILITY_POOL_MAX_WORKERS", "8"))
STEDI_MAX_CONCURRENT = int(os.getenv("STEDI_MAX_CONCURRENT", "5"))

# Process-wide semaphore — import this in stedi_eligibility_client.py and
# acquire it around the HTTP POST. Safe to use from any thread.
stedi_concurrency = threading.BoundedSemaphore(STEDI_MAX_CONCURRENT)

_pool: concurrent.futures.ThreadPoolExecutor | None = None
_pool_lock = threading.Lock()


def _get_pool() -> concurrent.futures.ThreadPoolExecutor:
    """Lazy-init the pool so tests/imports don't spawn threads unnecessarily."""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                logger.info(
                    f"[ELG-POOL] starting pool | workers={MAX_WORKERS} "
                    f"stedi_concurrency={STEDI_MAX_CONCURRENT}"
                )
                _pool = concurrent.futures.ThreadPoolExecutor(
                    max_workers=MAX_WORKERS,
                    thread_name_prefix="elg-worker",
                )
    return _pool


def submit(fn: Callable, *args, **kwargs) -> concurrent.futures.Future:
    """Enqueue a job. Returns the Future (ignored by fire-and-forget callers)."""
    return _get_pool().submit(fn, *args, **kwargs)


def pool_stats() -> dict:
    """Return current pool utilization snapshot — handy for debug endpoints."""
    p = _get_pool()
    # ThreadPoolExecutor doesn't expose queue depth publicly; _work_queue is
    # the documented backing queue in CPython and is safe to peek at.
    queue_depth = p._work_queue.qsize()  # type: ignore[attr-defined]
    active = MAX_WORKERS - (stedi_concurrency._value)  # type: ignore[attr-defined]
    return {
        "max_workers":         MAX_WORKERS,
        "stedi_max_concurrent": STEDI_MAX_CONCURRENT,
        "stedi_in_flight":     active if active >= 0 else 0,
        "queued_jobs":         queue_depth,
    }
