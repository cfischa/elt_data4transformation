"""Shared HTTP fetch policy — retry, backoff, and politeness for every
source client and the fulltext loop.

Before this module each fetcher (`ssoar`, `openalex`, `bundestag_dip`,
`dawum`, `eurostat`, `fulltext.fetch_url`) called ``client.get`` with no
retry, and the fulltext loop bursted dozens of PDF fetches at one host with
no delay. Standard scraper craft: jittered exponential backoff, honour
``Retry-After`` on 429/503, and a small per-request politeness delay.

Everything funnels through :func:`request_with_retry`. It is deliberately
transport-agnostic: callers pass their own ``httpx.Client`` (so
``from_file`` fixture modes, which never touch the network, are unaffected),
and the ``sleep`` seam lets tests assert retry behaviour with
``httpx.MockTransport`` without waiting in real time.
"""

from __future__ import annotations

import random
import time
from typing import Any, Callable, Optional

import httpx
from tenacity import (
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
)

# 429 = rate limited; 5xx = transient upstream trouble. 4xx (except 429) are
# the caller's fault and must NOT be retried (a 404 stays a 404).
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})

_DEFAULT_MAX_ATTEMPTS = 4
_DEFAULT_BASE_DELAY = 0.5
_MAX_BACKOFF = 30.0
# Upstream Retry-After is honoured (it's a legitimate signal, unlike our own
# backoff guess), but uncapped it can stall a whole job: OpenAlex has been
# observed sending Retry-After values in the tens of minutes under sustained
# rate limiting, and three retries at that scale alone burn hours. Cap it
# well above our own backoff ceiling but still bounded.
_MAX_RETRY_AFTER = 120.0


class RetryableResponse(Exception):
    """Internal signal: a response arrived but its status is worth retrying.

    Carries the response so the caller can still inspect/raise on it once
    retries are exhausted (we hand the last response back rather than
    swallowing it)."""

    def __init__(self, response: httpx.Response, retry_after: Optional[float]):
        super().__init__(f"retryable status {response.status_code}")
        self.response = response
        self.retry_after = retry_after


def _parse_retry_after(response: httpx.Response) -> Optional[float]:
    """Seconds to wait per the ``Retry-After`` header, if present and numeric.
    HTTP-date form is ignored (we fall back to backoff) — the delta-seconds
    form is what rate limiters actually send."""
    raw = response.headers.get("retry-after")
    if not raw:
        return None
    try:
        seconds = float(raw.strip())
    except ValueError:
        return None
    return max(0.0, seconds)


def _wait_seconds(state: Any, base_delay: float) -> float:
    """Honour ``Retry-After`` when the upstream gave one, else exponential
    backoff with full jitter, capped."""
    exc = state.outcome.exception() if state.outcome else None
    if isinstance(exc, RetryableResponse) and exc.retry_after is not None:
        return min(exc.retry_after, _MAX_RETRY_AFTER)
    attempt = max(1, state.attempt_number)
    ceiling = min(base_delay * (2 ** (attempt - 1)), _MAX_BACKOFF)
    return random.uniform(0.0, ceiling)  # full jitter


def request_with_retry(
    client: httpx.Client,
    method: str,
    url: str,
    *,
    max_attempts: int = _DEFAULT_MAX_ATTEMPTS,
    base_delay: float = _DEFAULT_BASE_DELAY,
    sleep: Callable[[float], None] = time.sleep,
    **kwargs: Any,
) -> httpx.Response:
    """Issue ``client.request(method, url, **kwargs)`` with retries.

    Retries on transport errors and on :data:`RETRYABLE_STATUS` responses,
    honouring ``Retry-After``. Returns the final response; on a persistently
    retryable status it returns that last response (callers keep their
    existing ``raise_for_status()``). Transport errors that outlast the
    retries propagate unchanged.
    """
    retrying = Retrying(
        stop=stop_after_attempt(max_attempts),
        wait=lambda state: _wait_seconds(state, base_delay),
        retry=retry_if_exception_type((httpx.TransportError, RetryableResponse)),
        sleep=sleep,
        reraise=True,
    )

    def _attempt() -> httpx.Response:
        response = client.request(method, url, **kwargs)
        if response.status_code in RETRYABLE_STATUS:
            raise RetryableResponse(response, _parse_retry_after(response))
        return response

    try:
        return retrying(_attempt)
    except RetryableResponse as exc:
        # Retries exhausted on e.g. a stubborn 503 — hand the response back so
        # the caller's raise_for_status()/status handling behaves as before.
        return exc.response


def get_with_retry(
    client: httpx.Client, url: str, **kwargs: Any
) -> httpx.Response:
    """Convenience wrapper for the common GET case."""
    return request_with_retry(client, "GET", url, **kwargs)


def polite_sleep(
    delay: float, *, sleep: Callable[[float], None] = time.sleep
) -> None:
    """Small inter-request pause so a tight fetch loop doesn't hammer one
    host. No-op for non-positive delays."""
    if delay and delay > 0:
        sleep(delay)
