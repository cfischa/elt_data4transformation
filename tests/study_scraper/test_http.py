"""Shared HTTP retry/backoff/politeness policy (agent:task #32).

Exercised with httpx.MockTransport and an injected no-op sleeper, so retry
behaviour is asserted without any real waiting or network.
"""

from __future__ import annotations

from typing import Callable, List

import httpx
import pytest

from study_scraper.http import (
    RETRYABLE_STATUS,
    get_with_retry,
    polite_sleep,
    request_with_retry,
)


def _client(handler: Callable[[httpx.Request], httpx.Response]) -> httpx.Client:
    return httpx.Client(transport=httpx.MockTransport(handler))


def _recording_sleeper() -> "tuple[Callable[[float], None], List[float]]":
    slept: List[float] = []
    return (lambda s: slept.append(s)), slept


def test_retries_429_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(429, text="slow down")
        return httpx.Response(200, text="ok")

    sleep, slept = _recording_sleeper()
    with _client(handler) as client:
        resp = get_with_retry(client, "https://x.test/a", sleep=sleep)

    assert resp.status_code == 200
    assert calls["n"] == 2
    assert len(slept) == 1  # slept once between the two attempts


def test_no_retry_on_404() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(404, text="nope")

    sleep, slept = _recording_sleeper()
    with _client(handler) as client:
        resp = get_with_retry(client, "https://x.test/missing", sleep=sleep)

    assert resp.status_code == 404
    assert calls["n"] == 1  # 4xx (non-429) is not retried
    assert slept == []


def test_honours_retry_after_header() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            return httpx.Response(503, headers={"Retry-After": "7"})
        return httpx.Response(200)

    sleep, slept = _recording_sleeper()
    with _client(handler) as client:
        resp = get_with_retry(client, "https://x.test/b", sleep=sleep)

    assert resp.status_code == 200
    assert slept == [7.0]  # exact Retry-After, not backoff


def test_caps_large_retry_after_header() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            # OpenAlex-style large Retry-After (tens of minutes) — must not
            # be honoured verbatim or a single 429 can stall the whole job.
            return httpx.Response(429, headers={"Retry-After": "3000"})
        return httpx.Response(200)

    sleep, slept = _recording_sleeper()
    with _client(handler) as client:
        resp = get_with_retry(client, "https://x.test/g", sleep=sleep)

    assert resp.status_code == 200
    assert slept == [120.0]  # capped, not the raw 3000s


def test_retries_transport_error_then_succeeds() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("boom", request=request)
        return httpx.Response(200)

    sleep, _ = _recording_sleeper()
    with _client(handler) as client:
        resp = get_with_retry(client, "https://x.test/c", sleep=sleep)

    assert resp.status_code == 200
    assert calls["n"] == 2


def test_returns_last_response_after_exhausting_retries() -> None:
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(503)

    sleep, slept = _recording_sleeper()
    with _client(handler) as client:
        resp = request_with_retry(
            client, "GET", "https://x.test/d", max_attempts=3, sleep=sleep
        )

    assert resp.status_code == 503  # handed back, not swallowed
    assert calls["n"] == 3  # exactly max_attempts
    assert len(slept) == 2  # waited between the 3 attempts


def test_transport_error_propagates_after_retries() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("down", request=request)

    sleep, _ = _recording_sleeper()
    with _client(handler) as client:
        with pytest.raises(httpx.ConnectError):
            request_with_retry(
                client, "GET", "https://x.test/e", max_attempts=2, sleep=sleep
            )


def test_params_pass_through() -> None:
    seen = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(200)

    with _client(handler) as client:
        get_with_retry(client, "https://x.test/f", params={"q": "hi", "n": 2})

    assert "q=hi" in seen["url"] and "n=2" in seen["url"]


def test_polite_sleep_skips_nonpositive() -> None:
    sleep, slept = _recording_sleeper()
    polite_sleep(0.0, sleep=sleep)
    polite_sleep(-1, sleep=sleep)
    assert slept == []
    polite_sleep(0.25, sleep=sleep)
    assert slept == [0.25]


def test_retryable_status_set() -> None:
    assert 429 in RETRYABLE_STATUS
    assert 503 in RETRYABLE_STATUS
    assert 404 not in RETRYABLE_STATUS
    assert 200 not in RETRYABLE_STATUS
