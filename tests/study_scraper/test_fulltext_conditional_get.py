"""Conditional GET for fulltext refetch (issue #34).

Pure tests -- mocked transport, no network, no DB -- for the two
building blocks that avoid re-processing an unchanged document:
`conditional_headers` (provenance -> If-None-Match/If-Modified-Since)
and `fetch_url`'s 304 short-circuit.
"""

from __future__ import annotations

import httpx

from study_scraper.fulltext import conditional_headers, fetch_url


class TestConditionalHeaders:
    def test_empty_when_nothing_captured_yet(self) -> None:
        assert conditional_headers({}) == {}

    def test_etag_only(self) -> None:
        headers = conditional_headers({"fetch_etag": '"abc123"'})
        assert headers == {"If-None-Match": '"abc123"'}

    def test_last_modified_only(self) -> None:
        headers = conditional_headers(
            {"fetch_last_modified": "Wed, 01 Jul 2026 00:00:00 GMT"}
        )
        assert headers == {
            "If-Modified-Since": "Wed, 01 Jul 2026 00:00:00 GMT"
        }

    def test_both_present(self) -> None:
        headers = conditional_headers({
            "fetch_etag": '"abc123"',
            "fetch_last_modified": "Wed, 01 Jul 2026 00:00:00 GMT",
        })
        assert headers == {
            "If-None-Match": '"abc123"',
            "If-Modified-Since": "Wed, 01 Jul 2026 00:00:00 GMT",
        }

    def test_ignores_unrelated_provenance_keys(self) -> None:
        headers = conditional_headers({
            "discovery_source": "ssoar", "resolved_pdf_url": "https://x/y.pdf",
        })
        assert headers == {}


class TestFetchUrlConditionalGet:
    def test_sends_conditional_headers_and_returns_body_on_200(self) -> None:
        seen = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["if_none_match"] = request.headers.get("if-none-match")
            return httpx.Response(
                200, content=b"hello", headers={"ETag": '"v2"'},
            )

        content, content_type, resp = fetch_url(
            "https://example.org/doc.pdf",
            headers={"If-None-Match": '"v1"'},
            transport=httpx.MockTransport(handler),
        )
        assert seen["if_none_match"] == '"v1"'
        assert content == b"hello"
        assert resp.headers.get("etag") == '"v2"'

    def test_304_short_circuits_content_to_none(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(304)

        content, content_type, resp = fetch_url(
            "https://example.org/doc.pdf",
            headers={"If-None-Match": '"v1"'},
            transport=httpx.MockTransport(handler),
        )
        assert content is None
        assert content_type is None
        assert resp.status_code == 304

    def test_no_headers_means_unconditional_get(self) -> None:
        seen = {}

        def handler(request: httpx.Request) -> httpx.Response:
            seen["if_none_match"] = request.headers.get("if-none-match")
            return httpx.Response(200, content=b"hello")

        content, _content_type, _resp = fetch_url(
            "https://example.org/doc.pdf",
            transport=httpx.MockTransport(handler),
        )
        assert seen["if_none_match"] is None
        assert content == b"hello"
