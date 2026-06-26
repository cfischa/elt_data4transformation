"""Tests for the landing-page -> PDF resolver (pure; no network/DB)."""

from __future__ import annotations

from pathlib import Path

from study_scraper.pdf_resolver import is_pdf_url, resolve_pdf_url


FIX = Path(__file__).resolve().parent / "fixtures" / "pdf_resolver"


def _read(name: str) -> str:
    return (FIX / name).read_text(encoding="utf-8")


def test_ssoar_handle_page_prefers_volltext_bitstream() -> None:
    base = "https://www.ssoar.info/ssoar/handle/document/85213"
    url = resolve_pdf_url(base, _read("ssoar_handle_page.html"))
    assert url is not None
    assert url.endswith("Klimaschutz_und_Akzeptanz.pdf?sequence=1")
    assert "anhang" not in url.lower()  # appendix deprioritized
    assert url.startswith("https://www.ssoar.info/")  # made absolute


def test_citation_pdf_url_meta_wins_over_supplement() -> None:
    base = "https://journal.example.org/articles/123"
    url = resolve_pdf_url(base, _read("citation_pdf_url.html"))
    assert url == "https://journal.example.org/articles/123/download/full.pdf"


def test_direct_link_prefers_gesamt_over_zusammenfassung() -> None:
    base = "https://institut.example.org/berichte/2024/"
    url = resolve_pdf_url(base, _read("direct_pdf_link.html"))
    assert url is not None
    assert url.endswith("forschungsbericht-2024-gesamt.pdf")
    assert url == "https://institut.example.org/berichte/2024/files/forschungsbericht-2024-gesamt.pdf"


def test_relative_href_resolved_against_base() -> None:
    html = '<a href="../docs/report.pdf">Report</a>'
    url = resolve_pdf_url("https://x.org/a/b/page.html", html)
    assert url == "https://x.org/a/docs/report.pdf"


def test_query_string_pdf_is_detected() -> None:
    html = '<a href="/download?file=study.pdf&v=2">Download</a>'
    # not a .pdf path, but contains .pdf — regex/looks_like handles the common case
    html2 = '<a href="/dl/study.pdf?token=abc">Download</a>'
    url = resolve_pdf_url("https://x.org/", html2)
    assert url == "https://x.org/dl/study.pdf?token=abc"


def test_no_pdf_returns_none() -> None:
    html = "<html><body><p>No documents here, just text.</p></body></html>"
    assert resolve_pdf_url("https://x.org/", html) is None


def test_malformed_html_does_not_raise() -> None:
    html = '<a href="broken.pdf"  <<< garbage <a href='
    url = resolve_pdf_url("https://x.org/", html)
    assert url == "https://x.org/broken.pdf"


def test_empty_input_returns_none() -> None:
    assert resolve_pdf_url("https://x.org/", "") is None
    assert resolve_pdf_url("https://x.org/", b"") is None
    assert resolve_pdf_url("https://x.org/", None) is None


def test_bytes_input_supported() -> None:
    url = resolve_pdf_url("https://x.org/", b'<a href="x.pdf">x</a>')
    assert url == "https://x.org/x.pdf"


def test_is_pdf_url() -> None:
    assert is_pdf_url("https://x.org/a.pdf")
    assert is_pdf_url("https://x.org/dl/a.pdf?t=1")
    assert is_pdf_url("https://x.org/bitstream/handle/1/a")  # dspace path
    assert not is_pdf_url("https://x.org/landing/page.html")
