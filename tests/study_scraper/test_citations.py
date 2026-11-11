from study_scraper.citations import extract_dois, extract_urls


def test_extract_dois_normalizes_and_dedups():
    text = "See doi:10.1000/ABC.123, and https://doi.org/10.1000/abc.123. Also 10.5555/x-y)."
    assert extract_dois(text) == ["10.1000/abc.123", "10.5555/x-y"]


def test_extract_urls_skips_doi_resolver_and_trailing_punct():
    text = "Quelle: https://example.org/studie.pdf, https://doi.org/10.1/x und https://example.org/studie.pdf."
    assert extract_urls(text) == ["https://example.org/studie.pdf"]


def test_empty_inputs():
    assert extract_dois("") == []
    assert extract_urls(None) == []
