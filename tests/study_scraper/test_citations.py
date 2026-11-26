from study_scraper.citations import extract_dois, extract_urls, references_section


def test_extract_dois_normalizes_and_dedups():
    text = "See doi:10.1000/ABC.123, and https://doi.org/10.1000/abc.123. Also 10.5555/x-y)."
    assert extract_dois(text) == ["10.1000/abc.123", "10.5555/x-y"]


def test_extract_urls_skips_doi_resolver_and_trailing_punct():
    text = "Quelle: https://example.org/studie.pdf, https://doi.org/10.1/x und https://example.org/studie.pdf."
    assert extract_urls(text) == ["https://example.org/studie.pdf"]


def test_empty_inputs():
    assert extract_dois("") == []
    assert extract_urls(None) == []


def test_references_section_uses_last_heading():
    from study_scraper.citations import references_section
    text = "Inhalt\nLiteraturverzeichnis\n...\nKapitel 1 doi:10.1/body\n\n7. Literaturverzeichnis\nMüller (2020) 10.2/ref\n"
    sec = references_section(text)
    assert "10.2/ref" in sec and "10.1/body" not in sec


def test_references_section_absent():
    from study_scraper.citations import references_section
    assert references_section("kein Verzeichnis hier") == ""
    assert references_section(None) == ""


def test_mine_citations_only_reads_references_section():
    from study_scraper.citations import mine_citations
    text = "Body https://body.example/x doi:10.1001/body\nReferences\nA (2020) 10.1002/ref https://ref.example/s.pdf.\n"
    assert mine_citations(text) == {"dois": ["10.1002/ref"], "urls": ["https://ref.example/s.pdf"]}


def test_mine_citations_no_section():
    from study_scraper.citations import mine_citations
    assert mine_citations("doi:10.1001/x") == {"dois": [], "urls": []}


def test_citation_candidates_normalizes_and_dedupes():
    from study_scraper.citations import citation_candidates

    text = (
        "Body see https://ignored.example/x\n"
        "Literatur\n"
        "A. doi:10.1000/ABC.1. B. https://doi.org/10.1000/abc.1\n"
        "C. https://example.org/study.pdf, D. https://example.org/study.pdf\n"
    )
    assert citation_candidates(text) == [
        "https://doi.org/10.1000/abc.1",
        "https://example.org/study.pdf",
    ]


def test_citation_candidates_no_references_heading():
    from study_scraper.citations import citation_candidates

    assert citation_candidates("see 10.1000/x https://a.org") == []


def test_references_section_german_heading_variants():
    for heading in (
        "Referenzen",
        "Literaturangaben",
        "Quellen- und Literaturverzeichnis",
        "Quellen und Literaturverzeichnis",
        "Literatur und Quellen",
        "6. Quellenangaben:",
        "Literaturliste",
        "Quellenliste",
        "Literaturnachweis",
        "Literaturnachweise",
        "Quellennachweise",
        "Works Cited",
        "Zitierte Literatur",
        "Verwendete Literatur",
        "Verwendete Quellen",
        "References Cited",
        "Bibliografie",
        "Reference List",
        "Literature Cited",
        "Literature",
        "Sources",
        "Reference",
        "Weiterführende Literatur",
        "Weiterfuehrende Literatur",
        "Further Reading",
    ):
        assert references_section(f"Text\n{heading}\nhttps://a.de/x") == "https://a.de/x"


def test_references_section_ignores_heading_inside_sentence():
    assert references_section("Siehe Referenzen im Anhang\nmehr") == ""
