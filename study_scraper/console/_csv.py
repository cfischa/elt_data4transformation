"""Topic CSV helpers used by `pages/1_Topics.py`.

Extracted so unit tests can exercise them without importing the
Streamlit page (which would execute top-level `st.*` calls).
"""

from __future__ import annotations

import csv
import io
from pathlib import Path
from typing import Dict, List

from study_scraper.topics import Topic


LIST_SEP = "|"
CSV_COLUMNS = (
    "id",
    "name",
    "description",
    "language",
    "include_keywords",
    "exclude_keywords",
    "synonyms",
)


def terms_to_pipe(terms: List[str]) -> str:
    return LIST_SEP.join(terms)


def pipe_to_terms(text: str) -> List[str]:
    return [t.strip() for t in text.split(LIST_SEP) if t.strip()]


def write_csv(path: Path, topics: List[Topic]) -> None:
    """Write a list of topics back to a CSV, one row per (topic, locale)."""
    rows: List[Dict[str, str]] = []
    for topic in topics:
        for language, locale in topic.locales.items():
            rows.append(
                {
                    "id": topic.id,
                    "name": locale.name,
                    "description": locale.description,
                    "language": language,
                    "include_keywords": terms_to_pipe(locale.include_keywords),
                    "exclude_keywords": terms_to_pipe(locale.exclude_keywords),
                    "synonyms": terms_to_pipe(locale.synonyms),
                }
            )
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=CSV_COLUMNS)
    writer.writeheader()
    writer.writerows(rows)
    path.write_text(buf.getvalue(), encoding="utf-8")
