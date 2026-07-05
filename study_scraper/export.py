"""Open dataset export (product-expansion Axis 3).

Writes the findings layer as a versioned, reusable open dataset:

    findings.csv   one row per stored attribution, with study context
    studies.csv    metadata of every kept study (no abstracts/full text)
    manifest.json  counts, generation time, method + legal notes

Legal shape (per the product-expansion note): facts + metadata + links
only. Toplines and provenance are facts and safe to republish;
abstracts and full texts are NOT exported.
"""

from __future__ import annotations

import csv
import datetime as _dt
import json
from pathlib import Path
from typing import Any, Dict, List

FINDINGS_COLUMNS = [
    "question", "position", "percentage", "population", "confidence",
    "model", "sample_size", "publication_date", "publisher", "title",
    "source_id", "canonical_url", "topic_ids",
]
STUDIES_COLUMNS = [
    "id", "title", "publisher", "publication_date", "language",
    "topic_ids", "has_quantitative_data", "source_id", "canonical_url",
]

_MANIFEST_NOTES = {
    "method": (
        "Findings are machine-extracted (question, position, percentage) "
        "triples with model confidence; verify against canonical_url "
        "before citing. Percentages are as published by the named source."
    ),
    "content": (
        "Facts and metadata only: published toplines, bibliographic "
        "fields, and links. No abstracts or full texts are included."
    ),
}


def _cell(value: Any) -> Any:
    if isinstance(value, list):
        return "|".join(str(v) for v in value)
    if isinstance(value, _dt.datetime):
        return value.date().isoformat()
    if isinstance(value, _dt.date):
        return value.isoformat()
    return value


def _write_csv(path: Path, columns: List[str], rows: List[Dict[str, Any]]) -> int:
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({c: _cell(row.get(c)) for c in columns})
    return len(rows)


def run_export(storage: Any, out_dir: Path) -> Dict[str, Any]:
    """Write the dataset into `out_dir`; returns the manifest dict."""
    out_dir.mkdir(parents=True, exist_ok=True)

    findings = storage.list_attributions_for_export()
    studies = storage.list_studies_for_export()

    n_findings = _write_csv(out_dir / "findings.csv", FINDINGS_COLUMNS, findings)
    n_studies = _write_csv(out_dir / "studies.csv", STUDIES_COLUMNS, studies)

    manifest = {
        "generated_at": _dt.datetime.now(_dt.timezone.utc).isoformat(),
        "findings": n_findings,
        "studies": n_studies,
        "files": ["findings.csv", "studies.csv"],
        "notes": _MANIFEST_NOTES,
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    return manifest
