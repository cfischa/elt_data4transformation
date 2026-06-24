"""DAWUM polling-data source (lake-style; Q16-v2).

DAWUM publishes the entire poll database as one JSON document at
`https://api.dawum.de/`. The document has:

  - `Surveys`     : { survey_id -> survey dict }
  - `Institutes`  : { institute_id -> {Name, Url, ...} }
  - `Taskers`     : { tasker_id -> {Name, ...} }      (commissioner)
  - `Parliaments` : { parliament_id -> {Name, Shortcut, ...} }
  - `Methods`     : { method_id -> {Name, ...} }
  - `Parties`     : { party_id -> {Name, Shortcut, ...} }

This source emits one `SourceRecord` per survey. The survey's payload
is **denormalised** at fetch time -- we resolve Institute_ID etc. into
labels and embed them in the row's payload. Two reasons:

1. **Lake principle (Q16-v2).** "Collect as it was at fetch time."
   If DAWUM later renames an institute, older rows keep the old name.
2. **Self-contained rows simplify views.** `dawum_polls` (see migration
   0005) just reads `payload->'institute'->>'name'`; no joins.

Two modes (same parser, mirrors the SSOAR / OpenAlex pattern):

  * `from_file=<path>`: reads a saved JSON response from disk. Used by
    tests and by environments without outbound HTTP (the dev sandbox).
  * Live (default): GETs `https://api.dawum.de/`. ODbL license, no auth.

License: ODC-ODbL. We record this on every emitted `SourceRecord` so
the dock's downstream view can show it.
"""

from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional

import httpx

from study_scraper.models import SourceRecord


LOGGER = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://api.dawum.de/"
DAWUM_LICENSE = "ODC-ODbL"
RECORD_FORMAT = "dawum_survey_json"


class DAWUMSource:
    """Live or fixture-driven DAWUM ingest. Emits `SourceRecord`s."""

    source_id = "dawum"

    def __init__(
        self,
        *,
        base_url: str = DEFAULT_BASE_URL,
        from_file: Optional[Path] = None,
        client: Optional[httpx.Client] = None,
        timeout: float = 60.0,
        user_agent: str = "study-scraper/0.0.1 (+https://github.com/cfischa/elt_data4transformation)",
    ) -> None:
        self._base_url = base_url
        self._from_file = from_file
        self._owns_client = client is None
        self._client = client or httpx.Client(
            timeout=timeout,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
            follow_redirects=True,
        )

    def close(self) -> None:
        if self._owns_client:
            self._client.close()

    def __enter__(self) -> "DAWUMSource":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Public iterator
    # ------------------------------------------------------------------

    def iter_records(
        self,
        *,
        run_id: str,
        limit: Optional[int] = None,
    ) -> Iterator[SourceRecord]:
        """Yield one SourceRecord per DAWUM survey."""
        if self._from_file is not None:
            payload = json.loads(self._from_file.read_text(encoding="utf-8"))
        else:
            LOGGER.info("DAWUM GET %s", self._base_url)
            resp = self._client.get(self._base_url)
            resp.raise_for_status()
            payload = resp.json()

        # Reference tables — used for label denormalisation.
        institutes  = (payload.get("Institutes")  or {})
        taskers     = (payload.get("Taskers")     or {})
        parliaments = (payload.get("Parliaments") or {})
        methods     = (payload.get("Methods")     or {})
        parties     = (payload.get("Parties")     or {})

        surveys = payload.get("Surveys") or {}
        now = datetime.now(timezone.utc)

        yielded = 0
        for survey_id, survey in surveys.items():
            if not isinstance(survey, dict):
                continue
            denorm = self._denormalise_survey(
                survey_id=survey_id,
                survey=survey,
                institutes=institutes,
                taskers=taskers,
                parliaments=parliaments,
                methods=methods,
                parties=parties,
            )
            canonical_url = self._canonical_url_for(denorm)
            payload_bytes = json.dumps(
                denorm, ensure_ascii=False, sort_keys=True
            ).encode("utf-8")
            content_hash = hashlib.sha256(payload_bytes).hexdigest()

            yield SourceRecord.build(
                source_id=self.source_id,
                source_record_id=str(survey_id),
                canonical_url=canonical_url,
                format=RECORD_FORMAT,
                content_type="application/json",
                content_hash=content_hash,
                fetched_at=now,
                discovery_run_id=run_id,
                payload=denorm,
                license=DAWUM_LICENSE,
                provenance={
                    "fetch_source": "dawum_api_v1",
                    "base_url": self._base_url,
                    "from_file": str(self._from_file) if self._from_file else None,
                },
            )
            yielded += 1
            if limit is not None and yielded >= limit:
                return

    # ------------------------------------------------------------------
    # Denormalisation (label resolution)
    # ------------------------------------------------------------------

    @staticmethod
    def _label(table: Dict[str, Any], key: Any) -> Dict[str, Any]:
        if key is None:
            return {"id": None, "name": None}
        entry = table.get(str(key)) or {}
        return {
            "id": str(key) if key is not None else None,
            "name": entry.get("Name"),
            "shortcut": entry.get("Shortcut"),
            "url": entry.get("Url"),
        }

    @staticmethod
    def _denormalise_results(
        results: Dict[str, Any], parties: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        if not isinstance(results, dict):
            return []
        out: List[Dict[str, Any]] = []
        for party_id, percentage in results.items():
            party_entry = parties.get(str(party_id)) or {}
            try:
                pct = float(percentage)
            except (TypeError, ValueError):
                pct = None
            out.append(
                {
                    "party_id":   str(party_id),
                    "party_name": party_entry.get("Name"),
                    "shortcut":   party_entry.get("Shortcut"),
                    "percentage": pct,
                }
            )
        # Stable order so content_hash is reproducible.
        out.sort(key=lambda r: r["party_id"])
        return out

    def _denormalise_survey(
        self,
        *,
        survey_id: str,
        survey: Dict[str, Any],
        institutes: Dict[str, Any],
        taskers: Dict[str, Any],
        parliaments: Dict[str, Any],
        methods: Dict[str, Any],
        parties: Dict[str, Any],
    ) -> Dict[str, Any]:
        period = survey.get("Survey_Period") or {}
        sample = survey.get("Surveyed_Persons")
        # DAWUM sometimes returns the count as a string.
        try:
            sample_int = int(sample) if sample not in (None, "", "0") else None
        except (TypeError, ValueError):
            sample_int = None
        # DAWUM's `Date` is yyyy-mm-dd; keep as string in payload to
        # avoid lossy conversion (the view casts to ::date at query time).
        return {
            "survey_id":  str(survey_id),
            "date":       survey.get("Date"),
            "survey_period": {
                "start": period.get("Date_Start"),
                "end":   period.get("Date_End"),
            },
            "sample_size": sample_int,
            "institute":  self._label(institutes, survey.get("Institute_ID")),
            "tasker":     self._label(taskers, survey.get("Tasker_ID")),
            "parliament": self._label(parliaments, survey.get("Parliament_ID")),
            "method":     self._label(methods, survey.get("Method_ID")),
            "results":    self._denormalise_results(
                survey.get("Results") or {}, parties
            ),
        }

    @staticmethod
    def _canonical_url_for(denorm: Dict[str, Any]) -> str:
        """DAWUM exposes a per-parliament/per-survey page at
        dawum.de/<Parliament_Shortcut>/<Survey_ID>. When the shortcut
        isn't available we fall back to a generic id-only URL."""
        survey_id = denorm["survey_id"]
        shortcut = (denorm.get("parliament") or {}).get("shortcut")
        if shortcut:
            return f"https://dawum.de/{shortcut}/{survey_id}"
        return f"https://dawum.de/Survey/{survey_id}"
