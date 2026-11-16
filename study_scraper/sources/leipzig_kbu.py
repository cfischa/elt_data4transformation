"""Leipzig Kommunale Buergerumfrage (KBU) lake source (issue #232).

opendata.leipzig.de is a CKAN portal that publishes the city's annual
opinion survey (Kommunale Buergerumfrage) as open data. This source reuses
the GovData CKAN code (`package_search`, fixture mode, provenance) pointed
at the Leipzig portal and lands the dataset/resource *listing* as study
records. Row-level respondent data is out of scope (resources are linked in
the preserved payload, never downloaded).

Licence gate: only datasets whose CKAN `license_id` is on an open-licence
allowlist are kept; anything else is skipped with a warning, so a licence
change upstream cannot silently land unusable records.
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Optional

from study_scraper.models import SourceRecord
from study_scraper.sources.govdata import GovDataSource


LOGGER = logging.getLogger(__name__)

LEIPZIG_BASE_URL = "https://opendata.leipzig.de/api/3/action/package_search"
DEFAULT_QUERY = "Bürgerumfrage"

# Datenlizenz Deutschland (by / zero 2.0) and CC variants permitting reuse.
ALLOWED_LICENSE_IDS = frozenset(
    {"dl-de-by-2.0", "dl-de-zero-2.0", "cc-zero", "cc-by", "cc-by-sa"}
)


class LeipzigKBUSource(GovDataSource):
    """Lake source for Leipzig's CKAN-published Buergerumfrage datasets."""

    source_id = "leipzig_kbu"
    dataset_url_base = "https://opendata.leipzig.de/dataset/"
    fetch_source = "leipzig_ckan_package_search"

    def __init__(self, **kwargs: Any) -> None:
        kwargs.setdefault("base_url", LEIPZIG_BASE_URL)
        if not kwargs.get("queries") and kwargs.get("from_file") is None:
            kwargs["queries"] = [DEFAULT_QUERY]
        super().__init__(**kwargs)

    def _build_record(
        self,
        *,
        dataset: Dict[str, Any],
        now: datetime,
        run_id: str,
        query: Optional[str] = None,
    ) -> Optional[SourceRecord]:
        license_id = str(dataset.get("license_id") or "").strip().lower()
        if license_id not in ALLOWED_LICENSE_IDS:
            LOGGER.warning(
                "leipzig_kbu skipping %r: licence %r not on allowlist",
                dataset.get("name") or dataset.get("id"),
                license_id or None,
            )
            return None
        return super()._build_record(
            dataset=dataset, now=now, run_id=run_id, query=query
        )
