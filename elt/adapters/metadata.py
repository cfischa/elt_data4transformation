"""
Metadata adapters that normalise source specific payloads (DESTATIS, GESIS, ...).
The goal is to provide a canonical structure for downstream classification and storage.
"""

from __future__ import annotations

import json
import logging
import hashlib
from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class CanonicalDatasetMetadata(BaseModel):
    """Canonical representation of metadata across heterogeneous sources."""

    source: str
    dataset_id: str
    title: str
    description: str
    keywords: List[str] = Field(default_factory=list)
    variables: List[str] = Field(default_factory=list)
    latest_update: Optional[datetime] = None
    raw_metadata: Dict[str, Any] = Field(default_factory=dict)
    metadata_hash: Optional[str] = None

    class Config:
        json_encoders = {
            datetime: lambda dt: dt.isoformat()
        }

    def compute_hash(self) -> str:
        """Compute a stable hash based on the canonical payload."""
        payload = {
            "source": self.source,
            "dataset_id": self.dataset_id,
            "title": self.title,
            "description": self.description,
            "keywords": sorted(set(self.keywords)),
            "variables": sorted(set(self.variables)),
            "latest_update": self.latest_update.isoformat() if self.latest_update else None,
        }
        raw = json.dumps(payload, ensure_ascii=True, sort_keys=True)
        return hashlib.md5(raw.encode("utf-8")).hexdigest()

    @property
    def text_blob(self) -> str:
        """Return concatenated text fields for keyword or embedding matching."""
        parts = [
            self.title or "",
            self.description or "",
            " ".join(self.keywords or []),
            " ".join(self.variables or []),
        ]
        return " ".join(part for part in parts if part).strip()


def _clean_list_values(values: Any) -> List[str]:
    """Normalise potential list-like structures into plain string lists."""
    if values is None:
        return []
    if isinstance(values, list):
        return [str(v).strip() for v in values if str(v).strip()]
    if isinstance(values, str):
        # Try to parse JSON array first
        stripped = values.strip()
        if stripped.startswith("[") and stripped.endswith("]"):
            try:
                parsed = json.loads(stripped)
                if isinstance(parsed, list):
                    return [str(v).strip() for v in parsed if str(v).strip()]
            except json.JSONDecodeError:
                pass
        # Fallback: split on comma/semicolon
        return [part.strip() for part in stripped.replace(";", ",").split(",") if part.strip()]
    return [str(values).strip()]


def adapt_destatis_metadata(row: Dict[str, Any]) -> CanonicalDatasetMetadata:
    """Convert a DESTATIS catalogue row into canonical metadata."""
    title = str(row.get("content") or row.get("cube_code") or "").strip()
    if not title:
        title = f"Destatis Dataset {row.get('cube_code', 'unknown')}"

    description_parts: List[str] = []
    if row.get("state"):
        description_parts.append(str(row["state"]))
    if row.get("time_coverage"):
        description_parts.append(f"Zeitabdeckung: {row['time_coverage']}")
    if row.get("information"):
        description_parts.append(f"Information: {row['information']}")

    description = " | ".join(description_parts) if description_parts else title

    keywords = _clean_list_values(row.get("state"))
    if row.get("time_coverage"):
        keywords.append(str(row["time_coverage"]))

    latest_update = row.get("latest_update")
    if isinstance(latest_update, str) and latest_update:
        try:
            latest_update = datetime.fromisoformat(latest_update)
        except ValueError:
            logger.debug("Could not parse latest_update %s for %s", latest_update, row.get("cube_code"))
            latest_update = None

    metadata = CanonicalDatasetMetadata(
        source="destatis",
        dataset_id=str(row.get("cube_code") or ""),
        title=title,
        description=description,
        keywords=[kw for kw in keywords if kw],
        variables=[],
        latest_update=latest_update,
        raw_metadata=row,
    )
    metadata.metadata_hash = metadata.compute_hash()
    return metadata


def adapt_gesis_metadata(row: Dict[str, Any]) -> CanonicalDatasetMetadata:
    """Convert a GESIS metadata row into canonical metadata."""
    raw_data = row.get("raw_data")
    raw_payload: Dict[str, Any] = {}
    if isinstance(raw_data, str):
        try:
            raw_payload = json.loads(raw_data)
        except json.JSONDecodeError:
            logger.debug("raw_data is not JSON for resource_id=%s", row.get("resource_id"))

    title = str(row.get("title") or raw_payload.get("title") or "").strip()
    if not title:
        title = f"GESIS Dataset {row.get('resource_id', 'unknown')}"

    description = str(row.get("description") or raw_payload.get("description") or title)

    variables = _clean_list_values(row.get("variables") or raw_payload.get("variables"))
    keywords = _clean_list_values(raw_payload.get("keywords") or raw_payload.get("tags"))
    if row.get("creator"):
        keywords.append(str(row["creator"]))

    latest_update: Optional[datetime] = None
    issued = row.get("issued") or raw_payload.get("issued")
    if isinstance(issued, str):
        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y-%m", "%Y"):
            try:
                latest_update = datetime.strptime(issued[: len(fmt)], fmt)
                break
            except ValueError:
                continue

    metadata = CanonicalDatasetMetadata(
        source="gesis",
        dataset_id=str(row.get("resource_id") or ""),
        title=title,
        description=description,
        keywords=[kw for kw in keywords if kw],
        variables=[var for var in variables if var],
        latest_update=latest_update,
        raw_metadata=raw_payload or row,
    )
    metadata.metadata_hash = metadata.compute_hash()
    return metadata


AdapterFunc = Any
_ADAPTERS: Dict[str, AdapterFunc] = {}


def register_adapter(source: str, adapter: AdapterFunc) -> None:
    """Register an adapter that converts source metadata into canonical form."""
    _ADAPTERS[source.lower()] = adapter


def canonicalize_metadata(source: str, row: Dict[str, Any]) -> CanonicalDatasetMetadata:
    """Dispatch helper to convert a row based on its source identifier."""
    source_key = source.lower()
    adapter = _ADAPTERS.get(source_key)
    if adapter is None:
        raise ValueError(f"Unsupported source '{source}' for canonicalization")
    metadata = adapter(row)
    metadata.source = source_key
    return metadata


# Register built-in adapters
register_adapter("destatis", adapt_destatis_metadata)
register_adapter("gesis", adapt_gesis_metadata)
