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
        json_encoders = {datetime: lambda dt: dt.isoformat()}

    def compute_hash(self) -> str:
        """Compute a stable hash based on the canonical payload."""
        payload = {
            "source": self.source,
            "dataset_id": self.dataset_id,
            "title": self.title,
            "description": self.description,
            "keywords": sorted(set(self.keywords)),
            "variables": sorted(set(self.variables)),
            "latest_update": (
                self.latest_update.isoformat() if self.latest_update else None
            ),
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
        return [
            part.strip()
            for part in stripped.replace(";", ",").split(",")
            if part.strip()
        ]
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
            logger.debug(
                "Could not parse latest_update %s for %s",
                latest_update,
                row.get("cube_code"),
            )
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
            logger.debug(
                "raw_data is not JSON for resource_id=%s", row.get("resource_id")
            )

    title = str(row.get("title") or raw_payload.get("title") or "").strip()
    if not title:
        title = f"GESIS Dataset {row.get('resource_id', 'unknown')}"

    description = str(row.get("description") or raw_payload.get("description") or title)

    variables = _clean_list_values(row.get("variables") or raw_payload.get("variables"))
    keywords = _clean_list_values(
        raw_payload.get("keywords") or raw_payload.get("tags")
    )
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


def adapt_eurostat_metadata(row: Dict[str, Any]) -> CanonicalDatasetMetadata:
    """Convert Eurostat metadata rows into the canonical representation."""

    raw_metadata = row.get("raw_metadata") or row.get("raw") or {}
    parsed_raw: Dict[str, Any] = {}

    if isinstance(raw_metadata, str) and raw_metadata:
        try:
            parsed_raw = json.loads(raw_metadata)
        except json.JSONDecodeError:
            logger.debug(
                "raw_metadata is not JSON for dataset_code=%s", row.get("dataset_code")
            )
            parsed_raw = {}
    elif isinstance(raw_metadata, dict):
        parsed_raw = raw_metadata

    dataset_block = (
        parsed_raw.get("dataset")
        if isinstance(parsed_raw.get("dataset"), dict)
        else parsed_raw
    )
    dimension_block = (
        parsed_raw.get("dimension")
        or (dataset_block.get("dimension") if isinstance(dataset_block, dict) else {})
    )

    dataset_code = (
        row.get("dataset_code")
        or row.get("code")
        or (dataset_block.get("code") if isinstance(dataset_block, dict) else None)
        or row.get("dataset_id")
        or ""
    )

    title = str(
        row.get("title")
        or (dataset_block.get("label") if isinstance(dataset_block, dict) else "")
        or (dataset_block.get("title") if isinstance(dataset_block, dict) else "")
        or dataset_code
    )

    description = str(
        row.get("description")
        or (
            dataset_block.get("description")
            if isinstance(dataset_block, dict)
            else ""
        )
        or title
    )

    def _parse_last_update(value: Any) -> Optional[datetime]:
        if isinstance(value, datetime):
            return value
        if isinstance(value, str) and value:
            cleaned = value.replace("Z", "+00:00")
            try:
                return datetime.fromisoformat(cleaned)
            except ValueError:
                for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
                    try:
                        return datetime.strptime(value[: len(fmt)], fmt)
                    except ValueError:
                        continue
        return None

    last_update = _parse_last_update(
        row.get("last_update")
        or (
            dataset_block.get("lastUpdate")
            if isinstance(dataset_block, dict)
            else None
        )
        or (
            dataset_block.get("last_update")
            if isinstance(dataset_block, dict)
            else None
        )
    )

    keywords = _clean_list_values(
        row.get("keywords")
        or (
            dataset_block.get("keywords") if isinstance(dataset_block, dict) else None
        )
        or (dataset_block.get("tags") if isinstance(dataset_block, dict) else None)
    )

    themes = _clean_list_values(
        row.get("themes")
        or (
            dataset_block.get("themes") if isinstance(dataset_block, dict) else None
        )
        or (
            dataset_block.get("categories")
            if isinstance(dataset_block, dict)
            else None
        )
    )

    # Preserve order while removing duplicates
    combined_keywords: List[str] = []
    for value in keywords + themes:
        if value and value not in combined_keywords:
            combined_keywords.append(value)

    dimension_ids: List[str] = []
    if isinstance(dimension_block, dict):
        ids = dimension_block.get("id")
        if isinstance(ids, list):
            dimension_ids = [str(item) for item in ids if item]
    elif isinstance(dimension_block, list):
        dimension_ids = [str(item) for item in dimension_block if item]

    fallback_dimensions = row.get("dimensions")
    if not dimension_ids and isinstance(fallback_dimensions, list):
        dimension_ids = [str(item) for item in fallback_dimensions if item]

    variables: List[str] = []
    if isinstance(dimension_block, dict):
        for dim_id in dimension_ids:
            meta = dimension_block.get(dim_id, {})
            label = meta.get("label")
            if label:
                variables.append(f"{dim_id}:{label}")
            else:
                variables.append(dim_id)
    else:
        variables = dimension_ids

    metadata = CanonicalDatasetMetadata(
        source="eurostat",
        dataset_id=str(dataset_code),
        title=title,
        description=description,
        keywords=[kw for kw in combined_keywords if kw],
        variables=[var for var in variables if var],
        latest_update=last_update,
        raw_metadata=parsed_raw or row,
    )
    metadata.metadata_hash = metadata.compute_hash()
    return metadata


def adapt_soep_metadata(row: Dict[str, Any]) -> CanonicalDatasetMetadata:
    """Convert a SOEP monitor metadata row into canonical form."""

    raw_payload: Dict[str, Any] = {}
    for candidate in (row.get("raw_detail"), row.get("raw_summary")):
        if isinstance(candidate, dict):
            raw_payload = candidate
            break
        if isinstance(candidate, str) and candidate.strip():
            try:
                raw_payload = json.loads(candidate)
                break
            except json.JSONDecodeError:
                logger.debug(
                    "SOEP raw payload is not JSON for slug=%s", row.get("slug")
                )

    slug = str(row.get("slug") or raw_payload.get("slug") or "").strip()

    title = str(row.get("title") or raw_payload.get("title") or slug).strip()
    if not title:
        title = f"SOEP Indicator {slug or 'unknown'}"

    description_value = (
        row.get("description")
        or raw_payload.get("long_description_html")
        or raw_payload.get("short_description")
        or title
    )
    description = str(description_value)

    topics_field = row.get("topics") or raw_payload.get("topics") or []
    keywords: List[str] = []
    if isinstance(topics_field, list):
        for entry in topics_field:
            if isinstance(entry, dict):
                label = entry.get("title") or entry.get("label") or entry.get("id")
                if label:
                    keywords.append(str(label))
            elif entry:
                keywords.append(str(entry))
    elif isinstance(topics_field, str):
        keywords.extend(_clean_list_values(topics_field))

    dimensions_field = row.get("dimensions") or raw_payload.get("dimensions") or {}
    dimensions: Dict[str, Any] = {}
    if isinstance(dimensions_field, dict):
        dimensions = dimensions_field
    elif isinstance(dimensions_field, list):
        for entry in dimensions_field:
            if isinstance(entry, dict):
                key = entry.get("dimension") or entry.get("name")
                if key:
                    dimensions[str(key)] = entry.get("categories")
    elif isinstance(dimensions_field, str):
        try:
            parsed = json.loads(dimensions_field)
            if isinstance(parsed, dict):
                dimensions = parsed
            elif isinstance(parsed, list):
                for entry in parsed:
                    if isinstance(entry, dict):
                        key = entry.get("dimension") or entry.get("name")
                        if key:
                            dimensions[str(key)] = entry.get("categories")
        except json.JSONDecodeError:
            logger.debug("SOEP dimensions not JSON for slug=%s", slug)

    variables: List[str] = []
    for dim_name, categories in dimensions.items():
        dim_label = str(dim_name).strip()
        if not dim_label:
            continue
        variables.append(dim_label)
        if isinstance(categories, list):
            for cat in categories:
                if isinstance(cat, dict):
                    label = cat.get("label") or cat.get("value")
                    if label:
                        variables.append(f"{dim_label}:{label}")
                elif cat:
                    variables.append(f"{dim_label}:{cat}")

    latest_update: Optional[datetime] = None
    last_updated = row.get("last_updated_at") or raw_payload.get("last_updated_at")
    if isinstance(last_updated, str) and last_updated:
        cleaned = last_updated.strip()
        if cleaned.endswith("Z"):
            cleaned_iso = cleaned[:-1] + "+00:00"
        else:
            cleaned_iso = cleaned
        try:
            latest_update = datetime.fromisoformat(cleaned_iso)
            if latest_update.tzinfo is not None:
                latest_update = latest_update.replace(tzinfo=None)
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%Y-%m"):
                try:
                    latest_update = datetime.strptime(cleaned[: len(fmt)], fmt)
                    break
                except ValueError:
                    continue

    metadata = CanonicalDatasetMetadata(
        source="soep",
        dataset_id=slug,
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
register_adapter("eurostat", adapt_eurostat_metadata)
register_adapter("soep", adapt_soep_metadata)
