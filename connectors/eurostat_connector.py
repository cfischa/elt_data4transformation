"""Eurostat REST API connector implementation."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional, Tuple, Union

from .base_connector import BaseConnector, ConnectorConfig
from elt.metadata import DataSource, IngestionStatus, RawIngestion
from elt.utils.logging_config import get_logger
from elt.utils.persistence import PersistenceManager


DEFAULT_BASE_URL = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0"


class EurostatConfig(ConnectorConfig):
    """Configuration object for the Eurostat connector."""

    def __init__(self, **kwargs: Any) -> None:  # pragma: no cover - simple delegation
        super().__init__(
            base_url=kwargs.get("base_url", DEFAULT_BASE_URL),
            api_key=kwargs.get("api_key") or os.getenv("EUROSTAT_API_KEY"),
            rate_limit_requests=kwargs.get("rate_limit_requests", 80),
            rate_limit_period=kwargs.get("rate_limit_period", 60),
            timeout=kwargs.get("timeout", 60),
            max_retries=kwargs.get("max_retries", 3),
        )


class EurostatConnector(BaseConnector):
    """Async connector for the Eurostat statistics dissemination API."""

    def __init__(self, config: Optional[EurostatConfig] = None) -> None:
        self.config = config or EurostatConfig()
        super().__init__(self.config)
        self.logger = get_logger(self.__class__.__name__)
        self.persistence = PersistenceManager("./data/raw/eurostat")

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------
    def _get_auth_headers(self) -> Dict[str, str]:
        """Return headers expected by the Eurostat API."""

        headers = {
            "Accept": "application/json",
            "User-Agent": "BnB-Data4Transformation/1.0",
        }
        if self.config.api_key:
            headers["Authorization"] = f"Bearer {self.config.api_key}"
        return headers

    # ------------------------------------------------------------------
    # Metadata discovery
    # ------------------------------------------------------------------
    async def list_datasets(
        self,
        page: int = 1,
        page_size: int = 100,
        *,
        search: Optional[str] = None,
        since: Optional[datetime] = None,
        language: str = "en",
    ) -> Dict[str, Any]:
        """Return a paginated list of Eurostat datasets."""

        params: Dict[str, Any] = {
            "page": max(page, 1),
            "pageSize": max(page_size, 1),
            "lang": language,
        }
        if search:
            params["search"] = search
        if since:
            # API expects ISO-8601 without microseconds
            if since.tzinfo is None:
                since_utc = since.replace(tzinfo=timezone.utc)
            else:
                since_utc = since.astimezone(timezone.utc)
            params["lastUpdateFrom"] = since_utc.isoformat().replace("+00:00", "Z")

        payload = await self.get("datasets", params=params)
        datasets = payload.get("datasets") or payload.get("dataSets") or []

        items: List[Dict[str, Any]] = []
        for entry in datasets:
            last_update_raw = entry.get("lastUpdate") or entry.get("last_update")
            last_update: Optional[datetime] = None
            if isinstance(last_update_raw, str):
                try:
                    last_update = datetime.fromisoformat(last_update_raw.replace("Z", "+00:00"))
                except ValueError:
                    self.logger.debug("Could not parse lastUpdate '%s'", last_update_raw)

            items.append(
                {
                    "code": entry.get("code") or entry.get("id"),
                    "title": entry.get("label") or entry.get("title") or "",
                    "description": entry.get("description") or entry.get("label") or "",
                    "last_update": last_update,
                    "keywords": list(entry.get("keywords") or entry.get("tags") or []),
                    "themes": list(entry.get("themes") or entry.get("categories") or []),
                    "dimensions": list(entry.get("dimensions") or entry.get("dimensionIds") or []),
                    "values_count": entry.get("valuesCount") or entry.get("values_count"),
                    "raw": entry,
                }
            )

        return {
            "page": payload.get("page", page),
            "page_size": payload.get("pageSize", page_size),
            "total": payload.get("total"),
            "items": items,
            "links": payload.get("links", {}),
        }

    async def get_available_datasets(
        self,
        page: int = 1,
        page_size: int = 100,
        *,
        search: Optional[str] = None,
        since: Optional[datetime] = None,
        language: str = "en",
    ) -> List[Dict[str, Any]]:
        """Convenience wrapper returning only the dataset list."""

        page_payload = await self.list_datasets(
            page=page,
            page_size=page_size,
            search=search,
            since=since,
            language=language,
        )
        return page_payload["items"]

    async def get_dataset_metadata(
        self,
        dataset_id: str,
        *,
        language: str = "en",
    ) -> Dict[str, Any]:
        """Fetch rich metadata for a given dataset code."""

        params = {"lang": language}
        response = await self.get(f"datasets/{dataset_id}", params=params)
        dataset_info = response.get("dataset", response)
        dimension_info = response.get("dimension") or dataset_info.get("dimension") or {}

        last_update_raw = dataset_info.get("lastUpdate") or dataset_info.get("last_update")
        last_update: Optional[datetime] = None
        if isinstance(last_update_raw, str):
            try:
                last_update = datetime.fromisoformat(last_update_raw.replace("Z", "+00:00"))
            except ValueError:
                self.logger.debug("Could not parse dataset lastUpdate '%s'", last_update_raw)

        return {
            "id": dataset_id,
            "title": dataset_info.get("label") or dataset_info.get("title") or dataset_id,
            "description": dataset_info.get("description") or dataset_info.get("label") or "",
            "last_update": last_update,
            "keywords": list(dataset_info.get("keywords") or dataset_info.get("tags") or []),
            "themes": list(dataset_info.get("themes") or dataset_info.get("categories") or []),
            "unit": dataset_info.get("unit"),
            "contact": dataset_info.get("contact"),
            "dimension": dimension_info,
            "raw": response,
        }

    async def get_countries(self, language: str = "en") -> List[Dict[str, Any]]:
        """Retrieve the Eurostat geographic classification."""

        params = {"lang": language, "pageSize": 500}
        try:
            response = await self.get("classifications/geo", params=params)
        except Exception as exc:  # pragma: no cover - graceful degradation
            self.logger.warning("Eurostat geo classification lookup failed: %s", exc)
            return []

        items = response.get("items") or response.get("geo") or []
        countries: List[Dict[str, Any]] = []
        for entry in items:
            countries.append(
                {
                    "code": entry.get("code") or entry.get("id"),
                    "name": entry.get("label") or entry.get("name") or entry.get("description") or "",
                    "raw": entry,
                }
            )
        return countries

    # ------------------------------------------------------------------
    # Data extraction
    # ------------------------------------------------------------------
    async def fetch_dataset(
        self,
        dataset_id: str,
        *,
        filters: Optional[Dict[str, Iterable[str]]] = None,
        language: str = "en",
        time_range: Optional[Union[str, Tuple[str, str]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Fetch a dataset and expand JSON-stat payload into row records."""

        params: Dict[str, Any] = {"lang": language}

        if isinstance(time_range, tuple):
            start, end = time_range
            params["time"] = f"{start}:{end}"
        elif isinstance(time_range, str):
            params["time"] = time_range

        if filters:
            for dimension, values in filters.items():
                values_list = list(values)
                if not values_list:
                    continue
                params[f"filter[{dimension}]"] = ",".join(values_list)

        raw_data = await self.get(f"data/{dataset_id}", params=params)
        expanded_records = self._expand_dataset_values(raw_data)

        timestamp = datetime.now(timezone.utc)
        file_stem = f"{dataset_id}_{timestamp.strftime('%Y%m%dT%H%M%SZ')}"
        output_path = self.persistence.save_json(raw_data, file_stem, subfolder="datasets")

        dataset_metadata = metadata or await self.get_dataset_metadata(dataset_id, language=language)

        ingestion = RawIngestion(
            source=DataSource.EUROSTAT,
            dataset=dataset_id,
            timestamp=timestamp,
            records_count=len(expanded_records),
            file_path=str(output_path),
            status=IngestionStatus.SUCCESS,
            metadata={
                "filters": filters,
                "time_range": time_range,
                "language": language,
            },
        )
        self.logger.info("Eurostat ingestion: %s", ingestion.dict())

        return {
            "dataset_id": dataset_id,
            "fetched_at": timestamp,
            "records": expanded_records,
            "raw_path": str(output_path),
            "metadata": dataset_metadata,
            "raw": raw_data,
        }

    async def fetch_data(
        self,
        *,
        datasets: Optional[Iterable[Union[str, Dict[str, Any]]]] = None,
        language: str = "en",
        default_filters: Optional[Dict[str, Iterable[str]]] = None,
        default_time_range: Optional[Union[str, Tuple[str, str]]] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Yield expanded payloads for one or more datasets."""

        if datasets is None:
            raise ValueError("datasets parameter must be provided")

        for entry in datasets:
            if isinstance(entry, str):
                dataset_id = entry
                filters = default_filters
                time_range = default_time_range
            elif isinstance(entry, dict):
                dataset_id = str(entry.get("dataset") or entry.get("dataset_id"))
                filters = entry.get("filters") or default_filters
                time_range = entry.get("time_range") or default_time_range
            else:
                raise TypeError("Dataset entry must be a string or mapping")

            try:
                result = await self.fetch_dataset(
                    dataset_id,
                    filters=filters,
                    language=language,
                    time_range=time_range,
                )
                yield result
            except Exception as exc:  # pragma: no cover - defensive logging
                self.logger.exception("Eurostat dataset %s failed: %s", dataset_id, exc)
                timestamp = datetime.now(timezone.utc)
                failed_ingestion = RawIngestion(
                    source=DataSource.EUROSTAT,
                    dataset=dataset_id,
                    timestamp=timestamp,
                    records_count=0,
                    status=IngestionStatus.FAILED,
                    error_message=str(exc),
                    metadata={"filters": filters, "time_range": time_range, "language": language},
                )
                self.logger.error("Eurostat failed ingestion: %s", failed_ingestion.dict())
                yield {
                    "dataset_id": dataset_id,
                    "fetched_at": timestamp,
                    "error": str(exc),
                    "status": "error",
                }

    async def get_incremental_data(
        self,
        since: datetime,
        *,
        language: str = "en",
        filters: Optional[Dict[str, Iterable[str]]] = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Yield datasets updated since a given timestamp."""

        datasets = await self.get_available_datasets(since=since, language=language)
        codes = [item["code"] for item in datasets if item.get("code")]
        async for result in self.fetch_data(
            datasets=codes,
            language=language,
            default_filters=filters,
        ):
            yield result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------
    def _expand_dataset_values(self, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Expand JSON-stat values into flattened row dictionaries."""

        dimension_block = payload.get("dimension") or {}
        dimension_ids = list(dimension_block.get("id") or [])
        dimension_sizes = list(dimension_block.get("size") or [])

        if not dimension_ids or not dimension_sizes:
            # Fallback for simplified datasets
            value = payload.get("value")
            if isinstance(value, dict):
                return [{"value": v} for v in value.values()]
            if isinstance(value, list):
                return [{"value": v} for v in value]
            return []

        categories: List[List[Dict[str, str]]] = []
        for dim_id in dimension_ids:
            info = dimension_block.get(dim_id, {})
            category = info.get("category", {})
            index_map = category.get("index", {})
            label_map = category.get("label", {})
            if not index_map:
                # Fallback when only labels array is present
                values = category.get("values") or []
                categories.append(
                    [
                        {
                            "code": str(value),
                            "label": str(label_map.get(str(value), value)),
                        }
                        for value in values
                    ]
                )
                continue

            ordered = sorted(index_map.items(), key=lambda pair: pair[1])
            categories.append(
                [
                    {
                        "code": str(code),
                        "label": str(label_map.get(code, code)),
                    }
                    for code, _ in ordered
                ]
            )

        value_map = payload.get("value") or {}
        status_map = payload.get("status") or {}
        flags_map = payload.get("flags") or {}

        records: List[Dict[str, Any]] = []
        sizes = dimension_sizes
        id_count = len(dimension_ids)

        for key, value in value_map.items():
            try:
                index = int(key)
            except (ValueError, TypeError):
                self.logger.debug("Unexpected Eurostat value key: %s", key)
                continue

            coordinates = self._decode_index(index, sizes)
            if len(coordinates) != id_count:
                self.logger.debug("Coordinate mismatch for key %s", key)
                continue

            row: Dict[str, Any] = {"value": value}
            for idx, (dim_id, coordinate) in enumerate(zip(dimension_ids, coordinates)):
                choices = categories[idx] if idx < len(categories) else []
                if coordinate >= len(choices):
                    self.logger.debug(
                        "Coordinate %s out of range for dimension %s", coordinate, dim_id
                    )
                    continue
                selection = choices[coordinate]
                row[dim_id] = selection["code"]
                row[f"{dim_id}_label"] = selection["label"]

            if status_map:
                status_value = status_map.get(key)
                if status_value is not None:
                    row["status"] = status_value
            if flags_map:
                flag_value = flags_map.get(key)
                if flag_value is not None:
                    row["flags"] = flag_value

            records.append(row)

        return records

    @staticmethod
    def _decode_index(index: int, sizes: List[int]) -> List[int]:
        """Convert a flat JSON-stat index into per-dimension coordinates."""

        coordinates: List[int] = []
        remainder = index
        for size in reversed(sizes):
            if size <= 0:
                coordinates.append(0)
                continue
            coordinates.append(remainder % size)
            remainder //= size
        coordinates.reverse()
        return coordinates


__all__ = ["EurostatConfig", "EurostatConnector"]

