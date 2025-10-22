"""SOEP Monitor API connector.

Implements asynchronous access to the SOEP Monitor public API, exposing
indicator metadata and time-series observations for ingestion pipelines.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Dict, Iterable, List, Optional

from pydantic import Field

from .base_connector import BaseConnector, ConnectorConfig

USER_AGENT = "BnB-Data4Transformation/soep-connector"


class SOEPConfig(ConnectorConfig):
    """Configuration for SOEP API connector."""

    base_url: str = "https://monitor.soep.de/api/v1"
    rate_limit_requests: int = 60
    rate_limit_period: int = 60
    timeout: int = 60
    per_page: int = Field(default=100, ge=1, le=500)


class SOEPConnector(BaseConnector):
    """Async client for the SOEP Monitor API."""

    def __init__(self, config: Optional[SOEPConfig] = None) -> None:
        self.config = config or SOEPConfig()
        super().__init__(self.config)

    def _get_auth_headers(self) -> Dict[str, str]:
        """SOEP Monitor relies on anonymous JSON requests."""
        headers = {
            "Accept": "application/json",
            "User-Agent": USER_AGENT,
        }
        return headers

    async def list_indicators(
        self,
        *,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Return a list of indicator summaries from the Monitor API."""

        per_page = page_size or self.config.per_page
        page = 1
        summaries: List[Dict[str, Any]] = []

        while True:
            params = {"page": page, "per_page": per_page}
            try:
                response = await self.get("indicators", params=params)
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.error(
                    "SOEP list_indicators failed on page %s: %s", page, exc
                )
                break

            data = response.get("data") if isinstance(response, dict) else None
            if not data:
                self.logger.debug("SOEP list_indicators page %s returned no data", page)
                break

            if not isinstance(data, list):
                self.logger.warning(
                    "Unexpected indicators payload type: %s", type(data)
                )
                break

            summaries.extend(data)

            meta = response.get("meta") or {}
            total_pages = int(meta.get("total_pages") or 0)

            if max_pages is not None and page >= max_pages:
                break
            if total_pages and page >= total_pages:
                break

            links = response.get("links") or {}
            if not links.get("next"):
                break

            page += 1

        return summaries

    async def get_indicator_detail(self, slug: str) -> Dict[str, Any]:
        """Fetch detailed metadata for a single indicator."""

        try:
            response = await self.get(f"indicators/{slug}")
        except Exception as exc:  # pragma: no cover - defensive
            self.logger.error("Failed to fetch indicator detail for %s: %s", slug, exc)
            raise

        if isinstance(response, dict) and "data" in response:
            payload = response.get("data")
            if isinstance(payload, dict):
                return payload
        if isinstance(response, dict):
            return response
        self.logger.warning(
            "Indicator detail payload for %s not dict: %s", slug, type(response)
        )
        return {}

    async def fetch_metadata(
        self,
        *,
        page_size: Optional[int] = None,
        max_pages: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Collect rich metadata for all indicators."""

        summaries = await self.list_indicators(page_size=page_size, max_pages=max_pages)
        metadata: List[Dict[str, Any]] = []

        for summary in summaries:
            slug = summary.get("slug")
            if not slug:
                self.logger.debug("Skipping indicator without slug: %s", summary)
                continue

            try:
                detail = await self.get_indicator_detail(slug)
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.warning(
                    "Continuing after detail fetch failure for %s: %s", slug, exc
                )
                detail = {}

            metadata.append(self._merge_summary_and_detail(summary, detail))

            await asyncio.sleep(0)  # allow cooperative scheduling

        return metadata

    async def fetch_data(
        self,
        *,
        datasets: Optional[Iterable[str]] = None,
        indicator_slug: Optional[str] = None,
        **filters: Any,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Yield observation payloads for requested indicators."""

        slugs = set()
        if indicator_slug:
            slugs.add(indicator_slug)
        if datasets:
            slugs.update(str(item) for item in datasets if item)

        if not slugs:
            raise ValueError("At least one indicator slug must be provided")

        for slug in slugs:
            observations: List[Dict[str, Any]] = []
            async for record in self.stream_observations(slug, **filters):
                observations.append(record)

            yield {
                "source": "soep",
                "dataset": slug,
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "observations": observations,
                "filters": filters,
            }

    async def get_incremental_data(
        self,
        since: datetime,
        *,
        datasets: Optional[Iterable[str]] = None,
        indicator_slug: Optional[str] = None,
        **filters: Any,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Yield observations filtered to years >= ``since``."""

        async for payload in self.fetch_data(
            datasets=datasets,
            indicator_slug=indicator_slug,
            **filters,
        ):
            observations = payload.get("observations", [])
            filtered = [
                obs for obs in observations if self._observation_is_recent(obs, since)
            ]
            if filtered:
                yield {
                    **payload,
                    "observations": filtered,
                }

    async def stream_observations(
        self,
        slug: str,
        *,
        per_page: Optional[int] = None,
        **filters: Any,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Stream paginated observations for an indicator."""

        page = 1
        page_size = per_page or self.config.per_page

        while True:
            params = {"page": page, "per_page": page_size, **filters}
            try:
                response = await self.get(
                    f"indicators/{slug}/observations", params=params
                )
            except Exception as exc:  # pragma: no cover - defensive
                self.logger.error(
                    "Observation fetch failed for %s page %s: %s", slug, page, exc
                )
                break

            rows = response.get("data") if isinstance(response, dict) else None
            if not rows:
                break
            if not isinstance(rows, list):
                self.logger.warning(
                    "Unexpected observations payload type for %s: %s", slug, type(rows)
                )
                break

            for row in rows:
                yield row

            meta = response.get("meta") or {}
            total_pages = int(meta.get("total_pages") or 0)
            if total_pages and page >= total_pages:
                break

            links = response.get("links") or {}
            if not links.get("next"):
                break

            page += 1

    def _merge_summary_and_detail(
        self, summary: Dict[str, Any], detail: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Combine list response and detail payload into a single record."""

        topics = self._normalise_topics(summary, detail)
        dimensions = self._normalise_dimensions(summary, detail)
        unit = self._resolve_unit(summary, detail)

        metadata = {
            "id": summary.get("id") or detail.get("id"),
            "slug": summary.get("slug") or detail.get("slug"),
            "title": detail.get("title") or summary.get("title"),
            "subtitle": detail.get("subtitle") or summary.get("subtitle"),
            "short_description": summary.get("short_description")
            or detail.get("short_description"),
            "long_description_html": detail.get("long_description_html"),
            "topics": topics,
            "dimensions": dimensions,
            "unit": unit,
            "source": detail.get("source") or summary.get("source"),
            "methodology": detail.get("methodology"),
            "available_years": detail.get("available_years") or [],
            "last_updated_at": detail.get("last_updated_at")
            or summary.get("last_updated_at"),
            "raw_summary": summary,
            "raw_detail": detail,
        }
        return metadata

    def _normalise_topics(
        self, summary: Dict[str, Any], detail: Dict[str, Any]
    ) -> List[str]:
        """Extract list of topic titles."""

        topics_field = detail.get("topics") or summary.get("topics") or []
        if isinstance(topics_field, list):
            titles = []
            for item in topics_field:
                if isinstance(item, dict):
                    title = item.get("title") or item.get("name")
                    if title:
                        titles.append(str(title))
                elif item:
                    titles.append(str(item))
            return titles
        if isinstance(topics_field, dict):
            return [str(value) for value in topics_field.values() if value]
        if topics_field:
            return [str(topics_field)]
        return []

    def _normalise_dimensions(
        self, summary: Dict[str, Any], detail: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Return consistent dimension metadata."""

        detail_dims = detail.get("dimensions")
        if isinstance(detail_dims, list):
            result: Dict[str, Any] = {}
            for dim in detail_dims:
                if not isinstance(dim, dict):
                    continue
                key = dim.get("dimension") or dim.get("name")
                categories = dim.get("categories")
                if key:
                    result[str(key)] = categories
            if result:
                return result

        summary_dims = summary.get("dimensions")
        if isinstance(summary_dims, dict):
            return summary_dims
        return {}

    def _resolve_unit(
        self, summary: Dict[str, Any], detail: Dict[str, Any]
    ) -> Optional[str]:
        """Return a printable unit description."""

        detail_unit = detail.get("unit")
        if isinstance(detail_unit, dict):
            label = detail_unit.get("label") or detail_unit.get("name")
            description = detail_unit.get("description")
            if label and description:
                return f"{label} ({description})"
            if label:
                return str(label)
        if isinstance(detail_unit, str):
            return detail_unit

        summary_unit = summary.get("unit")
        return str(summary_unit) if summary_unit else None

    def _observation_is_recent(
        self, observation: Dict[str, Any], since: datetime
    ) -> bool:
        """Return True when the observation year is >= ``since`` year."""

        year_value = observation.get("year")
        if year_value is None:
            return False
        try:
            year_int = int(year_value)
        except (TypeError, ValueError):
            self.logger.debug("Observation year not numeric: %s", year_value)
            return False
        return year_int >= since.year


async def _demo() -> None:  # pragma: no cover - manual exploration helper
    async with SOEPConnector() as connector:
        metadata = await connector.fetch_metadata(max_pages=1)
        print(f"Fetched {len(metadata)} metadata entries")
        if metadata:
            first = metadata[0]["slug"]
            print("Streaming observations for", first)
            async for obs in connector.stream_observations(first, per_page=50):
                print(obs)
                break


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(_demo())
