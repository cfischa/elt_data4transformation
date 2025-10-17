from __future__ import annotations

from functools import cached_property
from typing import Any

import clickhouse_connect

from .config import ClickHouseConfig


class ClickHouseClient:
    """Thin wrapper around clickhouse-connect with convenience helpers."""

    def __init__(self, cfg: ClickHouseConfig | None = None) -> None:
        self._cfg = cfg or ClickHouseConfig.from_env()

    @cached_property
    def _client(self) -> clickhouse_connect.driver.Client:
        return clickhouse_connect.get_client(
            host=self._cfg.host,
            port=self._cfg.port,
            username=self._cfg.user,
            password=self._cfg.password,
            database=self._cfg.database,
        )

    def query_df(self, sql: str, parameters: dict[str, Any] | None = None):
        return self._client.query_df(sql, parameters=parameters)

    def query(self, sql: str, parameters: dict[str, Any] | None = None):
        return self._client.query(sql, parameters=parameters)


def get_clickhouse_client() -> ClickHouseClient:
    return ClickHouseClient()
