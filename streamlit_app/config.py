from dataclasses import dataclass
import os


@dataclass(frozen=True)
class ClickHouseConfig:
    host: str
    port: int
    user: str | None
    password: str | None
    database: str

    @classmethod
    def from_env(cls) -> "ClickHouseConfig":
        return cls(
            host=os.getenv("CLICKHOUSE_HOST", "localhost"),
            port=int(os.getenv("CLICKHOUSE_HTTP_PORT", "8123")),
            user=os.getenv("CLICKHOUSE_USER") or None,
            password=os.getenv("CLICKHOUSE_PASSWORD") or None,
            database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        )
