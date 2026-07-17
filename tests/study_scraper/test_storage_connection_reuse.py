"""Unit tests for `PostgresStorage`'s connection reuse (#56).

`connection()` used to open a brand-new `psycopg.connect()` (fresh
TCP+TLS+auth round trip) on every single call -- the dominant cost
behind the ~1.2-1.4s/record ingest rate that made `dawum` (~4228
records) blow through the 600s per-command CI timeout. These tests use
a fake `psycopg.connect` (no real DB) to verify the connection is
cached and reused across calls, reconnects if dropped, and still
commits/rolls back per call the same way a fresh connection did.
"""

from __future__ import annotations

from typing import Any, List

import pytest

import study_scraper.storage.postgres as postgres_module
from study_scraper.storage import PostgresStorage


class _FakeCursor:
    def __enter__(self) -> "_FakeCursor":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None

    def execute(self, *args: Any, **kwargs: Any) -> None:
        pass

    def fetchone(self) -> None:
        return None


class _FakeConnection:
    def __init__(self) -> None:
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self) -> _FakeCursor:
        return _FakeCursor()

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        self.rollbacks += 1

    def close(self) -> None:
        self.closed = True


@pytest.fixture
def fake_connect(monkeypatch: pytest.MonkeyPatch) -> List[_FakeConnection]:
    created: List[_FakeConnection] = []

    def _connect(*args: Any, **kwargs: Any) -> _FakeConnection:
        conn = _FakeConnection()
        created.append(conn)
        return conn

    monkeypatch.setattr(postgres_module.psycopg, "connect", _connect)
    return created


def test_connection_is_reused_across_calls(
    fake_connect: List[_FakeConnection],
) -> None:
    storage = PostgresStorage("postgresql://fake/db")
    with storage.connection() as conn1:
        pass
    with storage.connection() as conn2:
        pass
    with storage.connection() as conn3:
        pass

    assert conn1 is conn2 is conn3
    assert len(fake_connect) == 1


def test_connection_commits_on_success(
    fake_connect: List[_FakeConnection],
) -> None:
    storage = PostgresStorage("postgresql://fake/db")
    with storage.connection() as conn:
        pass
    assert conn.commits == 1
    assert conn.rollbacks == 0


def test_connection_rolls_back_and_propagates_on_error(
    fake_connect: List[_FakeConnection],
) -> None:
    storage = PostgresStorage("postgresql://fake/db")
    with pytest.raises(RuntimeError):
        with storage.connection() as conn:
            raise RuntimeError("boom")
    assert conn.commits == 0
    assert conn.rollbacks == 1
    # The connection itself is still usable afterwards -- only the
    # transaction was rolled back, not the underlying connection closed.
    assert conn.closed is False


def test_dropped_connection_triggers_reconnect(
    fake_connect: List[_FakeConnection],
) -> None:
    storage = PostgresStorage("postgresql://fake/db")
    with storage.connection() as conn1:
        pass
    conn1.closed = True  # simulate the server dropping the connection
    with storage.connection() as conn2:
        pass

    assert conn1 is not conn2
    assert len(fake_connect) == 2


def test_close_clears_cached_connection(
    fake_connect: List[_FakeConnection],
) -> None:
    storage = PostgresStorage("postgresql://fake/db")
    with storage.connection() as conn1:
        pass
    storage.close()
    assert conn1.closed is True

    with storage.connection() as conn2:
        pass
    assert conn1 is not conn2
    assert len(fake_connect) == 2
