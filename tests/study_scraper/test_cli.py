"""CLI smoke tests."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest
from typer.testing import CliRunner

from study_scraper import __version__
from study_scraper.cli import _resolve_eurostat_codes, app


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_help_exits_zero(runner: CliRunner) -> None:
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "study scraper" in result.stdout.lower()


def test_version_prints_package_version(runner: CliRunner) -> None:
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert __version__ in result.stdout


def test_topics_lists_seeded_ids(runner: CliRunner) -> None:
    result = runner.invoke(app, ["topics"])
    assert result.exit_code == 0
    for topic_id in ("steuern", "klima", "migration_einwanderung", "bildung"):
        assert topic_id in result.stdout


def test_run_rejects_unknown_topic(runner: CliRunner) -> None:
    result = runner.invoke(
        app, ["run", "--source", "ssoar", "--topic", "not-a-topic"]
    )
    assert result.exit_code != 0
    # In this Click/Typer combo BadParameter goes to stdout via runner.
    assert "unknown topic" in (result.output or "").lower()


def test_run_rejects_unknown_source(runner: CliRunner) -> None:
    # SSOAR and OpenAlex are wired up; Bundestag lands later. An
    # unknown source must error cleanly.
    result = runner.invoke(
        app, ["run", "--source", "bundestag", "--topic", "klima"]
    )
    assert result.exit_code != 0
    assert "unknown source" in (result.output or "").lower()


def test_run_sources_listed_in_help_and_error(runner: CliRunner) -> None:
    from study_scraper.cli import RUN_SOURCES

    assert "econstor" in RUN_SOURCES and "core" in RUN_SOURCES
    bad = runner.invoke(app, ["run", "--source", "nope", "--topic", "klima"])
    assert bad.exit_code != 0
    for src in RUN_SOURCES:
        assert src in bad.output
    helped = runner.invoke(app, ["run", "--help"])
    for src in RUN_SOURCES:
        assert src in helped.output


def test_run_sources_match_status_catalog_registry() -> None:
    """`run --source` choices and status' catalog registry must not drift:
    a source runnable but absent from the registry would never show in
    `never_run_sources`, and vice versa."""
    from study_scraper.cli import RUN_SOURCES
    from study_scraper.status import CATALOG_SOURCE_IDS

    assert set(RUN_SOURCES) == set(CATALOG_SOURCE_IDS)
    assert len(RUN_SOURCES) == len(set(RUN_SOURCES))


def test_python_dash_m_entrypoint_resolves() -> None:
    # The __main__.py module must import without side effects.
    import study_scraper.__main__  # noqa: F401

    assert Path(study_scraper.__main__.__file__).name == "__main__.py"


def test_cli_importable_without_bs4() -> None:
    # #163: cli.py used to eagerly import BMASSource, which imports bs4 at
    # module scope -- meaning any command (status/attribute/ask/...) crashed
    # if beautifulsoup4 wasn't installed, not just `ingest --source bmas`.
    # Simulate bs4 being absent (regardless of whether it's actually
    # installed here) via the standard sys.modules=None trick, in a
    # subprocess so it can't pollute this test run's import cache.
    script = "import sys; sys.modules['bs4'] = None; import study_scraper.cli"
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


class TestResolveEurostatCodes:
    def test_explicit_code_wins(self) -> None:
        assert _resolve_eurostat_codes(["nrg_cb_e"], None) == ["nrg_cb_e"]

    def test_falls_back_to_configured_default(self) -> None:
        assert _resolve_eurostat_codes([], None) == ["env_air_gge", "nrg_bal_s"]

    def test_from_file_bypasses_default_even_when_empty(self) -> None:
        # --from-file supplies its own codes via the fixture payload;
        # no fallback (and no error) should kick in.
        assert _resolve_eurostat_codes([], Path("fixture.json")) == []

    def test_raises_when_no_code_and_no_default(self, monkeypatch) -> None:
        from study_scraper import cli as cli_module
        from study_scraper.config import Settings

        monkeypatch.setattr(
            cli_module, "get_settings", lambda: Settings(
                _env_file=None, eurostat_default_codes=""
            )
        )
        with pytest.raises(Exception, match="eurostat requires"):
            _resolve_eurostat_codes([], None)


def test_lake_sources_match_status_lake_registry() -> None:
    """`ingest` source choices and status' lake registry must not drift
    (same guard as RUN_SOURCES vs CATALOG_SOURCE_IDS)."""
    from study_scraper.cli import LAKE_SOURCES
    from study_scraper.status import LAKE_SOURCE_IDS

    assert set(LAKE_SOURCES) == set(LAKE_SOURCE_IDS)
    assert len(LAKE_SOURCES) == len(set(LAKE_SOURCES))


def test_ingest_unknown_source_error_lists_every_lake_source(
    runner: CliRunner,
) -> None:
    from study_scraper.cli import LAKE_SOURCES

    bad = runner.invoke(app, ["ingest", "--source", "nope"])
    assert bad.exit_code != 0
    for src in LAKE_SOURCES:
        assert src in bad.output


def _discovered_source_ids(package_name: str) -> set:
    """`source_id` of every class *defined* in a module of `package_name`
    that carries a string `source_id` class attribute (the Protocol-style
    source classes; `Candidate` only has a pydantic field, so it's skipped)."""
    import importlib
    import inspect
    import pkgutil

    pkg = importlib.import_module(package_name)
    ids = set()
    for info in pkgutil.iter_modules(pkg.__path__):
        mod = importlib.import_module(f"{package_name}.{info.name}")
        for _, cls in inspect.getmembers(mod, inspect.isclass):
            if cls.__module__ != mod.__name__:
                continue
            sid = getattr(cls, "source_id", None)
            if isinstance(sid, str):
                ids.add(sid)
    return ids


def test_status_registries_match_source_classes() -> None:
    """status' CATALOG/LAKE registries must equal the `source_id` of the
    source classes found by scanning the packages -- a new source class
    not registered there would never surface in `never_run_sources`."""
    from study_scraper.status import CATALOG_SOURCE_IDS, LAKE_SOURCE_IDS

    catalog = _discovered_source_ids("study_scraper.discovery")
    lake = _discovered_source_ids("study_scraper.sources")
    # Non-vacuous: discovery must actually find sources.
    assert catalog and lake
    assert catalog == set(CATALOG_SOURCE_IDS)
    assert lake == set(LAKE_SOURCE_IDS)
