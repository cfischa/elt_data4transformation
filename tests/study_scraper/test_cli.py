"""CLI smoke tests."""

from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from study_scraper import __version__
from study_scraper.cli import app


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
    # SSOAR is the only source wired up in Phase 4; OpenAlex / Bundestag
    # come later. An unknown source must error cleanly.
    result = runner.invoke(
        app, ["run", "--source", "openalex", "--topic", "klima"]
    )
    assert result.exit_code != 0
    assert "unknown source" in (result.output or "").lower()


def test_python_dash_m_entrypoint_resolves() -> None:
    # The __main__.py module must import without side effects.
    import study_scraper.__main__  # noqa: F401

    assert Path(study_scraper.__main__.__file__).name == "__main__.py"
