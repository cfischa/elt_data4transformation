"""Command-line interface for the study scraper.

Phase 4 surface: real source dispatch (SSOAR), DB persistence, listing.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import typer

from study_scraper import __version__
from study_scraper.config import get_settings
from study_scraper.discovery.ssoar import SSOARSource
from study_scraper.pipeline import run_one
from study_scraper.storage import PostgresStorage, StorageError, resolve_database_url
from study_scraper.topics import Topic, load_topics

app = typer.Typer(
    add_completion=False,
    help="Study scraper — see docs/study_scraper/ for goal and roadmap.",
    no_args_is_help=True,
)


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )


@app.callback()
def _root(
    verbose: bool = typer.Option(
        False, "--verbose", "-v", help="Enable debug logging."
    ),
) -> None:
    _setup_logging(verbose)


@app.command()
def version() -> None:
    """Print the package version."""
    typer.echo(__version__)


@app.command("topics")
def list_topics() -> None:
    """List configured topics from the topics CSV."""
    settings = get_settings()
    topics = load_topics(settings.topics_csv_path)
    if not topics:
        typer.echo("(no topics)")
        return
    for topic in topics:
        locales = ",".join(sorted(topic.locales))
        typer.echo(f"{topic.id}\t{topic.primary_name}\t[{locales}]")


def _storage_from_settings() -> PostgresStorage:
    settings = get_settings()
    try:
        url = resolve_database_url(
            postgres_url=settings.postgres_url,
            supabase_url=settings.supabase_url,
            supabase_service_key=settings.supabase_service_key,
        )
    except StorageError as exc:
        raise typer.BadParameter(str(exc)) from exc
    return PostgresStorage(url)


def _load_topic(topic_id: str) -> Topic:
    settings = get_settings()
    topics = load_topics(settings.topics_csv_path)
    by_id = {t.id: t for t in topics}
    if topic_id not in by_id:
        raise typer.BadParameter(
            f"unknown topic {topic_id!r}; known: {', '.join(sorted(by_id))}"
        )
    return by_id[topic_id]


@app.command()
def migrate() -> None:
    """Apply pending SQL migrations to the configured Postgres database."""
    storage = _storage_from_settings()
    applied = storage.migrate()
    if not applied:
        typer.echo("schema up to date; no migrations applied")
    else:
        typer.echo(f"applied {len(applied)} migration(s): {applied}")


@app.command()
def run(
    source: str = typer.Option(..., "--source", help="Discovery source id."),
    topic: str = typer.Option(..., "--topic", help="Topic id from topics.csv."),
    limit: Optional[int] = typer.Option(
        None, "--limit", help="Max candidates to process."
    ),
    from_file: Optional[Path] = typer.Option(
        None,
        "--from-file",
        help="Read OAI-PMH XML from a local file instead of hitting the live "
        "endpoint. Use this for tests, demos, and any environment where "
        "outbound network access is blocked.",
    ),
    min_score: float = typer.Option(
        0.2, "--min-score", help="Minimum stage-1 topic score to keep."
    ),
) -> None:
    """Run a discovery source against one topic and persist results."""
    if source != "ssoar":
        raise typer.BadParameter(
            f"unknown source {source!r}; supported: ssoar (Phase 4)"
        )
    topic_obj = _load_topic(topic)
    storage = _storage_from_settings()
    ctx = SSOARSource(from_file=from_file)

    with ctx as src:
        crawl_run = run_one(
            source=src,
            topic=topic_obj,
            storage=storage,
            limit=limit,
            min_score=min_score,
        )

    typer.echo(
        f"run {crawl_run.id}: seen={crawl_run.candidates_seen} "
        f"kept={crawl_run.candidates_kept} errors={crawl_run.errors}"
    )


@app.command("list")
def list_studies(
    topic: Optional[str] = typer.Option(None, "--topic"),
    source: Optional[str] = typer.Option(None, "--source"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Print persisted studies, newest first."""
    storage = _storage_from_settings()
    rows = storage.list_studies(topic_id=topic, source_id=source, limit=limit)
    if not rows:
        typer.echo("(no rows)")
        return
    for row in rows:
        topic_ids = ",".join(row.get("topic_ids") or []) or "-"
        topic_scores = row.get("topic_scores") or {}
        score_str = ",".join(
            f"{k}={v:.2f}" for k, v in topic_scores.items()
        ) or "-"
        title = (row.get("title") or "").replace("\n", " ")
        if len(title) > 90:
            title = title[:87] + "..."
        typer.echo(
            f"{row['id'][:8]}  {topic_ids:<25}  {score_str:<18}  "
            f"{row.get('language') or '--'}  {title}"
        )
        typer.echo(f"{'':10}  {row['canonical_url']}")


if __name__ == "__main__":
    app()
