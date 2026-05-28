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
from study_scraper.discovery.openalex import OpenAlexSource
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
    if source == "ssoar":
        ctx = SSOARSource(from_file=from_file)
    elif source == "openalex":
        ctx = OpenAlexSource(from_file=from_file)
    else:
        raise typer.BadParameter(
            f"unknown source {source!r}; supported: ssoar, openalex"
        )
    topic_obj = _load_topic(topic)
    storage = _storage_from_settings()

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


@app.command()
def status() -> None:
    """Print a coverage / health overview to stdout (cron-friendly)."""
    from study_scraper.status import build_status, format_text

    storage = _storage_from_settings()
    report = build_status(storage)
    typer.echo(format_text(report))


@app.command("list")
def list_studies(
    topic: Optional[str] = typer.Option(None, "--topic"),
    source: Optional[str] = typer.Option(None, "--source"),
    status: str = typer.Option(
        "kept",
        "--status",
        help="kept (default) | pending | rejected | all",
    ),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Print persisted studies, newest first. Status defaults to kept."""
    storage = _storage_from_settings()
    status_arg = None if status == "all" else status
    if status_arg is not None and status_arg not in {"pending", "kept", "rejected"}:
        raise typer.BadParameter(
            f"--status must be one of: kept, pending, rejected, all"
        )
    rows = storage.list_studies(
        topic_id=topic, source_id=source, status=status_arg, limit=limit
    )
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


@app.command()
def search(
    query: str = typer.Argument(
        ...,
        help="Keyword(s) to look for inside extracted claim snippets.",
    ),
    unit: str = typer.Option(
        "%",
        "--unit",
        help="Claim unit to filter on. Default '%'. Pass 'all' to disable.",
    ),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Search extracted claims by keyword.

    Use this to answer questions like "what % of Germans support X?" —
    each hit shows the matched snippet, the numeric value, and a link
    back to the underlying study.

    Coverage caveat: only abstracts have been scanned; full-text PDF
    extraction is a later phase. A miss here means *we haven't ingested
    a study whose abstract contains the answer*, not necessarily that
    no such study exists.
    """
    storage = _storage_from_settings()
    unit_arg = None if unit == "all" else unit
    rows = storage.search_claims(query=query, unit=unit_arg, limit=limit)
    if not rows:
        typer.echo("(no claims matched)")
        return
    for row in rows:
        value = row.get("numeric_value")
        value_str = f"{float(value):>5.1f}{row.get('unit') or ''}" if value is not None else "  —"
        title = (row.get("title") or "").replace("\n", " ")
        if len(title) > 70:
            title = title[:67] + "..."
        typer.echo(
            f"{value_str}  [{row.get('source_id'):<8}]  {title}"
        )
        snippet = (row.get("claim_text") or "").replace("\n", " ")
        if len(snippet) > 160:
            snippet = snippet[:157] + "..."
        typer.echo(f"          \"{snippet}\"")
        typer.echo(f"          {row.get('canonical_url')}")


review_app = typer.Typer(
    help="Human review of pending candidates (Q12 review queue).",
    no_args_is_help=True,
)
app.add_typer(review_app, name="review")


@review_app.command("pending")
def review_pending(
    topic: Optional[str] = typer.Option(None, "--topic"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """List candidates awaiting human review."""
    storage = _storage_from_settings()
    rows = storage.list_studies(
        topic_id=topic, status="pending", limit=limit
    )
    if not rows:
        typer.echo("(none pending)")
        return
    for row in rows:
        score = row.get("topic_scores") or {}
        score_str = ",".join(f"{k}={v:.2f}" for k, v in score.items())
        title = (row.get("title") or "").replace("\n", " ")
        if len(title) > 80:
            title = title[:77] + "..."
        typer.echo(f"{row['id'][:12]}  {score_str:<18}  {title}")
        typer.echo(f"{'':14}  {row['canonical_url']}")


@review_app.command("promote")
def review_promote(
    study_id: str = typer.Argument(...),
    by: str = typer.Option(..., "--by", help="Reviewer name / handle."),
) -> None:
    """Promote a pending candidate to kept."""
    storage = _storage_from_settings()
    changed = storage.promote_study(study_id, reviewed_by=by)
    typer.echo("promoted" if changed else "no change (already kept or unknown id)")


@review_app.command("reject")
def review_reject(
    study_id: str = typer.Argument(...),
    by: str = typer.Option(..., "--by", help="Reviewer name / handle."),
    reason: Optional[str] = typer.Option(None, "--reason"),
) -> None:
    """Reject a candidate. Decision is sticky across re-runs."""
    storage = _storage_from_settings()
    changed = storage.reject_study(study_id, reviewed_by=by, reason=reason)
    typer.echo("rejected" if changed else "no change (already rejected or unknown id)")


if __name__ == "__main__":
    app()
