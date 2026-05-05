"""Command-line interface for the study scraper.

Phase 2 surface: topic listing + a placeholder `run` command. Real
discovery sources land in Phase 4.
"""

from __future__ import annotations

import logging
from typing import Optional

import typer

from study_scraper import __version__
from study_scraper.config import get_settings
from study_scraper.topics import load_topics

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


@app.command()
def run(
    source: str = typer.Option(..., "--source", help="Discovery source id."),
    topic: str = typer.Option(..., "--topic", help="Topic id from topics.csv."),
    limit: Optional[int] = typer.Option(
        None, "--limit", help="Max candidates to process."
    ),
) -> None:
    """Run the scraper for one source + topic.

    Phase 2 stub: validates inputs only. Real source dispatch lands in
    Phase 4 (see docs/study_scraper/TODO.md).
    """
    settings = get_settings()
    topics = load_topics(settings.topics_csv_path)
    topic_ids = {t.id for t in topics}
    if topic not in topic_ids:
        raise typer.BadParameter(
            f"unknown topic {topic!r}; known: {', '.join(sorted(topic_ids))}"
        )
    typer.echo(
        f"[stub] would run source={source} topic={topic} limit={limit}; "
        f"discovery sources land in Phase 4."
    )


if __name__ == "__main__":
    app()
