"""Command-line interface for the study scraper.

Phase 4 surface: real source dispatch (SSOAR), DB persistence, listing.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import List, Optional

import typer

from study_scraper import __version__
from study_scraper.config import get_settings
from study_scraper.discovery.openalex import OpenAlexSource
from study_scraper.discovery.ssoar import SSOARSource
from study_scraper.ingest import run_lake_ingest
from study_scraper.sources.dawum import DAWUMSource
from study_scraper.sources.eurostat import EurostatSource
from study_scraper.sources.gesis import GESISSource
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
def status(
    as_json: bool = typer.Option(
        False, "--json", help="Emit machine-readable JSON (for cron / CI)."
    ),
) -> None:
    """Print a coverage / health overview to stdout (cron-friendly)."""
    import dataclasses
    import json as _json

    from study_scraper.status import build_status, format_text

    storage = _storage_from_settings()
    report = build_status(storage)
    if as_json:
        payload = dataclasses.asdict(report)
        typer.echo(_json.dumps(payload, default=str, ensure_ascii=False))
    else:
        typer.echo(format_text(report))


@app.command()
def fulltext(
    limit: int = typer.Option(
        20, "--limit", help="Max studies to fetch documents for."
    ),
    study_id: Optional[str] = typer.Option(
        None, "--study-id", help="Process exactly one study by id."
    ),
    refetch: bool = typer.Option(
        False, "--refetch",
        help="Also re-process studies that already have an artifact "
             "(use after extractor upgrades).",
    ),
) -> None:
    """Fetch full documents (PDF/HTML) for kept studies and extract
    statistics from the FULL text (extractor regex-v2, A20).

    Network required — run from a machine with outbound HTTP. Claims
    land in the same `claims` table as the abstract pass; studies that
    still yield no claims appear on the `reading-list`.
    """
    from study_scraper.fulltext import run_fulltext

    storage = _storage_from_settings()
    results = run_fulltext(
        storage=storage, limit=limit, study_id=study_id, refetch=refetch,
    )
    if not results:
        typer.echo("(nothing queued — all kept studies have artifacts)")
        return
    ok = sum(1 for r in results if r["status"] == "ok")
    claims = sum(r.get("claims", 0) for r in results)
    for r in results:
        typer.echo(
            f"{r['study_id'][:12]}  {r['status']:<24} "
            f"claims={r.get('claims', 0):>3}  {r.get('canonical_url', '')}"
        )
    typer.echo(f"-- {ok}/{len(results)} ok, {claims} full-text claims extracted")


@app.command("reading-list")
def reading_list(
    limit: int = typer.Option(25, "--limit"),
) -> None:
    """Kept studies with NO extracted claims — the human-reading track
    of the A20 hybrid model (quantitative stats + studies to read)."""
    storage = _storage_from_settings()
    rows = storage.query_view("reading_list", limit=limit)
    if not rows:
        typer.echo("(reading list empty — every kept study has claims)")
        return
    for row in rows:
        title = (row.get("title") or "").replace("\n", " ")
        if len(title) > 70:
            title = title[:67] + "..."
        typer.echo(
            f"{row['id'][:12]}  [{row['reason']:<11}]  {title}"
        )
        typer.echo(f"{'':14}  {row['canonical_url']}")


@app.command()
def follow(
    fetch: bool = typer.Option(
        False, "--fetch",
        help="Actually fetch pending works via the OpenAlex API "
             "(network required). Without it: dry-run listing.",
    ),
    topic: Optional[str] = typer.Option(
        None, "--topic",
        help="Topic to filter fetched works against (required with --fetch).",
    ),
    limit: int = typer.Option(100, "--limit"),
) -> None:
    """Reference follower (Phase 5d): walk the citation graph captured
    from OpenAlex and ingest referenced works we don't have yet."""
    from study_scraper.follow import fetch_references, pending_references

    storage = _storage_from_settings()
    if not fetch:
        ids = pending_references(storage, limit=limit)
        if not ids:
            typer.echo("(no pending references)")
            return
        for work_id in ids:
            typer.echo(work_id)
        typer.echo(f"-- {len(ids)} pending; run with --fetch --topic <id> to ingest")
        return

    if not topic:
        raise typer.BadParameter("--fetch requires --topic <id>")
    topic_obj = _load_topic(topic)
    runs = fetch_references(
        storage=storage, topic=topic_obj, limit=limit,
    )
    seen = sum(r.candidates_seen for r in runs)
    kept = sum(r.candidates_kept for r in runs)
    typer.echo(f"follower: {len(runs)} batch(es), seen={seen} kept={kept}")


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


@app.command()
def ask(
    query: str = typer.Argument(
        ..., help="Keyword(s) to look for inside attribution questions."
    ),
    limit: int = typer.Option(20, "--limit"),
    dedup: bool = typer.Option(
        True, "--dedup/--no-dedup",
        help="Collapse the same finding across studies to one "
             "confidence-weighted representative (default on).",
    ),
    since: Optional[int] = typer.Option(
        None, "--since",
        help="Only findings from studies published in this year or "
             "later (undated studies are excluded).",
    ),
) -> None:
    """Query structured attributions (question, position, %) — the
    llm-v1 answer layer (A21). Richer than `search`: each hit names the
    question, the stance, and the figure. By default duplicates of the
    same finding are merged; pass --no-dedup to see every raw row.
    """
    storage = _storage_from_settings()
    rows = (
        storage.search_attributions_deduped(query=query, limit=limit, since=since)
        if dedup
        else storage.search_attributions(query=query, limit=limit, since=since)
    )
    if not rows:
        typer.echo("(no attributions matched — run `attribute` first?)")
        return
    for row in rows:
        pct = row.get("percentage")
        pct_str = f"{float(pct):>5.1f}%" if pct is not None else "   — "
        pos = (row.get("position") or "").ljust(11)
        q = (row.get("question") or "").replace("\n", " ")
        dup = row.get("dup_count") or 1
        if dup > 1:
            q = f"{q}  (+{dup - 1} more)"
        pub = row.get("publication_date")
        ctx_bits = []
        if pub:
            ctx_bits.append(str(pub.year))
        n = row.get("sample_size")
        if n is not None:
            ctx_bits.append(f"n={int(n)}")
        ctx = f" [{', '.join(ctx_bits)}]" if ctx_bits else ""
        typer.echo(f"{pct_str}  {pos}  {q}{ctx}")
        title = (row.get("title") or "").replace("\n", " ")
        if len(title) > 70:
            title = title[:67] + "..."
        typer.echo(f"          [{row.get('source_id')}] {title}")
        typer.echo(f"          {row.get('canonical_url')}")


@app.command()
def answer(
    query: str = typer.Argument(
        ..., help="Keyword(s) matched against attribution questions."
    ),
    since: Optional[int] = typer.Option(
        None, "--since",
        help="Only findings from studies published in this year or later.",
    ),
    limit: int = typer.Option(
        10, "--limit", help="Max question clusters to print."
    ),
) -> None:
    """Poll-of-polls answer (ROADMAP D): aggregate matching findings
    across institutes into a recency- and sample-size-weighted average
    per question cluster, with the spread shown honestly. Where `ask`
    lists findings, `answer` synthesizes them.
    """
    from study_scraper.aggregate import aggregate_findings, format_answer

    storage = _storage_from_settings()
    rows = storage.search_attributions_deduped(
        query=query, limit=500, since=since
    )
    answers = aggregate_findings(rows)
    if not answers:
        typer.echo("(no findings with percentages matched — run `attribute` first?)")
        return
    for a in answers[:limit]:
        typer.echo(format_answer(a))
        typer.echo("")
    typer.echo(
        "method: weighted mean per question cluster; weights = recency "
        "(3y half-life) × sqrt(n/1000) clamped to [0.3, 3]."
    )


@app.command()
def attribute(
    limit: int = typer.Option(
        20, "--limit", help="Max queued studies to attribute."
    ),
    study_id: Optional[str] = typer.Option(
        None, "--study-id", help="Attribute exactly one study."
    ),
    model: Optional[str] = typer.Option(
        None, "--model",
        help="Override the LLM model (default: $STUDY_SCRAPER_LLM_MODEL "
             "or claude-opus-4-8). Use claude-haiku-4-5 for cheap bulk runs.",
    ),
) -> None:
    """LIVE Option A: run the llm-v1 extractor over the attribution
    queue via the Anthropic API. Requires ANTHROPIC_API_KEY (metered).

    For the zero-extra-cost path, use `attribute-prompts` +
    `attribute-apply` with a Cowork session instead.
    """
    from study_scraper.attribute import run_attribute

    storage = _storage_from_settings()
    results = run_attribute(
        storage=storage, limit=limit, study_id=study_id, model=model
    )
    if not results:
        typer.echo("(attribution queue empty — every kept study with claims is attributed)")
        return
    ok = sum(1 for r in results if r["status"] == "ok")
    total = sum(r.get("attributions", 0) for r in results)
    for r in results:
        typer.echo(f"{r['study_id'][:12]}  {r['status']:<22}  triples={r.get('attributions', 0)}")
    typer.echo(f"-- {ok}/{len(results)} ok, {total} attributions extracted")


@app.command("attribute-prompts")
def attribute_prompts(
    limit: int = typer.Option(20, "--limit"),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Write JSONL {study_id, prompt} here (else stdout)."
    ),
) -> None:
    """OFFLINE step 1 (zero extra API cost): dump the llm-v1 prompts for
    queued studies so a Cowork session can answer them. The shared
    system prompt is printed first, then one user prompt per study.
    """
    import json as _json

    from study_scraper.attribute import dump_prompts
    from study_scraper.extractors.llm_v1 import SYSTEM_PROMPT

    storage = _storage_from_settings()
    items = dump_prompts(storage=storage, limit=limit)
    if not items:
        typer.echo("(attribution queue empty)")
        return
    if out is not None:
        with out.open("w", encoding="utf-8") as fh:
            for item in items:
                fh.write(_json.dumps(item, ensure_ascii=False) + "\n")
        typer.echo(f"wrote {len(items)} prompt(s) to {out}")
        typer.echo("SYSTEM PROMPT (apply to every item):")
        typer.echo(SYSTEM_PROMPT)
    else:
        typer.echo("=== SYSTEM PROMPT (apply to every item) ===")
        typer.echo(SYSTEM_PROMPT)
        for item in items:
            typer.echo(f"\n=== study {item['study_id']} ===")
            typer.echo(item["prompt"])


@app.command("attribute-apply")
def attribute_apply(
    responses_file: Path = typer.Argument(
        ..., help="JSONL of {study_id, response} — model outputs to apply."
    ),
) -> None:
    """OFFLINE step 2 (zero extra API cost): apply captured model
    responses (e.g. from a Cowork session) to the attributions table.
    No API key needed — uses the pure llm-v1 parser.
    """
    import json as _json

    from study_scraper.attribute import apply_responses

    storage = _storage_from_settings()
    responses: dict = {}
    with responses_file.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            obj = _json.loads(line)
            sid = obj.get("study_id")
            text = obj.get("response") or obj.get("text") or ""
            if sid:
                responses[sid] = text
    if not responses:
        typer.echo("(no responses found in file)")
        return
    results = apply_responses(storage=storage, responses=responses)
    total = sum(r.get("attributions", 0) for r in results)
    typer.echo(f"applied {len(results)} response(s), {total} attributions stored")


@app.command()
def ingest(
    source: str = typer.Option(..., "--source", help="Lake source id."),
    limit: Optional[int] = typer.Option(
        None, "--limit", help="Max records to ingest."
    ),
    from_file: Optional[Path] = typer.Option(
        None,
        "--from-file",
        help="Read the source response from a local file instead of "
             "hitting the live endpoint. Same parser path either way.",
    ),
    topic: Optional[str] = typer.Option(
        None, "--topic",
        help="Optional operator tag applied to every record this run "
             "captures. Lake sources don't topic-filter; this is just "
             "metadata for downstream views.",
    ),
    code: List[str] = typer.Option(
        [], "--code",
        help="Eurostat-only: dataset code(s) to fetch (repeatable). "
             "Example: --code env_air_gge --code nrg_cb_e.",
    ),
    geo: str = typer.Option(
        "DE", "--geo",
        help="Eurostat-only: country filter (default DE). Use '' to "
             "fetch all countries — WARNING: some tables are >60 MB "
             "unfiltered and will be skipped by the size guard.",
    ),
) -> None:
    """Ingest a structured-data source into the lake (`source_records`).

    The payload is captured as-is. Per-source typed projections are
    queryable via the `view` command and via SQL views (see migration
    0005 for `dawum_polls` / `dawum_poll_results`).
    """
    if source == "dawum":
        src = DAWUMSource(from_file=from_file)
    elif source == "gesis":
        src = GESISSource(from_file=from_file)
    elif source == "eurostat":
        if not from_file and not code:
            raise typer.BadParameter(
                "eurostat requires --code <dataset_code> (repeatable) "
                "or --from-file PATH."
            )
        src = EurostatSource(
            codes=code,
            from_file=from_file,
            filters={"geo": geo} if geo else {},
        )
    else:
        raise typer.BadParameter(
            f"unknown lake source {source!r}; "
            f"supported: dawum, gesis, eurostat"
        )
    storage = _storage_from_settings()
    topic_ids = [topic] if topic else None
    with src as s:
        run = run_lake_ingest(
            source=s, storage=storage, limit=limit, topic_ids=topic_ids
        )
    typer.echo(
        f"lake run {run.id}: source={run.source_id} "
        f"seen={run.candidates_seen} new={run.candidates_kept} "
        f"errors={run.errors}"
    )


@app.command()
def view(
    name: str = typer.Argument(
        ..., help="View name in the study_scraper schema (e.g. dawum_polls)."
    ),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Read N rows from a SQL view over `source_records`.

    Per-source projections are SQL views (migration 0005) -- not Python
    code. New view -> add to a future migration -> visible here.
    """
    storage = _storage_from_settings()
    try:
        rows = storage.query_view(name, limit=limit)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if not rows:
        typer.echo("(no rows)")
        return
    keys = list(rows[0].keys())
    typer.echo("  ".join(keys))
    for row in rows:
        cells = []
        for k in keys:
            v = row[k]
            if v is None:
                cells.append("—")
            else:
                cells.append(str(v))
        line = "  ".join(cells)
        if len(line) > 200:
            line = line[:197] + "..."
        typer.echo(line)


@app.command()
def export(
    out: Path = typer.Option(
        Path("dataset"), "--out",
        help="Directory for findings.csv / studies.csv / manifest.json.",
    ),
) -> None:
    """Export the findings layer as an open dataset: published toplines
    with provenance (findings.csv), study metadata (studies.csv), and a
    manifest. Facts + links only — no abstracts or full texts."""
    from study_scraper.export import run_export

    storage = _storage_from_settings()
    manifest = run_export(storage, out)
    typer.echo(
        f"wrote {out}/: {manifest['findings']} findings, "
        f"{manifest['studies']} studies"
    )


@app.command()
def dossier(
    query: str = typer.Argument(
        ..., help="The policy question, as keyword(s) matched against "
                  "attribution questions."
    ),
    since: Optional[int] = typer.Option(
        None, "--since", help="Only findings from this year or later."
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Write the Markdown dossier here (else stdout)."
    ),
) -> None:
    """Research dossier: one policy question rendered as a citable
    Markdown report — poll-of-polls summary, every finding with year /
    n / population / institute, methodology caveats, and a provenance
    appendix. What a journalist or NGO would assemble by hand."""
    from study_scraper.dossier import build_dossier

    storage = _storage_from_settings()
    text = build_dossier(storage, query, since=since)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        typer.echo(f"wrote {out}")
    else:
        typer.echo(text)


@app.command()
def gaps(
    topic: Optional[str] = typer.Option(
        None, "--topic", help="Limit to one topic id (default: all attributed)."
    ),
    out: Optional[Path] = typer.Option(
        None, "--out", help="Write the Markdown report here (else stdout)."
    ),
) -> None:
    """Evidence-gap report: per topic, which question clusters have
    data, how fresh and how broadly sourced — and where the holes are
    (stale, single-source, no percentages). The coverage-first store's
    unique product: it shows what we DON'T know."""
    from study_scraper.dossier import build_gap_report

    storage = _storage_from_settings()
    text = build_gap_report(storage, topic=topic)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        typer.echo(f"wrote {out}")
    else:
        typer.echo(text)


watch_app = typer.Typer(
    help="Standing questions for the monitoring digest (product Axis 1).",
    no_args_is_help=True,
)
app.add_typer(watch_app, name="watch")


@watch_app.command("add")
def watch_add(
    query: str = typer.Argument(
        ..., help="Keyword(s) matched against attribution questions."
    ),
    label: Optional[str] = typer.Option(
        None, "--label", help="Human-readable heading for the digest."
    ),
    since: Optional[int] = typer.Option(
        None, "--since", help="Only findings from this year or later."
    ),
) -> None:
    """Register a standing question. Each `digest` run aggregates its
    findings and reports shifts/novelties against the previous run."""
    storage = _storage_from_settings()
    watch_id = storage.add_watch(query=query, label=label, since_year=since)
    typer.echo(f"watch {watch_id} active: {label or query!r}")


@watch_app.command("list")
def watch_list(
    all_: bool = typer.Option(
        False, "--all", help="Include deactivated watches."
    ),
) -> None:
    """List watches."""
    storage = _storage_from_settings()
    rows = storage.list_watches(active_only=not all_)
    if not rows:
        typer.echo("(no watches — add one with `watch add <query>`)")
        return
    for row in rows:
        state = "active" if row["active"] else "off"
        since = f" since={row['since_year']}" if row.get("since_year") else ""
        label = f"  — {row['label']}" if row.get("label") else ""
        typer.echo(f"{row['id']:>3}  [{state:<6}] {row['query']!r}{since}{label}")


@watch_app.command("rm")
def watch_rm(watch_id: int = typer.Argument(...)) -> None:
    """Deactivate a watch (snapshot history is kept)."""
    storage = _storage_from_settings()
    changed = storage.remove_watch(watch_id)
    typer.echo("deactivated" if changed else "no change (unknown or already off)")


@app.command()
def digest(
    out: Optional[Path] = typer.Option(
        None, "--out", help="Write the Markdown digest here (else stdout)."
    ),
    dry_run: bool = typer.Option(
        False, "--dry-run",
        help="Compute without saving snapshots (repeatable preview).",
    ),
) -> None:
    """Run the monitoring digest over all active watches: aggregate each
    standing question, report shifts (≥5 pts) and newly tracked
    questions vs the previous digest, store the new snapshot. Designed
    to run after each scheduled crawl."""
    from study_scraper.digest import format_digest_markdown, run_digest

    storage = _storage_from_settings()
    digests = run_digest(storage, save=not dry_run)
    text = format_digest_markdown(digests)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(text, encoding="utf-8")
        news = sum(1 for d in digests if d.has_news)
        typer.echo(
            f"wrote {out} — {len(digests)} watch(es), {news} with news"
        )
    else:
        typer.echo(text)


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


@app.command("eval")
def eval_cmd(
    out: Optional[Path] = typer.Option(
        Path("docs/study_scraper/eval/accuracy.md"), "--out",
        help="Where to write the markdown report.",
    ),
) -> None:
    """Run the offline accuracy harness over study_scraper/eval/gold/ and
    write a report. No DB / API needed — scores topic filter, claims, and
    the attribution parser against gold files. Replace the sample gold with
    a curated set for a meaningful number (see docs/study_scraper/ACCURACY.md)."""
    from study_scraper.eval.harness import format_report, run_all

    results = run_all()
    report = format_report(results)
    typer.echo(report)
    if out is not None:
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(report + "\n", encoding="utf-8")
        typer.echo(f"\nwrote {out}")


@app.command("audit")
def audit(
    sample_size: int = typer.Option(
        20, "--sample-size", help="How many random findings to show."
    ),
) -> None:
    """Accuracy spot-check: print N RANDOM stored attributions with their
    evidence (source_span, grounded flag, distribution check) and source
    link, one block each, so a human can mark pass/fail in minutes.
    Read-only. (ACCURACY.md measurement C.)"""
    storage = _storage_from_settings()
    rows = storage.sample_attributions(limit=sample_size)
    if not rows:
        typer.echo("(no attributions stored yet — run `attribute` first)")
        return
    for i, row in enumerate(rows, start=1):
        raw = row.get("raw") or {}
        pct = row.get("percentage")
        pct_str = f"{float(pct):.1f}%" if pct is not None else "—"
        grounded = raw.get("grounded")
        grounded_str = {True: "✓ grounded", False: "✗ UNGROUNDED"}.get(
            grounded, "— unchecked"
        )
        dist = raw.get("distribution_check")
        dist_str = "  ⚠ distribution>120%" if dist is False else ""
        conf = row.get("confidence")
        conf_str = f"{float(conf):.2f}" if conf is not None else "—"
        typer.echo(f"[{i}] {pct_str}  {row.get('position')}  {row.get('question')}")
        typer.echo(f"    evidence : {raw.get('source_span') or '(no span recorded)'}")
        typer.echo(f"    checks   : {grounded_str}  conf={conf_str}{dist_str}")
        if row.get("population"):
            typer.echo(f"    population: {row['population']}")
        typer.echo(f"    source   : [{row.get('source_id')}] {row.get('title')}")
        typer.echo(f"               {row.get('canonical_url')}")
        typer.echo("")
    typer.echo(
        f"reviewed 0/{len(rows)} — mark each pass/fail; "
        "ungrounded findings are the priority queue."
    )


@review_app.command("auto")
def review_auto(
    topic: Optional[str] = typer.Option(None, "--topic"),
    limit: int = typer.Option(100, "--limit"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Preview, don't write."),
) -> None:
    """Auto-triage the pending queue (zero-touch loop): promote/reject every
    pending study, coverage-first. Stamps reviewed_by='auto' + a rationale."""
    from study_scraper.auto_review import run_auto_review

    storage = _storage_from_settings()
    results = run_auto_review(
        storage=storage, limit=limit, topic=topic, dry_run=dry_run,
    )
    if not results:
        typer.echo("(nothing pending)")
        return
    kept = sum(1 for r in results if r["decision"] == "kept")
    rejected = sum(1 for r in results if r["decision"] == "rejected")
    prefix = "[dry-run] " if dry_run else ""
    for r in results:
        typer.echo(f"{prefix}{r['study_id'][:12]}  {r['decision']:<8}  {r['rationale']}")
    typer.echo(f"{prefix}auto-review: {kept} kept, {rejected} rejected")


if __name__ == "__main__":
    app()
