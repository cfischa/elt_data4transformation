"""
Rule-based topic classifier pipeline.

Reads canonicalised metadata (via analytics.datasets_unified view or source adapters),
applies keyword/pattern matching driven by the taxonomy configuration,
and persists classification results plus run metrics to ClickHouse.
"""

from __future__ import annotations

import argparse
import fnmatch
import json
import logging
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import yaml

from elt.adapters import CanonicalDatasetMetadata, canonicalize_metadata
from elt.loader_clickhouse import ClickHouseLoader

LOGGER = logging.getLogger("topic_classifier")
CLASSIFIER_VERSION = "rule_based_v0.1"
HIGH_SCORE_THRESHOLD = 0.5
LOW_SCORE_THRESHOLD = 0.3


@dataclass
class TopicRule:
    """Resolved matching rules for a single topic."""

    topic_id: str
    include_terms: List[str]
    synonym_terms: List[str]
    exclude_terms: List[str]
    dataset_hints: List[Tuple[str, str]]  # (source, pattern)
    description: str


def load_taxonomy(path: Path) -> Tuple[str, List[TopicRule]]:
    """Load taxonomy YAML and build topic rules."""
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    taxonomy_version = str(payload.get("version", "0.0.0"))
    topics = payload.get("topics", [])
    rules: List[TopicRule] = []

    for topic in topics:
        topic_id = topic["id"]
        include_terms: List[str] = []
        synonym_terms: List[str] = []
        exclude_terms: List[str] = []

        for entry in topic.get("include_keywords", []):
            include_terms.extend([term.lower() for term in entry.get("terms", [])])
        for entry in topic.get("synonyms", []):
            synonym_terms.extend([term.lower() for term in entry.get("terms", [])])
        for entry in topic.get("exclude_keywords", []):
            exclude_terms.extend([term.lower() for term in entry.get("terms", [])])

        dataset_hints: List[Tuple[str, str]] = []
        for hint in topic.get("dataset_hints", []):
            dataset_hints.append(
                (
                    str(hint.get("source", "")).lower(),
                    str(
                        hint.get("dataset_id")
                        or hint.get("dataset_id_pattern")
                        or ""
                    ).lower(),
                )
            )

        rules.append(
            TopicRule(
                topic_id=topic_id,
                include_terms=[term for term in include_terms if term],
                synonym_terms=[term for term in synonym_terms if term],
                exclude_terms=[term for term in exclude_terms if term],
                dataset_hints=[hint for hint in dataset_hints if hint[1]],
                description=topic.get("description", ""),
            )
        )

    return taxonomy_version, rules


def ensure_schema(loader: ClickHouseLoader) -> None:
    """Run DDL statements to make sure required tables/views exist."""
    ddl_files = [
        "sql/create_dataset_topics_table.sql",
        "sql/create_topic_classifier_runs_table.sql",
        "sql/create_topic_indicator_facts_table.sql",
        "sql/create_dataset_topics_review_table.sql",
        "sql/create_dataset_topics_excluded_table.sql",
        "sql/create_datasets_unified_view.sql",
    ]
    for ddl_path in ddl_files:
        path = Path(ddl_path)
        if not path.exists():
            LOGGER.warning("DDL file missing: %s", ddl_path)
            continue
        with path.open("r", encoding="utf-8") as handle:
            ddl = handle.read().strip()
        try:
            for statement in [stmt.strip() for stmt in ddl.split(";") if stmt.strip()]:
                loader.client.command(statement)
        except Exception as exc:  # pragma: no cover - defensive
            LOGGER.error("Failed to apply DDL from %s: %s", ddl_path, exc)
            raise


def _rows_to_dicts(column_names: Sequence[str], rows: Sequence[Sequence[Any]]) -> List[Dict[str, Any]]:
    return [dict(zip(column_names, row)) for row in rows]


def fetch_metadata_via_view(
    loader: ClickHouseLoader,
    since: Optional[str],
    sources: Optional[Iterable[str]],
    limit: Optional[int],
) -> List[CanonicalDatasetMetadata]:
    """Fetch metadata through the analytics.datasets_unified view."""
    clauses = []
    params: Dict[str, Any] = {}

    if since:
        clauses.append("(latest_update >= parseDateTimeBestEffort(%(since)s) OR latest_update IS NULL)")
        params["since"] = since
    if sources:
        clauses.append("source IN %(sources)s")
        params["sources"] = tuple(set([src.lower() for src in sources]))

    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
    limit_clause = f"LIMIT {int(limit)}" if limit else ""

    query = f"""
        SELECT
            source,
            dataset_id,
            title,
            description,
            latest_update,
            keywords,
            variables,
            raw_metadata
        FROM analytics.datasets_unified
        {where}
        ORDER BY source, dataset_id
        {limit_clause}
    """
    result = loader.client.query(query, parameters=params or None)
    metadata_objects: List[CanonicalDatasetMetadata] = []
    for row in _rows_to_dicts(result.column_names, result.result_rows):
        raw_metadata: Dict[str, Any] = {}
        raw_field = row.get("raw_metadata")
        if isinstance(raw_field, str) and raw_field:
            try:
                raw_metadata = json.loads(raw_field)
            except json.JSONDecodeError:
                raw_metadata = {"raw": raw_field}

        metadata = CanonicalDatasetMetadata(
            source=str(row.get("source")),
            dataset_id=str(row.get("dataset_id")),
            title=str(row.get("title") or ""),
            description=str(row.get("description") or ""),
            keywords=row.get("keywords") or [],
            variables=row.get("variables") or [],
            latest_update=row.get("latest_update"),
            raw_metadata=raw_metadata,
        )
        metadata.metadata_hash = metadata.compute_hash()
        metadata_objects.append(metadata)
    return metadata_objects


def fetch_metadata_with_adapters(
    loader: ClickHouseLoader,
    since: Optional[str],
    sources: Optional[Iterable[str]],
    limit: Optional[int],
) -> List[CanonicalDatasetMetadata]:
    """Fallback metadata fetch that uses source-specific adapters."""
    wanted_sources = set(src.lower() for src in sources) if sources else {"destatis", "gesis"}
    collected: List[CanonicalDatasetMetadata] = []

    if "destatis" in wanted_sources:
        clauses = []
        params: Dict[str, Any] = {}
        if since:
            clauses.append("latest_update >= parseDateTimeBestEffort(%(since)s)")
            params["since"] = since
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_clause = f"LIMIT {int(limit)}" if limit else ""
        query = f"""
            SELECT
                cube_code,
                content,
                state,
                time_coverage,
                latest_update,
                information,
                fetched_at
            FROM raw.destatis_metadata
            {where}
            ORDER BY cube_code
            {limit_clause}
        """
        result = loader.client.query(query, parameters=params or None)
        for row in _rows_to_dicts(result.column_names, result.result_rows):
            collected.append(canonicalize_metadata("destatis", row))

    if "gesis" in wanted_sources:
        clauses = []
        params = {}
        if since:
            clauses.append("tryParseDateTimeBestEffortOrNull(issued) >= parseDateTimeBestEffort(%(since)s)")
            params["since"] = since
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        limit_clause = f"LIMIT {int(limit)}" if limit else ""
        query = f"""
            SELECT
                resource_id,
                title,
                description,
                creator,
                issued,
                variables,
                raw_data
            FROM raw.gesis_metadata
            {where}
            ORDER BY resource_id
            {limit_clause}
        """
        result = loader.client.query(query, parameters=params or None)
        for row in _rows_to_dicts(result.column_names, result.result_rows):
            collected.append(canonicalize_metadata("gesis", row))

    return collected


def load_canonical_metadata(
    loader: ClickHouseLoader,
    since: Optional[str],
    sources: Optional[Iterable[str]],
    limit: Optional[int],
) -> List[CanonicalDatasetMetadata]:
    """Load metadata trying the unified view first, falling back to adapters."""
    try:
        return fetch_metadata_via_view(loader, since, sources, limit)
    except Exception as exc:
        LOGGER.warning("Falling back to source adapters because view failed: %s", exc)
        return fetch_metadata_with_adapters(loader, since, sources, limit)


def classify_dataset(
    metadata: CanonicalDatasetMetadata,
    rules: List[TopicRule],
) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]], Optional[Dict[str, Any]]]:
    """Apply rule-based matching to a single dataset.

    Returns a tuple of (assignments, review_candidate, exclusion_candidate).
    """

    text_blob = metadata.text_blob.lower()
    dataset_id_lower = metadata.dataset_id.lower()
    source_lower = metadata.source.lower()

    assignments: List[Dict[str, Any]] = []
    review_candidates: List[Dict[str, Any]] = []
    exclusion_candidates: List[Dict[str, Any]] = []

    for rule in rules:
        exclude_hits = [term for term in rule.exclude_terms if term and term in text_blob]
        if exclude_hits:
            exclusion_candidates.append(
                {
                    "topic_id": rule.topic_id,
                    "exclude_terms": exclude_hits,
                }
            )
            continue

        matched_hint_terms: List[str] = []
        matched_include_terms: List[str] = []
        matched_synonym_terms: List[str] = []

        for src, pattern in rule.dataset_hints:
            if src and src != source_lower:
                continue
            if pattern and fnmatch.fnmatch(dataset_id_lower, pattern):
                matched_hint_terms.append(f"dataset:{pattern}")

        for term in rule.include_terms:
            if term and term in text_blob:
                matched_include_terms.append(term)

        if not matched_include_terms:
            for term in rule.synonym_terms:
                if term and term in text_blob:
                    matched_synonym_terms.append(term)

        if not (matched_hint_terms or matched_include_terms or matched_synonym_terms):
            continue

        score = 0.0
        if matched_hint_terms:
            score += 0.5
        score += min(0.4, 0.2 * len(matched_include_terms))
        score += min(0.2, 0.1 * len(matched_synonym_terms))
        score = min(1.0, score)

        matched_terms = matched_hint_terms + matched_include_terms + matched_synonym_terms
        rationale = f"Matched terms: {', '.join(matched_terms)}"
        record = {
            "topic_id": rule.topic_id,
            "score": float(score),
            "matched_terms": matched_terms,
            "rationale": rationale,
        }

        if score >= HIGH_SCORE_THRESHOLD:
            assignments.append(record)
        else:
            review_candidates.append(record)

    review_record: Optional[Dict[str, Any]] = None
    if not assignments and review_candidates:
        review_record = max(review_candidates, key=lambda rec: rec["score"])

    exclusion_record: Optional[Dict[str, Any]] = None
    if not assignments and review_record is None and exclusion_candidates:
        # Use the first exclusion candidate (additional ones likely redundant)
        exclusion_record = exclusion_candidates[0]

    return assignments, review_record, exclusion_record


def persist_results(
    loader: ClickHouseLoader,
    classifications: List[Dict[str, Any]],
    review_records: List[Dict[str, Any]],
    exclusion_records: List[Dict[str, Any]],
    taxonomy_version: str,
    run_id: uuid.UUID,
    started_at: datetime,
    status: str,
    datasets_scanned: int,
    error_message: str = "",
) -> None:
    """Persist classifier output and run metrics."""
    if classifications:
        loader.client.insert(
            "analytics.dataset_topics",
            [
                (
                    record["dataset_id"],
                    record["source"],
                    record["topic_id"],
                    record["score"],
                    CLASSIFIER_VERSION,
                    json.dumps(record["matched_terms"], ensure_ascii=False),
                    record["rationale"],
                    record["metadata_hash"],
                    record["decided_at"],
                )
                for record in classifications
            ],
            column_names=[
                "dataset_id",
                "source",
                "topic_id",
                "score",
                "classifier_version",
                "matched_terms",
                "rationale",
                "metadata_hash",
                "decided_at",
            ],
        )

    if review_records:
        loader.client.insert(
            "analytics.dataset_topics_review",
            [
                (
                    record["dataset_id"],
                    record["source"],
                    record["candidate_topic"],
                    record["score"],
                    json.dumps(record["matched_terms"], ensure_ascii=False),
                    record["rationale"],
                    record["metadata_hash"],
                    record["decided_at"],
                )
                for record in review_records
            ],
            column_names=[
                "dataset_id",
                "source",
                "candidate_topic",
                "score",
                "matched_terms",
                "rationale",
                "metadata_hash",
                "decided_at",
            ],
        )

    if exclusion_records:
        loader.client.insert(
            "analytics.dataset_topics_excluded",
            [
                (
                    record["dataset_id"],
                    record["source"],
                    record["topic_id"],
                    json.dumps(record["exclude_terms"], ensure_ascii=False),
                    record["metadata_hash"],
                    record["decided_at"],
                )
                for record in exclusion_records
            ],
            column_names=[
                "dataset_id",
                "source",
                "topic_id",
                "exclude_terms",
                "metadata_hash",
                "decided_at",
            ],
        )

    unique_classified = len({(c["source"], c["dataset_id"]) for c in classifications})

    metrics_row = {
        "run_id": run_id,
        "started_at": started_at,
        "completed_at": datetime.utcnow(),
        "datasets_scanned": datasets_scanned,
        "datasets_classified": unique_classified,
        "topics_assigned": len(classifications),
        "taxonomy_version": taxonomy_version,
        "classifier_version": CLASSIFIER_VERSION,
        "status": status,
        "error_message": error_message,
    }

    loader.client.insert(
        "analytics.topic_classifier_runs",
        [tuple(metrics_row[col] for col in metrics_row.keys())],
        column_names=list(metrics_row.keys()),
    )


def _parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run rule-based topic classifier.")
    parser.add_argument("--taxonomy", type=Path, default=Path("config/topics/taxonomy.yml"), help="Path to taxonomy YAML.")
    parser.add_argument("--sources", nargs="*", help="Optional list of sources to limit classification to.")
    parser.add_argument("--since", help="ISO timestamp to only process updated datasets.")
    parser.add_argument("--limit", type=int, help="Limit total datasets processed (for testing).")
    parser.add_argument("--dry-run", action="store_true", help="Do not persist results; print summary instead.")
    parser.add_argument("--log-level", default="INFO", help="Logging level (default: INFO).")
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = _parse_args(argv)
    logging.basicConfig(stream=sys.stdout, level=getattr(logging, args.log_level.upper(), logging.INFO), format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    taxonomy_version, topic_rules = load_taxonomy(args.taxonomy)
    LOGGER.info("Loaded taxonomy version %s with %d topics", taxonomy_version, len(topic_rules))

    started_at = datetime.utcnow()
    run_id = uuid.uuid4()
    status = "success"
    error_message = ""

    try:
        with ClickHouseLoader() as loader:
            ensure_schema(loader)
            metadata_items = load_canonical_metadata(loader, args.since, args.sources, args.limit)
            LOGGER.info("Loaded %d metadata records for classification", len(metadata_items))

            classifications: List[Dict[str, Any]] = []
            review_records: List[Dict[str, Any]] = []
            exclusion_records: List[Dict[str, Any]] = []
            per_topic_counts = defaultdict(int)

            for metadata in metadata_items:
                assignments, review_candidate, exclusion_candidate = classify_dataset(metadata, topic_rules)
                if assignments:
                    decided_at = datetime.utcnow()
                    for match in assignments:
                        match_record = {
                            "dataset_id": metadata.dataset_id,
                            "source": metadata.source,
                            "topic_id": match["topic_id"],
                            "score": match["score"],
                            "matched_terms": match["matched_terms"],
                            "rationale": match["rationale"],
                            "metadata_hash": metadata.metadata_hash,
                            "decided_at": decided_at,
                        }
                        classifications.append(match_record)
                        per_topic_counts[match["topic_id"]] += 1
                elif review_candidate:
                    review_records.append(
                        {
                            "dataset_id": metadata.dataset_id,
                            "source": metadata.source,
                            "candidate_topic": review_candidate["topic_id"],
                            "score": review_candidate["score"],
                            "matched_terms": review_candidate["matched_terms"],
                            "rationale": review_candidate["rationale"],
                            "metadata_hash": metadata.metadata_hash,
                            "decided_at": datetime.utcnow(),
                        }
                    )
                elif exclusion_candidate:
                    exclusion_records.append(
                        {
                            "dataset_id": metadata.dataset_id,
                            "source": metadata.source,
                            "topic_id": exclusion_candidate["topic_id"],
                            "exclude_terms": exclusion_candidate["exclude_terms"],
                            "metadata_hash": metadata.metadata_hash,
                            "decided_at": datetime.utcnow(),
                        }
                    )

            LOGGER.info(
                "Classification produced %d topic assignments across %d datasets"
                " (review: %d, excluded: %d)",
                len(classifications),
                len({(c['source'], c['dataset_id']) for c in classifications}),
                len(review_records),
                len(exclusion_records),
            )
            for topic_id, count in per_topic_counts.items():
                LOGGER.info("Topic %s -> %d assignments", topic_id, count)

            if args.dry_run:
                LOGGER.info("Dry-run enabled; skipping persistence.")
                return 0

            persist_results(
                loader,
                classifications,
                review_records,
                exclusion_records,
                taxonomy_version,
                run_id,
                started_at,
                status,
                datasets_scanned=len(metadata_items),
                error_message=error_message,
            )

    except Exception as exc:  # pragma: no cover - defensive
        status = "failed"
        error_message = str(exc)
        LOGGER.exception("Topic classification failed: %s", exc)
        try:
            with ClickHouseLoader() as loader:
                ensure_schema(loader)
                persist_results(
                    loader,
                    [],
                    [],
                    [],
                    taxonomy_version,
                    run_id,
                    started_at,
                    status,
                    datasets_scanned=0,
                    error_message=error_message,
                )
        except Exception as persist_exc:  # pragma: no cover - defensive
            LOGGER.error("Failed to log failed run: %s", persist_exc)
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
