from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

import pandas as pd
import streamlit as st

try:
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - handled gracefully in UI
    yaml = None

from streamlit_app.services import get_clickhouse_client


st.set_page_config(page_title="Topic Browser", layout="wide")
st.title("Topic Classification Browser")


def format_number(value: float | int) -> str:
    try:
        return f"{int(value):,}".replace(",", ".")
    except (TypeError, ValueError):
        return str(value)


def format_terms(value: str | None) -> str:
    if not value:
        return ""
    try:
        data = json.loads(value)
        if isinstance(data, list):
            return ", ".join(str(item) for item in data)
    except json.JSONDecodeError:
        pass
    return str(value)


@st.cache_data(show_spinner=False)
def load_topic_labels() -> dict[str, str]:
    """Load topic id -> human readable name mapping from taxonomy."""
    taxonomy_path = Path(__file__).resolve().parent.parent.parent / "config" / "topics" / "taxonomy.yml"
    if not taxonomy_path.exists() or yaml is None:
        return {}

    try:
        with taxonomy_path.open("r", encoding="utf-8") as handle:
            payload = yaml.safe_load(handle)
    except Exception:
        return {}

    labels: dict[str, str] = {}
    for topic in payload.get("topics", []):
        topic_id = topic.get("id")
        if topic_id:
            labels[topic_id] = topic.get("name", topic_id)
    return labels


if yaml is None:
    st.warning(
        "PyYAML ist nicht installiert – Topic-Namen werden als Roh-IDs angezeigt. "
        "Installiere `pyyaml`, um die Labels aus der Taxonomie zu laden."
    )

topic_labels = load_topic_labels()
client = get_clickhouse_client()


@st.cache_data(ttl=60)
def load_classification_coverage() -> pd.DataFrame:
    query = """
        WITH source_totals AS (
            SELECT source, countDistinct(dataset_id) AS total_datasets
            FROM analytics.datasets_unified
            GROUP BY source
        ),
        topics AS (
            SELECT DISTINCT source, dataset_id, 'topic' AS status
            FROM analytics.dataset_topics
        ),
        review AS (
            SELECT DISTINCT source, dataset_id, 'review' AS status
            FROM analytics.dataset_topics_review
        ),
        excluded AS (
            SELECT DISTINCT source, dataset_id, 'excluded' AS status
            FROM analytics.dataset_topics_excluded
        ),
        handled AS (
            SELECT source, dataset_id, status FROM topics
            UNION ALL
            SELECT source, dataset_id, status FROM review
            UNION ALL
            SELECT source, dataset_id, status FROM excluded
        ),
        status_counts AS (
            SELECT
                source,
                uniqExactIf(dataset_id, status = 'topic') AS labeled_datasets,
                uniqExactIf(dataset_id, status = 'review') AS review_datasets,
                uniqExactIf(dataset_id, status = 'excluded') AS excluded_datasets,
                uniqExact(dataset_id) AS handled_datasets
            FROM handled
            GROUP BY source
        )
        SELECT
            t.source,
            t.total_datasets,
            ifNull(s.labeled_datasets, 0) AS labeled_datasets,
            ifNull(s.review_datasets, 0) AS review_datasets,
            ifNull(s.excluded_datasets, 0) AS excluded_datasets,
            (t.total_datasets - ifNull(s.handled_datasets, 0)) AS unlabeled_datasets,
            round(ifNull(s.labeled_datasets, 0) / nullIf(t.total_datasets, 0), 3) AS coverage
        FROM source_totals AS t
        LEFT JOIN status_counts AS s ON t.source = s.source
        ORDER BY t.source
    """
    return client.query_df(query)


@st.cache_data(ttl=60)
def load_summary() -> pd.DataFrame:
    query = """
        WITH latest AS (
            SELECT
                dataset_id,
                source,
                topic_id,
                anyHeavy(score) AS score
            FROM analytics.dataset_topics
            GROUP BY dataset_id, source, topic_id
        )
        SELECT
            topic_id,
            source,
            countDistinct(dataset_id) AS datasets,
            count() AS assignments,
            avg(score) AS avg_score,
            quantileExact(0.1)(score) AS score_p10,
            quantileExact(0.5)(score) AS score_median,
            quantileExact(0.9)(score) AS score_p90,
            min(score) AS min_score,
            max(score) AS max_score
        FROM latest
        GROUP BY topic_id, source
        ORDER BY topic_id, source
    """
    return client.query_df(query)


summary_df = load_summary()

if summary_df.empty:
    st.info(
        "Keine Klassifikationsdaten gefunden. "
        "Führe `python -m pipeline.topic_classifier --sources destatis` aus, "
        "um Topic-Zuweisungen zu generieren."
    )
    st.stop()

summary_df["topic_name"] = summary_df["topic_id"].apply(lambda tid: topic_labels.get(tid, tid))

coverage_df = load_classification_coverage()

with st.sidebar:
    st.header("Filter")
    topic_options = summary_df["topic_id"].unique().tolist()
    topic_options.sort()
    topic_selection = st.selectbox(
        "Topic",
        ["(Alle)"] + [topic_labels.get(tid, tid) for tid in topic_options],
    )

    if topic_selection == "(Alle)":
        active_topic_ids = topic_options
    else:
        # Reverse lookup topic id by label or id
        active_topic_ids = [
            tid for tid, label in topic_labels.items() if label == topic_selection
        ]
        if not active_topic_ids and topic_selection in topic_options:
            active_topic_ids = [topic_selection]
    selected_sources = st.multiselect(
        "Quelle",
        sorted(summary_df["source"].unique().tolist()),
        default=sorted(summary_df["source"].unique().tolist()),
    )
    min_score = st.slider("Mindest-Score", 0.0, 1.0, 0.3, 0.05)
    show_all_rows = st.checkbox("Alle Ergebnisse anzeigen", value=False)
    max_rows = None
    if not show_all_rows:
        max_rows = st.slider("Max. Datensätze", 50, 5000, 500, 50)

selected_coverage = coverage_df[coverage_df["source"].isin(selected_sources)] if selected_sources else coverage_df
total_datasets = int(selected_coverage["total_datasets"].sum())
classified_datasets = int(selected_coverage["labeled_datasets"].sum())
review_datasets = int(selected_coverage["review_datasets"].sum())
excluded_datasets = int(selected_coverage["excluded_datasets"].sum())
unlabeled_datasets = int(selected_coverage["unlabeled_datasets"].sum())
coverage_ratio = (classified_datasets / total_datasets) if total_datasets else 0.0
handled_ratio = ((classified_datasets + review_datasets + excluded_datasets) / total_datasets) if total_datasets else 0.0

metric_cols = st.columns(5)
metric_cols[0].metric("Datensätze gesamt", format_number(total_datasets))
metric_cols[1].metric("Klassifiziert", format_number(classified_datasets))
metric_cols[2].metric("Zur Prüfung", format_number(review_datasets))
metric_cols[3].metric("Nicht relevant", format_number(excluded_datasets))
metric_cols[4].metric("Offen", format_number(unlabeled_datasets))

metric_cols_2 = st.columns(2)
metric_cols_2[0].metric("Abdeckung (klassifiziert)", f"{coverage_ratio * 100:.1f} %")
metric_cols_2[1].metric("Abdeckung (gesamt behandelt)", f"{handled_ratio * 100:.1f} %")


@st.cache_data(ttl=60)
def load_details(
    topics: tuple[str, ...],
    sources: tuple[str, ...],
    min_score_threshold: float,
    limit: Optional[int],
) -> pd.DataFrame:
    placeholders = {
        "topics": topics,
        "sources": sources,
        "min_score": min_score_threshold,
    }
    limit_clause = ""
    if limit is not None:
        placeholders["limit"] = limit
        limit_clause = "LIMIT %(limit)s"
    query = f"""
        WITH latest AS (
            SELECT
                dataset_id,
                source,
                topic_id,
                anyHeavy(score) AS score,
                anyLastOrNull(matched_terms) AS matched_terms,
                anyLastOrNull(rationale) AS rationale,
                anyLastOrNull(decided_at) AS decided_at
            FROM analytics.dataset_topics
            GROUP BY dataset_id, source, topic_id
        )
        SELECT
            lt.topic_id,
            lt.source,
            lt.dataset_id,
            lt.score,
            lt.matched_terms,
            lt.rationale,
            lt.decided_at,
            du.title,
            du.description,
            du.latest_update
        FROM latest AS lt
        LEFT JOIN analytics.datasets_unified AS du
            ON lt.source = du.source
            AND lt.dataset_id = du.dataset_id
        WHERE lt.topic_id IN %(topics)s
          AND lt.source IN %(sources)s
          AND lt.score >= %(min_score)s
        ORDER BY lt.score DESC
        {limit_clause}
    """
    return client.query_df(query, parameters=placeholders)

filtered_summary = summary_df[
    summary_df["topic_id"].isin(active_topic_ids) & summary_df["source"].isin(selected_sources)
].copy()

if filtered_summary.empty:
    st.warning("Keine Ergebnisse für die aktuelle Filterkombination.")
else:
    st.subheader("Übersicht")
    display_df = filtered_summary[[
        "topic_name",
        "source",
        "datasets",
        "assignments",
        "avg_score",
        "score_p10",
        "score_median",
        "score_p90",
    ]].rename(
        columns={
            "topic_name": "Topic",
            "source": "Quelle",
            "datasets": "Eindeutige Datensätze",
            "assignments": "Zuweisungen",
            "avg_score": "Ø Score",
            "score_p10": "Score p10",
            "score_median": "Score Median",
            "score_p90": "Score p90",
        }
    )
    st.dataframe(display_df, use_container_width=True, hide_index=True)


details_df = pd.DataFrame()
if active_topic_ids and selected_sources:
    details_df = load_details(
        topics=tuple(active_topic_ids),
        sources=tuple(selected_sources),
        min_score_threshold=min_score,
        limit=max_rows,
    )

if details_df.empty:
    st.info("Keine Datensätze für die Detailansicht gefunden.")
else:
    st.subheader("Detailansicht")
    details_df["topic_name"] = details_df["topic_id"].apply(lambda tid: topic_labels.get(tid, tid))

    def _unpack_terms(value: str | None) -> str:
        if not value:
            return ""
        try:
            data = json.loads(value)
            if isinstance(data, list):
                return ", ".join(str(item) for item in data)
        except json.JSONDecodeError:
            pass
        return str(value)

    details_df["matched_terms_pretty"] = details_df["matched_terms"].apply(_unpack_terms)
    display_cols = [
        "topic_name",
        "source",
        "dataset_id",
        "score",
        "matched_terms_pretty",
        "title",
        "latest_update",
        "rationale",
    ]
    st.dataframe(
        details_df[display_cols].rename(
            columns={
                "topic_name": "Topic",
                "source": "Quelle",
                "dataset_id": "Dataset",
                "score": "Score",
                "matched_terms_pretty": "Matched Terms",
                "title": "Titel",
                "latest_update": "Letztes Update",
                "rationale": "Begründung",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

    with st.expander("Rohdaten anzeigen"):
        st.dataframe(details_df, use_container_width=True, hide_index=True)


@st.cache_data(ttl=60)
def load_unlabeled(sources: tuple[str, ...], limit: int = 200) -> pd.DataFrame:
    query = """
        WITH labeled AS (
            SELECT DISTINCT source, dataset_id
            FROM analytics.dataset_topics
            UNION DISTINCT
            SELECT DISTINCT source, dataset_id
            FROM analytics.dataset_topics_review
            UNION DISTINCT
            SELECT DISTINCT source, dataset_id
            FROM analytics.dataset_topics_excluded
        )
        SELECT
            du.source,
            du.dataset_id,
            du.title,
            du.description,
            du.latest_update
        FROM analytics.datasets_unified AS du
        LEFT JOIN labeled AS l
            ON du.source = l.source
            AND du.dataset_id = l.dataset_id
        WHERE l.dataset_id IS NULL
          AND du.source IN %(sources)s
        ORDER BY du.latest_update DESC NULLS LAST
        LIMIT %(limit)s
    """
    params = {"sources": sources, "limit": limit}
    return client.query_df(query, parameters=params)


@st.cache_data(ttl=60)
def load_review_entries(sources: tuple[str, ...], limit: int = 200) -> pd.DataFrame:
    query = """
        SELECT
            source,
            dataset_id,
            anyHeavy(candidate_topic) AS candidate_topic,
            max(score) AS score,
            anyLastOrNull(matched_terms) AS matched_terms,
            anyLastOrNull(rationale) AS rationale,
            max(decided_at) AS decided_at
        FROM analytics.dataset_topics_review
        WHERE source IN %(sources)s
        GROUP BY source, dataset_id
        ORDER BY score DESC
        LIMIT %(limit)s
    """
    review = client.query_df(query, parameters={"sources": sources, "limit": limit})
    if review.empty:
        return review

    details = client.query_df(
        """
        SELECT source, dataset_id, title, description, latest_update
        FROM analytics.datasets_unified
        WHERE (source, dataset_id) IN %(pairs)s
        """,
        parameters={"pairs": tuple(zip(review["source"], review["dataset_id"]))},
    )
    return review.merge(details, on=["source", "dataset_id"], how="left")


@st.cache_data(ttl=60)
def load_excluded_entries(sources: tuple[str, ...], limit: int = 200) -> pd.DataFrame:
    query = """
        SELECT
            source,
            dataset_id,
            anyHeavy(topic_id) AS topic_id,
            anyLastOrNull(exclude_terms) AS exclude_terms,
            max(decided_at) AS decided_at
        FROM analytics.dataset_topics_excluded
        WHERE source IN %(sources)s
        GROUP BY source, dataset_id
        ORDER BY decided_at DESC
        LIMIT %(limit)s
    """
    excluded = client.query_df(query, parameters={"sources": sources, "limit": limit})
    if excluded.empty:
        return excluded

    details = client.query_df(
        """
        SELECT source, dataset_id, title, description, latest_update
        FROM analytics.datasets_unified
        WHERE (source, dataset_id) IN %(pairs)s
        """,
        parameters={"pairs": tuple(zip(excluded["source"], excluded["dataset_id"]))},
    )
    return excluded.merge(details, on=["source", "dataset_id"], how="left")


st.subheader("Nicht klassifizierte Datensätze")
if unlabeled_datasets == 0:
    st.success("Alle Datensätze der ausgewählten Quellen sind klassifiziert.")
else:
    st.warning(
        f"{format_number(unlabeled_datasets)} von {format_number(total_datasets)} Datensätzen sind noch offen."
    )

if selected_sources:
    unlabeled_df = load_unlabeled(tuple(selected_sources))
else:
    unlabeled_df = pd.DataFrame(columns=["source", "dataset_id", "title", "description", "latest_update"])

st.caption("Top 200 Datensätze ohne Topic-Zuweisung (nach letztem Update).")
st.dataframe(
    unlabeled_df.rename(
        columns={
            "source": "Quelle",
            "dataset_id": "Dataset",
            "title": "Titel",
            "description": "Beschreibung",
            "latest_update": "Letztes Update",
        }
    ),
    use_container_width=True,
    hide_index=True,
)

st.subheader("Zur Prüfung (unter Schwelle)")
if selected_sources:
    review_df = load_review_entries(tuple(selected_sources))
else:
    review_df = pd.DataFrame()

if review_df.empty:
    st.info("Keine Datensätze im Prüfstatus.")
else:
    review_df["matched_terms_pretty"] = review_df["matched_terms"].apply(format_terms)
    st.dataframe(
        review_df[[
            "source",
            "dataset_id",
            "candidate_topic",
            "score",
            "matched_terms_pretty",
            "title",
            "latest_update",
        ]].rename(
            columns={
                "source": "Quelle",
                "dataset_id": "Dataset",
                "candidate_topic": "Vorgeschlagenes Topic",
                "score": "Score",
                "matched_terms_pretty": "Matched Terms",
                "title": "Titel",
                "latest_update": "Letztes Update",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Nicht relevant (verworfen)")
if selected_sources:
    excluded_df = load_excluded_entries(tuple(selected_sources))
else:
    excluded_df = pd.DataFrame()

if excluded_df.empty:
    st.info("Keine Datensätze als nicht relevant markiert.")
else:
    excluded_df["exclude_terms_pretty"] = excluded_df["exclude_terms"].apply(format_terms)
    st.dataframe(
        excluded_df[[
            "source",
            "dataset_id",
            "topic_id",
            "exclude_terms_pretty",
            "title",
            "latest_update",
        ]].rename(
            columns={
                "source": "Quelle",
                "dataset_id": "Dataset",
                "topic_id": "Topic",
                "exclude_terms_pretty": "Ausgeschlossene Begriffe",
                "title": "Titel",
                "latest_update": "Letztes Update",
            }
        ),
        use_container_width=True,
        hide_index=True,
    )

st.subheader("Klassifikationsstatus je Quelle")
coverage_display = (selected_coverage if selected_sources else coverage_df).rename(
    columns={
        "source": "Quelle",
        "total_datasets": "Datensätze gesamt",
        "labeled_datasets": "Klassifiziert",
        "review_datasets": "Zur Prüfung",
        "excluded_datasets": "Nicht relevant",
        "unlabeled_datasets": "Nicht klassifiziert",
        "coverage": "Abdeckung",
    }
)
coverage_display["Abdeckung"] = coverage_display["Abdeckung"].fillna(0.0)
if coverage_display.empty:
    st.info("Keine Quellen ausgewählt.")
else:
    st.dataframe(
        coverage_display,
        use_container_width=True,
        hide_index=True,
    )
