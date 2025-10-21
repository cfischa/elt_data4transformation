from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

try:
    import yaml  # type: ignore
except ModuleNotFoundError:  # pragma: no cover - UI fallback
    yaml = None

from streamlit_app.services import get_clickhouse_client


st.set_page_config(page_title="Topic Terms", layout="wide")
st.title("Topic Terms & Ingestion Monitor")


def load_taxonomy(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or yaml is None:
        return []
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}
    return payload.get("topics", [])


def extract_terms(topics: list[dict[str, Any]]) -> pd.DataFrame:
    records: list[dict[str, Any]] = []
    for topic in topics:
        topic_id = topic.get("id", "")
        name = topic.get("name", topic_id)

        def _collect(entries: list[dict[str, Any]]) -> list[str]:
            terms: list[str] = []
            for entry in entries:
                terms.extend(str(term).strip() for term in entry.get("terms", []) if str(term).strip())
            return sorted(set(terms))

        include_terms = _collect(topic.get("include_keywords", []))
        synonyms = _collect(topic.get("synonyms", []))
        exclude_terms = _collect(topic.get("exclude_keywords", []))
        dataset_hints = [
            f"{hint.get('source', '')}:{hint.get('dataset_id') or hint.get('dataset_id_pattern')}"
            for hint in topic.get("dataset_hints", [])
            if hint.get("dataset_id") or hint.get("dataset_id_pattern")
        ]

        records.append(
            {
                "Topic ID": topic_id,
                "Name": name,
                "Include Keywords": ", ".join(include_terms),
                "Synonyms": ", ".join(synonyms),
                "Exclude Keywords": ", ".join(exclude_terms),
                "Dataset Hints": ", ".join(dataset_hints),
            }
        )
    return pd.DataFrame(records)


taxonomy_path = Path(__file__).resolve().parent.parent.parent / "config" / "topics" / "taxonomy.yml"
topic_records = extract_terms(load_taxonomy(taxonomy_path))

st.text_area(
    "Hinweis",
    "Bearbeiten von Topics und Suchwörtern als Funktion implementieren "
    "(aktuell in Datei config/topics/taxonomy.yml).",
    height=80,
)

if topic_records.empty:
    st.warning("Keine Topic-Taxonomie gefunden oder PyYAML ist nicht installiert.")
else:
    st.subheader("Topics & zugehörige Suchwörter")
    st.dataframe(topic_records, use_container_width=True, hide_index=True)


client = get_clickhouse_client()


@st.cache_data(ttl=60)
def load_ingestion_overview(limit: int = 50) -> pd.DataFrame:
    query = """
        WITH ranked AS (
            SELECT
                dataset_id,
                source,
                topic_id,
                ingestion_status,
                payload_format,
                records_count,
                ingestion_started_at,
                ingestion_completed_at,
                payload,
                row_number() OVER (
                    PARTITION BY source, dataset_id, topic_id
                    ORDER BY ingestion_completed_at DESC
                ) AS rn
            FROM raw.topic_selected_payloads
        )
        SELECT
            r.source,
            r.dataset_id,
            r.topic_id,
            r.ingestion_status,
            r.payload_format,
            r.records_count,
            r.ingestion_started_at,
            r.ingestion_completed_at,
            du.title,
            du.description,
            du.latest_update,
            du.keywords,
            du.variables,
            r.payload
        FROM ranked AS r
        LEFT JOIN analytics.datasets_unified AS du
            ON r.source = du.source
           AND r.dataset_id = du.dataset_id
        WHERE r.rn = 1
        ORDER BY r.ingestion_completed_at DESC NULLS LAST
        LIMIT %(limit)s
    """
    return client.query_df(query, parameters={"limit": limit})


def infer_columns(row: pd.Series) -> str:
    payload = row.get("payload")
    if not isinstance(payload, str) or not payload:
        return ""
    fmt = str(row.get("payload_format") or "").lower()
    if fmt == "csv":
        header_line = payload.splitlines()[0] if payload.splitlines() else ""
        delimiter = ";" if header_line.count(";") > header_line.count(",") else ","
        columns = [col.strip() for col in header_line.split(delimiter) if col.strip()]
        return ", ".join(columns[:20]) + (" ..." if len(columns) > 20 else "")
    if fmt == "json":
        try:
            parsed = json.loads(payload)
            if isinstance(parsed, dict):
                return ", ".join(list(parsed.keys())[:20])
            if isinstance(parsed, list) and parsed and isinstance(parsed[0], dict):
                return ", ".join(list(parsed[0].keys())[:20])
        except json.JSONDecodeError:
            return ""
    return ""


ingestion_df = load_ingestion_overview(limit=50)

st.subheader("Zuletzt eingeladene Datensätze")
if ingestion_df.empty:
    st.info("Es liegen noch keine Einträge in raw.topic_selected_payloads vor.")
else:
    ingestion_df["Columns"] = ingestion_df.apply(infer_columns, axis=1)
    ingestion_df["Metadata Keywords"] = ingestion_df["keywords"].apply(
        lambda val: ", ".join(val) if isinstance(val, list) else ""
    )
    ingestion_df["Metadata Variables"] = ingestion_df["variables"].apply(
        lambda val: ", ".join(val) if isinstance(val, list) else ""
    )
    ingestion_df["Payload Preview"] = ingestion_df["payload"].apply(
        lambda text: (text[:300] + "…") if isinstance(text, str) and len(text) > 300 else text
    )
    display_columns = [
        "source",
        "dataset_id",
        "topic_id",
        "ingestion_status",
        "records_count",
        "payload_format",
        "ingestion_completed_at",
        "title",
        "Columns",
        "Metadata Keywords",
        "Metadata Variables",
        "Payload Preview",
    ]
    st.dataframe(
        ingestion_df[display_columns],
        use_container_width=True,
        hide_index=True,
    )
