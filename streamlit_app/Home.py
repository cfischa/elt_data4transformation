import streamlit as st

from streamlit_app.services import get_clickhouse_client


st.set_page_config(page_title="ClickHouse Metadata Explorer", layout="wide")

st.title("ClickHouse Metadata Explorer")
st.write(
    """
    Use the sidebar to browse metadata dashboards and drill into schemas, tables, and usage metrics.
    """
)

st.caption("Home dashboard refreshed with system.tables summary")

client = get_clickhouse_client()

st.subheader("Databases")
databases_df = client.query_df(
    """
    SELECT
        database,
        any(engine) AS engine,
        formatReadableSize(sum(total_bytes)) AS total_size,
        formatReadableQuantity(sum(total_rows)) AS total_rows,
        count() AS tables
    FROM system.tables
    GROUP BY database
    ORDER BY sum(total_bytes) DESC
    """
)
st.dataframe(databases_df, use_container_width=True)
