import streamlit as st

from streamlit_app.services import get_clickhouse_client


st.set_page_config(page_title="Metadata Overview", layout="wide")

st.title("Metadata Overview")

client = get_clickhouse_client()

col1, col2, col3 = st.columns(3)

with col1:
    table_count = client.query("SELECT count() FROM system.tables")
    st.metric("Tables", table_count.result_rows[0][0])
with col2:
    column_count = client.query("SELECT count() FROM system.columns")
    st.metric("Columns", column_count.result_rows[0][0])
with col3:
    recent_queries = client.query_df(
        """
        SELECT count() AS query_count
        FROM system.query_log
        WHERE event_time >= now() - INTERVAL 1 DAY
          AND type = 'QueryFinish'
        """
    )
    st.metric("Queries (24h)", recent_queries.loc[0, "query_count"])

st.subheader("Top Tables by Size")
tables_df = client.query_df(
    """
    SELECT
        database,
        table,
        formatReadableSize(sum(bytes_on_disk)) AS size,
        sum(rows) AS rows
    FROM system.parts
    WHERE active
    GROUP BY database, table
    ORDER BY sum(bytes_on_disk) DESC
    LIMIT 20
    """
)
st.dataframe(tables_df, use_container_width=True)
