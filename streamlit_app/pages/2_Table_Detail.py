import pandas as pd
import streamlit as st

from streamlit_app.services import get_clickhouse_client

st.set_page_config(page_title="Table Explorer", layout="wide")
st.title("Table Explorer")

client = get_clickhouse_client()

schemas = client.query_df(
    """
    SELECT
        database,
        table,
        formatReadableSize(total_bytes) AS total_size
    FROM system.tables
    ORDER BY database, table
    """
)

available_databases = schemas["database"].unique().tolist()
if not available_databases:
    st.warning("No tables found in system.tables")
    st.stop()

with st.sidebar:
    st.header("Select Table")
    database = st.selectbox("Database", available_databases)
    tables = schemas[schemas["database"] == database]["table"].tolist()
    if not tables:
        st.warning(f"No tables found in database '{database}'")
        st.stop()
    table = st.selectbox("Table", tables)

st.subheader(f"{database}.{table}")

cols_df = client.query_df(
    """
    SELECT
        position AS ordinal_position,
        name AS column_name,
        type AS data_type,
        default_kind AS default_type,
        default_expression,
        comment
    FROM system.columns
    WHERE database = %(database)s
      AND table = %(table)s
    ORDER BY ordinal_position
    """,
    parameters={"database": database, "table": table},
)
st.markdown("**Columns**")
st.dataframe(cols_df, use_container_width=True, hide_index=True)

database_escaped = database.replace("`", "``")
table_escaped = table.replace("`", "``")
sample_df = client.query_df(
    f"SELECT * FROM `{database_escaped}`.`{table_escaped}` LIMIT 10"
)

if isinstance(sample_df, pd.DataFrame) and not sample_df.empty:
    st.markdown("**Sample Rows**")
    st.dataframe(sample_df, use_container_width=True, hide_index=True)
else:
    st.info("No data available to display.")
