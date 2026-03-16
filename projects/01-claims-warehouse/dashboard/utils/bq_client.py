"""BigQuery client utilities for the Claims Analytics dashboard.

Provides a cached BigQuery client and a helper function to run SQL queries,
returning results as pandas DataFrames. Query results are cached for 5 minutes
to minimise BigQuery costs during interactive exploration.
"""

import os

import pandas as pd
import streamlit as st
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "project-ad7a5be2-a1c7-4510-82d")
DATASET_ANALYTICS = "dev_claims_analytics"
DATASET_REPORTS = "dev_claims_reports"
DATASET_RAW = "dev_claims_raw"


def _fqn(dataset: str, table: str) -> str:
    """Return a fully-qualified BigQuery table name with backtick escaping."""
    return f"`{PROJECT_ID}`.`{dataset}`.`{table}`"


@st.cache_resource
def get_bq_client() -> bigquery.Client:
    """Return a cached BigQuery client bound to the project."""
    return bigquery.Client(project=PROJECT_ID)


@st.cache_data(ttl=300)
def query_bq(sql: str) -> pd.DataFrame:
    """Execute *sql* against BigQuery and return a DataFrame.

    Results are cached for 300 seconds (5 minutes) so repeated page
    interactions do not trigger new BigQuery jobs.
    """
    client = get_bq_client()
    df = client.query(sql).to_dataframe()
    # BigQuery NUMERIC/DECIMAL columns arrive as Python decimal.Decimal,
    # which breaks pandas arithmetic and Plotly rendering. Convert to float.
    import decimal
    for col in df.columns:
        if df[col].dtype == object and len(df) > 0:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, decimal.Decimal):
                df[col] = df[col].apply(
                    lambda x: float(x) if isinstance(x, decimal.Decimal) else x
                )
    return df
