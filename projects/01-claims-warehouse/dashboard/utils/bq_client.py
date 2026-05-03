"""BigQuery client utilities for the Claims Analytics dashboard.

Provides a cached BigQuery client and a helper to run SQL queries,
returning results as pandas DataFrames with a 5-minute TTL cache.
"""

import decimal
import os
import time
from functools import lru_cache

import pandas as pd
from google.cloud import bigquery

PROJECT_ID = os.environ.get("GCP_PROJECT_ID") or os.environ.get("GOOGLE_CLOUD_PROJECT")
if not PROJECT_ID:
    raise RuntimeError(
        "No GCP project configured: set GCP_PROJECT_ID (preferred) or "
        "GOOGLE_CLOUD_PROJECT in the Cloud Run service environment."
    )

DATASET_ANALYTICS = "dev_claims_analytics"
DATASET_REPORTS = "dev_claims_reports"
DATASET_RAW = "dev_claims_raw"


def _fqn(dataset: str, table: str) -> str:
    return f"`{PROJECT_ID}`.`{dataset}`.`{table}`"


@lru_cache(maxsize=1)
def _get_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


_cache: dict[str, tuple[float, pd.DataFrame]] = {}
_CACHE_TTL = 300


def query_bq(sql: str) -> pd.DataFrame:
    key = sql.strip()
    now = time.time()
    if key in _cache:
        ts, df = _cache[key]
        if now - ts < _CACHE_TTL:
            return df.copy()

    client = _get_client()
    df = client.query(sql).to_dataframe()

    for col in df.columns:
        if df[col].dtype == object and len(df) > 0:
            sample = df[col].dropna().iloc[0] if not df[col].dropna().empty else None
            if isinstance(sample, decimal.Decimal):
                df[col] = df[col].apply(
                    lambda x: float(x) if isinstance(x, decimal.Decimal) else x
                )

    _cache[key] = (now, df)
    return df.copy()
