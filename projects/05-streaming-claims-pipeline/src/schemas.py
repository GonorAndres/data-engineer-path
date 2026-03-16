"""BigQuery table schemas for the streaming claims pipeline.

Defines three output tables:
- Streaming hourly summary: windowed aggregations with pane timing metadata
- Late arrivals: tracks claims that arrived after the watermark
- Dead letters: captures parse/validation failures for debugging

All schemas include streaming-specific fields (pane_timing, firing_id) that
distinguish this from P03's batch-only schemas.
"""

from __future__ import annotations

# ---------------------------------------------------------------------------
# Default table coordinates -- override via pipeline options
# ---------------------------------------------------------------------------

_DEFAULT_PROJECT = "your-gcp-project"
_DEFAULT_DATASET = "claims_analytics"

# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

STREAMING_HOURLY_SUMMARY_SCHEMA = {
    "fields": [
        {"name": "window_start", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "coverage_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "claim_count", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "total_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "avg_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "min_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "max_amount_mxn", "type": "FLOAT", "mode": "REQUIRED"},
        {"name": "pane_timing", "type": "STRING", "mode": "REQUIRED"},
        {"name": "firing_id", "type": "STRING", "mode": "REQUIRED"},
    ]
}

STREAMING_LATE_ARRIVALS_SCHEMA = {
    "fields": [
        {"name": "claim_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "original_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "arrival_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "lateness_seconds", "type": "INTEGER", "mode": "REQUIRED"},
        {"name": "raw_data", "type": "STRING", "mode": "REQUIRED"},
        {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

STREAMING_DEAD_LETTERS_SCHEMA = {
    "fields": [
        {"name": "raw_data", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_reason", "type": "STRING", "mode": "REQUIRED"},
        {"name": "error_type", "type": "STRING", "mode": "REQUIRED"},
        {"name": "processing_timestamp", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}


# ---------------------------------------------------------------------------
# Table spec helpers
# ---------------------------------------------------------------------------


def get_summary_table_spec(
    project: str = _DEFAULT_PROJECT,
    dataset: str = _DEFAULT_DATASET,
) -> str:
    """Return BigQuery table spec for streaming hourly summaries."""
    return f"{project}:{dataset}.streaming_hourly_summary"


def get_late_table_spec(
    project: str = _DEFAULT_PROJECT,
    dataset: str = _DEFAULT_DATASET,
) -> str:
    """Return BigQuery table spec for late arrival records."""
    return f"{project}:{dataset}.streaming_late_arrivals"


def get_dlq_table_spec(
    project: str = _DEFAULT_PROJECT,
    dataset: str = _DEFAULT_DATASET,
) -> str:
    """Return BigQuery table spec for dead-letter records."""
    return f"{project}:{dataset}.streaming_dead_letters"
