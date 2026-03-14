"""Pipeline configuration for the insurance claims ELT pipeline.

Centralizes path resolution, SQL layer ordering, and raw table mappings
so that every runner (Dagster, Airflow, Cloud Run) shares the same config.

All paths are resolved relative to this project's root, which is the
grandparent of the directory containing this file
(``02-orchestrated-elt/``).
"""

from __future__ import annotations

from pathlib import Path

# ---------------------------------------------------------------------------
# Path resolution
# ---------------------------------------------------------------------------

# This file lives at:  02-orchestrated-elt/src/pipeline/config.py
# Project root:        02-orchestrated-elt/
PROJECT_ROOT: Path = Path(__file__).resolve().parent.parent.parent

# Project 1 holds the SQL transforms and sample data.
PROJECT_01_ROOT: Path = PROJECT_ROOT.parent / "01-claims-warehouse"

SQL_DIR: Path = PROJECT_01_ROOT / "sql"
DATA_DIR: Path = PROJECT_01_ROOT / "data" / "sample_data"
DEFAULT_DB_PATH: Path = PROJECT_ROOT / "data" / "claims_warehouse.duckdb"

# Directory that the sensor watches for new CSVs to arrive.
WATCH_DIR: Path = PROJECT_ROOT / "data" / "incoming"

# ---------------------------------------------------------------------------
# SQL layer execution order
# ---------------------------------------------------------------------------
# Each entry is (layer_name, [list of .sql filenames]).
# Layers must run sequentially; files within a layer can run in any order
# but are listed in a dependency-safe sequence.

SQL_LAYERS: list[tuple[str, list[str]]] = [
    ("staging", [
        "stg_policyholders.sql",
        "stg_policies.sql",
        "stg_claims.sql",
        "stg_claim_payments.sql",
        "stg_coverages.sql",
    ]),
    ("intermediate", [
        "int_claims_enriched.sql",
        "int_claim_payments_cumulative.sql",
        "int_policy_exposure.sql",
    ]),
    ("marts", [
        "dim_date.sql",
        "dim_policyholder.sql",
        "dim_policy.sql",
        "dim_coverage.sql",
        "fct_claims.sql",
        "fct_claim_payments.sql",
    ]),
    ("reports", [
        "rpt_loss_triangle.sql",
        "rpt_claim_frequency.sql",
    ]),
]

# ---------------------------------------------------------------------------
# Raw table mapping
# ---------------------------------------------------------------------------
# Maps CSV filenames to the DuckDB table names used by staging SQL.

RAW_TABLES: dict[str, str] = {
    "policyholders.csv": "raw_policyholders",
    "policies.csv": "raw_policies",
    "claims.csv": "raw_claims",
    "claim_payments.csv": "raw_claim_payments",
    "coverages.csv": "raw_coverages",
}
