"""Shared test fixtures for the claims warehouse tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import pytest

# Ensure src/ is on the path so we can import the generator.
SRC_DIR = Path(__file__).resolve().parent.parent / "src"
sys.path.insert(0, str(SRC_DIR))

from data_generator import ClaimsDataGenerator  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"


@pytest.fixture(scope="session")
def generator() -> ClaimsDataGenerator:
    """Shared data generator instance (seed=42)."""
    return ClaimsDataGenerator(seed=42)


@pytest.fixture(scope="session")
def generated_data(generator: ClaimsDataGenerator, tmp_path_factory) -> dict:
    """Generate all datasets once for the test session.

    Returns a dict with keys: policyholders, policies, claims, payments, coverages,
    plus 'output_dir' pointing to the temp directory with CSV files.
    """
    output_dir = tmp_path_factory.mktemp("sample_data")
    row_counts = generator.generate_all(str(output_dir))

    # Re-read the data for assertions.
    gen = ClaimsDataGenerator(seed=42)
    policyholders = gen.generate_policyholders()
    policies = gen.generate_policies(policyholders)
    claims = gen.generate_claims(policies)
    payments = gen.generate_claim_payments(claims, policies)
    coverages = gen.generate_coverages()

    return {
        "policyholders": policyholders,
        "policies": policies,
        "claims": claims,
        "payments": payments,
        "coverages": coverages,
        "row_counts": row_counts,
        "output_dir": str(output_dir),
    }


@pytest.fixture(scope="session")
def warehouse_con(generated_data: dict) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with all raw tables loaded and SQL transforms executed.

    This is the full pipeline in-memory, shared across the test session.
    """
    con = duckdb.connect(":memory:")
    output_dir = generated_data["output_dir"]

    # Load raw tables from CSVs.
    raw_tables = {
        "policyholders.csv": "raw_policyholders",
        "policies.csv": "raw_policies",
        "claims.csv": "raw_claims",
        "claim_payments.csv": "raw_claim_payments",
        "coverages.csv": "raw_coverages",
    }
    for csv_file, table_name in raw_tables.items():
        filepath = os.path.join(output_dir, csv_file)
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{filepath}')"
        )

    # Execute SQL transforms in order.
    sql_layers = [
        ("staging", [
            "stg_policyholders.sql", "stg_policies.sql", "stg_claims.sql",
            "stg_claim_payments.sql", "stg_coverages.sql",
        ]),
        ("intermediate", [
            "int_claims_enriched.sql", "int_claim_payments_cumulative.sql",
            "int_policy_exposure.sql",
        ]),
        ("marts", [
            "dim_date.sql", "dim_policyholder.sql", "dim_policy.sql",
            "dim_coverage.sql", "fct_claims.sql", "fct_claim_payments.sql",
        ]),
        ("reports", [
            "rpt_loss_triangle.sql", "rpt_claim_frequency.sql",
        ]),
    ]
    for layer_name, sql_files in sql_layers:
        for sql_file in sql_files:
            filepath = SQL_DIR / layer_name / sql_file
            sql = filepath.read_text(encoding="utf-8")
            con.execute(sql)

    yield con
    con.close()
