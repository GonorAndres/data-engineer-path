"""Shared test fixtures for the pricing ML pipeline tests."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import duckdb
import numpy as np
import pytest

# Ensure P01 src is on the path for ClaimsDataGenerator
P01_ROOT = Path(__file__).resolve().parent.parent.parent / "01-claims-warehouse"
P01_SRC = P01_ROOT / "src"
P01_SQL_DIR = P01_ROOT / "sql"
if str(P01_SRC) not in sys.path:
    sys.path.insert(0, str(P01_SRC))

# Ensure P06 src is on the path
P06_SRC = Path(__file__).resolve().parent.parent / "src"
if str(P06_SRC) not in sys.path:
    sys.path.insert(0, str(P06_SRC))

P06_SQL_DIR = Path(__file__).resolve().parent.parent / "sql"

# P01 SQL execution order
P01_SQL_LAYERS = [
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

P01_RAW_TABLES = {
    "policyholders.csv": "raw_policyholders",
    "policies.csv": "raw_policies",
    "claims.csv": "raw_claims",
    "claim_payments.csv": "raw_claim_payments",
    "coverages.csv": "raw_coverages",
}


@pytest.fixture(scope="session")
def warehouse_con(tmp_path_factory):
    """DuckDB connection with P01 mart tables loaded."""
    from data_generator import ClaimsDataGenerator

    con = duckdb.connect(":memory:")
    output_dir = tmp_path_factory.mktemp("p01_data")

    # Generate P01 data
    generator = ClaimsDataGenerator(seed=42)
    generator.generate_all(str(output_dir))

    # Load raw CSVs
    for csv_file, table_name in P01_RAW_TABLES.items():
        filepath = os.path.join(output_dir, csv_file)
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{filepath}')"
        )

    # Execute P01 SQL transforms
    for layer_name, sql_files in P01_SQL_LAYERS:
        for sql_file in sql_files:
            filepath = P01_SQL_DIR / layer_name / sql_file
            sql = filepath.read_text(encoding="utf-8")
            con.execute(sql)

    yield con
    con.close()


@pytest.fixture(scope="session")
def feature_con(warehouse_con):
    """DuckDB connection with P01 marts + P06 feature tables."""
    from feature_engineering import build_training_set, run_feature_transforms

    run_feature_transforms(warehouse_con, P06_SQL_DIR)
    build_training_set(warehouse_con, P06_SQL_DIR)

    return warehouse_con


@pytest.fixture(scope="session")
def trained_model(feature_con):
    """Trained Tweedie GLM on the training set."""
    from feature_engineering import get_training_data
    from model_training import train_pure_premium_model

    X_train, y_train, features = get_training_data(feature_con, "train")
    model = train_pure_premium_model(X_train, y_train, features)
    return model


@pytest.fixture(scope="session")
def training_data(feature_con):
    """Training data arrays (X, y, feature_names)."""
    from feature_engineering import get_training_data

    return get_training_data(feature_con, "train")


@pytest.fixture(scope="session")
def test_data(feature_con):
    """Test data arrays (X, y, feature_names)."""
    from feature_engineering import get_training_data

    return get_training_data(feature_con, "test")


@pytest.fixture(scope="session")
def test_predictions(trained_model, test_data):
    """Predictions on the test set."""
    from model_training import predict

    X_test, _, _ = test_data
    return np.maximum(predict(trained_model, X_test), 0.0)
