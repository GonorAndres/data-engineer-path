"""Feature engineering pipeline for insurance pricing.

Executes SQL-based feature transforms on P01 warehouse tables and prepares
the modeling dataset with one-hot encoded categoricals.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import numpy as np
import polars as pl


def run_feature_transforms(
    con: duckdb.DuckDBPyConnection,
    sql_dir: Path,
) -> dict[str, int]:
    """Execute feature SQL transforms in dependency order.

    Args:
        con: DuckDB connection with P01 mart tables loaded.
        sql_dir: Path to the sql/ directory containing feature SQL files.

    Returns:
        Dict mapping table name to row count.
    """
    feature_sqls = [
        "features/feat_policy_experience.sql",
        "features/feat_risk_segments.sql",
        "features/feat_historical_benchmarks.sql",
    ]
    results: dict[str, int] = {}
    for sql_file in feature_sqls:
        filepath = sql_dir / sql_file
        sql = filepath.read_text(encoding="utf-8")
        con.execute(sql)
        table_name = Path(sql_file).stem
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        results[table_name] = count
        print(f"  {table_name:<40s} {count:>6,d} rows")
    return results


def build_training_set(
    con: duckdb.DuckDBPyConnection,
    sql_dir: Path,
) -> dict[str, int]:
    """Execute model_training_set.sql and return split counts.

    Args:
        con: DuckDB connection with feature tables loaded.
        sql_dir: Path to the sql/ directory.

    Returns:
        Dict with total, train, and test counts.
    """
    filepath = sql_dir / "model" / "model_training_set.sql"
    sql = filepath.read_text(encoding="utf-8")
    con.execute(sql)

    total = con.execute("SELECT COUNT(*) FROM model_training_set").fetchone()[0]
    train = con.execute("SELECT COUNT(*) FROM model_training_set WHERE split = 'train'").fetchone()[
        0
    ]
    test = con.execute("SELECT COUNT(*) FROM model_training_set WHERE split = 'test'").fetchone()[0]

    print(f"  model_training_set: {total:,d} total ({train:,d} train, {test:,d} test)")
    return {"total": total, "train": train, "test": test}


def _get_all_category_values(
    con: duckdb.DuckDBPyConnection,
    categorical_cols: list[str],
) -> dict[str, list[str]]:
    """Get all distinct values for each categorical column from the full dataset.

    This ensures consistent one-hot encoding across train and test splits.
    """
    category_values: dict[str, list[str]] = {}
    for col in categorical_cols:
        result = con.execute(
            f"SELECT DISTINCT {col} FROM model_training_set ORDER BY {col}"
        ).fetchall()
        category_values[col] = [row[0] for row in result if row[0] is not None]
    return category_values


def get_training_data(
    con: duckdb.DuckDBPyConnection,
    split: str = "train",
) -> tuple[np.ndarray, np.ndarray, list[str]]:
    """Extract features and target from model_training_set.

    Queries the model_training_set table, one-hot encodes categorical columns
    using Polars, and returns numpy arrays ready for GLM fitting.

    One-hot encoding uses all distinct values from the full dataset (both
    train and test) to ensure consistent column alignment across splits.

    Args:
        con: DuckDB connection with model_training_set loaded.
        split: Which split to extract ("train" or "test").

    Returns:
        Tuple of (X, y_pure_premium, feature_names).
    """
    query = f"""
        SELECT
            log_premium,
            log_deductible,
            log_coverage_limit,
            exposure_years,
            benchmark_frequency,
            benchmark_severity,
            frequency_trend,
            severity_trend,
            age_band,
            state_risk_group,
            occupation_risk_group,
            coverage_type,
            gender,
            target_pure_premium
        FROM model_training_set
        WHERE split = '{split}'
    """
    result = con.execute(query).fetchall()
    columns = [desc[0] for desc in con.description]

    df = pl.DataFrame({col: [row[i] for row in result] for i, col in enumerate(columns)})

    # Separate numeric and categorical columns
    numeric_cols = [
        "log_premium",
        "log_deductible",
        "log_coverage_limit",
        "exposure_years",
        "benchmark_frequency",
        "benchmark_severity",
        "frequency_trend",
        "severity_trend",
    ]
    categorical_cols = [
        "age_band",
        "state_risk_group",
        "occupation_risk_group",
        "coverage_type",
        "gender",
    ]

    # Get all possible category values from the full dataset for consistent encoding
    all_categories = _get_all_category_values(con, categorical_cols)

    # One-hot encode categoricals manually for consistent columns across splits
    dummies_frames = []
    for col in categorical_cols:
        all_vals = all_categories[col]
        col_data = df[col]
        for val in all_vals:
            col_name = f"{col}_{val}"
            indicator = (col_data == val).cast(pl.Int8)
            dummies_frames.append(pl.DataFrame({col_name: indicator}))

    # Build feature matrix
    numeric_df = df.select(numeric_cols)
    feature_df = pl.concat([numeric_df] + dummies_frames, how="horizontal")

    # Extract target and drop rows with unknown claim cost
    y = df["target_pure_premium"].to_numpy().astype(np.float64)
    valid_mask = ~np.isnan(y)
    n_dropped = int((~valid_mask).sum())
    if n_dropped > 0:
        print(f"  Dropped {n_dropped} rows with NaN target_pure_premium ({split} split)")
    y = y[valid_mask]

    # Build feature matrix, filter to same rows
    X = feature_df.to_numpy().astype(np.float64)
    X = X[valid_mask]

    # Column-median imputation for continuous features
    n_numeric = len(numeric_cols)
    for col_idx in range(n_numeric):
        col = X[:, col_idx]
        nan_mask = np.isnan(col)
        if nan_mask.any():
            X[nan_mask, col_idx] = np.nanmedian(col)

    # For one-hot categoricals, NaN -> 0 is correct (means "not this category")
    X[:, n_numeric:] = np.nan_to_num(X[:, n_numeric:], nan=0.0)

    feature_names = feature_df.columns

    return X, y, feature_names
