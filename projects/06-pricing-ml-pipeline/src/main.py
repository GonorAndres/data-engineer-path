"""Insurance Pricing ML Pipeline orchestrator.

End-to-end pipeline that reads warehouse tables, engineers actuarial
features, trains a Tweedie GLM, evaluates model performance, and scores
all policies for pricing adequacy.

Supports two data sources:
  - synthetic: P01's ClaimsDataGenerator (default, ~2K policies)
  - fremtpl2:  Real French motor TPL dataset (~680K policies)

Usage:
    python src/main.py                                # Full pipeline (synthetic)
    python src/main.py --source fremtpl2              # Use real freMTPL2 data
    python src/main.py --source fremtpl2 --fremtpl2-path ./data/fremtpl2
    python src/main.py --target frequency             # Frequency model
    python src/main.py --persist                      # Save DuckDB to disk
    python src/main.py --seed 123                     # Custom seed
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import duckdb
import numpy as np

# Resolve paths relative to project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
SQL_DIR = PROJECT_ROOT / "sql"
P01_ROOT = PROJECT_ROOT.parent / "01-claims-warehouse"
DB_PATH = PROJECT_ROOT / "data" / "pricing_pipeline.duckdb"

# Add P01 src to path for importing ClaimsDataGenerator
P01_SRC = P01_ROOT / "src"
if str(P01_SRC) not in sys.path:
    sys.path.insert(0, str(P01_SRC))

from feature_engineering import (  # noqa: E402
    build_training_set,
    get_training_data,
    run_feature_transforms,
)
from model_evaluation import (  # noqa: E402
    compute_coefficient_summary,
    compute_gini_coefficient,
    compute_lift_table,
    compute_residual_summary,
    print_evaluation_report,
    run_sql_evaluation,
)
from model_scoring import (  # noqa: E402
    get_pricing_adequacy_report,
    print_scoring_report,
    score_all_policies,
)
from model_training import (  # noqa: E402
    predict,
    train_frequency_model,
    train_pure_premium_model,
    train_severity_model,
)

# P01 SQL execution order
P01_SQL_LAYERS = [
    (
        "staging",
        [
            "stg_policyholders.sql",
            "stg_policies.sql",
            "stg_claims.sql",
            "stg_claim_payments.sql",
            "stg_coverages.sql",
        ],
    ),
    (
        "intermediate",
        [
            "int_claims_enriched.sql",
            "int_claim_payments_cumulative.sql",
            "int_policy_exposure.sql",
        ],
    ),
    (
        "marts",
        [
            "dim_date.sql",
            "dim_policyholder.sql",
            "dim_policy.sql",
            "dim_coverage.sql",
            "fct_claims.sql",
            "fct_claim_payments.sql",
        ],
    ),
    (
        "reports",
        [
            "rpt_loss_triangle.sql",
            "rpt_claim_frequency.sql",
        ],
    ),
]

P01_RAW_TABLES = {
    "policyholders.csv": "raw_policyholders",
    "policies.csv": "raw_policies",
    "claims.csv": "raw_claims",
    "claim_payments.csv": "raw_claim_payments",
    "coverages.csv": "raw_coverages",
}


def ensure_p01_data(con: duckdb.DuckDBPyConnection, seed: int = 42) -> None:
    """Load P01 mart tables into DuckDB.

    If P01 sample CSVs exist, loads them directly. Otherwise, generates
    data using P01's ClaimsDataGenerator and runs the full P01 SQL pipeline.

    Args:
        con: DuckDB connection.
        seed: Random seed for data generation.
    """
    print("\n=== Loading P01 Warehouse Data ===")

    p01_data_dir = P01_ROOT / "data" / "sample_data"

    # Check if P01 CSVs exist
    csvs_exist = all((p01_data_dir / csv_file).exists() for csv_file in P01_RAW_TABLES)

    if not csvs_exist:
        print("  P01 CSVs not found, generating synthetic data...")
        from data_generator import ClaimsDataGenerator

        generator = ClaimsDataGenerator(seed=seed)
        p01_data_dir.mkdir(parents=True, exist_ok=True)
        generator.generate_all(str(p01_data_dir))

    # Load raw CSVs
    print("  Loading raw CSVs...")
    for csv_file, table_name in P01_RAW_TABLES.items():
        filepath = p01_data_dir / csv_file
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto('{filepath}')"
        )
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"    {table_name:<25s} {count:>6,d} rows")

    # Execute P01 SQL transforms
    p01_sql_dir = P01_ROOT / "sql"
    for layer_name, sql_files in P01_SQL_LAYERS:
        print(f"\n  --- P01 Layer: {layer_name} ---")
        for sql_file in sql_files:
            filepath = p01_sql_dir / layer_name / sql_file
            if not filepath.exists():
                print(f"    WARNING: {filepath} not found, skipping")
                continue
            sql = filepath.read_text(encoding="utf-8")
            con.execute(sql)
            table_name = sql_file.replace(".sql", "")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"    {table_name:<40s} {count:>6,d} rows")


def load_fremtpl2_data(
    con: duckdb.DuckDBPyConnection,
    fremtpl2_path: Path | None = None,
) -> None:
    """Load freMTPL2 real data into P01-compatible tables.

    Downloads the CSVs if not already present, then creates dim_policy,
    dim_policyholder, fct_claims, int_policy_exposure, rpt_claim_frequency,
    and dim_coverage tables that match P01's schema.

    Args:
        con: DuckDB connection.
        fremtpl2_path: Directory containing freMTPL2 CSVs. Uses
            PROJECT_ROOT/data/fremtpl2/ if None.
    """
    from fremtpl_adapter import download_fremtpl2, load_fremtpl2

    data_dir = fremtpl2_path or (PROJECT_ROOT / "data" / "fremtpl2")
    freq_path, sev_path = download_fremtpl2(data_dir)
    load_fremtpl2(con, freq_path, sev_path)


def run_pipeline(
    seed: int = 42,
    target: str = "pure_premium",
    db_path: Path | None = None,
    source: str = "synthetic",
    fremtpl2_path: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Run the full pricing ML pipeline.

    Steps:
    1. Load warehouse data (synthetic P01 or freMTPL2)
    2. Engineer features (SQL)
    3. Build training set
    4. Train GLM model
    5. Evaluate on test set
    6. Score all policies

    Args:
        seed: Random seed for data generation.
        target: Model target ("pure_premium", "frequency", or "severity").
        db_path: Path for persistent DuckDB file. None = in-memory.
        source: Data source ("synthetic" or "fremtpl2").
        fremtpl2_path: Directory with freMTPL2 CSVs (only used when
            source="fremtpl2").

    Returns:
        DuckDB connection with all tables.
    """
    if db_path:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))
    else:
        con = duckdb.connect(":memory:")

    # Step 1: Load data
    if source == "fremtpl2":
        load_fremtpl2_data(con, fremtpl2_path)
    else:
        ensure_p01_data(con, seed)

    # Step 2: Feature engineering
    print("\n=== Feature Engineering ===")
    run_feature_transforms(con, SQL_DIR)

    # Step 3: Build training set
    print("\n=== Building Training Set ===")
    build_training_set(con, SQL_DIR)

    # Step 4: Train model
    print(f"\n=== Training {target.replace('_', ' ').title()} Model ===")
    X_train, y_train, features = get_training_data(con, "train")

    if target == "pure_premium":
        model = train_pure_premium_model(X_train, y_train, features)
    elif target == "frequency":
        # Use claim count as target for frequency
        freq_query = """
            SELECT claim_count FROM model_training_set WHERE split = 'train'
        """
        y_freq = np.array([r[0] for r in con.execute(freq_query).fetchall()], dtype=np.float64)
        exp_query = """
            SELECT exposure_years FROM model_training_set WHERE split = 'train'
        """
        exposure = np.array([r[0] for r in con.execute(exp_query).fetchall()], dtype=np.float64)
        model = train_frequency_model(X_train, y_freq, features, exposure=exposure)
    elif target == "severity":
        model = train_severity_model(X_train, y_train, features)
    else:
        raise ValueError(f"Unknown target: {target}")

    print(f"  AIC:      {model.aic:,.2f}")
    print(f"  Deviance: {model.deviance:,.2f}")
    print(f"  Family:   {model.family}")

    # Step 5: Evaluate on test set
    print("\n=== Model Evaluation ===")
    X_test, y_test, _ = get_training_data(con, "test")
    y_pred = predict(model, X_test)

    # Clip predictions to non-negative
    y_pred = np.maximum(y_pred, 0.0)

    gini = compute_gini_coefficient(y_test, y_pred)
    lift = compute_lift_table(y_test, y_pred)
    residuals = compute_residual_summary(y_test, y_pred)
    coeffs = compute_coefficient_summary(model)
    print_evaluation_report(gini, lift, residuals, coeffs)

    # Step 6: Score all policies
    print("\n=== Pricing Adequacy Scoring ===")
    summary = score_all_policies(con, model, SQL_DIR)
    by_coverage = get_pricing_adequacy_report(con)
    print_scoring_report(summary, by_coverage)

    # SQL evaluation
    run_sql_evaluation(con, SQL_DIR)

    return con


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Insurance Pricing ML Feature Pipeline")
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for data generation (default: 42)",
    )
    parser.add_argument(
        "--target",
        choices=["pure_premium", "frequency", "severity"],
        default="pure_premium",
        help="Model target (default: pure_premium)",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Save DuckDB database to data/pricing_pipeline.duckdb",
    )
    parser.add_argument(
        "--source",
        choices=["synthetic", "fremtpl2"],
        default="synthetic",
        help="Data source: 'synthetic' (P01 generator) or 'fremtpl2' (real data, ~680K policies)",
    )
    parser.add_argument(
        "--fremtpl2-path",
        type=str,
        default=None,
        help="Directory containing freMTPL2 CSVs (downloads if not found)",
    )
    parser.add_argument(
        "--export",
        type=str,
        default=None,
        help="Export result tables to this directory as CSV",
    )
    args = parser.parse_args()

    db = DB_PATH if args.persist else None
    fremtpl2_dir = Path(args.fremtpl2_path) if args.fremtpl2_path else None
    con = run_pipeline(
        seed=args.seed,
        target=args.target,
        db_path=db,
        source=args.source,
        fremtpl2_path=fremtpl2_dir,
    )

    if args.export:
        export_dir = Path(args.export)
        export_dir.mkdir(parents=True, exist_ok=True)
        tables = [
            "feat_policy_experience",
            "feat_risk_segments",
            "feat_historical_benchmarks",
            "model_training_set",
            "model_predictions",
            "model_scoring",
            "model_evaluation",
        ]
        print(f"\n--- Exporting to {export_dir} ---")
        for table in tables:
            try:
                out_path = export_dir / f"{table}.csv"
                con.execute(f"COPY {table} TO '{out_path}' (HEADER, DELIMITER ',')")
                print(f"  {table}.csv")
            except Exception as e:
                print(f"  WARNING: Could not export {table}: {e}")

    con.close()
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
