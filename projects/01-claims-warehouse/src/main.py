"""Claims Warehouse pipeline orchestrator.

Runs the full ELT pipeline locally using DuckDB:
1. Generate synthetic data (or use existing CSVs)
2. Load raw CSVs into DuckDB tables
3. Execute SQL transforms: staging -> intermediate -> marts -> reports
4. Print summary statistics

Usage:
    python src/main.py                       # Full pipeline (generate + transform)
    python src/main.py --generate-only       # Only generate sample data
    python src/main.py --transform-only      # Only run transforms on existing data
    python src/main.py --export results/     # Export mart tables to CSV
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from data_generator import ClaimsDataGenerator

# Resolve paths relative to project root (parent of src/)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data" / "sample_data"
SQL_DIR = PROJECT_ROOT / "sql"
DB_PATH = PROJECT_ROOT / "data" / "claims_warehouse.duckdb"

# SQL execution order -- each layer depends on the previous.
SQL_LAYERS = [
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

# Raw CSV files and the DuckDB table names they map to.
RAW_TABLES = {
    "policyholders.csv": "raw_policyholders",
    "policies.csv": "raw_policies",
    "claims.csv": "raw_claims",
    "claim_payments.csv": "raw_claim_payments",
    "coverages.csv": "raw_coverages",
}


def generate_data(seed: int = 42) -> dict[str, int]:
    """Generate synthetic sample data CSVs."""
    print(f"\n--- Generating sample data (seed={seed}) ---")
    generator = ClaimsDataGenerator(seed=seed)
    return generator.generate_all(str(DATA_DIR))


def load_raw_tables(con: duckdb.DuckDBPyConnection) -> None:
    """Load CSV files into raw_* tables in DuckDB."""
    print("\n--- Loading raw CSVs into DuckDB ---")
    for csv_file, table_name in RAW_TABLES.items():
        filepath = DATA_DIR / csv_file
        if not filepath.exists():
            raise FileNotFoundError(f"Missing data file: {filepath}")
        con.execute(
            f"CREATE OR REPLACE TABLE {table_name} AS "
            f"SELECT * FROM read_csv_auto('{filepath}')"
        )
        count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
        print(f"  {table_name:<25s} {count:>6,d} rows")


def execute_sql_transforms(con: duckdb.DuckDBPyConnection) -> None:
    """Execute SQL transform files in dependency order."""
    for layer_name, sql_files in SQL_LAYERS:
        print(f"\n--- Layer: {layer_name} ---")
        for sql_file in sql_files:
            filepath = SQL_DIR / layer_name / sql_file
            if not filepath.exists():
                print(f"  WARNING: {filepath} not found, skipping")
                continue
            sql = filepath.read_text(encoding="utf-8")
            con.execute(sql)
            # Extract table name from filename (remove .sql extension)
            table_name = sql_file.replace(".sql", "")
            count = con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            print(f"  {table_name:<40s} {count:>6,d} rows")


def print_summary(con: duckdb.DuckDBPyConnection) -> None:
    """Print key metrics from the warehouse."""
    print("\n" + "=" * 60)
    print("WAREHOUSE SUMMARY")
    print("=" * 60)

    # Claims overview
    result = con.execute("""
        SELECT
            COUNT(*) AS total_claims,
            SUM(CASE WHEN claim_status = 'open' THEN 1 ELSE 0 END) AS open_claims,
            SUM(CASE WHEN claim_status = 'closed' THEN 1 ELSE 0 END) AS closed_claims,
            ROUND(SUM(total_paid), 2) AS total_paid,
            ROUND(SUM(incurred_amount), 2) AS total_incurred,
            ROUND(AVG(total_paid), 2) AS avg_paid_per_claim
        FROM fct_claims
    """).fetchone()
    print(f"\nClaims: {result[0]:,d} total ({result[1]:,d} open, {result[2]:,d} closed)")
    print(f"Total Paid: ${result[3]:,.2f} MXN")
    print(f"Total Incurred: ${result[4]:,.2f} MXN")
    print(f"Avg Paid/Claim: ${result[5]:,.2f} MXN")

    # Loss triangle preview
    print("\n--- Loss Triangle (Cumulative Paid, MXN) ---")
    triangle = con.execute("""
        SELECT
            accident_year,
            ROUND(dev_year_0, 0) AS "Dev 0",
            ROUND(dev_year_1, 0) AS "Dev 1",
            ROUND(dev_year_2, 0) AS "Dev 2",
            ROUND(dev_year_3, 0) AS "Dev 3",
            ROUND(dev_year_4, 0) AS "Dev 4",
            ROUND(dev_year_5, 0) AS "Dev 5",
            claim_count
        FROM rpt_loss_triangle
        ORDER BY accident_year
    """).fetchall()

    header = f"{'AY':>6s} {'Dev 0':>14s} {'Dev 1':>14s} {'Dev 2':>14s} {'Dev 3':>14s} {'Dev 4':>14s} {'Dev 5':>14s} {'Claims':>8s}"
    print(header)
    print("-" * len(header))
    for row in triangle:
        vals = [f"{v:>14,.0f}" if v is not None else f"{'':>14s}" for v in row[1:-1]]
        print(f"{row[0]:>6d} {''.join(vals)} {row[-1]:>8d}")

    # Frequency summary
    print("\n--- Claim Frequency by Coverage Type ---")
    freq = con.execute("""
        SELECT
            coverage_type,
            SUM(claim_count) AS claims,
            ROUND(SUM(exposure_years), 1) AS exposure_yrs,
            ROUND(CAST(SUM(claim_count) AS DOUBLE) / NULLIF(SUM(exposure_years), 0), 4) AS frequency,
            ROUND(SUM(total_incurred) / NULLIF(SUM(exposure_years), 0), 2) AS pure_premium
        FROM rpt_claim_frequency
        GROUP BY coverage_type
        ORDER BY coverage_type
    """).fetchall()
    print(f"{'Coverage':>12s} {'Claims':>8s} {'Exposure':>10s} {'Frequency':>10s} {'Pure Prem':>12s}")
    for row in freq:
        print(f"{row[0]:>12s} {row[1]:>8,d} {row[2]:>10,.1f} {row[3]:>10.4f} {row[4]:>12,.2f}")


def export_tables(con: duckdb.DuckDBPyConnection, export_dir: str) -> None:
    """Export mart and report tables to CSV."""
    os.makedirs(export_dir, exist_ok=True)
    tables_to_export = [
        "dim_date", "dim_policyholder", "dim_policy", "dim_coverage",
        "fct_claims", "fct_claim_payments",
        "rpt_loss_triangle", "rpt_claim_frequency",
    ]
    print(f"\n--- Exporting to {export_dir} ---")
    for table in tables_to_export:
        out_path = os.path.join(export_dir, f"{table}.csv")
        con.execute(f"COPY {table} TO '{out_path}' (HEADER, DELIMITER ',')")
        print(f"  {table}.csv")


def run_pipeline(
    generate: bool = True,
    transform: bool = True,
    export_dir: str | None = None,
    seed: int = 42,
    db_path: Path | None = None,
) -> duckdb.DuckDBPyConnection:
    """Run the full pipeline and return the DuckDB connection.

    Args:
        generate: Whether to generate sample data.
        transform: Whether to run SQL transforms.
        export_dir: If set, export mart tables to this directory.
        seed: Random seed for data generation.
        db_path: Path for the DuckDB database file. None = in-memory.

    Returns:
        Open DuckDB connection with all tables loaded.
    """
    if db_path:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        con = duckdb.connect(str(db_path))
    else:
        con = duckdb.connect(":memory:")

    if generate:
        generate_data(seed=seed)

    if transform:
        load_raw_tables(con)
        execute_sql_transforms(con)
        print_summary(con)

    if export_dir:
        export_tables(con, export_dir)

    return con


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Insurance Claims Data Warehouse -- DuckDB Pipeline"
    )
    parser.add_argument(
        "--generate-only",
        action="store_true",
        help="Only generate sample data, skip transforms",
    )
    parser.add_argument(
        "--transform-only",
        action="store_true",
        help="Only run SQL transforms on existing data",
    )
    parser.add_argument(
        "--export",
        type=str,
        default=None,
        help="Export mart tables to this directory",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for data generation (default: 42)",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Save DuckDB database to data/claims_warehouse.duckdb",
    )
    args = parser.parse_args()

    generate = not args.transform_only
    transform = not args.generate_only
    db = DB_PATH if args.persist else None

    con = run_pipeline(
        generate=generate,
        transform=transform,
        export_dir=args.export,
        seed=args.seed,
        db_path=db,
    )
    con.close()
    print("\nPipeline complete.")


if __name__ == "__main__":
    main()
