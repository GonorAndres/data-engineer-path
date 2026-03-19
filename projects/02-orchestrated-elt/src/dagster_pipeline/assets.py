"""Software-Defined Assets for the insurance claims ELT pipeline.

Each asset represents one logical layer of the warehouse build:

    raw_data  -->  staging_layer  -->  intermediate_layer  -->  marts_layer  -->  reports_layer

Assets use the ``PipelineRunner`` for actual execution and return
Dagster-native metadata (row counts, timing) so the Dagster UI shows
lineage and observability out of the box.
"""

import sys
import time
from pathlib import Path
from typing import Any

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    RetryPolicy,
    asset,
)

from dagster_pipeline.resources import DuckDBResource
from pipeline.config import DATA_DIR, SQL_DIR, SQL_LAYERS

# Add Project 1's src/ to the import path so we can use the data generator.
_PROJECT_01_SRC = (
    Path(__file__).resolve().parent.parent.parent.parent
    / ("01-claims-warehouse")
    / "src"
)
if str(_PROJECT_01_SRC) not in sys.path:
    sys.path.insert(0, str(_PROJECT_01_SRC))


# ---------------------------------------------------------------------------
# Shared retry policy
# ---------------------------------------------------------------------------

LAYER_RETRY_POLICY = RetryPolicy(max_retries=2, delay=10)

# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _row_counts_for_layer(
    con: Any,
    layer_name: str,
) -> dict[str, int]:
    """Return row counts for every table produced by a layer."""
    counts: dict[str, int] = {}
    for name, files in SQL_LAYERS:
        if name == layer_name:
            for sql_file in files:
                table = sql_file.replace(".sql", "")
                try:
                    result = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()
                    counts[table] = result[0] if result else 0
                except Exception:
                    counts[table] = -1
            break
    return counts


# ---------------------------------------------------------------------------
# Assets
# ---------------------------------------------------------------------------


@asset(
    description=(
        "Generate or verify source CSV files.  If sample data already "
        "exists, this asset is a no-op.  Otherwise it invokes the "
        "ClaimsDataGenerator from Project 1 to produce 5 CSV files."
    ),
    group_name="claims_warehouse",
    tags={"layer": "raw", "domain": "insurance"},
    retry_policy=LAYER_RETRY_POLICY,
)
def raw_data(
    context: AssetExecutionContext,
) -> MaterializeResult:
    """Ensure raw CSV source data is available."""
    expected_files = [
        "policyholders.csv",
        "policies.csv",
        "claims.csv",
        "claim_payments.csv",
        "coverages.csv",
    ]

    missing = [f for f in expected_files if not (DATA_DIR / f).exists()]

    if missing:
        context.log.info("Generating sample data -- missing files: %s", missing)
        from data_generator import ClaimsDataGenerator

        generator = ClaimsDataGenerator(seed=42)
        row_counts = generator.generate_all(str(DATA_DIR))
        context.log.info("Generated files: %s", row_counts)
        metadata = {
            f"rows/{name}": MetadataValue.int(count)
            for name, count in row_counts.items()
        }
        metadata["generated"] = MetadataValue.bool(True)
    else:
        context.log.info("All source CSVs already present in %s", DATA_DIR)
        metadata = {
            f"file/{f}": MetadataValue.path(str(DATA_DIR / f)) for f in expected_files
        }
        metadata["generated"] = MetadataValue.bool(False)

    return MaterializeResult(metadata=metadata)


@asset(
    deps=[raw_data],
    description=(
        "Load raw CSVs into DuckDB and execute staging SQL transforms "
        "(stg_policyholders, stg_policies, stg_claims, "
        "stg_claim_payments, stg_coverages)."
    ),
    group_name="claims_warehouse",
    tags={"layer": "staging", "domain": "insurance"},
    retry_policy=LAYER_RETRY_POLICY,
)
def staging_layer(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """Run raw-load + staging transforms."""
    from pipeline.runner import PipelineRunner

    start = time.monotonic()
    runner = PipelineRunner(
        db_path=duckdb_resource.db_path,
        sql_dir=SQL_DIR,
        data_dir=DATA_DIR,
    )

    raw_counts = runner.load_raw_tables()
    context.log.info("Raw tables loaded: %s", raw_counts)

    layer_result = runner.execute_sql_layer("staging")
    elapsed = round(time.monotonic() - start, 3)
    runner.close()

    if layer_result.errors:
        raise RuntimeError(f"Staging errors: {'; '.join(layer_result.errors)}")

    metadata: dict[str, Any] = {
        f"rows/{table}": MetadataValue.int(count)
        for table, count in layer_result.tables.items()
    }
    metadata["elapsed_seconds"] = MetadataValue.float(elapsed)
    for table, count in raw_counts.items():
        metadata[f"raw_rows/{table}"] = MetadataValue.int(count)

    context.log.info("Staging complete in %.2fs: %s", elapsed, layer_result.tables)
    return MaterializeResult(metadata=metadata)


@asset(
    deps=[staging_layer],
    description=(
        "Execute intermediate SQL transforms "
        "(int_claims_enriched, int_claim_payments_cumulative, "
        "int_policy_exposure)."
    ),
    group_name="claims_warehouse",
    tags={"layer": "intermediate", "domain": "insurance"},
    retry_policy=LAYER_RETRY_POLICY,
)
def intermediate_layer(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """Run intermediate transforms (requires staging tables to exist)."""
    from pipeline.runner import PipelineRunner

    start = time.monotonic()
    runner = PipelineRunner(
        db_path=duckdb_resource.db_path,
        sql_dir=SQL_DIR,
        data_dir=DATA_DIR,
    )

    # Verify upstream tables exist in persistent DB
    for table in ["stg_claims", "stg_policies", "stg_policyholders"]:
        if runner.get_table_row_count(table) < 0:
            raise RuntimeError(
                f"Upstream table '{table}' not found. Materialize staging_layer first."
            )

    layer_result = runner.execute_sql_layer("intermediate")
    elapsed = round(time.monotonic() - start, 3)
    runner.close()

    if layer_result.errors:
        raise RuntimeError(f"Intermediate errors: {'; '.join(layer_result.errors)}")

    metadata: dict[str, Any] = {
        f"rows/{table}": MetadataValue.int(count)
        for table, count in layer_result.tables.items()
    }
    metadata["elapsed_seconds"] = MetadataValue.float(elapsed)

    context.log.info("Intermediate complete in %.2fs: %s", elapsed, layer_result.tables)
    return MaterializeResult(metadata=metadata)


@asset(
    deps=[intermediate_layer],
    description=(
        "Execute marts SQL transforms "
        "(dim_date, dim_policyholder, dim_policy, dim_coverage, "
        "fct_claims, fct_claim_payments)."
    ),
    group_name="claims_warehouse",
    tags={"layer": "marts", "domain": "insurance"},
    retry_policy=LAYER_RETRY_POLICY,
)
def marts_layer(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """Run marts transforms (requires intermediate tables to exist)."""
    from pipeline.runner import PipelineRunner

    start = time.monotonic()
    runner = PipelineRunner(
        db_path=duckdb_resource.db_path,
        sql_dir=SQL_DIR,
        data_dir=DATA_DIR,
    )

    # Verify upstream tables exist in persistent DB
    for table in [
        "int_claims_enriched",
        "int_claim_payments_cumulative",
        "int_policy_exposure",
    ]:
        if runner.get_table_row_count(table) < 0:
            raise RuntimeError(
                f"Upstream table '{table}' not found. Materialize intermediate_layer first."
            )

    layer_result = runner.execute_sql_layer("marts")
    elapsed = round(time.monotonic() - start, 3)
    runner.close()

    if layer_result.errors:
        raise RuntimeError(f"Marts errors: {'; '.join(layer_result.errors)}")

    metadata: dict[str, Any] = {
        f"rows/{table}": MetadataValue.int(count)
        for table, count in layer_result.tables.items()
    }
    metadata["elapsed_seconds"] = MetadataValue.float(elapsed)

    context.log.info("Marts complete in %.2fs: %s", elapsed, layer_result.tables)
    return MaterializeResult(metadata=metadata)


@asset(
    deps=[marts_layer],
    description=(
        "Execute reports SQL transforms (rpt_loss_triangle, rpt_claim_frequency)."
    ),
    group_name="claims_warehouse",
    tags={"layer": "reports", "domain": "insurance"},
    retry_policy=LAYER_RETRY_POLICY,
)
def reports_layer(
    context: AssetExecutionContext,
    duckdb_resource: DuckDBResource,
) -> MaterializeResult:
    """Run report transforms (requires marts tables to exist)."""
    from pipeline.runner import PipelineRunner

    start = time.monotonic()
    runner = PipelineRunner(
        db_path=duckdb_resource.db_path,
        sql_dir=SQL_DIR,
        data_dir=DATA_DIR,
    )

    # Verify upstream tables exist in persistent DB
    for table in ["fct_claims", "fct_claim_payments", "dim_date"]:
        if runner.get_table_row_count(table) < 0:
            raise RuntimeError(
                f"Upstream table '{table}' not found. Materialize marts_layer first."
            )

    layer_result = runner.execute_sql_layer("reports")
    elapsed = round(time.monotonic() - start, 3)
    runner.close()

    if layer_result.errors:
        raise RuntimeError(f"Reports errors: {'; '.join(layer_result.errors)}")

    metadata: dict[str, Any] = {
        f"rows/{table}": MetadataValue.int(count)
        for table, count in layer_result.tables.items()
    }
    metadata["elapsed_seconds"] = MetadataValue.float(elapsed)

    context.log.info("Reports complete in %.2fs: %s", elapsed, layer_result.tables)
    return MaterializeResult(metadata=metadata)
