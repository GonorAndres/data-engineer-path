"""Insurance Claims ELT Pipeline -- Airflow DAG (Reference Implementation).

This DAG is a **production-quality reference** that demonstrates how the claims
ELT pipeline would be orchestrated in Cloud Composer (managed Airflow).  It is
NOT deployed to Composer because:

  1. Composer costs ~$400/month minimum (the cheapest environment).
  2. Cloud Scheduler + Cloud Run achieves the same result for ~$0.10/month.
  3. The Dagster pipeline in this project is the one actually used for local dev.

**Why include it then?**

  - Airflow is the most-asked-about orchestrator in data engineering interviews.
  - This DAG shows fluency with TaskGroups, BranchPythonOperator, retries,
    dependency chains, and production patterns (email alerts, SLAs, tags).
  - Interviewers can see that the candidate understands Airflow deeply enough
    to write a real DAG, even when choosing a more cost-effective deployment.

Pipeline layers (mirroring Project 01's SQL structure):

  generate_data -> staging (5 tasks) -> intermediate (3 tasks)
    -> marts (6 tasks) -> reports (2 tasks) -> quality_checks
    -> notify_success / notify_failure

Each SQL task would execute a DuckDB (local) or BigQuery (cloud) query.
The actual SQL files live in ``projects/01-claims-warehouse/sql/``.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

# ---------------------------------------------------------------------------
# Airflow imports -- these are available in any Composer environment.
# For local linting without an Airflow install, the DAG file is syntactically
# valid Python; the imports will fail gracefully if Airflow is not installed.
# ---------------------------------------------------------------------------
try:
    from airflow.decorators import dag
    from airflow.models import DAG
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.python import BranchPythonOperator, PythonOperator
    from airflow.utils.task_group import TaskGroup
except ImportError:
    # Allow this file to be parsed/linted without Airflow installed.
    # The DAG will not be functional, but IDE autocompletion and CI lint
    # passes will still work.
    pass

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

# SQL files per layer -- must match the files in projects/01-claims-warehouse/sql/.
# Keeping this explicit (not glob-based) because DAG definitions should be
# deterministic: adding a file should require a conscious DAG update.
STAGING_SQL_FILES: list[str] = [
    "stg_policyholders.sql",
    "stg_policies.sql",
    "stg_claims.sql",
    "stg_claim_payments.sql",
    "stg_coverages.sql",
]

INTERMEDIATE_SQL_FILES: list[str] = [
    "int_claims_enriched.sql",
    "int_claim_payments_cumulative.sql",
    "int_policy_exposure.sql",
]

MART_SQL_FILES: list[str] = [
    "dim_date.sql",
    "dim_policyholder.sql",
    "dim_policy.sql",
    "dim_coverage.sql",
    "fct_claims.sql",
    "fct_claim_payments.sql",
]

REPORT_SQL_FILES: list[str] = [
    "rpt_loss_triangle.sql",
    "rpt_claim_frequency.sql",
]

# Default arguments applied to every task in the DAG.
# retry_delay of 5 minutes handles transient BigQuery / network issues.
# email_on_failure requires Airflow SMTP to be configured in Composer.
DEFAULT_ARGS: dict[str, Any] = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-alerts@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    # SLA: each task should complete within 30 minutes.
    # If breached, Airflow sends an SLA-miss notification.
    "sla": timedelta(minutes=30),
}


# ---------------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------------
# In a real deployment these would import the pipeline runner and execute
# actual SQL against BigQuery or DuckDB.  Here they are stubs that log
# their actions so the DAG can be tested with `airflow dags test`.


def _generate_sample_data(**context: Any) -> dict[str, int]:
    """Generate synthetic insurance data using the ClaimsDataGenerator.

    In production this would either:
      - Generate data and upload CSVs to GCS (batch ingestion pattern), or
      - Be replaced by a GCS sensor waiting for upstream file drops.

    Returns:
        Dict mapping filename to row count (pushed to XCom automatically).
    """
    logger.info("Generating sample insurance data...")
    # Simulated output -- the real generator lives in
    # projects/01-claims-warehouse/src/data_generator.py
    row_counts = {
        "policyholders.csv": 500,
        "policies.csv": 800,
        "claims.csv": 600,
        "claim_payments.csv": 2400,
        "coverages.csv": 5,
    }
    logger.info("Data generation complete: %s", row_counts)
    return row_counts


def _run_sql_transform(layer: str, sql_file: str, **context: Any) -> str:
    """Execute a single SQL transform file.

    In production this would:
      - Read the SQL file from the DAGs folder or a GCS path.
      - Execute it against BigQuery using BigQueryInsertJobOperator.
      - For local dev, execute against DuckDB.

    We use PythonOperator instead of BigQueryInsertJobOperator here to keep
    the DAG self-contained (no GCP credentials needed to parse/test it).

    Args:
        layer: Pipeline layer name (staging, intermediate, marts, reports).
        sql_file: Name of the SQL file to execute.

    Returns:
        Confirmation string (pushed to XCom).
    """
    table_name = sql_file.replace(".sql", "")
    logger.info("Executing %s/%s -> table: %s", layer, sql_file, table_name)
    # In reality: con.execute(sql_content) or BigQuery API call
    return f"{layer}.{table_name} completed"


def _check_data_quality(**context: Any) -> str:
    """Run data quality checks and branch based on results.

    This is a BranchPythonOperator callable.  It returns the task_id of the
    next task to execute:
      - 'notify_success' if all checks pass
      - 'notify_failure' if any check fails

    Quality checks would include:
      - Row count thresholds (claims > 0, policies > 0)
      - Referential integrity (every claim references a valid policy)
      - Freshness checks (most recent data within expected window)
      - Value range checks (premiums > 0, dates within bounds)
      - Null rate checks (critical columns have < 1% nulls)
    """
    logger.info("Running data quality validation...")

    # Simulated quality checks -- in production these query the warehouse.
    checks = {
        "row_count_claims": True,
        "row_count_policies": True,
        "referential_integrity": True,
        "date_freshness": True,
        "premium_positive": True,
        "null_rate_acceptable": True,
    }

    all_passed = all(checks.values())
    failed = [name for name, passed in checks.items() if not passed]

    if all_passed:
        logger.info("All %d quality checks passed.", len(checks))
        return "notify_success"
    else:
        logger.error("Quality checks failed: %s", failed)
        return "notify_failure"


def _notify(status: str, **context: Any) -> None:
    """Send pipeline completion notification.

    In production this would:
      - Post to Slack via a webhook
      - Send an email summary
      - Update a monitoring dashboard
      - Write a run record to a metadata table

    Args:
        status: Either 'success' or 'failure'.
    """
    dag_run = context.get("dag_run")
    run_id = dag_run.run_id if dag_run else "unknown"
    logger.info(
        "Pipeline %s notification sent (run_id=%s, status=%s)",
        "completion" if status == "success" else "failure",
        run_id,
        status,
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="claims_elt_pipeline",
    description=(
        "Daily ELT pipeline for insurance claims data warehouse. "
        "Generates synthetic data, applies layered SQL transforms "
        "(staging -> intermediate -> marts -> reports), and validates "
        "data quality before signaling completion."
    ),
    # Schedule: daily at 06:00 UTC (midnight CST / 01:00 CDT).
    # Chosen because:
    #   - After any overnight batch file drops from source systems
    #   - Before business hours in Mexico (analysts start at ~09:00 CST)
    #   - Allows time for reruns before morning dashboards are checked
    schedule="0 6 * * *",
    start_date=datetime(2025, 1, 1),
    # catchup=False: do not backfill historical runs on first deploy.
    # For a real deployment you would backfill explicitly with
    # `airflow dags backfill` after validating the pipeline.
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["claims", "elt", "warehouse"],
    # max_active_runs=1: prevent overlapping runs that could cause
    # write conflicts in the warehouse.
    max_active_runs=1,
    # Render templates as native Python objects (not strings).
    # This allows passing dicts/lists through Jinja without serialization.
    render_template_as_native_obj=True,
    doc_md=__doc__,
) as dag:
    # -- 1. Data generation ---------------------------------------------------
    generate_data = PythonOperator(
        task_id="generate_data",
        python_callable=_generate_sample_data,
        doc="Generate synthetic insurance data (policyholders, policies, "
        "claims, payments, coverages). In production, this would be "
        "replaced by a GCS sensor or an extract from a source system.",
    )

    # -- 2. Staging layer (5 tasks) -------------------------------------------
    # TaskGroup creates a visual grouping in the Airflow UI and prefixes
    # task_ids with the group name (e.g., staging.stg_policyholders).
    with TaskGroup(group_id="staging") as staging_group:
        staging_tasks = []
        for sql_file in STAGING_SQL_FILES:
            task = PythonOperator(
                task_id=sql_file.replace(".sql", ""),
                python_callable=_run_sql_transform,
                op_kwargs={"layer": "staging", "sql_file": sql_file},
            )
            staging_tasks.append(task)
        # All staging tasks can run in parallel -- they read from raw_*
        # tables and have no inter-dependencies within the layer.

    # -- 3. Intermediate layer (3 tasks) --------------------------------------
    with TaskGroup(group_id="intermediate") as intermediate_group:
        intermediate_tasks = []
        for sql_file in INTERMEDIATE_SQL_FILES:
            task = PythonOperator(
                task_id=sql_file.replace(".sql", ""),
                python_callable=_run_sql_transform,
                op_kwargs={"layer": "intermediate", "sql_file": sql_file},
            )
            intermediate_tasks.append(task)

    # -- 4. Marts layer (6 tasks) ---------------------------------------------
    with TaskGroup(group_id="marts") as marts_group:
        mart_tasks = []
        for sql_file in MART_SQL_FILES:
            task = PythonOperator(
                task_id=sql_file.replace(".sql", ""),
                python_callable=_run_sql_transform,
                op_kwargs={"layer": "marts", "sql_file": sql_file},
            )
            mart_tasks.append(task)

    # -- 5. Reports layer (2 tasks) -------------------------------------------
    with TaskGroup(group_id="reports") as reports_group:
        report_tasks = []
        for sql_file in REPORT_SQL_FILES:
            task = PythonOperator(
                task_id=sql_file.replace(".sql", ""),
                python_callable=_run_sql_transform,
                op_kwargs={"layer": "reports", "sql_file": sql_file},
            )
            report_tasks.append(task)

    # -- 6. Quality checks (branching) ----------------------------------------
    quality_checks = BranchPythonOperator(
        task_id="quality_checks",
        python_callable=_check_data_quality,
        doc="Run data quality validation. Branches to notify_success or "
        "notify_failure based on check results.",
    )

    # -- 7. Notification tasks ------------------------------------------------
    # Using PythonOperator (not EmptyOperator) so we can log the notification.
    # trigger_rule="none_failed_min_one_success" is the default, which works
    # correctly with the BranchPythonOperator upstream.
    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify,
        op_kwargs={"status": "success"},
    )

    notify_failure = PythonOperator(
        task_id="notify_failure",
        python_callable=_notify,
        op_kwargs={"status": "failure"},
        # trigger_rule: this task runs when quality_checks branches to it,
        # even though the "success" branch was skipped.
        trigger_rule="none_failed_min_one_success",
    )

    # -- Dependency chain -----------------------------------------------------
    # Linear layer dependencies enforce the ELT execution order.
    # Within each TaskGroup, tasks run in parallel (no intra-group deps).
    (
        generate_data
        >> staging_group
        >> intermediate_group
        >> marts_group
        >> reports_group
        >> quality_checks
        >> [notify_success, notify_failure]
    )
