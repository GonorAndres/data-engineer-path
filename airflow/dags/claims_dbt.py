"""dbt build of the claims warehouse, orchestrated by Airflow.

This is the open-source equivalent of the Dagster orchestration in
``projects/02-orchestrated-elt``. There, software-defined assets (one per
warehouse layer) run the DuckDB SQL files from ``projects/01-claims-warehouse``
on a daily 06:00 UTC schedule, and Cloud Scheduler + Cloud Run trigger the same
pipeline in GCP. Here the transformation logic is the dbt project in
``dbt_claims/`` and Airflow only sequences the dbt commands:

    dbt_seed -> dbt_snapshot -> dbt_run -> dbt_test

Tasks use BashOperator, which needs no extra dependency beyond the standard
provider. The trade-off: Airflow sees four tasks, not one per model, so a
failure in one model fails the whole ``dbt_run`` task. Astronomer Cosmos can
render one Airflow task per dbt model (with per-model retries and a lineage
that matches dbt's); it is the better fit once the project grows.

Configuration (environment variables):
    DBT_PROJECT_DIR  Path to the dbt project. Default: ``../../dbt_claims``
                     relative to this file.
    DBT_TARGET       dbt target from profiles.yml. Default: ``duckdb``.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

DBT_PROJECT_DIR = os.environ.get(
    "DBT_PROJECT_DIR",
    str(Path(__file__).resolve().parents[2] / "dbt_claims"),
)
DBT_TARGET = os.environ.get("DBT_TARGET", "duckdb")

# profiles.yml lives inside the project, so --profiles-dir points at it.
DBT_FLAGS = f"--profiles-dir {DBT_PROJECT_DIR} --target {DBT_TARGET}"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="claims_dbt",
    description="dbt seed, snapshot, run and test for the claims warehouse.",
    schedule=None,  # manual; change to "@daily" to run on a schedule
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dbt", "claims"],
) as dag:
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt seed {DBT_FLAGS}",
    )
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt snapshot {DBT_FLAGS}",
    )
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run {DBT_FLAGS}",
    )
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test {DBT_FLAGS}",
    )

    dbt_seed >> dbt_snapshot >> dbt_run >> dbt_test
