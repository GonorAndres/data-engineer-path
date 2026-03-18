"""Dagster definitions entry point for the insurance claims ELT pipeline.

This module wires together all assets, resources, sensors, and schedules
into a single ``Definitions`` object that Dagster discovers via the
``[tool.dagster]`` section of ``pyproject.toml``.

Launch the Dagster UI locally with::

    cd projects/02-orchestrated-elt
    dagster dev -m dagster_pipeline.definitions
"""

from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
)

from dagster_pipeline.assets import (
    intermediate_layer,
    marts_layer,
    raw_data,
    reports_layer,
    staging_layer,
)
from dagster_pipeline.resources import DuckDBResource
from dagster_pipeline.sensors import new_data_sensor

# ---------------------------------------------------------------------------
# Schedule: daily at 06:00 UTC
# ---------------------------------------------------------------------------

daily_pipeline_schedule = ScheduleDefinition(
    name="daily_claims_pipeline",
    cron_schedule="0 6 * * *",  # 06:00 UTC every day
    target=AssetSelection.groups("claims_warehouse"),
    description=("Materialize the full claims warehouse pipeline daily at 06:00 UTC."),
)

# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = Definitions(
    assets=[
        raw_data,
        staging_layer,
        intermediate_layer,
        marts_layer,
        reports_layer,
    ],
    resources={
        "duckdb_resource": DuckDBResource(db_path="/tmp/claims_warehouse.duckdb"),
    },
    schedules=[daily_pipeline_schedule],
    sensors=[new_data_sensor],
)
