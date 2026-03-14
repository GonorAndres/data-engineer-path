---
tags: [tools, dagster, orchestration, local-development]
status: draft
created: 2026-03-14
updated: 2026-03-14
---

# Dagster for Local Orchestration

Dagster is an asset-centric orchestration framework. Unlike Airflow (which thinks in tasks/DAGs), Dagster thinks in **Software-Defined Assets** -- the data artifacts your pipeline produces. This mental model maps directly to how data engineers think: "I need to build `fct_claims`, which depends on `stg_claims`, which depends on `raw_claims`."

## Why Dagster for This Project

| Criterion | Dagster | Airflow (Composer) |
|-----------|---------|-------------------|
| Cost | $0 (local) | ~$400/month minimum |
| Mental model | Asset-centric (what data exists) | Task-centric (what work runs) |
| Local dev | First-class (`dagster dev`) | Requires Docker or full install |
| Testing | Built-in `materialize_to_memory` | Requires mocking or live infra |
| Lineage | Automatic from asset dependencies | Manual via task dependencies |
| UI | Dagster UI (dagit) included | Airflow UI requires webserver |

**Trade-off**: Airflow has a larger hiring pool and interview mindshare. That's why [[projects/02-orchestrated-elt]] includes both a Dagster pipeline (actually used) and an Airflow DAG (for interviews).

## Key Concepts

### Software-Defined Assets

```python
from dagster import asset

@asset(description="Staging claims -- cleaned and typed")
def stg_claims(raw_claims):
    # Transform raw_claims into stg_claims
    return execute_sql("sql/staging/stg_claims.sql")
```

The `raw_claims` parameter name creates a dependency edge. Dagster builds the DAG automatically.

### Resources

```python
from dagster import ConfigurableResource

class DuckDBResource(ConfigurableResource):
    db_path: str = ":memory:"

    def get_connection(self):
        return duckdb.connect(self.db_path)
```

Resources are injectable dependencies -- swap DuckDB for BigQuery without changing asset code.

### Sensors

```python
from dagster import sensor, RunRequest

@sensor(job=my_job, minimum_interval_seconds=60)
def new_data_sensor(context):
    if new_files_in_gcs():
        yield RunRequest(run_key="new-data-detected")
```

Sensors watch for external events (new files, API changes) and trigger runs.

## Running the Claims Pipeline Locally

```bash
cd projects/02-orchestrated-elt
source .venv/bin/activate

# Start Dagster UI
dagster dev

# Or materialize all assets from CLI
dagster asset materialize --select '*'
```

The Dagster UI at `http://localhost:3000` shows the asset graph, run history, and metadata.

## Further Reading

- [[orchestration]] -- Orchestration concepts (DAGs, backfill, idempotency)
- [[orchestrator-selection]] -- When to choose Dagster vs Airflow vs Prefect
- [[cloud-composer-guide]] -- Airflow/Composer when Dagster isn't the right fit
- [[cost-effective-orchestration]] -- Cloud Scheduler + Cloud Run as deployment target
