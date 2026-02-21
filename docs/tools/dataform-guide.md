---
tags:
  - gcp
  - dataform
  - sql
  - bigquery
  - transformation
  - elt
status: draft
created: 2026-02-21
updated: 2026-02-21
---

# Dataform -- SQL Transformation for BigQuery

Dataform is Google's native SQL workflow service for [[bigquery-guide]]. Acquired by Google in 2020, it provides a dbt-like experience for defining, testing, and scheduling SQL transformations with dependency management, version control, and documentation -- all integrated into the BigQuery console. It is the recommended tool for the "T" in [[etl-vs-elt]] when your warehouse is BigQuery.

## Core Concepts

| Concept | Description |
|---|---|
| **SQLX files** | SQL files extended with JavaScript templating -- the primary authoring format |
| **Declarations** | References to source tables (external data that Dataform does not create) |
| **Tables** | Output tables created by Dataform: `table` (full refresh), `incremental`, or `view` |
| **Assertions** | Data quality checks that run after a table builds -- see [[data-quality]] |
| **`ref()` function** | References another Dataform table, automatically creating a dependency edge in the DAG |
| **Repositories** | Git-backed code repositories (GitHub, GitLab, Bitbucket) |
| **Release configurations** | Compilation settings per environment (dev, staging, prod) |
| **Workflow invocations** | Execution runs -- manual, scheduled, or API-triggered |

### How the DAG Works

Every `ref("table_name")` call creates a dependency. Dataform compiles all SQLX files into a DAG and executes tables in topological order. This is conceptually identical to how [[orchestration]] tools like Airflow manage task dependencies, but scoped to SQL transformations only.

## SQLX File Anatomy

A SQLX file has two parts: a **config block** (JavaScript) and a **SQL body** (with optional JS templating).

### Example: Full-Refresh Table

```sql
-- definitions/staging/stg_claims.sqlx

config {
  type: "table",
  schema: "staging",
  description: "Cleaned claims from raw source. One row per claim event.",
  tags: ["staging", "claims"],
  assertions: {
    uniqueKey: ["claim_id"],
    nonNull: ["claim_id", "policy_id", "loss_date", "reported_date"]
  }
}

SELECT
  claim_id,
  policy_id,
  PARSE_DATE('%Y-%m-%d', loss_date_str)  AS loss_date,
  PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%S', reported_date_str) AS reported_date,
  CAST(estimated_amount AS NUMERIC)      AS estimated_amount,
  line_of_business,
  claimant_state,
  CURRENT_TIMESTAMP()                    AS _loaded_at
FROM
  ${ref("raw_claims")}
WHERE
  claim_id IS NOT NULL
```

### Example: Incremental Table

```sql
-- definitions/intermediate/int_claim_transactions.sqlx

config {
  type: "incremental",
  schema: "intermediate",
  uniqueKey: ["transaction_id"],
  bigquery: {
    partitionBy: "transaction_date",
    clusterBy: ["claim_id", "transaction_type"]
  },
  description: "Claim financial transactions, incrementally loaded."
}

SELECT
  transaction_id,
  claim_id,
  transaction_type,
  transaction_date,
  amount,
  CURRENT_TIMESTAMP() AS _loaded_at
FROM
  ${ref("stg_claim_transactions")}

${ when(incremental(), `WHERE transaction_date > (SELECT MAX(transaction_date) FROM ${self()})`) }
```

The `when(incremental(), ...)` block adds the incremental filter only during incremental runs -- on full refresh, it processes all data.

Assertions **fail** if they return any rows. You can define them inline (as shown above in `assertions: {}`) or as standalone SQLX files of type `"assertion"` for complex custom checks.

## JavaScript Templating in SQLX

SQLX embeds JavaScript for dynamic SQL generation -- Dataform's equivalent of dbt's Jinja templating.

### Compilation Variables (Environment Switching)

```sql
config {
  type: "table",
  schema: dataform.projectConfig.vars.env === "prod" ? "analytics" : "dev_analytics"
}
```

### Reusable Macros (JavaScript Functions)

```javascript
// includes/helpers.js
function surrogate_key(columns) {
  return `FARM_FINGERPRINT(CONCAT(${columns.map(c => `CAST(${c} AS STRING)`).join(", ")}))`;
}
module.exports = { surrogate_key };
```

```sql
-- Usage in SQLX: ${helpers.surrogate_key(["policy_id", "effective_date"])} AS dim_policy_key
```

JavaScript also supports loops (`.map().join()`) for generating repeated CASE/WHEN or UNION patterns. More powerful than Jinja for complex logic, but introduces a learning curve for SQL-focused engineers.

## Dataform vs dbt

| Criterion | Dataform | dbt Core / dbt Cloud |
|---|---|---|
| **Warehouse support** | BigQuery only | BigQuery, Snowflake, Redshift, Databricks, 30+ |
| **Templating language** | JavaScript (SQLX) | Jinja (SQL) |
| **Hosting** | Integrated in BigQuery Console | Self-hosted (Core) or dbt Cloud (SaaS) |
| **Cost** | Free (you pay for BQ compute) | Core: free; Cloud: ~$100/user/month |
| **Git integration** | GitHub, GitLab, Bitbucket | GitHub, GitLab, Azure DevOps |
| **Testing** | Assertions (basic) | Tests (generic, singular, custom) -- more mature |
| **Documentation** | Auto-generated, integrates with Data Catalog | Auto-generated docs site, more customizable |
| **Packages / macros** | JavaScript packages (npm-like) | dbt packages (hub.getdbt.com) -- larger ecosystem |
| **Scheduling** | Native (or via [[cloud-composer-guide]], Workflows) | dbt Cloud scheduler, or external (Airflow, etc.) |
| **Incremental models** | Supported | Supported (more strategies: merge, delete+insert, insert_overwrite) |
| **SCD Type 2 snapshots** | Limited / manual | Built-in snapshot strategy |
| **Community** | Small but growing | Large, active community and Slack |
| **Multi-environment** | Release configurations (compilation variables) | Profiles + targets |
| **IDE** | BigQuery Console (web-based) | dbt Cloud IDE or VS Code extension |

### Decision Framework: When to Choose Each

```
START: I need SQL transformations in my warehouse
|
+-- Is BigQuery your ONLY warehouse?
|   |
|   +-- YES: Does your team already know dbt?
|   |   |
|   |   +-- YES: dbt (leverage existing skills, larger ecosystem)
|   |   +-- NO: Continue
|   |       |
|   |       +-- Do you need SCD Type 2 snapshots?
|   |           |
|   |           +-- YES: dbt (built-in snapshot support)
|   |           +-- NO: Do you want zero additional tooling cost?
|   |               |
|   |               +-- YES: Dataform (free, native in BQ console)
|   |               +-- NO: Either works -- evaluate IDE preference
|   |
|   +-- NO (multi-warehouse): dbt (only option that supports multiple warehouses)
```

**Short version**:
- **Choose Dataform** if you are 100% BigQuery, want zero licensing cost, and prefer native console integration.
- **Choose dbt** if you use multiple warehouses, need the mature package ecosystem, require SCD Type 2 snapshots, or your team already knows Jinja/dbt.

## Key Configuration Decisions

### 1. Repository Structure

| Approach | When to Use |
|---|---|
| **Monorepo** (single repo, all domains) | Small team, shared conventions, simpler CI/CD |
| **Multi-repo** (one repo per domain) | Large org, domain ownership boundaries, independent release cycles |

For a learning/portfolio project, monorepo is almost always the right starting point.

### 2. Environment Separation

Use compilation variables in `dataform.json` (`"vars": {"env": "dev"}`) and override to `"prod"` in release configurations. This points all tables to the correct datasets without changing SQL.

### 3. Naming Conventions

Follow [[data-modeling-overview]] and [[sql-patterns]] conventions: `stg_` (staging), `int_` (intermediate), `fct_` (facts), `dim_` (dimensions). Map each prefix to a Dataform schema (`staging`, `intermediate`, `analytics`).

### 4. Assertion Strategy

Add assertions on every critical table: uniqueness on primary keys, non-null on required fields, custom SQL for domain rules (e.g., `loss_date <= reported_date`). See [[data-quality]] for the broader framework.

### 5. Scheduling

| Complexity | Recommended Scheduler |
|---|---|
| Simple daily/hourly SQL-only pipeline | Dataform native scheduling |
| SQL pipeline + non-SQL steps (API calls, file loads, ML) | [[cloud-composer-guide]] triggers Dataform via API |
| SQL pipeline + lightweight service orchestration | Cloud Workflows + Cloud Scheduler |

## Actuarial Example: Claims Transformation Pipeline

A complete Dataform project for an insurance claims warehouse, following [[data-modeling-overview]] principles:

```
definitions/
  declarations/
    raw_claims.sqlx          -- Source declaration
    raw_policies.sqlx        -- Source declaration
    raw_claimants.sqlx       -- Source declaration
  staging/
    stg_claims.sqlx          -- Clean, type-cast, deduplicate
    stg_policies.sqlx        -- Normalize policy data
    stg_claimants.sqlx       -- Standardize names, addresses
  intermediate/
    int_claim_enriched.sqlx  -- Join claims + policies + claimants
    int_loss_triangles.sqlx  -- Pivot claims into development triangles
  marts/
    fct_claim_reserves.sqlx  -- Fact table: one row per claim valuation
    fct_loss_development.sqlx -- Fact table: loss triangle cells
    dim_policies.sqlx         -- Dimension: policy attributes
    dim_claimants.sqlx        -- Dimension: claimant demographics
    dim_date.sqlx             -- Dimension: date spine
  assertions/
    assert_no_negative_reserves.sqlx
    assert_loss_date_before_reported.sqlx
    assert_triangle_completeness.sqlx
```

This pipeline transforms raw claims data into actuarial-ready tables for reserving analysis, loss development triangles, and experience studies -- all version-controlled and tested.

## Common Pitfalls

| Pitfall | Impact | Mitigation |
|---|---|---|
| JavaScript learning curve | SQL engineers unfamiliar with JS struggle with SQLX macros | Start with simple config blocks; introduce JS templating gradually |
| BigQuery-only lock-in | Cannot reuse Dataform models if you move to Snowflake or Databricks | Accept the trade-off if you are committed to GCP; otherwise consider dbt |
| Assertion limitations | Less expressive than dbt's generic/custom test framework | Write standalone assertion SQLX files for complex validation logic |
| No SCD Type 2 snapshots | Manual implementation required for slowly changing dimensions | Build custom incremental logic or evaluate dbt for snapshot-heavy workloads |
| Immature package ecosystem | Fewer community packages than dbt Hub | Write project-level `includes/` helpers; contribute to the ecosystem as it grows |
| Ignoring compilation variables | Hardcoded dataset names break environment separation | Always use `dataform.projectConfig.vars` for environment-dependent values |
| Skipping assertions | Bad data propagates silently to downstream tables | Enforce assertion coverage in code review; treat assertion failures as build failures |

## Related Docs

- [[bigquery-guide]] -- The warehouse Dataform targets exclusively
- [[sql-patterns]] -- SQL conventions and CTEs used within SQLX files
- [[data-modeling-overview]] -- Dimensional modeling principles that guide table design
- [[etl-vs-elt]] -- Dataform enables the "T" in ELT, transforming data inside BigQuery
- [[data-quality]] -- Assertion strategies and broader data quality frameworks
- [[orchestration]] -- How Dataform fits into larger pipeline orchestration
- [[cloud-composer-guide]] -- Triggering Dataform workflows from Airflow DAGs
- [[storage-format-selection]] -- Format decisions for data landing in GCS before BigQuery load
