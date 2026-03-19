# Future Ideas: Data Engineering Platform

Ideas organized by theme, not priority. Each one makes the platform deeper, broader, or more production-credible.

---

## Real Data Integration

- [ ] **freMTPL2 as default data source for P06**: The adapter already exists (`fremtpl_adapter.py`). Make it the default so the GLM trains on real actuarial data. A Gini coefficient on freMTPL2 is benchmarkable against published literature; on synthetic data it proves nothing. Keep synthetic as CI fallback (faster, deterministic).
- [ ] **CNSF open data for P01**: The Comision Nacional de Seguros y Fianzas publishes aggregate claims data by line of business. Load real Mexican insurance market aggregates into the warehouse as a `ref_market_benchmarks` table and compare portfolio performance against the market.
- [ ] **Banxico API for economic features**: Pull TIIE, inflation, and exchange rate series from Banxico's SIE API. Add as time-varying features in P06 to capture macroeconomic effects on claim severity (inflation drives repair costs up, peso depreciation affects imported parts pricing in auto).

## Data Quality & Observability

- [ ] **Great Expectations / Soda on each warehouse layer**: Define expectations (no nulls in PKs, referential integrity, claim amounts > 0, dates in valid range, loss ratios within 3 sigma of historical mean). Run as a pipeline step and fail loudly on violations. This is the most common interview question gap.
- [ ] **Data freshness monitoring**: BigQuery scheduled query that checks `MAX(processing_timestamp)` per table and alerts if stale beyond 24 hours. Add as a Terraform-managed Cloud Monitoring alert policy.
- [ ] **Structured logging with correlation IDs**: Each pipeline run gets a UUID. Every log line, BigQuery row, and Pub/Sub message carries it. When something breaks, `grep` the correlation ID across Cloud Logging to reconstruct the full trace.
- [ ] **Custom Cloud Monitoring metrics**: Rows processed per layer, transform latency, error rates, DLQ volume. Export from Cloud Run via the Monitoring API. Build a single Looker Studio dashboard showing platform health.

## Schema & Contract Evolution

- [ ] **Schema registry with Protobuf or JSON Schema**: Define claim event schemas in `schemas/` at the repo root. The Pub/Sub publisher validates against the schema before publishing; the subscriber validates on receipt. Version schemas (v1, v2) to support backward-compatible evolution.
- [ ] **Contract tests between projects**: P06 depends on P01's `fct_claims` schema. A contract test asserts that the columns P06 expects exist in P01's output. If P01 adds/removes a column, the contract test fails before P06 silently produces wrong results.
- [ ] **Coverage type mapping table**: P01 uses high-level types (`auto`, `home`, `liability`). P05 uses granular sub-types (`auto_colision`, `gastos_medicos_mayores`). Create a `dim_coverage_mapping` in the warehouse that normalizes streaming types to warehouse types. This is the single most-asked interview question about platform integration.

## Production Hardening

- [ ] **Idempotent streaming writes**: Use BigQuery's `insertId` parameter in P03's subscriber to deduplicate at the BigQuery level. For Beam batch writes, use `WRITE_TRUNCATE` per partition or add a post-write MERGE step.
- [ ] **Backfill strategy with date-parameterized execution**: Add a `--execution-date` parameter to the pipeline so it can be run for a specific day. Cloud Run accepts it as a query param; Dagster accepts it as a partition key. This enables re-running Saturday's failed pipeline on Monday without re-processing the entire history.
- [ ] **Dataset-level IAM instead of project-level**: The Terraform IAM module grants `bigquery.dataEditor` at the project level. Move to `google_bigquery_dataset_iam_member` for each dataset so the pipeline SA can only touch claims datasets, not everything in the project. Real security review flag.
- [ ] **Error handling: fail fast, not silently**: P01's `main.py` silently skips missing SQL files. P02's `runner.py` catches generic `Exception` and continues. Change to fail immediately on unexpected errors; only catch known recoverable errors (like transient network issues) with retry.

## Testing Depth

- [ ] **End-to-end integration test**: A single test at the repo root that generates data (P01) -> runs the warehouse pipeline (P01) -> triggers the orchestrator (P02) -> publishes a claim event (P03) -> verifies it lands in BigQuery. Run with DuckDB + Pub/Sub emulator. This one test proves the "integrated platform" claim.
- [ ] **Property-based testing for data generator**: Use `hypothesis` to test that the generator produces valid data for any seed, any policyholder count, and any date range. Catches edge cases like empty policy lists or zero-claim scenarios that fixed-seed tests miss.
- [ ] **Model baseline test for P06**: Assert that the Tweedie GLM beats a naive baseline (predict the mean pure premium for all policies). If it doesn't, the features aren't adding value. Also add coefficient sign checks: higher `coverage_limit` should increase predicted premium, older `age_band` should increase life insurance cost.
- [ ] **Dagster asset tests with `build_asset_context()`**: The current tests use `materialize()` which runs everything in sequence. Add individual asset tests that materialize a single asset with pre-populated upstream tables, verifying that each asset works in isolation.

## Architecture Extensions

- [ ] **Merge P03 + P05 into a single streaming project**: Two streaming projects with a comparison table in the README is a red flag. Consolidate into one project with two modes: batch (deployed, cost-effective) and streaming (local, Dataflow-ready). The progression from batch to true streaming is the story.
- [ ] **Replace sys.path hacks with proper packaging**: Add a `pyproject.toml` at the repo root or to P01 so downstream projects (P02, P06) can `pip install -e ../01-claims-warehouse` instead of manipulating `sys.path`. Makes imports explicit and IDE-friendly.
- [ ] **Reverse ETL: BigQuery to operational system**: Add a project that reads pricing adequacy results from BigQuery and pushes recommendations back to a mock policy administration system (a simple FastAPI endpoint). Demonstrates the full feedback loop: data in -> analytics -> decisions -> data out.
- [ ] **CDC (Change Data Capture) with Debezium**: Replace the batch CSV ingestion with a Debezium connector that streams changes from a PostgreSQL source (simulating a policy admin system) into Pub/Sub. Shows understanding of real-time data integration patterns used in production insurance systems.

## Actuarial Depth

- [ ] **Incurred loss triangles alongside paid**: The current `rpt_loss_triangle` uses cumulative paid losses. Add `rpt_loss_triangle_incurred` using paid + outstanding reserve. Actuaries use both: paid for long-tail lines (liability), incurred for short-tail (auto). The schema change: add `incurred_amount` to `fct_claim_payments`.
- [ ] **IBNR estimation with chain-ladder**: Compute age-to-age development factors from the loss triangle, apply chain-ladder to project ultimate losses, subtract cumulative paid to get IBNR reserve. Add `rpt_ibnr_estimate` with estimated ultimates by accident year. This is the actuarial analysis that sits on top of the warehouse.
- [ ] **Tweedie power parameter profiling**: The GLM hardcodes `var_power=1.5`. Add a grid search over `p` values (1.1 to 1.9) comparing deviance or AIC. Use `tweedie.profile` or manual log-likelihood profiling. Document the optimal `p` for the dataset and explain what it means actuarially.
- [ ] **Cross-validation for temporal insurance data**: Replace the single train/test split with time-series cross-validation (expanding window). With 6 accident years, use years 1-3 for training, 4 for validation; then 1-4 for training, 5 for validation; etc. Report mean and std of Gini across folds.
- [ ] **Regulatory compliance layer (LFPDPPP)**: Mexico's data protection law requires PII handling. Add column-level encryption for `first_name`, `last_name`, `date_of_birth` in the warehouse using BigQuery's AEAD functions or a crypto-shredding pattern (encrypt with per-policyholder keys, delete the key to "forget" the data). Add a `scripts/gdpr_delete.py` that removes a policyholder by destroying their encryption key.

## Developer Experience

- [ ] **Makefile at repo root**: `make test-all`, `make lint`, `make generate-data`, `make deploy-p01`. Saves the mental overhead of remembering which venv to activate and which directory to cd into.
- [ ] **Pre-commit hooks**: `ruff check`, `ruff format`, `terraform fmt`, `sqlfluff lint` on every commit. Prevent style drift without manual review.
- [ ] **Local dev environment with docker-compose**: Single `docker-compose up` that starts DuckDB, Pub/Sub emulator, Dagster UI, and the Streamlit dashboard. New contributor goes from clone to running platform in one command.

## Visualization & Storytelling

- [ ] **Looker Studio executive dashboard**: One dashboard connecting all projects: ingestion volume (P03), pipeline status (P02), warehouse KPIs (P01), pricing adequacy distribution (P06). The kind of view a Chief Actuary opens every morning.
- [ ] **Architecture diagram as interactive web page**: Replace the static Mermaid diagrams with a React/D3 interactive visualization showing data flow. Click on a component to see its status, last run time, row count. Deploy on the portfolio site.
- [ ] **Cost calculator**: A simple web tool where an employer inputs their data volume and the calculator estimates what this platform would cost at their scale. Shows you understand that cost is not fixed; it scales with usage patterns.
