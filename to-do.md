# To-Do

Last updated: 2026-03-16

## Status Snapshot

| Project | Code | Tests | Local Run | GCP Deploy |
|---------|------|-------|-----------|------------|
| 01-claims-warehouse | done | done | done | done |
| 02-orchestrated-elt | done | done | done | done |
| 03-streaming-claims-intake | done | done | done | done |
| 04-data-platform-terraform | done | n/a | plan only | done |
| 05-streaming-claims-pipeline | done | done (42) | done | not started |
| 06-pricing-ml-pipeline | done | done (30) | done | not started |

Knowledge base: 34 docs written, 0 referenced in INDEX.md but not yet created.
CI/CD: GitHub Actions workflow covers projects 01, 02, 03, 05, 06 + Terraform fmt for 04.
`tools/`: 3 scripts (bq_cost_estimator, gcs_profiler, config_loader). `scripts/`: setup_dev_env.sh.

---

## 1. GCP Deployment & Validation

- [x] [L] Deploy Project 01 to BigQuery + GCS (load sample data, run Dataform) (2026-03-16)
- [x] [L] Deploy Project 02 to Cloud Run + Cloud Scheduler (end-to-end ELT trigger) (2026-03-16)
- [x] [L] Deploy Project 03 to Pub/Sub + Cloud Run subscriber + Dataflow batch job (2026-03-16)
- [x] [M] Run `terraform plan` against a live GCP project for Project 04 (2026-03-16)
- [x] [M] Run `terraform apply` and validate all resources created (2026-03-16)
- [x] [S] Write a deployment log doc (`docs/deployment-log.md`) capturing what worked, what broke, config gotchas (2026-03-16)
- [x] [S] Add "What Broke During Deployment" notes to each project README (per-project, not just the log) (2026-03-16)
- [x] [S] Add deployment evidence to each project README: GCP console screenshots, cost estimates, live URLs (2026-03-16)
- [x] [S] Add a **Deployment** section to each project README (URL, evidence, cost, gotchas) (2026-03-16)

## 2. Projects -- Remaining Build

### Unbuilt projects
- [x] [L] `projects/05-streaming-claims-pipeline` -- full implementation (42 tests passing) (2026-03-15)
- [x] [L] `projects/06-pricing-ml-pipeline` -- full implementation (30 tests passing) (2026-03-15)

### Polish for all implemented projects (01, 02, 03, 04)
- [ ] [M] Add screenshots/terminal output evidence to each project README
- [ ] [M] Add integration/E2E tests for Projects 03 and 04

## 3. CI/CD & Quality

- [ ] [M] Fix/validate GitHub Actions workflow runs green (push and verify)

## 4. Knowledge Base -- Docs

### Existing docs -- promote to complete
- [ ] [S] Review and promote 6 GCP tool guides (bigquery, cloud-composer, dataflow, dataform, gcs-as-data-lake, pubsub) -- add `status: complete` frontmatter

## 5. Visualization & Presentation

- [x] [L] Build Streamlit dashboard deployed to Cloud Run (public URL) -- 4 pages (2026-03-16)
- [ ] [S] Add Dataform dependency graph (screenshot or generated) to Project 01 README

---

## Done

- [x] Add "Decisions & Trade-offs" table to P01, P02, P03, P04 READMEs (2026-03-15)
- [x] Add "What I Would Change" retrospective to P01, P02, P03, P04 READMEs (2026-03-15)
- [x] Write `docs/tools/ci-cd-for-data.md` (215 lines) (2026-03-15)
- [x] Write `docs/tools/infrastructure-as-code.md` (219 lines) (2026-03-15)
- [x] Write `docs/tools/monitoring-observability.md` (284 lines) (2026-03-15)
- [x] Create `tools/bq_cost_estimator.py` -- BigQuery dry-run cost estimator (2026-03-15)
- [x] Create `tools/gcs_profiler.py` -- CSV/Parquet data profiler (2026-03-15)
- [x] Create `tools/requirements.txt` -- dependencies for tool scripts (2026-03-15)
- [x] Add Projects 03 and 04 to CI pipeline (lint + test steps) (2026-03-15)
- [x] Add CI status badge to top-level README.md (2026-03-15)
- [x] Set up pre-commit hooks (ruff lint/format, trailing whitespace, YAML lint) (2026-03-15)
- [x] Add `terraform fmt --check` step to CI (2026-03-15)
- [x] Remove `continue-on-error: true` from Project 02 test step (2026-03-15)
- [x] Write `docs/fundamentals/data-warehouse-concepts.md` (2026-03-15)
- [x] Write `docs/fundamentals/storage-layer.md` (2026-03-15)
- [x] Write `docs/fundamentals/compute-layer.md` (2026-03-15)
- [x] Write `docs/fundamentals/data-governance.md` (2026-03-15)
- [x] Write `docs/architecture/reference-architectures.md` (2026-03-15)
- [x] Write `docs/architecture/cost-optimization.md` (2026-03-15)
- [x] Write `docs/fundamentals/testing-strategies.md` (2026-03-15)
- [x] Write `docs/fundamentals/schema-evolution.md` (2026-03-15)
- [x] Promote 4 strong drafts to status: complete (batch-vs-stream, orchestrator-selection, loss-triangle-construction, data-quality) (2026-03-15)
- [x] Add missing frontmatter to existing tool guides (verified all had it) (2026-03-15)
- [x] Remove empty `docs/diagrams/` dir -- diagrams are inline (2026-03-15)
- [x] Create `tools/config_loader.py` -- centralized GCP config loader (2026-03-15)
- [x] Create `scripts/setup_dev_env.sh` -- dev environment bootstrap (2026-03-15)
- [x] Create data lineage Mermaid diagram in root README (2026-03-15)
- [x] Add test summary table to root README (2026-03-15)
- [x] Verify all project READMEs have consistent structure (2026-03-15)
- [x] Add missing frontmatter to P03 README (2026-03-15)
- [x] Add missing Tech Stack table to P04 README (2026-03-15)
- [x] Build Project 05: Streaming Claims Pipeline -- 18 files, 42 tests passing (2026-03-15)
- [x] Build Project 06: Pricing ML Feature Pipeline -- 21 files, 30 tests passing (2026-03-15)
- [x] Add P05 and P06 to CI/CD workflow (lint + test steps) (2026-03-15)
- [x] Move P05/P06 from "Planned" to main Projects table in root README (2026-03-15)
- [x] Update test summary to 185 tests across 5 projects (2026-03-15)
