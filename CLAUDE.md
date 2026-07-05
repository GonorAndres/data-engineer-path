# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

A comprehensive **Data Engineering knowledge base and portfolio repository** for an actuarial sciences graduate building deep expertise in the data engineering discipline. The project combines:
- **Decision-oriented documentation** (Obsidian-compatible markdown) covering DE fundamentals, tools, and architecture patterns
- **Hands-on portfolio projects** using actuarial/insurance domain data plus public datasets
- **Reusable tools and scripts** that grow in complexity as understanding deepens

Primary tech focus: **GCP-native infrastructure** (BigQuery, Cloud Run, Cloud Scheduler, Dataflow, Pub/Sub, GCS, Dataform), with thorough coverage of alternatives and when to choose what. Cloud Composer was intentionally rejected in favor of Cloud Run + Cloud Scheduler on cost grounds -- see P02 decision docs.

## Repository Structure

```
data-enginer/
├── docs/                    # Knowledge base (Obsidian vault)
│   ├── fundamentals/        # Core DE concepts: modeling, SQL, ETL/ELT, etc.
│   ├── tools/               # Tool-specific guides: BigQuery, Airflow, dbt, etc.
│   ├── architecture/        # Architecture patterns, reference architectures
│   ├── decisions/           # Decision frameworks: "when to use X vs Y"
│   └── diagrams/            # Mermaid/draw.io diagrams for data flows
├── projects/                # Hands-on portfolio projects (each self-contained)
│   └── <project-name>/     # Each project has its own README, src/, data/, tests/
├── tools/                   # Reusable scripts and utilities
├── scripts/                 # One-off helper scripts (setup, data download, etc.)
└── subagents_outputs/       # Claude Code subagent working files (gitignored)
```

## Live Deployments & Public Links

This section is the **single source of truth** for all live URLs related to this repository. Update it whenever a service is deployed, torn down, or changes visibility.

### Public narrative entry points

These are the two surfaces used to share this portfolio externally (recruiters, peers, etc.):

- **Blog walkthrough**: https://gonorandres.github.io/blog/data-engineering-platform/ -- decision-oriented narrative across all six projects
- **GitHub repository**: https://github.com/GonorAndres/data-engineer-path -- code, docs, and CI badge

### Deployment registry

| Ref | URL | Visibility | Project | Status |
|-----|-----|------------|---------|--------|
| Dashboard | https://claims-dashboard-451451662791.us-central1.run.app | Public | P01 Claims Warehouse | Live (PostHog tracking active) |
| ELT Pipeline | https://dev-claims-elt-pipeline-451451662791.us-central1.run.app | Internal (IAM-auth, owner-only) | P02 Orchestrated ELT | Live (internal) |
| Subscriber | https://dev-claims-subscriber-451451662791.us-central1.run.app | Internal (IAM-auth, owner-only) | P03 Streaming Intake | Live (internal) |
| CI/CD badge | https://github.com/GonorAndres/data-engineer-path/actions/workflows/ci-cd.yml | Public | All projects | Live |

**Visibility key:**
- `Public` -- unauthenticated, safe to share externally (blog, resume, interviews)
- `Internal (IAM-auth, owner-only)` -- deployed to GCP and reachable only by the repo owner via IAM auth; not a transient state, do NOT share these URLs
- `Private` -- not deployed / local only

**Status key:** `Live`, `Live (internal)`, `Torn down`, `Not deployed`

### Maintenance rules

- Each `projects/<name>/README.md` **Deployment** section must list its own URL (if any), and that URL must match this registry.
- When deploying, tearing down, or changing visibility of a service, update this table in the same commit.
- Do not add links to private resources (Google Drive folders, personal docs, academic material) here -- this table is for repo-associated services only.

### CI integration

- **Branching**: long-lived `dev` integration branch. Feature branches merge into `dev` (lint + test only); `dev` promotes to `main` via PR (full pipeline: lint + test + build + deploy). Both branches are protected; `main` requires a PR plus green status checks, `dev` requires only status checks.
- **GitHub Environment**: a single `production` environment holds the GCP WIF secrets (`GCP_WIF_PROVIDER`, `GCP_WIF_SERVICE_ACCOUNT`, `GCP_PROJECT_ID`) used by both the `build` and `deploy` jobs in `.github/workflows/ci-cd.yml`. There is no separate dev environment -- P02/P03 already serve that role as owner-only internal services in GCP.
- **Deploy targets**: two Cloud Run services managed by the same workflow.
  - `dev-claims-elt-pipeline` (P02) with `--no-allow-unauthenticated` — rebuilt on every `main` push, preserves IAM-auth visibility.
  - `claims-dashboard` (P01) with `--allow-unauthenticated` — path-filtered via `dorny/paths-filter@v3`, only rebuilt when `projects/01-claims-warehouse/dashboard/**` changes. Flags match the live service: `--memory=512Mi --cpu=1 --max-instances=2`. Runtime env vars (currently `GCP_PROJECT_ID`) are injected via the deploy action's `env_vars:` block — see the runtime env vars bullet below.
- Full pipeline end-to-end verified 2026-04-12, covering **both** path-filter outcomes: a PR that does not touch `dashboard/**` correctly skips `build-dashboard` + `deploy-dashboard` (PR #15), and a PR that does touch `dashboard/**` correctly triggers both jobs and produces a fresh Cloud Run revision with the required env vars (PR #17, revision `claims-dashboard-00004-zbp`).
- **PostHog web analytics** is live on the dashboard. The snippet is baked into Streamlit's static `index.html` at Docker build time by `projects/01-claims-warehouse/dashboard/scripts/patch_streamlit_index.py` (runs inside the `Dockerfile`) -- `st.html` strips `<script>` tags and `st.components.v1.html` scopes to an iframe, neither works for exposing `window.posthog` on the parent page. Token is a public write-only key (`phc_...`), safe to commit; mirrors the `data-analyst-path` portfolio.
- **Runtime env vars for Cloud Run services** are injected by the deploy jobs via the `env_vars:` input of `google-github-actions/deploy-cloudrun@v2`, not stored on the service. This is load-bearing: the dashboard previously shipped with zero env vars across 3 revisions and crashed at import on a missing `GCP_PROJECT_ID`. The workflow is now the single source of truth -- every new revision ships with the required vars. When adding a new runtime env var, add it to the relevant deploy job's `env_vars` block rather than running `gcloud run services update` manually. Dashboard code (`utils/bq_client.py`) reads `GCP_PROJECT_ID` first, falls back to Google's conventional `GOOGLE_CLOUD_PROJECT`, and raises a clear `RuntimeError` at startup if neither is set -- so a missed CI injection surfaces as an obvious boot-time error, not a silent `KeyError` on every page load.
- **Scheduled health-check** at `.github/workflows/health-check.yml` pings Public-visibility URLs on a weekly cron and fails on non-200. Only URLs in this registry marked `Public` should be added to that workflow; `Internal` URLs are owner-only and not health-checked from CI.

## Conventions

### Documentation (docs/)
- All docs are **Obsidian-compatible markdown**: use `[[wikilinks]]` for cross-references between notes
- Each doc starts with a YAML frontmatter block: `tags`, `status` (draft/review/complete), `created`, `updated`
- Decision docs follow a consistent format: Context > Options > Trade-offs > Recommendation > When to revisit
- Diagrams use **Mermaid** syntax (rendered natively in Obsidian and GitHub)
- Theory is decision-oriented: focus on WHEN to use, trade-offs, and selection criteria rather than textbook definitions

### Projects (projects/)
- Each project is self-contained with its own `README.md`, `requirements.txt` or `pyproject.toml`
- Projects use actuarial/insurance domain data where possible (claims, pricing, mortality, exposure)
- Every project README states: **What it demonstrates**, **Tech stack**, **How to run**
- Data files go in `<project>/data/sample_data/` (small samples committed) and `<project>/data/` (large files gitignored)

### Tools and Scripts
- Reusable tools in `tools/` include clear docstrings explaining WHAT they do and WHY they're useful
- Scripts in `scripts/` are for setup/one-off tasks

### Python
- Python 3.10+ target
- Use `pyproject.toml` for project-level dependencies
- Prefer: `polars` over `pandas` for new work (performance), `duckdb` for local SQL analytics
- Follow Google Python Style Guide (the user works in GCP ecosystem)

### SQL
- BigQuery SQL dialect as primary, note dialect differences where relevant
- Use CTEs over subqueries for readability
- Name conventions: `snake_case` for tables and columns, prefix staging tables with `stg_`, intermediate with `int_`, final with `fct_` (facts) or `dim_` (dimensions)

## Key Commands

```bash
# Python environment (from any project directory)
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Run a specific project
cd projects/<project-name>
python src/main.py

# DuckDB local analytics (if installed)
duckdb <database.db> < query.sql

# GCP authentication (when working with GCP services)
gcloud auth application-default login
gcloud config set project <PROJECT_ID>

# Obsidian vault -- the docs/ folder IS the vault, open it directly in Obsidian
```

## Production & Deployment Standards

- Every project README must include a **Deployment** section with: live URL (or "internal"), GCP console screenshot, cost estimate (monthly), and "What Broke During Deployment" notes
- Every project README must include a **Decisions & Trade-offs** table: what was chosen, alternatives considered, and why
- Every project README must include a **What I Would Change** retrospective subsection
- CI must pass (lint + test) before any deployment. Green CI badge required on root README.
- Deployment evidence is mandatory: screenshots, terminal output, or logs proving the service ran in GCP -- not just local execution

## Improvement Roadmap

Two expert audits (2026-05-03) live in `subagents_outputs/` and should guide future work:

- **`subagents_outputs/de_expert_audit.md`** -- Data engineering best-practices audit. Covers schema evolution, monitoring, exactly-once semantics, incremental loads, security hardening. Prioritized tier list with time estimates.
- **`subagents_outputs/portfolio_review.md`** -- Portfolio impact review from a recruiter/hiring-manager perspective. Covers what impresses, what hurts credibility, actionable improvements ranked by impact, future feature roadmap, and recruiter perception by company (Rappi, Kavak, Clip, Nubank, MercadoLibre, FAANG).

When starting new work on this repo, check these documents for context on what to prioritize next.

## Content Philosophy

This is a **learning-first** repository. Every piece of code and documentation should help the author understand:
1. **What** the tool/pattern does
2. **Why** it exists (what problem it solves)
3. **When** to use it vs alternatives (decision criteria)
4. **How** it works under the hood (enough to debug, not academic depth)

When adding new content, prioritize building on existing docs via `[[wikilinks]]` rather than creating standalone pages. The knowledge graph connections are as valuable as the content itself.
