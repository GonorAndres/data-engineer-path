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
| Dashboard | https://data-engineer.gonor.me | Public | P01 Claims Warehouse | Live (PostHog `app_id: claims-dashboard`) |
| ELT Pipeline | https://dev-claims-elt-pipeline-451451662791.us-central1.run.app | Internal (IAM-auth, owner-only) | P02 Orchestrated ELT | Live (internal) |
| Subscriber | https://dev-claims-subscriber-451451662791.us-central1.run.app | Internal (IAM-auth, owner-only) | P03 Streaming Intake | Live (internal) |
| CI/CD badge | https://github.com/GonorAndres/data-engineer-path/actions/workflows/ci-cd.yml | Public | All projects | Live |

**Visibility key:**
- `Public` -- unauthenticated, safe to share externally (blog, resume, interviews)
- `Internal (IAM-auth, owner-only)` -- deployed to GCP and reachable only by the repo owner via IAM auth; not a transient state, do NOT share these URLs
- `Private` -- not deployed / local only

**Status key:** `Live`, `Live (internal)`, `Torn down`, `Not deployed`

### The dashboard answers on three hostnames, only one is canonical

`data-engineer.gonor.me` is a Cloud Run domain mapping, and a mapping is a route *in*, not
a replacement. Both `claims-dashboard-451451662791.us-central1.run.app` and
`claims-dashboard-d3qj5vwxtq-uc.a.run.app` keep answering for as long as the service
exists; they cannot be turned off. Left alone that splits traffic three ways and scatters
the search ranking -- measured elsewhere in this portfolio at 41-44% of pageviews landing
on the unbranded provider URL.

So the app 301s them. `canonical_host_redirect` in `dashboard/main.py` matches the
`.run.app` suffix (Cloud Run has no preview hostnames to protect, unlike Pages or Vercel)
and redirects to the canonical host with path and query preserved. It is gated on the
`CANONICAL_REDIRECT_HOST` env var, injected by the `deploy-dashboard` job: unset means no
redirect, which is what local development needs and what kept the service up during the
window before DNS propagated. Every page also carries a `<link rel="canonical">` at the
custom domain regardless of which hostname served it.

`health-check.yml` asserts both halves -- 200 on the custom domain, and a 301 from the
provider URL to the matching path. The redirect is what keeps already-published links
alive (blog posts, the portfolio site, the CV), so a silent regression there is worse than
an outage: nothing breaks visibly, traffic just starts splitting again.

### Rebuilding the claims warehouse (P01)

The dashboard is a presentation layer over BigQuery. When it renders its shell but every
panel reports `Not found: Table dev_claims_analytics.fct_claims`, the warehouse is gone,
not the app -- do not redeploy Cloud Run. This happened on 2026-08-14 with all four
`dev_claims_*` datasets empty. Nothing was lost: the raw CSVs live in
`gs://dev-claims-data-<PROJECT_ID>/raw/` and the Dataform repository survives
independently.

Rebuild in this order. Each step below cost real time to rediscover.

1. **Load the raw layer first.** The `raw_*` models are Dataform `declaration`s -- Dataform
   reads them and never creates them, so running the workflow against empty
   `dev_claims_raw` fails at the first staging model. `scripts/setup_gcp.sh` has the loads.

2. **`coverages.csv` needs an explicit schema.** It is 5 rows of reference data, three
   string columns, ~470 bytes. `--autodetect` cannot separate the header from the data at
   that size: it loads the header as a row and names the columns `string_field_0..2`.
   `stg_coverages` then fails with `Unrecognized name: coverage_type` and skips the 10
   models downstream of it -- one tiny file taking out most of the warehouse. The script
   declares its schema with `--skip_leading_rows=1`; the other four autodetect fine.

3. **Run the workflow** with `python scripts/deploy_dataform.py --project <PROJECT_ID>`.
   It needs Application Default Credentials, which `gcloud auth login` does *not* provide
   -- run `gcloud auth application-default login` separately.

Two IAM facts the API errors do not make obvious:

- The invocation must name a service account or it is rejected with *"Service account must
  be set when strict act as checks are enabled"*. `deploy_dataform.py` passes
  `--service-account`, defaulting to `dev-claims-pipeline-sa`, which holds
  `bigquery.dataEditor` and `bigquery.jobUser`.
- The Dataform service agent `service-<PROJECT_NUMBER>@gcp-sa-dataform.iam.gserviceaccount.com`
  needs `roles/iam.serviceAccountTokenCreator` on that account. **It takes roughly two
  minutes to propagate, and retries before then fail with the identical error** -- which
  reads exactly like the grant did not work. Wait before concluding anything.

`--dry-run` is not write-free. It still uploads the local `dataform/` tree into
`deploy-workspace` and compiles; it skips only the BigQuery execution.

A healthy rebuild produces 16 tables across staging, intermediate, analytics and reports
with every assertion passing. Cost is negligible -- 281 KiB of source CSV, loads unbilled,
queries far inside the free tier, with `max_bytes_billed` capped at 10 GiB per query.

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
- **The dashboard is FastAPI + Jinja2, not Streamlit.** It was rewritten in PR #20; the
  `patch_streamlit_index.py` workaround and its `st.html` caveats are gone with it. The
  PostHog snippet now renders straight into `templates/base.html` from
  `dashboard/utils/analytics.py`. Token is a public write-only key (`phc_...`), safe to commit.
- **PostHog on the dashboard follows the portfolio standard** (the `posthog-analytics` skill
  in `GonorAndres/gonor-skills` is authoritative). All five parts are in place:
  `app_id: 'claims-dashboard'` -- the only reliable site separator, since all 13 portfolio
  sites report into one project and `$host` therefore cannot tell them apart --
  `capture_pageview: 'history_change'`, an `environment` derived from `location.hostname`,
  `deployment_platform: 'cloud-run'`, and a same-origin `/ingest` proxy served by the app
  itself (`posthog_proxy` in `main.py`, forwarding `/ingest/static/*` to
  `us-assets.i.posthog.com` and everything else to `us.i.posthog.com`). `api_host` is
  `location.origin + '/ingest'` rather than a hardcoded host, so it stays same-origin on
  whichever hostname served the page and needs no CORS.
- **Runtime env vars for Cloud Run services** are injected by the deploy jobs via the `env_vars:` input of `google-github-actions/deploy-cloudrun@v2`, not stored on the service. This is load-bearing: the dashboard previously shipped with zero env vars across 3 revisions and crashed at import on a missing `GCP_PROJECT_ID`. The workflow is now the single source of truth -- every new revision ships with the required vars. When adding a new runtime env var, add it to the relevant deploy job's `env_vars` block rather than running `gcloud run services update` manually. Dashboard code (`utils/bq_client.py`) reads `GCP_PROJECT_ID` first, falls back to Google's conventional `GOOGLE_CLOUD_PROJECT`, and raises a clear `RuntimeError` at startup if neither is set -- so a missed CI injection surfaces as an obvious boot-time error, not a silent `KeyError` on every page load.
- **Scheduled health-check** at `.github/workflows/health-check.yml` pings Public-visibility URLs on a weekly cron and fails on non-200. Only URLs in this registry marked `Public` should be added to that workflow; `Internal` URLs are owner-only and not health-checked from CI.
- **ruff is pinned, on purpose.** The lint job installed `ruff>=0.4.0` until 2026-08-14, which resolves to whatever is newest when the job runs -- so the gate's verdict tracked ruff's release schedule rather than this repo. 0.16.3 shipped, enabled rules the code predates, and P02 started failing with 10 findings in files untouched since the last green run, blocking an unrelated PR. It is now `ruff==0.16.4`, raised from 0.15.21 with every finding it surfaced fixed in the same PR. Keep doing it that way, and reproduce CI locally with the pinned version rather than whatever `pip install ruff` gives you.
- **P02 lint debt is cleared.** The nine mechanical findings were auto-fixed. Of the three
  `BLE001` blind-excepts, only one was a real defect: `_row_counts_for_layer` in `assets.py`
  swallowed the exception into a `-1` sentinel with no trace, so a missing table and a
  permissions error looked identical in the Dagster UI. It logs with `exc_info` now, and
  ruff stops flagging BLE001 once the error goes somewhere, so that site needs no `noqa`.
  The two in `runner.py` keep their broad catch behind a `noqa` with the reason written
  next to it: `execute_sql_layer` returns partial results plus the list of files that
  failed, and `run_full_pipeline` is the outermost boundary that turns any failure into a
  reportable result. Narrowing either would change the contract the project is built on.
- **What is still not linted at all:** CI runs ruff over `projects/0X/src/` only. The
  dashboard under `projects/01-claims-warehouse/dashboard/` is the one public-facing
  service in the repo and no lint job touches it. Four findings sit there, one of them the
  minified PostHog loader line, which cannot be shortened and would need a per-file ignore.
  Closing the gap also means a `ruff format` pass over `main.py`. Worth its own PR.

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
