# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Purpose

A comprehensive **Data Engineering knowledge base and portfolio repository** for an actuarial sciences graduate building deep expertise in the data engineering discipline. The project combines:
- **Decision-oriented documentation** (Obsidian-compatible markdown) covering DE fundamentals, tools, and architecture patterns
- **Hands-on portfolio projects** using actuarial/insurance domain data plus public datasets
- **Reusable tools and scripts** that grow in complexity as understanding deepens

Primary tech focus: **GCP-native infrastructure** (BigQuery, Composer, Dataflow, Pub/Sub, GCS, Dataform), with thorough coverage of alternatives and when to choose what.

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

## Content Philosophy

This is a **learning-first** repository. Every piece of code and documentation should help the author understand:
1. **What** the tool/pattern does
2. **Why** it exists (what problem it solves)
3. **When** to use it vs alternatives (decision criteria)
4. **How** it works under the hood (enough to debug, not academic depth)

When adding new content, prioritize building on existing docs via `[[wikilinks]]` rather than creating standalone pages. The knowledge graph connections are as valuable as the content itself.
