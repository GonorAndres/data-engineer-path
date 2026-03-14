# =============================================================================
# Dockerfile -- Orchestrated ELT Pipeline
# =============================================================================
#
# Multi-stage build for the insurance claims ELT pipeline.
#
# Design decisions:
#   - python:3.12-slim over alpine: glibc compatibility with DuckDB, NumPy,
#     and other C-extension packages.  Alpine's musl libc causes build failures
#     and runtime segfaults with these libraries.
#   - Single stage (not multi-stage): the pipeline is lightweight enough that
#     a builder stage adds complexity without meaningful size savings.  DuckDB
#     and NumPy are runtime deps, not just build deps.
#   - Non-root user: defense-in-depth for Cloud Run.  Cloud Run already
#     sandboxes containers, but least-privilege is a good habit.
#   - PYTHONPATH set to /app/src: matches the project layout so imports
#     resolve the same way locally and in the container.
#
# Build:
#   docker build -t claims-elt-pipeline .
#
# Run locally:
#   docker run --rm claims-elt-pipeline                    # Run pipeline
#   docker run --rm -p 8080:8080 claims-elt-pipeline \     # Cloud Run mode
#       python /app/cloud_run/entrypoint.py
#
# =============================================================================

FROM python:3.12-slim AS runtime

# -- Metadata labels (OCI standard) ------------------------------------------
# These show up in Artifact Registry and `docker inspect`.
LABEL org.opencontainers.image.title="claims-elt-pipeline"
LABEL org.opencontainers.image.description="Insurance claims ELT pipeline with DuckDB transforms"
LABEL org.opencontainers.image.source="https://github.com/andtega349/data-engineer-path"
LABEL org.opencontainers.image.authors="Andres Gonzalez Ortega"

# -- System setup -------------------------------------------------------------
# Install only what we need: curl for healthchecks, no dev tooling in prod.
# Combining RUN commands reduces image layers.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# -- Non-root user ------------------------------------------------------------
# Cloud Run runs as root by default, but we create a dedicated user for
# defense-in-depth.  The user needs write access to /app/data for DuckDB files.
RUN groupadd --gid 1000 pipeline \
    && useradd --uid 1000 --gid pipeline --create-home pipeline

# -- Working directory --------------------------------------------------------
WORKDIR /app

# -- Install Python dependencies first (layer caching) -----------------------
# Copy pyproject.toml separately so that dependency installation is cached
# unless pyproject.toml changes.  This saves minutes on rebuilds when only
# source code changes.
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir ".[dev]"

# -- Copy source code ---------------------------------------------------------
COPY src/ src/
COPY cloud_run/ cloud_run/
COPY tests/ tests/

# -- Copy SQL files from Project 01 ------------------------------------------
# The pipeline reads SQL files at runtime.  In a real deployment these would
# come from a shared package or be embedded in the container.
# During docker build, you need to provide these via build context or
# docker-compose volume mounts (see docker-compose.yml).
# For standalone builds, copy them into the image:
#   COPY ../01-claims-warehouse/sql/ sql/
# We create the directory structure so the pipeline has a place to look.
RUN mkdir -p sql/staging sql/intermediate sql/marts sql/reports \
    && mkdir -p data/sample_data

# -- Environment variables ----------------------------------------------------
# PYTHONPATH: lets `import data_generator` work from src/
# PYTHONUNBUFFERED: ensures print() and logging output appears immediately
#   in Cloud Logging (otherwise Python buffers stdout in non-TTY mode).
# PORT: Cloud Run injects this; default to 8080 for local testing.
ENV PYTHONPATH=/app/src \
    PYTHONUNBUFFERED=1 \
    PORT=8080

# -- Health check --------------------------------------------------------------
# Docker HEALTHCHECK (used by docker-compose; Cloud Run has its own probe).
# Checks the /health endpoint every 30 seconds.
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:${PORT}/health || exit 1

# -- Switch to non-root user --------------------------------------------------
RUN chown -R pipeline:pipeline /app
USER pipeline

# -- Default entrypoint -------------------------------------------------------
# Default: run the pipeline directly (used by Cloud Scheduler via Cloud Run).
# Override with `docker run ... python cloud_run/entrypoint.py` for HTTP mode.
CMD ["python", "-m", "pipeline"]
