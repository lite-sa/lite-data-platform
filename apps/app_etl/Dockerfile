# Build context is the repo root (uv workspace: root pyproject.toml + uv.lock
# cover every apps/* member). Build from the root, e.g.:
#   docker build -f apps/app_etl/Dockerfile -t app-etl .
FROM python:3.13-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /uvx /usr/local/bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:${PATH}"

# Two-step sync so the third-party venv layer (~350MB) only rebuilds —
# and re-pushes — when the lockfile changes, not on every code edit.
COPY pyproject.toml uv.lock ./
COPY apps/app_etl/pyproject.toml apps/app_etl/pyproject.toml
RUN uv sync --locked --no-dev --package app-etl --no-install-workspace

COPY apps/app_etl/app_etl apps/app_etl/app_etl
RUN uv sync --locked --no-dev --package app-etl

# The dbt project rides in the same image (dbt-bigquery is already a runtime
# dep of app-etl); the transform Cloud Run Job overrides the entrypoint with
# `dbt build`, pointed here via DBT_PROJECT_DIR/DBT_PROFILES_DIR env vars
# (not --args: gcloud's --args rejects duplicate values, and both flags
# would take this same path). target/ and logs/ stay out via .dockerignore.
# /app is root-owned and the job runs as appuser, so dbt runs must set
# DBT_TARGET_PATH and DBT_LOG_PATH to a writable dir (e.g. under /tmp).
COPY apps/app_etl/dbt apps/app_etl/dbt

RUN useradd --create-home --uid 10001 appuser
USER appuser

# No default job on purpose: every Cloud Run Job sets its own command
# (`--command=python --args="-m,app_etl.ingestion.<database>"`, or dbt build
# for the transform job). A bare run fails loudly instead of silently
# succeeding as a no-op execution.
ENTRYPOINT ["python", "-c", "import sys; sys.exit('no default job: run python -m app_etl.ingestion.<database>, or dbt build')"]
