# Build context is the repo root (uv workspace: root pyproject.toml + uv.lock
# cover every apps/* member). Build from the root, e.g.:
#   docker build -f apps/app_etl/Dockerfile -t sample-ingest .
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.25 /uv /uvx /usr/local/bin/

WORKDIR /app

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/app/.venv/bin:${PATH}"

COPY pyproject.toml uv.lock ./
COPY apps/app_etl/pyproject.toml apps/app_etl/pyproject.toml
COPY apps/app_etl/app_etl apps/app_etl/app_etl

RUN uv sync --locked --no-dev --package app-etl

RUN useradd --create-home --uid 10001 appuser
USER appuser

ENTRYPOINT ["python", "-m", "app_etl.sample_ingest"]
