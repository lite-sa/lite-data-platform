"""Descriptions in the persisted dbt folders must fit BigQuery.

persist_docs copies every model and column description onto the BigQuery
relation, and BigQuery rejects a column description longer than 1,024
characters, which fails the whole build. Tables allow more, but Metabase
shows the table description as one paragraph, so the same cap applies.
Static check over the YAML, no warehouse needed (CI has no credentials).
"""

from pathlib import Path

import yaml

DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"
PERSISTED_FOLDERS = ("models/marts", "models/litecore")
BIGQUERY_DESCRIPTION_LIMIT = 1024


def _descriptions():
    for folder in PERSISTED_FOLDERS:
        for path in sorted((DBT_PROJECT_DIR / folder).rglob("*.yml")):
            doc = yaml.safe_load(path.read_text()) or {}
            for model in doc.get("models", []):
                yield f"{model['name']}", model.get("description") or ""
                for column in model.get("columns", []):
                    yield f"{model['name']}.{column['name']}", column.get("description") or ""


def test_descriptions_fit_bigquery():
    too_long = [
        f"{name}: {len(text.strip())} chars"
        for name, text in _descriptions()
        if len(text.strip()) > BIGQUERY_DESCRIPTION_LIMIT
    ]
    assert not too_long, "\n".join(too_long)


def test_persisted_descriptions_exist():
    assert sum(1 for _ in _descriptions()) > 0
