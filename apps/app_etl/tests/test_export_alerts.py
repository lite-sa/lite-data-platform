"""Pure-function coverage for the alert exporter — serialization and the
scenario registry, no GCP. The BQ/GCS path is exercised against dev (same
posture as the dbt models: CI parses, dev runs)."""

import json
import re
from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from app_etl.export.aml_alerts import SCENARIOS, _json_value, to_jsonl


def test_to_jsonl_matches_the_aml_008_record_contract():
    """One row shaped exactly like the exporter's query result for AML-008:
    the fixed spine in table order (the suppression block already dropped,
    scenario-specific data only inside `evidence`), `evidence` as the
    decoded dict the BigQuery client returns for a JSON column — it must
    land as a nested object, never a re-encoded string. Changing the spine
    or a scenario's evidence shape should force this test to be updated
    consciously."""
    rows = [
        {
            "scenario_id": "AML-008",
            "alert_id": "ab" * 16,
            "scenario_name": "POS transactions outside business hours",
            "target_level": "merchant",
            "target_id": "m-1",
            "run_date": date(2026, 7, 16),
            "evaluated_at": datetime(2026, 7, 16, 3, 45, tzinfo=timezone.utc),
            "evidence": {
                "rule": [
                    {
                        "feature": "night_authorize_count",
                        "value": 12,
                        "operator": ">=",
                        "threshold": 10,
                    }
                ],
                # inside the JSON column these are already strings/None,
                # not date objects — mirror what the client hands back
                "context": {
                    "behavior_date": "2026-07-16",
                    "example_night_pos_payment_id": "pay-1",
                    "example_night_pos_terminal_id": "term-9",
                },
            },
        }
    ]

    lines = to_jsonl(rows).splitlines()

    assert len(lines) == 1
    assert json.loads(lines[0]) == {
        "scenario_id": "AML-008",
        "alert_id": "ab" * 16,
        "scenario_name": "POS transactions outside business hours",
        "target_level": "merchant",
        "target_id": "m-1",
        "run_date": "2026-07-16",
        "evaluated_at": "2026-07-16T03:45:00+00:00",
        "evidence": {
            "rule": [
                {
                    "feature": "night_authorize_count",
                    "value": 12,
                    "operator": ">=",
                    "threshold": 10,
                }
            ],
            "context": {
                "behavior_date": "2026-07-16",
                "example_night_pos_payment_id": "pay-1",
                "example_night_pos_terminal_id": "term-9",
            },
        },
    }


def test_json_value_covers_bigquery_scalar_types():
    """The `json.dumps` default= hook for what BigQuery hands back that
    JSON lacks: DATE/TIMESTAMP → ISO strings; NUMERIC (Decimal) → int when
    integral (minor units must not grow a ".0"), float otherwise; anything
    unexpected fails loudly instead of shipping garbage."""
    assert _json_value(date(2026, 7, 16)) == "2026-07-16"
    assert _json_value(datetime(2026, 7, 16, 3, 45, tzinfo=timezone.utc)) == (
        "2026-07-16T03:45:00+00:00"
    )
    assert _json_value(Decimal("1250")) == 1250
    assert _json_value(Decimal("12.50")) == 12.5
    with pytest.raises(TypeError):
        _json_value(object())


def test_to_jsonl_zero_alerts_is_empty_file():
    # Contract: an empty file means "evaluated, nothing raised" — not "[]",
    # not a header line, zero bytes.
    assert to_jsonl([]) == ""


def test_to_jsonl_one_line_per_alert():
    rows = [{"alert_id": "a"}, {"alert_id": "b"}]
    text = to_jsonl(rows)
    assert text.endswith("\n")
    assert [json.loads(line)["alert_id"] for line in text.splitlines()] == ["a", "b"]


def test_scenario_registry_matches_contract_naming():
    # File names in the bucket are `<scenario_id>.jsonl`; ids follow AML-nnn.
    assert SCENARIOS, "at least one active scenario"
    for scenario_id, table in SCENARIOS.items():
        assert re.fullmatch(r"AML-\d{3}", scenario_id)
        assert re.fullmatch(r"aml_alerts_\d{3}", table)
