"""Notebook helpers: SQL building against a fake schema, frame helpers on
literal frames. No BigQuery is needed.
"""

import json
from types import SimpleNamespace

import pandas as pd

from app_etl.utils import bq_helpers as nb


class FakeClient:
    project = "lite-data-prod"
    tables = {
        "payment_v2__payments": {
            "id": "STRING",
            "merchant_id": "STRING",
            "created_at": "TIMESTAMP",
            "updated_at": "TIMESTAMP",
            "_dlt_load_id": "STRING",
        },
        "payments_daily_summary": {
            "merchant_id": "STRING",
            "payment_creation_date": "DATE",
        },
        "lem__agreements": {"id": "STRING", "_dlt_load_id": "STRING"},
    }

    def get_table(self, fqn):
        cols = self.tables[fqn.rsplit(".", 1)[1]]
        return SimpleNamespace(
            schema=[SimpleNamespace(name=n, field_type=t) for n, t in cols.items()]
        )


CLIENT = FakeClient()


def test_latest_dedups_then_filters_on_local_days():
    sql = nb.latest(
        "payment_v2__payments",
        start="2026-09-01",
        end="2026-10-01",
        sql_only=True,
        client=CLIENT,
    )
    inner, outer = sql.split(")\nwhere")
    # Partition prune inside the dedup, date filter on the latest version.
    assert "updated_at >= timestamp('2026-09-01 00:00:00', 'Asia/Riyadh')" in inner
    assert "partition by id order by updated_at desc, _dlt_load_id desc" in inner
    assert "created_at >= timestamp('2026-09-01 00:00:00', 'Asia/Riyadh')" in outer
    assert "created_at < timestamp('2026-10-01 00:00:00', 'Asia/Riyadh')" in outer
    for merchant in nb.EXCLUDED_MERCHANT_IDS:
        assert merchant in outer


def test_versions_keeps_duplicates():
    sql = nb.versions(
        "payment_v2__payments", start="2026-09-01", sql_only=True, client=CLIENT
    )
    assert "row_number" not in sql
    assert "updated_at >= timestamp(" in sql


def test_core_date_column_and_merchant_opt_out():
    sql = nb.core(
        "payments_daily_summary",
        start="2026-09-01",
        date_col="payment_creation_date",
        exclude_merchants=False,
        sql_only=True,
        client=CLIENT,
    )
    assert "payment_creation_date >= date '2026-09-01'" in sql
    assert "merchant_id" not in sql
    assert "updated_at" not in sql


def test_latest_without_updated_at_orders_by_load():
    sql = nb.latest("lem__agreements", sql_only=True, client=CLIENT)
    assert "partition by id order by _dlt_load_id desc" in sql


def test_tables_unions_datasets_and_filters_prefix():
    sql = nb.tables(
        "payment_v2", dataset=["raw_litecore", "core"], sql_only=True, client=CLIENT
    )
    raw, core = sql.split("\nunion all\n")
    assert "`lite-data-prod.raw_litecore.__TABLES__`" in raw
    assert "`lite-data-prod.core.__TABLES__`" in core
    assert sql.count("starts_with(table_id, 'payment_v2')") == 2
    assert sql.endswith("order by dataset, table_name")
    assert "where" not in nb.tables(sql_only=True, client=CLIENT)


def test_add_local_time_inserts_next_to_source():
    df = pd.DataFrame(
        {"created_at": pd.to_datetime(["2026-09-30 22:00"], utc=True), "x": [1]}
    )
    out = nb.add_local_time(df)
    assert list(out.columns) == ["created_at", "created_at_local", "x"]
    assert out["created_at_local"].iloc[0] == pd.Timestamp("2026-10-01 01:00")


def test_json_get_walks_double_encoded_and_lists():
    blob = json.dumps({"a": {"b": 1}, "m": json.dumps({"code": "05"}), "l": [{"x": 9}]})
    df = pd.DataFrame({"blob": [blob, None]})
    b = nb.json_get(df, "blob", "a.b")["b"]
    assert b.iloc[0] == 1 and pd.isna(b.iloc[1])
    assert nb.json_get(df, "blob", "m.code")["code"].iloc[0] == "05"
    assert nb.json_get(df, "blob", "l.0.x", name="lx")["lx"].iloc[0] == 9


def test_json_flatten_and_sql():
    df = pd.DataFrame({"blob": ['{"a": {"b": 1}, "c": "z"}', None]})
    out = nb.json_flatten(df, "blob", drop=True)
    assert sorted(out.columns) == ["blob_a_b", "blob_c"]
    arr = pd.DataFrame({"splits": ['[{"amount": 20, "ref": "tips"}, {"amount": 5}]']})
    assert list(nb.json_flatten(arr, "splits", drop=True).columns) == [
        "splits_0_amount",
        "splits_0_ref",
        "splits_1_amount",
    ]
    assert nb.json_sql("instrument_data", "card_brand") == (
        "json_value(instrument_data, '$.card_brand') as card_brand"
    )


def test_json_explode_one_row_per_element():
    df = pd.DataFrame(
        {"id": ["a", "b"], "splits": ['[{"amount": 20}, {"amount": 5}]', None]}
    )
    out = nb.json_explode(df, "splits")
    assert list(out.columns) == ["id", "splits_amount"]
    assert out["id"].tolist() == ["a", "a", "b"]
    assert out["splits_amount"].iloc[:2].tolist() == [20, 5]
    assert pd.isna(out["splits_amount"].iloc[2])
