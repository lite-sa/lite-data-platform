"""Notebook helpers for BigQuery reads and pandas frames.

    from app_etl.utils.bq_helpers import *

    # dbt-built tables in core: marts, the litecore mirror (latest version,
    # PII removed), staging views. First choice for most questions.
    pay = core("payments", start="2026-09-01", end="2026-10-01")
    ops = core("payment_v2__payment_operations", start=days_ago(7))

    # Raw, latest version per id: every source column, PII and blobs included.
    pay = latest("payment_v2__payments", start=days_ago(30),
                 columns=["id", "merchant_id", "status", "created_at",
                          json_sql("instrument_data", "card_brand")])

    # Raw, every version (one row per update).
    hist = versions("payment_v2__payments", where="id = 'abc'")

    pay = add_local_time(pay)                      # created_at -> created_at_local
    pay = json_get(pay, "instrument_data", "card_brand")
    pay = json_flatten(pay, "instrument_data")     # instrument_data_<key> columns
    legs = json_explode(pay, "splits")             # one row per array element
    df = query("select count(*) n from {core}.payments")

    # Row counts and sizes from table metadata, no scan.
    tables("payment_v2")                              # raw_litecore
    tables("payment_v2", dataset=["raw_litecore", "core"])  # raw vs mirror
    datasets()

start/end are business-local (Riyadh) days, end exclusive. Frames from
core/latest/versions drop EXCLUDED_MERCHANT_IDS when the table has a
merchant_id column (exclude_merchants=False keeps them). sql_only=True
returns the SQL instead of running it. Everything defaults to prod; pass
client=bq("lite-data-dev") for dev.

    settlement_overview = add_merchant_name(settlement_overview, client)
    windows = add_merchant_name(windows, client,
                                cols=("merchant_name", "business_account_name"))
"""

from __future__ import annotations

import json
from collections.abc import Sequence
from datetime import date, timedelta

import pandas as pd
from google.cloud import bigquery

PROD_PROJECT = "lite-data-prod"
LOCATION = "me-central2"
RAW_DATASET = "raw_litecore"
CORE_DATASET = "core"
LOCAL_TZ = "Asia/Riyadh"

# Internal test merchants. Canonical lists are the dbt seeds test_merchants.csv
# and deleted_merchants.csv; import these, never copy-paste them.
TEST_MERCHANT_IDS = [
    "97476ad7-37a0-4288-8f7e-57d0988d3ec8",  # lite Advanced Financial Company
    "e8a61e1b-f4b1-4a14-b630-cf08b0656576",  # Naif Testing Company
]

# Mock merchants hard-deleted at source on 2026-09-29; raw keeps their history.
DELETED_MERCHANT_IDS = [
    "fc8bf7fe-da2a-4327-afde-641a5929f05e",
    "2e2a8eab-0e8e-4d95-9a48-917e42249e3a",
]

# Exclude these from every analysis frame.
EXCLUDED_MERCHANT_IDS = TEST_MERCHANT_IDS + DELETED_MERCHANT_IDS

# The dbt merchants registry: one row per merchant_id across business_management
# and lem.
_MERCHANT_SQL = "select * from `{project}.{dataset}.merchants`"

_merchant_cache: dict[str, pd.DataFrame] = {}


def merchant_directory(
    client: bigquery.Client, *, refresh: bool = False
) -> pd.DataFrame:
    """core.merchants, cached per project for the kernel session."""
    project = client.project
    if refresh or project not in _merchant_cache:
        _merchant_cache[project] = client.query(
            _MERCHANT_SQL.format(project=project, dataset=CORE_DATASET)
        ).to_dataframe()
    return _merchant_cache[project]


def add_merchant_name(
    df: pd.DataFrame,
    client: bigquery.Client,
    *,
    on: str = "merchant_id",
    cols: Sequence[str] = ("merchant_name",),
    refresh: bool = False,
) -> pd.DataFrame:
    """Left-join core.merchants columns onto a frame whose `on` column holds
    a merchant id. validate="m:1" fails loudly on a fan-out.
    """
    directory = merchant_directory(client, refresh=refresh)
    lookup = directory[["merchant_id", *cols]].rename(columns={"merchant_id": on})
    return df.merge(lookup, how="left", on=on, validate="m:1")


# --- Fetching ---------------------------------------------------------------

_clients: dict[str, bigquery.Client] = {}
_schemas: dict[str, dict[str, str]] = {}


def bq(project: str = PROD_PROJECT) -> bigquery.Client:
    """One cached client per project for the kernel session."""
    if project not in _clients:
        _clients[project] = bigquery.Client(project=project, location=LOCATION)
    return _clients[project]


def days_ago(n: int) -> date:
    """The local (Riyadh) calendar date n days before today."""
    return pd.Timestamp.now(tz=LOCAL_TZ).date() - timedelta(days=n)


def query(sql: str, *, client: bigquery.Client | None = None) -> pd.DataFrame:
    """Run SQL; {raw} and {core} expand to the client's project datasets."""
    client = client or bq()
    sql = sql.replace("{raw}", f"`{client.project}.{RAW_DATASET}`")
    sql = sql.replace("{core}", f"`{client.project}.{CORE_DATASET}`")
    return client.query(sql).to_dataframe()


def core(table: str, **kwargs) -> pd.DataFrame | str:
    """A table or view from core: marts, litecore mirror, staging.

    kwargs: start, end, date_col, columns, where, limit,
    exclude_merchants, sql_only, client (see _fetch).
    """
    return _fetch(CORE_DATASET, table, **kwargs)


def latest(table: str, *, key: str = "id", **kwargs) -> pd.DataFrame | str:
    """Raw table deduped to the latest version per key.

    The date filter applies to the latest version, so a payment re-dated at
    source lands on its corrected day.
    """
    return _fetch(RAW_DATASET, table, dedup_key=key, **kwargs)


def versions(table: str, **kwargs) -> pd.DataFrame | str:
    """Raw table as landed: one row per extracted version, duplicates kept."""
    return _fetch(RAW_DATASET, table, **kwargs)


def datasets(*, client: bigquery.Client | None = None) -> list[str]:
    """Dataset names in the client's project."""
    client = client or bq()
    return sorted(d.dataset_id for d in client.list_datasets())


# Type codes: 1 table, 2 view, 3 external. Views and external tables report 0
# rows, so row_count is null for them.
_TABLES_SQL = """select
  '{dataset}' as dataset,
  table_id as table_name,
  case type when 1 then 'table' when 2 then 'view' else 'external' end as type,
  if(type = 1, row_count, null) as row_count,
  round(size_bytes / pow(1024, 2), 1) as size_mb,
  datetime(timestamp_millis(last_modified_time), '{tz}') as last_modified_local
from `{project}.{dataset}.__TABLES__`"""


def tables(
    prefix: str | None = None,
    *,
    dataset: str | Sequence[str] = RAW_DATASET,
    sql_only: bool = False,
    client: bigquery.Client | None = None,
) -> pd.DataFrame | str:
    """Tables with row counts, size and last-modified time, sorted by name.

    prefix matches the start of the table name ("payment_v2"). dataset takes
    one name or a list. Reads __TABLES__ metadata, so it scans no data. Raw
    counts are versions, not distinct ids.
    """
    client = client or bq()
    names = [dataset] if isinstance(dataset, str) else list(dataset)
    parts = []
    for name in names:
        part = _TABLES_SQL.format(dataset=name, tz=LOCAL_TZ, project=client.project)
        if prefix:
            part += f"\nwhere starts_with(table_id, '{prefix}')"
        parts.append(part)
    sql = "\nunion all\n".join(parts) + "\norder by dataset, table_name"
    if sql_only:
        return sql
    return client.query(sql).to_dataframe()


def _fetch(
    dataset: str,
    table: str,
    *,
    start: str | date | None = None,
    end: str | date | None = None,
    date_col: str = "created_at",
    columns: Sequence[str] | None = None,
    where: str | None = None,
    limit: int | None = None,
    exclude_merchants: bool = True,
    dedup_key: str | None = None,
    sql_only: bool = False,
    client: bigquery.Client | None = None,
) -> pd.DataFrame | str:
    client = client or bq()
    fqn = f"{client.project}.{dataset}.{table}"
    schema = _schema(client, fqn)

    filters = []
    prune = None
    if start is not None or end is not None:
        if date_col not in schema:
            raise KeyError(f"{table} has no column {date_col}; pass date_col=")
        if start is not None:
            filters.append(f"{date_col} >= {_literal(start, schema[date_col])}")
            # Raw is partitioned on updated_at, and updated_at >= created_at,
            # so this skips old partitions without dropping any row in range.
            if (
                dataset == RAW_DATASET
                and "updated_at" in schema
                and date_col in ("created_at", "updated_at")
            ):
                prune = f"updated_at >= {_literal(start, 'TIMESTAMP')}"
        if end is not None:
            filters.append(f"{date_col} < {_literal(end, schema[date_col])}")
    if exclude_merchants and "merchant_id" in schema:
        ids = ", ".join(f"'{m}'" for m in EXCLUDED_MERCHANT_IDS)
        filters.append(f"coalesce(merchant_id, '') not in ({ids})")
    if where:
        filters.append(f"({where})")

    if dedup_key:
        if dedup_key not in schema:
            raise KeyError(f"{table} has no column {dedup_key}; pass key=")
        order = (
            "updated_at desc, _dlt_load_id desc"
            if "updated_at" in schema
            else "_dlt_load_id desc"
        )
        source = (
            f"(\n  select * from `{fqn}`\n"
            + (f"  where {prune}\n" if prune else "")
            + f"  qualify row_number() over (partition by {dedup_key} order by {order}) = 1\n)"
        )
    else:
        source = f"`{fqn}`"
        if prune:
            filters.insert(0, prune)

    sql = f"select {', '.join(columns) if columns else '*'}\nfrom {source}"
    if filters:
        sql += "\nwhere " + "\n  and ".join(filters)
    if limit:
        sql += f"\nlimit {limit}"
    if sql_only:
        return sql
    return client.query(sql).to_dataframe()


def _schema(client: bigquery.Client, fqn: str) -> dict[str, str]:
    if fqn not in _schemas:
        _schemas[fqn] = {f.name: f.field_type for f in client.get_table(fqn).schema}
    return _schemas[fqn]


def _literal(value: str | date, field_type: str) -> str:
    """A local wall-clock date/time as a literal of the column's type."""
    ts = pd.Timestamp(value)
    if ts.tzinfo is not None:
        ts = ts.tz_convert(LOCAL_TZ).tz_localize(None)
    if field_type == "DATE":
        return f"date '{ts.date()}'"
    if field_type == "DATETIME":
        return f"datetime '{ts}'"
    return f"timestamp('{ts}', '{LOCAL_TZ}')"


# --- Frame helpers ----------------------------------------------------------


def add_local_time(
    df: pd.DataFrame, cols: Sequence[str] | None = None, *, tz: str = LOCAL_TZ
) -> pd.DataFrame:
    """Add <col>_local (naive Riyadh wall clock) next to each timestamp column.

    Default: every tz-aware column. Naive columns are taken as UTC.
    """
    if cols is None:
        cols = [c for c in df.columns if isinstance(df[c].dtype, pd.DatetimeTZDtype)]
    out = df.copy()
    for c in cols:
        ts = pd.to_datetime(out[c], utc=True)
        name = f"{c}_local"
        if name in out.columns:
            out = out.drop(columns=name)
        out.insert(
            out.columns.get_loc(c) + 1, name, ts.dt.tz_convert(tz).dt.tz_localize(None)
        )
    return out


def json_sql(col: str, path: str, alias: str | None = None) -> str:
    """SQL expression for one JSON key, for the columns= argument.

    json_sql("instrument_data", "card_brand")
      -> "json_value(instrument_data, '$.card_brand') as card_brand"
    """
    return f"json_value({col}, '$.{path}') as {alias or path.split('.')[-1]}"


def json_get(
    df: pd.DataFrame, col: str, path: str, name: str | None = None
) -> pd.DataFrame:
    """Add one key of a JSON column as a new column.

    path is dotted ("raw_provider_response.code"; "items.0" indexes a list).
    JSON-encoded strings met along the path are decoded, which covers
    double-encoded blobs like payment_operations.metadata.
    """
    keys = path.split(".")

    def pick(blob):
        for k in keys:
            if isinstance(blob, str):
                try:
                    blob = json.loads(blob)
                except ValueError:
                    return None
            if isinstance(blob, dict):
                blob = blob.get(k)
            elif isinstance(blob, list) and k.isdigit() and int(k) < len(blob):
                blob = blob[int(k)]
            else:
                return None
        return blob

    out = df.copy()
    out[name or keys[-1]] = out[col].map(pick)
    return out


def _parse_json(blob):
    if isinstance(blob, str):
        try:
            return json.loads(blob)
        except ValueError:
            return None
    return blob


def _flat_items(blob, key: str):
    if isinstance(blob, dict):
        for k, v in blob.items():
            yield from _flat_items(v, f"{key}_{k}")
    elif isinstance(blob, list):
        for i, v in enumerate(blob):
            yield from _flat_items(v, f"{key}_{i}")
    else:
        yield key, blob


def json_flatten(
    df: pd.DataFrame, col: str, *, prefix: str | None = None, drop: bool = False
) -> pd.DataFrame:
    """Spread a JSON column into one column per key (nested keys joined by _).

    List elements spread by position (splits_0_amount, splits_1_amount); use
    json_explode for one row per element instead.
    """
    prefix = prefix if prefix is not None else col
    rows = []
    for blob in df[col]:
        blob = _parse_json(blob)
        rows.append(
            dict(_flat_items(blob, prefix)) if isinstance(blob, (dict, list)) else {}
        )
    flat = pd.DataFrame(rows, index=df.index)
    out = df.drop(columns=col) if drop else df
    return pd.concat([out, flat], axis=1)


def json_explode(
    df: pd.DataFrame, col: str, *, prefix: str | None = None
) -> pd.DataFrame:
    """One row per element of a JSON array column, element keys as columns.

    json_explode(pay, "splits") -> splits_amount, splits_account_reference, ...
    Rows with a null or empty array stay as one row with null element columns.
    The original index repeats per element.
    """
    out = df.copy()
    out[col] = out[col].map(_parse_json)
    out = out.explode(col)
    return json_flatten(out, col, prefix=prefix, drop=True)


__all__ = [
    "DELETED_MERCHANT_IDS",
    "EXCLUDED_MERCHANT_IDS",
    "LOCAL_TZ",
    "add_local_time",
    "add_merchant_name",
    "bq",
    "core",
    "datasets",
    "days_ago",
    "json_explode",
    "json_flatten",
    "json_get",
    "json_sql",
    "latest",
    "merchant_directory",
    "query",
    "tables",
    "TEST_MERCHANT_IDS",
    "versions",
]
