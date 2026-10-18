"""Settlement report v3: the daily finance file keyed on window end.

File D carries whole windows, read as of the build cutoff (D+1 at
BUILD_TIME, Riyadh): every window whose window_end falls on D and that raw
showed closed at the cutoff, plus every late-closing window, in the first
file built after raw saw it closed. Every settlement and payment table is
read as of the cutoff, so a rerun of D reproduces D. Merchant and channel
names come from the dbt registry in core, current at run time.

Grain: one row per included settlement transaction. All merchants in one
file, test merchants kept, hard-deleted merchants dropped. The header is
v2's EXPORT_COLUMNS. Payment-grain rows (declines, unbooked sales) are out
of scope.

Writes local files only: the report, a per-window sidecar, a late-changes
sidecar when a shipped window changed since the previous cutoff, and with
--compare-v2 or --compare-file a row diff against v2.

    python -m app_etl.export.settlement_report_v3 --date 2026-10-10 \\
        --out-dir .scratch/settlement-v3 --compare-v2
"""

from __future__ import annotations

import argparse
import tempfile
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
from google.cloud import bigquery

from app_etl.config import Settings
from app_etl.export import settlement_daily_report as v2
from app_etl.export.common import (
    LOCAL_TIMEZONE,
    REQUIRED_EXTRACTIONS,
    latest_version,
    merchant_channels,
    merchant_directory,
    resolve_days,
)
from app_etl.export.settlement_daily_report import (
    REVERSAL_ADJUSTMENT,
    REVERSAL_ADJUSTMENT_SUFFIX,
    build_report,
    derive_adjustment_fees,
    export_columns,
    fetch_operations,
    informational_notes,
    pivot_fees_txn,
    pos_identifiers,
    run_checks,
)
from app_etl.utils.bq_helpers import DELETED_MERCHANT_IDS

# The export run, Riyadh. File D reads raw as of D+1 at this time.
BUILD_TIME = time(7, 30)

FILE_STEM = "all_merchants_daily_transaction_report_v3"

# v2's informational_notes also reads the day's REFUND/REVERSE ops; v3 has
# no such query, so it passes this empty stand-in.
_NO_DAY_OPS = pd.DataFrame(
    {
        "operation_type": pd.Series(dtype="string"),
        "op_id": pd.Series(dtype="string"),
        "has_settlement_row": pd.Series(dtype=bool),
    }
)

# Columns that move with the payout and payment lifecycle after the cutoff.
# A file read later (v2 reads latest) differs on them by read time alone.
# value_day moves when the payout executes.
READ_TIME_COLS = (
    "status",
    "settlement_status",
    "is_settled",
    "window_status",
    "value_day",
)

# Columns a late change compares. is_settled and HELD -> ACTIVE flips are
# lifecycle, not money, so they are not here.
_MONEY_COLS = ("amount_minor", "settled_amount_minor", "fees")


def cutoff_for(report_day: date, build_time: time = BUILD_TIME) -> datetime:
    """UTC instant file D reads raw as of: D+1 at build_time, Riyadh."""
    local = datetime.combine(
        report_day + timedelta(days=1), build_time, tzinfo=ZoneInfo(LOCAL_TIMEZONE)
    )
    return local.astimezone(timezone.utc)


def as_of_version(raw: str, table: str, cutoff_param: str = "cutoff") -> str:
    """Subquery for the latest version per id among loads that finished
    before @cutoff_param: the table as the export saw it then.
    """
    return f"""(
            select t.*
            from `{raw}.{table}` t
            join `{raw}._dlt_loads` l
                on l.load_id = t._dlt_load_id
                and l.status = 0
                and l.inserted_at < @{cutoff_param}
            qualify row_number() over (
                partition by t.id order by t.updated_at desc, t._dlt_load_id desc
            ) = 1
        )"""


# ---------------------------------------------------------------------------
# Extraction
# ---------------------------------------------------------------------------


def require_cutoff_loads(
    client: bigquery.Client, raw: str, report_day: date, cutoff: datetime
) -> None:
    """Exit unless each REQUIRED_EXTRACTIONS database has a successful load
    between the local close of report_day and the cutoff.
    """
    day_end = datetime.combine(
        report_day + timedelta(days=1), time.min, tzinfo=ZoneInfo(LOCAL_TIMEZONE)
    ).astimezone(timezone.utc)
    query = f"""
        select distinct schema_name
        from `{raw}._dlt_loads`
        where status = 0
          and schema_name in unnest(@schemas)
          and inserted_at >= @day_end
          and inserted_at < @cutoff
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter(
                "schemas", "STRING", list(REQUIRED_EXTRACTIONS)
            ),
            bigquery.ScalarQueryParameter("day_end", "TIMESTAMP", day_end),
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
        ]
    )
    covered = {
        row["schema_name"]
        for row in client.query_and_wait(query, job_config=job_config)
    }
    missing = sorted(set(REQUIRED_EXTRACTIONS) - covered)
    if missing:
        raise SystemExit(
            f"no successful {', '.join(missing)} load between "
            f"{day_end:%Y-%m-%d %H:%M} and {cutoff:%Y-%m-%d %H:%M} UTC, so the "
            f"cutoff does not cover {report_day}, not exporting"
        )


def fetch_windows(
    client: bigquery.Client, raw: str, core: str, cutoff: datetime
) -> pd.DataFrame:
    """Every window as of the cutoff, with when it closed: `closed_at` is the
    time raw first held a version past OPENED, `closed_at` that version's
    source time, `window_end_at_close` its end. Picking by load time keeps
    closed_seen_at fixed across cutoffs, so a window never ships twice.
    """
    query = f"""
        with versions as (
            select w.*, l.inserted_at as loaded_at
            from `{raw}.settlement__settlement_window` w
            join `{raw}._dlt_loads` l
                on l.load_id = w._dlt_load_id
                and l.status = 0
                and l.inserted_at < @cutoff
        ),
        first_closed as (
            select
                id,
                window_end,
                updated_at as closed_at,
                loaded_at as closed_seen_at
            from versions
            where status <> 'OPENED'
            qualify row_number() over (
                partition by id order by loaded_at, updated_at
            ) = 1
        ),
        at_cutoff as (
            select * from versions
            qualify row_number() over (
                partition by id order by updated_at desc, _dlt_load_id desc
            ) = 1
        )
        select
            a.id as settlement_window_id,
            a.merchant_id,
            b.merchant_name,
            a.status as window_status,
            a.window_start,
            c.window_end as window_end_at_close,
            a.window_end as window_end_at_cutoff,
            coalesce(a.collection_date, date(datetime(a.created_at, @tz)))
                as collection_day,
            a.settled_amount as window_settled_amount_minor,
            format_date('%F', date(datetime(a.value_date, @tz))) as value_day,
            c.closed_at,
            c.closed_seen_at
        from at_cutoff a
        left join first_closed c using (id)
        left join {merchant_directory(core)} b
            on b.merchant_id = a.merchant_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def fetch_spine(
    client: bigquery.Client,
    raw: str,
    core: str,
    window_ids: list[str],
    cutoff: datetime,
) -> pd.DataFrame:
    """Every settlement transaction of the given windows as of the cutoff,
    with the columns v2's gate and build step read. REMOVED rows stay,
    flagged `included_in_window`. Joins as in v2's fetch_window_transactions.
    """
    txn = as_of_version(raw, "settlement__transaction")
    query = f"""
        with txn as (
            select
                t.*,
                t.operation = @adjustment_operation as is_reversal_adjustment,
                if(
                    ends_with(t.external_reference_id, @adjustment_suffix),
                    left(
                        t.external_reference_id,
                        length(t.external_reference_id)
                            - length(@adjustment_suffix)
                    ),
                    t.external_reference_id
                ) as original_operation_id
            from {txn} t
            where t.settlement_window_id in unnest(@window_ids)
        ),
        original as (
            select external_reference_id, status, amount, settled_amount, fees
            from {txn}
            where operation is distinct from @adjustment_operation
            qualify row_number() over (
                partition by external_reference_id order by created_at
            ) = 1
        )
        select
            t.id as settlement_transaction_id,
            coalesce(t.parent_payment_id, o.payment_id) as payment_id,
            t.external_reference_id as operation_id,
            t.original_operation_id,
            t.operation,
            t.status as settlement_status,
            t.status is distinct from 'REMOVED' as included_in_window,
            t.is_settled,
            t.amount as amount_minor,
            t.settled_amount as settled_amount_minor,
            t.fees,
            t.currency,
            t.hold_at,
            t.created_at as txn_created_at,
            w.id as settlement_window_id,
            w.merchant_id,
            w.status as window_status,
            w.settled_amount as window_settled_amount_minor,
            w.currency as window_currency,
            format_date('%F', date(datetime(w.value_date, @tz))) as value_day,
            b.merchant_name,
            o.id is not null as op_resolved,
            p.id is not null as payment_resolved,
            t.parent_payment_id is null and p.id is not null
                as payment_resolved_via_op,
            orig.status as original_txn_status,
            orig.amount as original_amount_minor,
            orig.settled_amount as original_settled_amount_minor,
            orig.fees as original_fees,
            p.merchant_id as payment_merchant_id,
            p.status as payment_status,
            p.currency as payment_currency,
            p.capture_mode,
            p.payment_instrument_id,
            p.instrument_data,
            p.channel_id,
            p.channel_type,
            p.order_data,
            p.splits,
            p.created_at,
            c.channel_name,
            c.merchant_id as channel_merchant_id
        from txn t
        join {as_of_version(raw, "settlement__settlement_window")} w
            on w.id = t.settlement_window_id
        left join {merchant_directory(core)} b
            on b.merchant_id = w.merchant_id
        left join {as_of_version(raw, "payment_v2__payment_operations")} o
            on o.id = t.original_operation_id
        left join original orig
            on t.is_reversal_adjustment
            and orig.external_reference_id = t.original_operation_id
        left join {as_of_version(raw, "payment_v2__payments")} p
            on p.id = coalesce(t.parent_payment_id, o.payment_id)
        left join {merchant_channels(core)} c
            on c.channel_id = p.channel_id
        order by t.created_at, t.id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("window_ids", "STRING", window_ids),
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
            bigquery.ScalarQueryParameter("tz", "STRING", LOCAL_TIMEZONE),
            bigquery.ScalarQueryParameter(
                "adjustment_operation", "STRING", REVERSAL_ADJUSTMENT
            ),
            bigquery.ScalarQueryParameter(
                "adjustment_suffix", "STRING", REVERSAL_ADJUSTMENT_SUFFIX
            ),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def fetch_changes(
    client: bigquery.Client, raw: str, prev_cutoff: datetime, cutoff: datetime
) -> pd.DataFrame:
    """Settlement transactions with a version loaded between the two
    cutoffs, each as of both. `has_clawback`: a reversal_adjustment row
    mirrors it at the cutoff.
    """
    query = f"""
        with changed as (
            select distinct t.id
            from `{raw}.settlement__transaction` t
            join `{raw}._dlt_loads` l
                on l.load_id = t._dlt_load_id
                and l.status = 0
                and l.inserted_at >= @prev_cutoff
                and l.inserted_at < @cutoff
        ),
        cur as {as_of_version(raw, "settlement__transaction", "cutoff")},
        prev as {as_of_version(raw, "settlement__transaction", "prev_cutoff")}
        select
            c.id as settlement_transaction_id,
            c.parent_payment_id as payment_id,
            c.external_reference_id,
            c.operation,
            p.settlement_window_id as prev_window_id,
            c.settlement_window_id as window_id,
            p.status as prev_status,
            c.status,
            p.amount as prev_amount_minor,
            c.amount as amount_minor,
            p.settled_amount as prev_settled_amount_minor,
            c.settled_amount as settled_amount_minor,
            p.fees as prev_fees,
            c.fees,
            c.updated_at,
            exists (
                select 1 from cur a
                where a.operation = @adjustment_operation
                  and a.external_reference_id
                      = concat(c.external_reference_id, @adjustment_suffix)
            ) as has_clawback
        from changed
        join cur c on c.id = changed.id
        left join prev p on p.id = changed.id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("prev_cutoff", "TIMESTAMP", prev_cutoff),
            bigquery.ScalarQueryParameter("cutoff", "TIMESTAMP", cutoff),
            bigquery.ScalarQueryParameter(
                "adjustment_operation", "STRING", REVERSAL_ADJUSTMENT
            ),
            bigquery.ScalarQueryParameter(
                "adjustment_suffix", "STRING", REVERSAL_ADJUSTMENT_SUFFIX
            ),
        ]
    )
    return client.query(query, job_config=job_config).to_dataframe()


def fetch_booked_payment_ids(
    client: bigquery.Client, raw: str, payment_ids: list[str]
) -> set[str]:
    """Payments among payment_ids with a sale row (positive, not REMOVED) in
    any window, latest version."""
    query = f"""
        select distinct coalesce(t.parent_payment_id, o.payment_id) as payment_id
        from {latest_version(f"{raw}.settlement__transaction")} t
        left join {latest_version(f"{raw}.payment_v2__payment_operations")} o
            on o.id = t.external_reference_id
        where t.status <> 'REMOVED'
          and t.amount > 0
          and t.settlement_window_id is not null
          and coalesce(t.parent_payment_id, o.payment_id) in unnest(@payment_ids)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("payment_ids", "STRING", payment_ids),
        ]
    )
    return set(client.query(query, job_config=job_config).to_dataframe()["payment_id"])


# ---------------------------------------------------------------------------
# Transforms
# ---------------------------------------------------------------------------


def key_windows(windows: pd.DataFrame, build_time: time = BUILD_TIME) -> pd.DataFrame:
    """Add window_end, report_day, late_close and ship_day.

    window_end is the end at close, else at the cutoff, else the end of the
    collection day (windows older than the column; `window_end_inferred`).
    A window raw saw closed before its own file's cutoff ships on its
    report day; one seen later ships in the first file whose cutoff
    follows. ship_day is NaN while raw has not seen the window closed.
    """
    w = windows.copy()
    build_td = pd.Timedelta(hours=build_time.hour, minutes=build_time.minute)
    end_of_day = (
        (
            pd.to_datetime(w["collection_day"].astype(str))
            + pd.Timedelta(days=1)
            - pd.Timedelta(seconds=1)
        )
        .dt.tz_localize(LOCAL_TIMEZONE)
        .dt.tz_convert("UTC")
    )
    w["window_end_inferred"] = (
        w["window_end_at_close"].isna() & w["window_end_at_cutoff"].isna()
    )
    w["window_end"] = (
        w["window_end_at_close"].fillna(w["window_end_at_cutoff"]).fillna(end_of_day)
    )
    end_local = w["window_end"].dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)
    report_day = end_local.dt.normalize()
    build_at = (report_day + pd.Timedelta(days=1) + build_td).dt.tz_localize(
        LOCAL_TIMEZONE
    )
    seen = w["closed_seen_at"]
    w["report_day"] = report_day.dt.date
    w["late_close"] = seen.notna() & (seen >= build_at)
    late_day = (
        seen.dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None) - build_td
    ).dt.date
    w["ship_day"] = (
        w["report_day"].where(~w["late_close"], late_day).where(seen.notna())
    )
    return w


def _differs(a: pd.Series, b: pd.Series) -> pd.Series:
    return a.astype("string").fillna("") != b.astype("string").fillna("")


def classify_late_changes(
    changes: pd.DataFrame, shipped_before: set[str]
) -> pd.DataFrame:
    """Changes to rows of windows that shipped in an earlier file, with a
    `kind`. Only window, inclusion, amounts and fees count.
    reversed_after_payout is the expected flip of a sale whose claw-back
    books into a later window.
    """
    c = changes.loc[
        changes["prev_window_id"].isin(shipped_before)
        | changes["window_id"].isin(shipped_before)
    ].copy()
    existed = c["prev_status"].notna()
    prev_incl = existed & c["prev_status"].ne("REMOVED")
    cur_incl = c["status"].ne("REMOVED")
    moved = existed & _differs(c["prev_window_id"], c["window_id"])
    money = pd.concat(
        [_differs(c[f"prev_{col}"], c[col]) for col in _MONEY_COLS], axis=1
    ).any(axis=1)
    c["kind"] = np.select(
        [
            ~existed & cur_incl,
            moved,
            prev_incl & ~cur_incl & c["has_clawback"].fillna(False).astype(bool),
            prev_incl & ~cur_incl,
            existed & ~prev_incl & cur_incl,
            cur_incl & money,
        ],
        [
            "added",
            "moved",
            "reversed_after_payout",
            "removed",
            "restored",
            "amount_changed",
        ],
        default="",
    )
    return c.loc[c["kind"].ne("")].reset_index(drop=True)


def _local(ts: pd.Series) -> pd.Series:
    return ts.dt.tz_convert(LOCAL_TIMEZONE).dt.tz_localize(None)


def window_summary(shipped: pd.DataFrame, report: pd.DataFrame) -> pd.DataFrame:
    """One row per shipped window: its keys and close times, its amount, and
    what the file carries for it (incident-excluded rows aside, equal)."""
    rows = report.groupby("settlement_window_id").agg(
        n_rows=("settlement_transaction_id", "size"),
        file_settled_amount_sar=("settled_amount_sar", "sum"),
    )
    out = shipped.merge(rows, on="settlement_window_id", how="left")
    out["n_rows"] = out["n_rows"].fillna(0).astype("int64")
    out["file_settled_amount_sar"] = out["file_settled_amount_sar"].fillna(0.0)
    out["window_settled_amount_sar"] = (
        out["window_settled_amount_minor"].astype("float") / 100
    )
    for col in ("window_start", "window_end", "closed_at", "closed_seen_at"):
        out[f"{col}_local"] = _local(out[col])
    return out[
        [
            "settlement_window_id",
            "merchant_id",
            "merchant_name",
            "window_status",
            "window_start_local",
            "window_end_local",
            "window_end_inferred",
            "collection_day",
            "report_day",
            "ship_day",
            "late_close",
            "closed_at_local",
            "closed_seen_at_local",
            "value_day",
            "window_settled_amount_sar",
            "file_settled_amount_sar",
            "n_rows",
        ]
    ].sort_values(
        ["merchant_name", "merchant_id", "window_end_local", "settlement_window_id"]
    )


def window_notes(
    windows: pd.DataFrame, report_day: date, cutoff: datetime
) -> list[str]:
    """Non-blocking notes on the window population at the cutoff."""
    notes: list[str] = []
    shipped = windows.loc[windows["ship_day"].eq(report_day)]
    late = shipped.loc[shipped["late_close"]]
    empty = int(shipped["window_settled_amount_minor"].fillna(0).eq(0).sum())
    notes.append(
        f"{len(shipped)} window(s) ship ({empty} with a zero or empty "
        f"settled_amount), {len(late)} after a late close"
        + (f": {late['settlement_window_id'].tolist()[:5]}" if len(late) else "")
    )
    inferred = int(shipped["window_end_inferred"].sum())
    if inferred:
        notes.append(
            f"{inferred} window(s) with no window_end, keyed on the end of "
            "their collection day"
        )
    still_open = windows.loc[
        windows["closed_seen_at"].isna() & (windows["window_end"] < cutoff)
    ]
    if len(still_open):
        listed = [
            f"{r.settlement_window_id} ({r.merchant_name}, ended {r.report_day})"
            for r in still_open.itertuples()
        ]
        notes.append(
            f"{len(still_open)} window(s) past their end still OPENED at the "
            f"cutoff, shipping in no file yet: {listed[:5]}"
        )
    moved_end = shipped.loc[
        shipped["window_end_at_close"].notna()
        & shipped["window_end_at_cutoff"].notna()
        & shipped["window_end_at_close"].ne(shipped["window_end_at_cutoff"])
    ]
    if len(moved_end):
        notes.append(
            f"{len(moved_end)} shipped window(s) whose window_end changed after "
            f"close (keyed on the end at close): "
            f"{moved_end['settlement_window_id'].tolist()[:5]}"
        )
    return notes


def write_file(report: pd.DataFrame, path: Path) -> int:
    """The report as CSV under v2's header, grouped by merchant."""
    rows = report.sort_values(
        ["merchant_name", "merchant_id", "created_at", "settlement_transaction_id"]
    ).reindex(columns=export_columns(report))
    rows.to_csv(path, index=False, float_format="%.2f")
    return len(rows)


# ---------------------------------------------------------------------------
# Comparison against v2
# ---------------------------------------------------------------------------


def build_v2_report(
    client: bigquery.Client, raw: str, core: str, report_day: date
) -> tuple[pd.DataFrame, list[str]]:
    """v2's --all-merchants report for the same day, built from v2's own
    steps as in its main(). Reads latest, not as of the cutoff. Returns the
    gate failures instead of raising them.
    """
    spine = v2.fetch_window_transactions(
        client, raw, core, report_day, all_merchants=True
    )
    spine = v2.derive_adjustment_fees(spine)
    spine, fee_cols = v2.pivot_fees_txn(spine)
    payments = v2.fetch_payments(client, raw, core, report_day, all_merchants=True)
    empty_settle, _ = v2.pivot_fees(v2._empty_settlement())
    empty_settle, _ = v2.dedupe_settlement(empty_settle)
    failures = v2.run_checks(spine) + v2.run_payment_checks(payments, empty_settle)
    payment_ids = sorted(set(spine["payment_id"].dropna()) | set(payments["id"]))
    ops = (
        v2.fetch_operations(client, raw, payment_ids)
        if payment_ids
        else v2._empty_operations()
    )
    pos_ids = v2.pos_identifiers(ops)
    txn_rows = v2.build_report(spine, pos_ids, fee_cols)
    leftover = payments.loc[~payments["id"].isin(set(txn_rows["id"]))]
    report = pd.concat(
        [txn_rows, v2.build_payment_rows(leftover, empty_settle, pos_ids)],
        ignore_index=True,
    )
    return report, failures


def read_report(path: Path) -> pd.DataFrame:
    """A report CSV as strings, empty cells as ''."""
    return pd.read_csv(path, dtype=str, keep_default_na=False)


def _sar(frame: pd.DataFrame, col: str = "settled_amount_sar") -> float:
    return float(pd.to_numeric(frame[col], errors="coerce").fillna(0).sum())


def compare_reports(
    v3: pd.DataFrame,
    other: pd.DataFrame,
    windows: pd.DataFrame,
    report_day: date,
    booked_payment_ids: set[str] | None = None,
) -> tuple[list[str], pd.DataFrame]:
    """Diff the v3 file against another file with v2's header, both as read
    back by read_report. Rows key on settlement_transaction_id; the other
    file's rows without one are payment-grain rows, out of scope in v3.
    `windows` (key_windows output, deleted merchants included) explains
    rows on one side only. Returns summary lines and one detail row per
    differing or one-sided row.
    """
    key = "settlement_transaction_id"
    win = windows.set_index("settlement_window_id")
    deleted = set(DELETED_MERCHANT_IDS)
    other_txn = other.loc[other[key].ne("")]
    other_pay = other.loc[other[key].eq("")]

    def only_other_reason(row: pd.Series) -> str:
        if row["merchant_id"] in deleted:
            return "hard-deleted merchant"
        if row["settlement_window_id"] not in win.index:
            return "window unknown at the cutoff"
        w = win.loc[row["settlement_window_id"]]
        if pd.isna(w["ship_day"]):
            if w["report_day"] > report_day:
                return f"window ends {w['report_day']}, not closed at the cutoff"
            return "window past its end, still OPENED at the cutoff"
        if w["ship_day"] != report_day:
            return f"v3 ships the window on {w['ship_day']}"
        return "row not in the window at the cutoff"

    def only_v3_reason(row: pd.Series) -> str:
        w = win.loc[row["settlement_window_id"]]
        if str(w["collection_day"]) != str(report_day):
            return f"v2 ships the window on collection day {w['collection_day']}"
        return "row not in v2's file"

    detail_cols = [
        key,
        "id",
        "settlement_window_id",
        "merchant_name",
        "status",
        "amount_sar",
        "settled_amount_sar",
    ]
    parts = []

    only_v3 = v3.loc[~v3[key].isin(set(other_txn[key]))]
    parts.append(
        only_v3[detail_cols].assign(
            category="only_v3",
            reason=[only_v3_reason(r) for _, r in only_v3.iterrows()],
        )
    )
    only_other = other_txn.loc[~other_txn[key].isin(set(v3[key]))]
    parts.append(
        only_other[detail_cols].assign(
            category="only_v2",
            reason=[only_other_reason(r) for _, r in only_other.iterrows()],
        )
    )

    both = v3.merge(other_txn, on=key, suffixes=("", "_v2"))
    cols = [c for c in v3.columns if c != key and c in other_txn.columns]
    all_diffs = pd.DataFrame(
        {c: both[c].ne(both[f"{c}_v2"]) for c in cols}, index=both.index
    )
    read_time = [c for c in cols if c in READ_TIME_COLS]
    diffs = all_diffs.drop(columns=read_time)
    differing = both.loc[diffs.any(axis=1)]
    parts.append(
        differing[detail_cols].assign(
            category="value_diff",
            reason=[
                ", ".join(r.index[r]) for _, r in diffs.loc[differing.index].iterrows()
            ],
        )
    )

    booked = booked_payment_ids or set()
    parts.append(
        other_pay[detail_cols].assign(
            category="v2_payment_row",
            reason=np.where(
                other_pay["id"].isin(booked),
                "sale booked in a window",
                "no settlement row",
            ),
        )
    )
    parts = [p for p in parts if len(p)]
    detail_header = ["category", "reason", *detail_cols]
    detail = (
        pd.concat(parts, ignore_index=True)[detail_header]
        if parts
        else pd.DataFrame(columns=detail_header)
    )

    v3_windows = set(v3["settlement_window_id"])
    v2_windows = set(other_txn["settlement_window_id"])
    lines = [
        f"v3: {len(v3)} rows, {len(v3_windows)} windows, settled SAR {_sar(v3):,.2f}",
        f"v2: {len(other)} rows ({len(other_txn)} transaction, {len(other_pay)} "
        f"payment-grain), {len(v2_windows)} windows, settled SAR "
        f"{_sar(other_txn):,.2f}",
        f"windows: {len(v3_windows & v2_windows)} in both, "
        f"{len(v3_windows - v2_windows)} only in v3, "
        f"{len(v2_windows - v3_windows)} only in v2",
        f"rows in both: {len(both)}, {len(differing)} with a value difference",
    ]
    read_counts = all_diffs[read_time].sum()
    if read_counts.any():
        lines.append(
            "  read-time columns (lifecycle since the cutoff, not in detail): "
            + ", ".join(
                f"{c} {int(n)}" for c, n in read_counts[read_counts > 0].items()
            )
        )
    if len(differing):
        counts = diffs.loc[differing.index].sum()
        lines.append(
            "  columns: "
            + ", ".join(f"{c} {int(n)}" for c, n in counts[counts > 0].items())
        )
    for category in ("only_v3", "only_v2", "v2_payment_row"):
        sub = detail.loc[detail["category"].eq(category)]
        if len(sub):
            lines.append(
                f"{category}: {len(sub)} rows, SAR {_sar(sub, 'amount_sar'):,.2f} gross"
            )
            for reason, n in sub["reason"].value_counts().items():
                lines.append(f"  {reason}: {n}")
    return lines, detail


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="report day (Riyadh calendar); default: yesterday",
    )
    parser.add_argument(
        "--build-time",
        type=time.fromisoformat,
        default=BUILD_TIME,
        help="cutoff time on D+1, Riyadh (default 07:30)",
    )
    parser.add_argument(
        "--out-dir", type=Path, default=None, help="default: a temp directory"
    )
    parser.add_argument(
        "--compare-v2",
        action="store_true",
        help="rebuild v2's finance file for the same day and diff against it",
    )
    parser.add_argument(
        "--compare-file",
        type=Path,
        default=None,
        help="diff against this v2 CSV (for example a delivered finance file)",
    )
    parser.add_argument(
        "--ignore-gate",
        action="store_true",
        help="write the file even when a blocking check fails",
    )
    args = parser.parse_args(argv)
    report_day, run_date = resolve_days(args)
    cutoff = cutoff_for(report_day, args.build_time)
    prev_cutoff = cutoff_for(report_day - timedelta(days=1), args.build_time)
    if datetime.now(timezone.utc) < cutoff:
        raise SystemExit(
            f"file {report_day} reads raw as of {cutoff:%Y-%m-%d %H:%M} UTC; "
            "run it after that"
        )

    settings = Settings.from_env()
    # BigQuery defaults to the US multi-region, which the org residency
    # policy rejects.
    client = bigquery.Client(project=settings.gcp_project, location="me-central2")
    raw = f"{settings.gcp_project}.{settings.bq_dataset_raw}"
    core = f"{settings.gcp_project}.{settings.bq_dataset_core}"
    require_cutoff_loads(client, raw, report_day, cutoff)

    all_windows = key_windows(
        fetch_windows(client, raw, core, cutoff), args.build_time
    )
    windows = all_windows.loc[~all_windows["merchant_id"].isin(DELETED_MERCHANT_IDS)]
    shipped = windows.loc[windows["ship_day"].eq(report_day)]

    spine = fetch_spine(
        client, raw, core, shipped["settlement_window_id"].tolist(), cutoff
    )
    spine = derive_adjustment_fees(spine)
    spine, fee_cols = pivot_fees_txn(spine)

    failures = run_checks(spine)
    if failures and not args.ignore_gate:
        raise SystemExit("report-blocking check failures:\n- " + "\n- ".join(failures))
    for failure in failures:
        print(f"GATE (ignored): {failure}")
    for note in window_notes(windows, report_day, cutoff):
        print(f"NOTE: {note}")
    # v2's notes minus the two v3 has no input for (day ops, moved rows) and
    # its open-window note: v3 ships closed windows only.
    for note in informational_notes(spine.assign(moved_across_days=False), _NO_DAY_OPS):
        if "not yet terminal" not in note:
            print(f"NOTE: {note}")

    payment_ids = sorted(set(spine["payment_id"].dropna()))
    ops = (
        fetch_operations(client, raw, payment_ids)
        if payment_ids
        else v2._empty_operations()
    )
    report = build_report(spine, pos_identifiers(ops), fee_cols)

    keyed = windows.dropna(subset=["ship_day"])
    shipped_before = set(
        keyed.loc[keyed["ship_day"] < report_day, "settlement_window_id"]
    )
    late = classify_late_changes(
        fetch_changes(client, raw, prev_cutoff, cutoff), shipped_before
    )
    if len(late):
        kinds = late["kind"].value_counts().to_dict()
        print(
            f"NOTE: {len(late)} change(s) since the previous cutoff to windows "
            f"that already shipped: {kinds}"
        )

    out_dir = args.out_dir or Path(tempfile.mkdtemp(prefix="settlement-v3-"))
    out_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{FILE_STEM}_{report_day}_run_{run_date}"
    path = out_dir / f"{stem}.csv"
    n_rows = write_file(report, path)
    print(f"all merchants: {n_rows} rows -> {path}")
    window_path = out_dir / f"{stem}_windows.csv"
    window_summary(shipped, report).to_csv(
        window_path, index=False, float_format="%.2f"
    )
    print(f"windows: {len(shipped)} -> {window_path}")
    if len(late):
        late_path = out_dir / f"{stem}_late_changes.csv"
        late.to_csv(late_path, index=False)
        print(f"late changes: {len(late)} -> {late_path}")

    other_path = args.compare_file
    if args.compare_v2:
        v2_report, v2_failures = build_v2_report(client, raw, core, report_day)
        for failure in v2_failures:
            print(f"V2 GATE: {failure}")
        other_path = out_dir / f"{stem}_v2_rebuilt.csv"
        write_file(v2_report, other_path)
    if other_path:
        other = read_report(other_path)
        pay_ids = sorted(
            set(other.loc[other["settlement_transaction_id"].eq(""), "id"])
        )
        booked = fetch_booked_payment_ids(client, raw, pay_ids) if pay_ids else set()
        lines, detail = compare_reports(
            read_report(path), other, all_windows, report_day, booked
        )
        print(f"=== compare against {other_path.name} ===")
        for line in lines:
            print(line)
        compare_path = out_dir / f"{stem}_compare.csv"
        detail.to_csv(compare_path, index=False)
        print(f"compare detail: {len(detail)} rows -> {compare_path}")


if __name__ == "__main__":
    main()
