"""ingest_merchants — daily full snapshot of litecore.merchants.

Configuration tables are small and mutable in place, so no watermark:
extract the whole table each run and load it into today's partition of
raw_litecore.merchants (WRITE_TRUNCATE on the partition). Idempotent —
a rerun replaces today's snapshot; history is one partition per day,
which is exactly what point-in-time joins downstream need.

Flow:
  1. extract = pg.stream_query("SELECT * FROM merchants"), chunked
  2. land    = gcs.write_parquet → raw/litecore/merchants/ingest_date=.../
  3. load    = bq.load_parquet_snapshot → raw_litecore.merchants$YYYYMMDD
  4. record  = state.commit_watermark("merchants", watermark=None, rows/uri)
               — snapshots have no watermark; run recorded for observability

Chunking still applies — "small today" is not a contract, and the
helper costs nothing extra.
"""

from __future__ import annotations


def main() -> None:
    raise NotImplementedError("step 2")


if __name__ == "__main__":
    main()
