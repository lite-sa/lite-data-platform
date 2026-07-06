"""ingest_payments — incremental extraction of litecore.payments.

Source table is append-only, so we watermark on created_at. Append-only
does NOT make watermarking safe by itself: in Postgres now() is the
transaction *start* time, so a row with created_at=10:00 can commit at
10:03 — after a 10:02 run already advanced the watermark past it. The
row would be skipped silently, forever.

One guard closes that race: never extract right up to now(). The window
upper bound trails wall clock by SAFETY_LAG, leaving room for in-flight
transactions to commit. Correct under one assumption, documented in the
README: no write transaction on the source outlives the lag. Zero
duplicates produced — no dedup contract downstream.

Flow (stateless between runs):
  1. watermark = state.get_watermark("payments", initial_default=launch date)
  2. window    = (watermark, now() - SAFETY_LAG]
  3. extract   = pg.stream_query over that window, chunked           (utils/pg.py)
  4. land      = gcs.write_parquet → raw/litecore/payments/...       (utils/gcs.py)
  5. load      = bq.load_parquet_append → raw_litecore.payments      (utils/bq.py)
  6. commit    = state.commit_watermark("payments", window upper bound,
                 rows/uri) — this insert IS the advance; only after 5
                 succeeds. Failures write nothing: absence is the signal.

The stored watermark is the window upper bound we computed, not
MAX(created_at) fished out of the data — an empty window still advances
it, and a normal no-op run is not an error.
"""

from __future__ import annotations

from datetime import timedelta

SAFETY_LAG = timedelta(minutes=10)


def main() -> None:
    raise NotImplementedError("step 2")


if __name__ == "__main__":
    main()
