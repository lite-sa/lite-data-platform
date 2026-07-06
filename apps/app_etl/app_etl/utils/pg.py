"""Postgres extraction helpers — plain functions, no framework.

The one rule: never fetch a whole table into memory. Every query goes
through a server-side cursor and comes back as a stream of pyarrow
RecordBatches, so a job's memory footprint is one chunk regardless of
table size.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa

CHUNK_ROWS = 50_000


def stream_query(
    dsn: str,
    sql: str,
    params: dict | None = None,
    chunk_rows: int = CHUNK_ROWS,
) -> Iterator[pa.RecordBatch]:
    """Run `sql` on a server-side (named) cursor and yield RecordBatches
    of at most `chunk_rows` rows, preserving column names and types.

    Implementation notes (step 2):
    - psycopg named cursor + fetchmany(chunk_rows); the replica holds the
      snapshot for the duration of the cursor, so one consistent read per run.
    - Type mapping pg → arrow handled once here; jobs never see raw tuples.
    """
    raise NotImplementedError("step 2")
