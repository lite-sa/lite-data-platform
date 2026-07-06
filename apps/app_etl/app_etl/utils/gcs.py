"""GCS landing helpers.

Landing layout convention (one file per job run, immutable once written):

    gs://{bucket}/raw/litecore/{table}/ingest_date=YYYY-MM-DD/{run_ts}.parquet

Files are never overwritten or appended; a rerun writes a new run_ts file.
"""

from __future__ import annotations

from collections.abc import Iterator

import pyarrow as pa


def write_parquet(
    bucket: str,
    object_path: str,
    batches: Iterator[pa.RecordBatch],
) -> str:
    """Stream RecordBatches into a single Parquet object on GCS
    (one row group per batch — memory stays one chunk). Returns the
    full gs:// URI of the written object.

    Implementation notes (step 2): pyarrow ParquetWriter over a GCS
    file handle; write-through, no local temp file.
    """
    raise NotImplementedError("step 2")
