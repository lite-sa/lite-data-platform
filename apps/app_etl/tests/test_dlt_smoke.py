"""dlt smoke test: a tiny pipeline runs end-to-end locally.

Exercises the same chain the real pipelines use — resource with an explicit
column list → extract → normalize → Parquet on a filesystem destination —
with file:// standing in for gs:// and no cloud credentials needed.
"""

from datetime import datetime, timezone

import dlt
import pyarrow.parquet as pq


def test_pipeline_runs_to_local_parquet(tmp_path):
    @dlt.resource(
        name="payments_smoke",
        write_disposition="append",
        columns={
            "id": {"data_type": "text", "nullable": False},
            "amount": {"data_type": "bigint", "nullable": False},
            "created_at": {"data_type": "timestamp", "nullable": False},
        },
    )
    def payments_smoke():
        yield {"id": "pay_1", "amount": 1000, "created_at": datetime(2026, 7, 1, tzinfo=timezone.utc)}
        yield {"id": "pay_2", "amount": 2500, "created_at": datetime(2026, 7, 2, tzinfo=timezone.utc)}

    pipeline = dlt.pipeline(
        pipeline_name="smoke",
        destination=dlt.destinations.filesystem(bucket_url=tmp_path.as_uri()),
        dataset_name="raw_test",
        pipelines_dir=str(tmp_path / "state"),  # keep state out of ~/.dlt
    )

    load_info = pipeline.run(payments_smoke, loader_file_format="parquet")
    load_info.raise_on_failed_jobs()

    files = list((tmp_path / "raw_test" / "payments_smoke").glob("*.parquet"))
    assert len(files) == 1

    table = pq.read_table(files[0])
    assert {"id", "amount", "created_at"} <= set(table.column_names)
    assert sorted(table.column("id").to_pylist()) == ["pay_1", "pay_2"]
