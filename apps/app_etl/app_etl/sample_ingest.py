"""sample-ingest — CI/CD deploy canary, not a real pipeline.

`payments` and `merchants` aren't implemented yet (see ingestion/), so this
module is what the `sample-ingest` Cloud Run Job actually runs: it loads
Settings from env and logs them, then exits. Its only job is to prove the
container image, IAM identity, and Cloud Run Jobs wiring work end-to-end —
cloudbuild/release.yaml builds, pushes, and deploys this image on every merge
to main. Once a real pipeline is ready to take over the `sample-ingest` job /
trigger, this module goes away.
"""

from __future__ import annotations

import logging

from app_etl.config import Settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("sample_ingest")


def main() -> None:
    settings = Settings.from_env()
    logger.info(
        "sample-ingest ok project=%s bucket=%s dataset_raw=%s dataset_ops=%s",
        settings.gcp_project,
        settings.gcs_bucket,
        settings.bq_dataset_raw,
        settings.bq_dataset_ops,
    )


if __name__ == "__main__":
    main()
