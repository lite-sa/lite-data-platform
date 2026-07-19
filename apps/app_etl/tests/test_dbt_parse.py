"""The dbt project must at least parse (SQL compiles, refs/sources/vars
resolve, profile renders) without a BigQuery connection — the same gate
CI gets, since pr.yaml runs pytest but has no GCP credentials.

Shells out to the `dbt` entry point in the workspace venv (resolved next
to the running interpreter) rather than importing dbt, so the test stays
hermetic to dbt's global CLI state.
"""

import os
import subprocess
import sys
from pathlib import Path

DBT_PROJECT_DIR = Path(__file__).resolve().parents[1] / "dbt"
DBT_BIN = Path(sys.executable).parent / "dbt"


def test_dbt_project_parses(tmp_path):
    env = os.environ | {"GCP_PROJECT": "parse-test-project"}

    result = subprocess.run(
        [
            str(DBT_BIN), "parse",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROJECT_DIR),
            "--target-path", str(tmp_path / "target"),
            "--log-path", str(tmp_path / "logs"),
            "--no-send-anonymous-usage-stats",
        ],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )

    assert result.returncode == 0, f"{result.stdout}\n{result.stderr}"
