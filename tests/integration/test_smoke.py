"""Integration test wrapping scripts/smoke_test.sh (verification steps 2-5).

Requires the stack already running (`make up` / `docker compose up -d
--build`) - this test does not start it and does not tear it down. In
CI, the workflow does both around the pytest invocation. See
scripts/smoke_test.sh for what each step actually checks, and DEFENSE.md
#10-13 for the four fixes that were needed to get step 5 passing.
"""

import subprocess
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def test_smoke_steps_2_through_5():
    result = subprocess.run(
        ["bash", "scripts/smoke_test.sh"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, (
        f"smoke_test.sh failed (exit {result.returncode}).\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
