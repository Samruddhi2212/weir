"""Unit tests for scripts/sync_benchmark_readme.py's pure functions.

Loaded by file path via importlib rather than added to sys.path - scripts/
isn't a package, and this avoids mutating global import state for other
test modules in the same pytest session.
"""

import importlib.util
import json
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "sync_benchmark_readme.py"
_spec = importlib.util.spec_from_file_location("sync_benchmark_readme", SCRIPT_PATH)
sync_benchmark_readme = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(sync_benchmark_readme)

render_block = sync_benchmark_readme.render_block
replace_block = sync_benchmark_readme.replace_block
BEGIN_MARKER = sync_benchmark_readme.BEGIN_MARKER
END_MARKER = sync_benchmark_readme.END_MARKER
NO_RUN_TEXT = sync_benchmark_readme.NO_RUN_TEXT


def test_render_block_no_result_file_returns_no_run_text():
    assert render_block(None) == NO_RUN_TEXT


def test_render_block_with_result_file(tmp_path):
    result_file = tmp_path / "run.json"
    result_file.write_text(
        json.dumps(
            {
                "run_at": "2026-08-18T00:00:00Z",
                "scenarios": [
                    {
                        "scenario": "kafka_broker_kill",
                        "detection_rate": 0.94,
                        "false_positive_rate": 0.02,
                        "detection_latency_ms": 1200,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    block = render_block(result_file)

    assert "run.json" in block
    assert "94.0%" in block
    assert "2.0%" in block
    assert "1200" in block


def test_render_block_missing_fields_render_as_na(tmp_path):
    result_file = tmp_path / "run.json"
    result_file.write_text(
        json.dumps({"run_at": "unknown", "scenarios": [{"scenario": "no_metrics"}]}),
        encoding="utf-8",
    )

    block = render_block(result_file)

    assert "n/a" in block


def test_replace_block_swaps_content_between_markers():
    readme_text = f"before\n{BEGIN_MARKER}\nold content\n{END_MARKER}\nafter"

    updated = replace_block(readme_text, "new content")

    assert "old content" not in updated
    assert "new content" in updated
    assert updated.startswith("before")
    assert updated.endswith("after")


def test_replace_block_raises_if_markers_missing():
    try:
        replace_block("no markers here", "content")
    except SystemExit:
        pass
    else:
        raise AssertionError("expected SystemExit when markers are missing")
