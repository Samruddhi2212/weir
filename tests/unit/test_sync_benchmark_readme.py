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


def sample_result():
    """The shape benchmarks/run_benchmark.py actually writes.

    An earlier version of this test asserted a per-scenario
    detection_rate/false_positive_rate shape that no runner ever
    produced - so it validated the renderer against an imagined schema,
    with invented numbers, and would have rendered every README row as
    "unknown | n/a" against a real result file.
    """
    return {
        "run_at": "2026-09-12T23:30:42Z",
        "scenarios": [
            {"name": "volume_partition_degradation", "expected_detector": "volume",
             "detected": True, "detection_latency_seconds": 36235.0},
            {"name": "freshness_gradual_delay", "expected_detector": "freshness",
             "detected": False, "detection_latency_seconds": None},
            {"name": "slow_volume_decline", "expected_detector": None,
             "detected": False, "detection_latency_seconds": None},
        ],
        "summary": {
            "detection_rate": 1 / 3, "detection_rate_fraction": "1/3",
            "targeted_detection_rate": 0.5, "targeted_detection_rate_fraction": "1/2",
            "false_positive_rate": 0.019143, "false_positive_rate_fraction": "126/6582",
            "false_positives_at_band_boundaries": 11,
            "detection_latency_median_seconds": 36235.0,
            "detection_latency_p95_seconds": 36235.0,
            "warmed_buckets": 12, "band_count": 4,
            "clean_event_time_hours": 2919.9166,
            "unattributed_injected_incidents": 180,
        },
    }


def test_render_block_with_result_file(tmp_path):
    result_file = tmp_path / "run.json"
    result_file.write_text(json.dumps(sample_result()), encoding="utf-8")

    block = render_block(result_file)

    assert "run.json" in block
    assert "| volume_partition_degradation | yes | 36235s |" in block
    assert "1/3" in block and "126/6582" in block
    # Expected misses are labelled, not quietly rendered as ordinary misses.
    assert "no (expected miss)" in block


def test_render_block_does_not_round_a_rate_away(tmp_path):
    result_file = tmp_path / "run.json"
    data = sample_result()
    data["summary"]["detection_rate"] = 3 / 7  # 42.857142...%
    result_file.write_text(json.dumps(data), encoding="utf-8")

    block = render_block(result_file)

    assert "42.857" in block, "a rate was rounded to something flattering"


def test_render_block_reports_unmeasured_as_na_never_zero(tmp_path):
    result_file = tmp_path / "run.json"
    data = sample_result()
    data["summary"]["detection_latency_median_seconds"] = None
    data["summary"]["detection_latency_p95_seconds"] = None
    result_file.write_text(json.dumps(data), encoding="utf-8")

    block = render_block(result_file)

    # "0s" would read as "flagged instantly" rather than "never measured".
    assert "n/a" in block
    assert "| 0s / 0s |" not in block


def test_render_block_missing_fields_render_as_na(tmp_path):
    result_file = tmp_path / "run.json"
    result_file.write_text(
        json.dumps({"run_at": "unknown", "scenarios": [{"name": "no_metrics"}]}),
        encoding="utf-8",
    )

    block = render_block(result_file)

    assert "n/a" in block


def test_rendered_block_survives_the_fabricated_metrics_guard(tmp_path):
    """The generated block must not trip check_no_metrics_in_markdown.sh,
    which scans README.md too - a generator whose output fails the repo's
    own guard could never actually be published (DEFENSE.md #9)."""
    import re

    result_file = tmp_path / "run.json"
    result_file.write_text(json.dumps(sample_result()), encoding="utf-8")

    block = render_block(result_file)

    pattern = re.compile(
        r"(detection rate|false[-\s]positive rate|detection latency|throughput)[^0-9]{0,40}[0-9]",
        re.IGNORECASE,
    )
    offenders = [line for line in block.splitlines() if pattern.search(line)]
    assert not offenders, f"generated block would fail the metrics guard: {offenders}"


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
