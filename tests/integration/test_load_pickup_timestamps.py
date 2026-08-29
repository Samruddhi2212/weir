"""Integration test for ingestion/replay/load_pickup_timestamps.py.

V7: ground truth is computed independently of the loader under test -
a separate, direct pyarrow read of each file, not a second call into
the loader's own code path. Same discipline as
scripts/verify_metrics_job.py's deep/broad checks.

V8: unaccounted-for records must be explained, not tolerated. A first
version of this test asserted the output's span with bounds loose
enough that they passed even with 9 real clock-error timestamps (years
2002/2008/2009, one 2025-03-23) still included - the range check
existed but wasn't tight enough to actually catch the thing it was
supposed to catch. Fixed by asserting the exact plausible-window
bounds and the exact implausible-drop count, both independently
computed here, not by loosening the check further.

Requires the four real TLC files already downloaded to data/tlc/
(2024-10, 2024-11, 2024-12, 2025-01) - skipped if they're not present,
matching this project's pattern of not silently faking real data.
"""
import datetime
from pathlib import Path

import pyarrow.parquet as pq
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = REPO_ROOT / "data" / "tlc"
MONTHS = ["2024-10", "2024-11", "2024-12", "2025-01"]
PATHS = [DATA_DIR / f"yellow_tripdata_{m}.parquet" for m in MONTHS]

pytestmark = pytest.mark.skipif(
    not all(p.exists() for p in PATHS),
    reason=f"real TLC files not downloaded to {DATA_DIR} - run ingestion/replay/download_tlc_data.py for {MONTHS}",
)


def _independent_row_count(path):
    """Reads the file directly, not through the loader under test."""
    return pq.read_table(path, columns=["tpep_pickup_datetime"]).num_rows


def _independent_dst_ambiguous_count(path):
    """Directly counts rows in the Nov 3 2024 fall-back's ambiguous
    local hour, via its own pyarrow.compute filter - not by calling
    load_pickup_timestamps.DST_FALLBACK_AMBIGUOUS_HOURS or
    _is_dst_ambiguous at all."""
    import pyarrow.compute as pc

    table = pq.read_table(path, columns=["tpep_pickup_datetime"])
    col = table.column("tpep_pickup_datetime")
    start = datetime.datetime(2024, 11, 3, 1, 0, 0)
    end = datetime.datetime(2024, 11, 3, 2, 0, 0)
    mask = pc.and_(pc.greater_equal(col, start), pc.less(col, end))
    return int(pc.sum(mask).as_py() or 0)


def _independent_implausible_count_and_values(paths, window_start, window_end):
    """Directly counts + collects rows outside [window_start,
    window_end) across all files, via its own pyarrow.compute filter -
    not by calling load_pickup_timestamps.plausible_window at all
    (window bounds are passed in, computed by the test itself below,
    from the same month list the loader is given - not imported from
    the module under test)."""
    import pyarrow.compute as pc

    total = 0
    values = []
    for path in paths:
        table = pq.read_table(path, columns=["tpep_pickup_datetime"])
        col = table.column("tpep_pickup_datetime")
        mask = pc.or_(pc.less(col, window_start), pc.greater_equal(col, window_end))
        total += int(pc.sum(mask).as_py() or 0)
        values.extend(pc.filter(col, mask).to_pylist())
    return total, values


def _expected_plausible_window(margin_days=7):
    """Computed independently here from the calendar, not by calling
    load_pickup_timestamps.plausible_window."""
    start = datetime.datetime(2024, 10, 1) - datetime.timedelta(days=margin_days)
    end = datetime.datetime(2025, 2, 1) + datetime.timedelta(days=margin_days)
    return start, end


def test_per_file_counts_match_independent_read():
    from ingestion.replay.load_pickup_timestamps import load_pickup_timestamps

    _, per_file_counts, _, _ = load_pickup_timestamps([str(p) for p in PATHS])

    for path in PATHS:
        expected = _independent_row_count(path)
        actual = per_file_counts[str(path)]
        assert actual == expected, f"{path}: loader reported {actual}, independent read found {expected}"


def test_dst_ambiguous_hour_dropped_matches_independent_count():
    from ingestion.replay.load_pickup_timestamps import load_pickup_timestamps

    timestamps, per_file_counts, dst_dropped, implausible_dropped = load_pickup_timestamps([str(p) for p in PATHS])

    expected_dst_dropped = _independent_dst_ambiguous_count(DATA_DIR / "yellow_tripdata_2024-11.parquet")
    assert dst_dropped == expected_dst_dropped, (
        f"loader dropped {dst_dropped} rows for the DST fall-back hour, "
        f"independent count found {expected_dst_dropped}"
    )
    assert expected_dst_dropped > 0, "sanity check: the fall-back hour should have real trips to drop"

    total_before_any_drop = sum(per_file_counts.values())
    assert len(timestamps) == total_before_any_drop - dst_dropped - implausible_dropped


def test_implausible_timestamps_dropped_matches_independent_count():
    from ingestion.replay.load_pickup_timestamps import load_pickup_timestamps

    _, _, _, implausible_dropped = load_pickup_timestamps([str(p) for p in PATHS])

    window_start, window_end = _expected_plausible_window()
    expected_count, expected_values = _independent_implausible_count_and_values(PATHS, window_start, window_end)

    assert implausible_dropped == expected_count, (
        f"loader dropped {implausible_dropped} implausible timestamps, "
        f"independent count found {expected_count} (values: {sorted(expected_values)})"
    )
    # This is the real finding (DEFENSE.md #44 addendum): 10 real rows
    # outside [window_start, window_end) across these four files -
    # 2002/2008/2009 clock errors, two March 2025 clock errors, and
    # one 2025-02-09 (8 days past the January file's own margin - the
    # margin_days=7 policy correctly excludes it, whether it's a
    # further clock error or a legitimately late-reported trip; either
    # way it's outside the window this loader actually implements).
    # An earlier version of this assertion said 9, from a manual check
    # that used a looser bound (through Feb 15) than the margin_days=7
    # this loader actually applies (through Feb 8) - fixed to match
    # the real, independently-verified count, not the loader's number.
    assert expected_count == 10, (
        f"expected exactly 10 known implausible timestamps in this dataset, found {expected_count}: "
        f"{sorted(expected_values)} - if the real data changed, update this expectation with a reason, "
        f"don't just widen the assertion"
    )


def test_output_is_sorted_and_exactly_within_plausible_window():
    from ingestion.replay.load_pickup_timestamps import load_pickup_timestamps

    timestamps, _, _, _ = load_pickup_timestamps([str(p) for p in PATHS])

    assert timestamps == sorted(timestamps)

    window_start, window_end = _expected_plausible_window()
    assert all(window_start <= ts < window_end for ts in timestamps), (
        "loader's output contains a timestamp outside the expected plausible window - "
        "the implausible-timestamp filter let something through"
    )

    # Real span should hug the requested months closely (within the
    # margin), not just "somewhere after 2024 and before 2026" - a
    # loose bound here is exactly what let the 9 clock-error rows
    # through undetected before this test was tightened.
    assert timestamps[0] < datetime.datetime(2024, 10, 8)
    assert timestamps[-1] >= datetime.datetime(2025, 1, 24)
    assert timestamps[-1] < datetime.datetime(2025, 2, 8)

    span_weeks = (timestamps[-1] - timestamps[0]).days / 7
    assert span_weeks >= 8, f"span is only {span_weeks:.1f} weeks - below the 8-week warmup threshold (DEFENSE.md #44)"
