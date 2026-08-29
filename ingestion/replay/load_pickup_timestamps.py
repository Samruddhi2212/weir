#!/usr/bin/env python3
"""Multi-month pickup-timestamp-only loader, for the volume detector's
baseline/ground-truth construction (V7: independent of the system
under test) - not for Kafka replay, which needs full rows.

Reads only `tpep_pickup_datetime` across several monthly TLC files and
concatenates them. That single column is schema-identical across
Oct/Nov/Dec 2024 and Jan 2025 even though those files otherwise are
not (Jan 2025 has `cbd_congestion_fee`, a Manhattan congestion-pricing
fee that started that month; the earlier months don't) - reading only
the one column this detector needs sidesteps that mismatch entirely.

Drops the DST fall-back's ambiguous local hour (Nov 3 2024,
01:00:00-01:59:59 local) rather than guessing at UTC normalization -
confirmed empirically (not assumed) that pickup timestamps are naive
America/New_York civil time, and that a naive timestamp in that one
hour cannot be disambiguated as pre- or post-transition. See
DEFENSE.md #44.

Also drops timestamps outside a plausible window around the requested
months (each requested month's start/end +/- 7 days) - found for
real, not assumed: across Oct 2024-Jan 2025's four files, 9 rows carry
pickup timestamps from 2002, 2008, or 2009, and one from March 2025 -
taxi-meter clock errors at the source, not legitimate month-boundary
spillover (which is a matter of hours/days, not years). Both drops are
counted and returned separately so neither is silently unaccounted
for (V8)."""
import datetime
import re
import sys

import pyarrow.parquet as pq

PICKUP_COLUMN = "tpep_pickup_datetime"
_YEAR_MONTH_RE = re.compile(r"(\d{4})-(\d{2})")

# Fall-back transitions affecting this project's Oct 2024-Jan 2025
# data range. Each is the local wall-clock hour that occurred twice
# that night - DEFENSE.md #44 confirmed Nov 3 2024 empirically; this
# is a fixed table, not detected at runtime, since DST transition
# dates are a matter of public record, not something to infer from
# the data each time.
DST_FALLBACK_AMBIGUOUS_HOURS = [
    (datetime.datetime(2024, 11, 3, 1, 0, 0), datetime.datetime(2024, 11, 3, 2, 0, 0)),
]


def _is_dst_ambiguous(ts):
    return any(start <= ts < end for start, end in DST_FALLBACK_AMBIGUOUS_HOURS)


def _month_start(year, month):
    return datetime.datetime(year, month, 1)


def _month_end_exclusive(year, month):
    if month == 12:
        return datetime.datetime(year + 1, 1, 1)
    return datetime.datetime(year, month + 1, 1)


def plausible_window(parquet_paths, margin_days=7):
    """The [start, end) window a legitimate pickup timestamp should
    fall in, given the requested files - each requested month's own
    span, padded by margin_days on both sides for ordinary month-
    boundary spillover (DEFENSE.md #41 found ~22 such rows for a
    single month; margin_days=7 is generous relative to that, while
    nowhere near enough to admit a timestamp that's years off)."""
    year_months = []
    for path in parquet_paths:
        m = _YEAR_MONTH_RE.search(str(path))
        if not m:
            raise SystemExit(f"FAIL: could not parse a YYYY-MM year-month out of {path}")
        year_months.append((int(m.group(1)), int(m.group(2))))
    starts = [_month_start(y, mo) for y, mo in year_months]
    ends = [_month_end_exclusive(y, mo) for y, mo in year_months]
    margin = datetime.timedelta(days=margin_days)
    return min(starts) - margin, max(ends) + margin


def load_pickup_timestamps(parquet_paths, margin_days=7):
    """Returns a single sorted list of pickup timestamps across all
    given files, with the DST fall-back's ambiguous hour and any
    implausible (years-off clock-error) timestamps dropped. Each file
    is checked for the pickup column's presence explicitly - TLC's
    schema has changed across releases before (see load_sorted_trips's
    own check in replay_producer.py). Returns (timestamps,
    per_file_counts, dst_dropped, implausible_dropped)."""
    all_timestamps = []
    per_file_counts = {}
    for path in parquet_paths:
        table = pq.read_table(path, columns=[PICKUP_COLUMN])
        if PICKUP_COLUMN not in table.column_names:
            raise SystemExit(f"FAIL: {path} has no '{PICKUP_COLUMN}' column")
        values = table.column(PICKUP_COLUMN).to_pylist()
        per_file_counts[str(path)] = len(values)
        all_timestamps.extend(values)

    before_dst_drop = len(all_timestamps)
    all_timestamps = [ts for ts in all_timestamps if not _is_dst_ambiguous(ts)]
    dst_dropped = before_dst_drop - len(all_timestamps)

    window_start, window_end = plausible_window(parquet_paths, margin_days=margin_days)
    before_range_drop = len(all_timestamps)
    implausible = [ts for ts in all_timestamps if not (window_start <= ts < window_end)]
    all_timestamps = [ts for ts in all_timestamps if window_start <= ts < window_end]
    implausible_dropped = before_range_drop - len(all_timestamps)
    if implausible:
        print(
            f"dropped {implausible_dropped} implausible timestamp(s) outside "
            f"[{window_start}, {window_end}): {sorted(implausible)}",
            file=sys.stderr,
        )

    all_timestamps.sort()
    return all_timestamps, per_file_counts, dst_dropped, implausible_dropped


def main():
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", nargs="+", required=True, help="one or more parquet file paths")
    args = parser.parse_args()

    timestamps, per_file_counts, dst_dropped, implausible_dropped = load_pickup_timestamps(args.input)
    for path, count in per_file_counts.items():
        print(f"{path}: {count} rows", file=sys.stderr)
    print(f"dropped {dst_dropped} row(s) in the DST fall-back's ambiguous hour", file=sys.stderr)
    print(f"dropped {implausible_dropped} row(s) outside the plausible date window", file=sys.stderr)
    print(f"total: {len(timestamps)} timestamps, span {timestamps[0]} to {timestamps[-1]}", file=sys.stderr)


if __name__ == "__main__":
    main()
