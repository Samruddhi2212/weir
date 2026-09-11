"""Single-pass runner for the null-rate detector. One instance per
monitored column (config_for_column). observed_value is null_count /
row_count for that column, per window - a rate in [0, 1], not a raw
count. Reuses adapter.process_window/register_detector and run.py's
timezone helpers unchanged, same as freshness.

null_count (COUNT(*) - COUNT(col)) is always a real, non-NULL value
for every real window_metrics row that has a matching column_metrics
row for this column (DEFENSE.md #49 addendum - COUNT-shaped metrics
are always computable, unlike MIN/MAX/AVG) - an INNER JOIN is safe,
not a source of silently-dropped windows.
"""
import argparse
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from reliability.nullrate.config import config_for_column
from reliability.volume.adapter import process_window, register_detector
from reliability.volume.run import to_naive_local, to_utc_instant


def fetch_eligible_windows(conn, config, column_name, assume_no_more_arrivals=False):
    """Same lag-buffer/assume_no_more_arrivals shape as
    reliability.volume.run.fetch_eligible_windows - rows are
    (window_start, window_end, null_rate)."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_processed_window_end FROM weir_incidents.detector_progress "
            "WHERE detector_name = %s",
            (config.detector_name,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"detector {config.detector_name!r} is not registered - call register_detector first"
            )
        frontier_naive_local = to_naive_local(row[0], config.timezone)

        cur.execute(
            "SELECT wm.window_start, wm.window_end, cm.metric_value, wm.row_count "
            "FROM weir_metrics.window_metrics wm "
            "JOIN weir_metrics.column_metrics cm "
            "  ON cm.window_start = wm.window_start AND cm.window_end = wm.window_end "
            "WHERE cm.column_name = %s AND cm.metric_name = 'null_count' "
            "  AND wm.window_end > %s "
            "ORDER BY wm.window_end ASC",
            (column_name, frontier_naive_local),
        )
        rows = cur.fetchall()

    results = [
        (window_start, window_end, null_count / row_count)
        for window_start, window_end, null_count, row_count in rows
    ]

    if not results or assume_no_more_arrivals:
        return results

    max_window_end_seen = max(r[1] for r in results)
    lag_buffer = datetime.timedelta(seconds=config.max_lag_seconds)
    return [r for r in results if r[1] <= max_window_end_seen - lag_buffer]


def run_once(conn, column_name, assume_no_more_arrivals=False):
    config = config_for_column(column_name)
    register_detector(conn, config.detector_name)

    statuses = []
    for window_start_naive, window_end_naive, null_rate in fetch_eligible_windows(
        conn, config, column_name, assume_no_more_arrivals=assume_no_more_arrivals
    ):
        window_start_utc = to_utc_instant(window_start_naive, config.timezone)
        window_end_utc = to_utc_instant(window_end_naive, config.timezone)
        status = process_window(conn, window_start_utc, window_end_utc, null_rate, config)
        statuses.append(status)
    return statuses


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    parser.add_argument("--column", required=True, help="the column_metrics column_name to monitor")
    parser.add_argument("--assume-no-more-arrivals", action="store_true")
    args = parser.parse_args()

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    try:
        statuses = run_once(conn, args.column, assume_no_more_arrivals=args.assume_no_more_arrivals)
        print(f"processed {len(statuses)} window(s) for column {args.column!r}: {statuses}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
