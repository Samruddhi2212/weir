"""Single-pass runner for the freshness detector. observed_value is
the arrival gap in seconds - (this window's window_end) minus (the
previous real window_metrics row's window_end) - not a lateness/delay
measurement. Freshness here means "is data still arriving on
schedule," directly computable from window_metrics.window_end alone,
already-verified real data - not blocked on the lateness_p50/p99/max
columns, which schema.sql's own comment already flags as never
populated by the current pipeline.

Gaps computed in UTC (to_utc_instant), not naive local - a duration
between two instants is only correct in a fixed-offset representation;
naive local arithmetic across the DST fall-back would be wrong.

The very first real window a freshly-registered detector ever sees has
no prior window to compute a gap from - skipped from the eligible
batch entirely (not scored, not given a placeholder), rather than
resurrecting a NULL-observed-value status for a boundary condition
that happens exactly once per detector's lifetime.
"""
import argparse
import os
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from reliability.freshness.config import DEFAULT_CONFIG
from reliability.volume.adapter import process_window, register_detector, release_snapshot
from reliability.volume.run import to_naive_local, to_utc_instant


def fetch_eligible_windows(conn, config, assume_no_more_arrivals=False):
    """Same lag-buffer/assume_no_more_arrivals shape as
    reliability.volume.run.fetch_eligible_windows (DEFENSE.md #45
    point 2 / #51) - rows are (window_start, window_end, gap_seconds)
    instead of (window_start, window_end, row_count)."""
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

        # Seed: the real window_metrics row immediately at-or-before the
        # frontier, so the first eligible row's gap is computed against
        # real prior data, not assumed.
        cur.execute(
            "SELECT window_end FROM weir_metrics.window_metrics "
            "WHERE window_end <= %s ORDER BY window_end DESC LIMIT 1",
            (frontier_naive_local,),
        )
        seed = cur.fetchone()
        previous_window_end_naive = seed[0] if seed else None

        cur.execute(
            "SELECT window_start, window_end FROM weir_metrics.window_metrics "
            "WHERE window_end > %s ORDER BY window_end ASC",
            (frontier_naive_local,),
        )
        rows = cur.fetchall()

    results = []
    prev = previous_window_end_naive
    for window_start_naive, window_end_naive in rows:
        if prev is not None:
            gap_seconds = (
                to_utc_instant(window_end_naive, config.timezone)
                - to_utc_instant(prev, config.timezone)
            ).total_seconds()
            results.append((window_start_naive, window_end_naive, gap_seconds))
        prev = window_end_naive

    release_snapshot(conn)

    if not results or assume_no_more_arrivals:
        return results

    max_window_end_seen = max(r[1] for r in results)
    lag_buffer = datetime.timedelta(seconds=config.max_lag_seconds)
    return [r for r in results if r[1] <= max_window_end_seen - lag_buffer]


def run_once(conn, config=DEFAULT_CONFIG, assume_no_more_arrivals=False, progress=None):
    """progress, if given, is called as progress(done, total) after each
    window - see reliability/volume/run.py's run_once for why."""
    register_detector(conn, config.detector_name)

    eligible = fetch_eligible_windows(
        conn, config, assume_no_more_arrivals=assume_no_more_arrivals
    )
    statuses = []
    for window_start_naive, window_end_naive, gap_seconds in eligible:
        window_start_utc = to_utc_instant(window_start_naive, config.timezone)
        window_end_utc = to_utc_instant(window_end_naive, config.timezone)
        status = process_window(conn, window_start_utc, window_end_utc, gap_seconds, config)
        statuses.append(status)
        if progress is not None:
            progress(len(statuses), len(eligible))
    return statuses


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    # No literal default. A guessable built-in default is worse than a
    # required variable: it lets a misconfigured run succeed quietly
    # against a credential anyone reading the repo already knows. Any
    # value works locally - see .env.example - but it has to be chosen.
    parser.add_argument("--password", default=os.environ.get("POSTGRES_PASSWORD"),
                        required=os.environ.get("POSTGRES_PASSWORD") is None,
                        help="Postgres password. Set POSTGRES_PASSWORD or pass this flag.")
    parser.add_argument("--assume-no-more-arrivals", action="store_true")
    args = parser.parse_args()

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    try:
        statuses = run_once(conn, assume_no_more_arrivals=args.assume_no_more_arrivals)
        print(f"processed {len(statuses)} window(s): {statuses}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
