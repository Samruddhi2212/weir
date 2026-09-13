#!/usr/bin/env python3
"""Measure detector scoring throughput in isolation from replay.

Benchmark run 34757520031 died at the 350-minute CI ceiling with 4h36m
spent silently inside clean-phase detector scoring. Replay was not the
problem - it finished in 1h12m. This script isolates the scoring half so
the feasible contiguous span can be computed from a measurement instead
of from arithmetic about fsync costs.

It seeds window_metrics/column_metrics directly with N consecutive
one-minute windows - no Kafka, no Flink, no parquet - then times
run_once() for each detector. Seeded row_counts are constant, so nothing
here says anything about detection quality; the only output is windows
scored per second. Detector code is untouched (CLAUDE.md hard rule 3):
the only variable is Postgres' synchronous_commit setting, which changes
durability, not any computed value.
"""
import argparse
import datetime
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reliability.freshness.run import run_once as run_freshness
from reliability.nullrate.run import run_once as run_nullrate
from reliability.volume.run import run_once as run_volume

# Naive local civil time, matching what the Flink job writes. Anchored
# well clear of a DST transition so no window is excluded as ambiguous
# and every seeded window actually reaches the scoring path.
SEED_ANCHOR = datetime.datetime(2024, 10, 1, 0, 0, 0)
NULLRATE_COLUMN = "passenger_count"
SEEDED_ROW_COUNT = 1000
SEEDED_NULL_COUNT = 50

INCIDENT_TABLES = (
    "weir_incidents.incidents",
    "weir_incidents.scored_windows",
    "weir_incidents.baseline_state",
    "weir_incidents.detector_progress",
)


def seed(conn, windows):
    """N consecutive one-minute windows, bulk-loaded via COPY."""
    with conn.cursor() as cur:
        cur.execute("TRUNCATE weir_metrics.window_metrics, weir_metrics.column_metrics")
        with cur.copy(
            "COPY weir_metrics.window_metrics (window_start, window_end, row_count) FROM STDIN"
        ) as copy:
            for i in range(windows):
                start = SEED_ANCHOR + datetime.timedelta(minutes=i)
                copy.write_row((start, start + datetime.timedelta(minutes=1), SEEDED_ROW_COUNT))
        with cur.copy(
            "COPY weir_metrics.column_metrics "
            "(window_start, window_end, column_name, metric_name, metric_value) FROM STDIN"
        ) as copy:
            for i in range(windows):
                start = SEED_ANCHOR + datetime.timedelta(minutes=i)
                copy.write_row(
                    (start, start + datetime.timedelta(minutes=1),
                     NULLRATE_COLUMN, "null_count", float(SEEDED_NULL_COUNT))
                )
    conn.commit()


def reset_incidents(conn):
    with conn.cursor() as cur:
        cur.execute(f"TRUNCATE {', '.join(INCIDENT_TABLES)}")
    conn.commit()


def time_detectors(conn):
    """run_once for all three detectors, wall-clock each. Returns
    (per-detector seconds, per-detector windows processed)."""
    timings = {}
    counts = {}
    for name, call in (
        ("volume", lambda: run_volume(conn)),
        ("freshness", lambda: run_freshness(conn)),
        (f"null_rate_{NULLRATE_COLUMN}", lambda: run_nullrate(conn, NULLRATE_COLUMN)),
    ):
        started = time.monotonic()
        statuses = call()
        timings[name] = time.monotonic() - started
        counts[name] = len(statuses)
    return timings, counts


def report(label, timings, counts):
    total_seconds = sum(timings.values())
    total_windows = sum(counts.values())
    print(f"\n--- synchronous_commit = {label} ---")
    for name in timings:
        rate = counts[name] / timings[name] if timings[name] else float("nan")
        print(f"  {name}: {counts[name]} windows in {timings[name]:.2f}s = {rate:.1f} windows/s")
    if not total_seconds or not total_windows:
        print("FAIL: nothing was scored, so there is no rate to report")
        sys.exit(1)
    aggregate = total_windows / total_seconds
    print(f"  ALL THREE: {total_windows} window-scorings in {total_seconds:.2f}s "
          f"= {aggregate:.1f} window-scorings/s")
    return aggregate


def project(aggregate_rate, span_weeks, detectors=3):
    """Hours of scoring for one replay phase at a given contiguous span."""
    windows = span_weeks * 7 * 24 * 60
    return (windows * detectors) / aggregate_rate / 3600.0


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--windows", type=int, default=20000,
                        help="consecutive one-minute windows to seed and score")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    args = parser.parse_args()

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    rates = {}
    try:
        print(f"seeding {args.windows} one-minute windows from {SEED_ANCHOR} ...")
        seed(conn, args.windows)

        # ALTER SYSTEM refuses to run inside a transaction block, and the
        # detector connection is deliberately autocommit=False, so the
        # cluster-wide setting is driven from a separate admin session.
        # ALTER SYSTEM rather than SET: it has to survive the transactions
        # run_once opens and closes for every window.
        admin = psycopg.connect(conninfo, autocommit=True, connect_timeout=10)
        try:
            for setting in ("on", "off"):
                reset_incidents(conn)
                with admin.cursor() as cur:
                    cur.execute(f"ALTER SYSTEM SET synchronous_commit = '{setting}'")
                    cur.execute("SELECT pg_reload_conf()")
                # Read it back on the connection that will actually do the
                # scoring - a reload the detector session hasn't picked up
                # would silently measure the wrong thing.
                conn.rollback()
                with conn.cursor() as cur:
                    cur.execute("SHOW synchronous_commit")
                    (effective,) = cur.fetchone()
                conn.rollback()
                if effective != setting:
                    print(f"FAIL: asked for synchronous_commit={setting}, "
                          f"the scoring session reports {effective}")
                    sys.exit(1)
                timings, counts = time_detectors(conn)
                rates[setting] = report(setting, timings, counts)

            with admin.cursor() as cur:
                cur.execute("ALTER SYSTEM RESET synchronous_commit")
                cur.execute("SELECT pg_reload_conf()")
        finally:
            admin.close()
    finally:
        conn.close()

    speedup = rates["off"] / rates["on"]
    print(f"\nsynchronous_commit=off is {speedup:.2f}x the throughput of on")
    print("\nprojected scoring hours for ONE replay phase (three detectors):")
    print(f"  {'span':>8}  {'on':>10}  {'off':>10}")
    for span_weeks in (8, 10, 12):
        print(f"  {span_weeks:>6}w  {project(rates['on'], span_weeks):>9.2f}h  "
              f"{project(rates['off'], span_weeks):>9.2f}h")
    print("\nA full benchmark is TWO phases (clean + injected), so double the "
          "above and add ~1.2h/phase of replay. The CI ceiling is 6h.")


if __name__ == "__main__":
    main()
