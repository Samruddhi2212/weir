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
import os
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
# The real Flink job writes 66 column_metrics rows per window.
REAL_METRICS_PER_WINDOW = 66
RELOAD_TIMEOUT_SECONDS = 10.0

INCIDENT_TABLES = (
    "weir_incidents.incidents",
    "weir_incidents.scored_windows",
    "weir_incidents.baseline_state",
    "weir_incidents.detector_progress",
)


def seed(conn, windows, metrics_per_window):
    """N consecutive one-minute windows, bulk-loaded via COPY.

    metrics_per_window controls how much filler goes into column_metrics
    beyond the one row the null-rate detector reads. The real Flink job
    writes 66 metrics per window, so a probe that seeds only the single
    row it needs leaves the database orders of magnitude smaller than a
    benchmark's - and a scoring loop whose index pages all fit in shared
    buffers is not measuring the same thing as one whose don't."""
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
                end = start + datetime.timedelta(minutes=1)
                copy.write_row(
                    (start, end, NULLRATE_COLUMN, "null_count", float(SEEDED_NULL_COUNT))
                )
                for f in range(metrics_per_window - 1):
                    copy.write_row((start, end, f"filler_{f}", "null_count", 0.0))
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("ANALYZE weir_metrics.window_metrics")
        cur.execute("ANALYZE weir_metrics.column_metrics")
        cur.execute(
            "SELECT pg_size_pretty(pg_total_relation_size('weir_metrics.column_metrics')), "
            "       pg_size_pretty(pg_database_size(current_database()))"
        )
        cm_size, db_size = cur.fetchone()
    conn.commit()
    print(f"  column_metrics {cm_size}, database {db_size} "
          f"(shared_buffers is Postgres' 128MB default)")


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


def time_accounting(conn, windows):
    """Wall-clock each post-scoring accounting read the benchmark does.

    These are imported from benchmarks/run_benchmark.py rather than
    reimplemented here: the point is to time the queries that actually
    ran, not equivalent-looking ones. Run 34757520031 spent 4h36m in the
    block that contains both scoring and these reads, and the measured
    scoring rate only accounts for about a third of it, so the remainder
    is somewhere in this list.

    assert_accounting is deliberately excluded - it exits the process on
    a mismatch, and this probe's synthetic windows are not the shape its
    invariants describe."""
    from benchmarks.run_benchmark import (
        bucket_sample_counts,
        clean_event_time_hours,
        collect_incidents,
        landed_row_total,
        scored_window_counts,
        warmed_bucket_count,
    )

    # No trailing window is appended here, so this sentinel matches no
    # real window_end - every 'WHERE window_end <> trailing' read
    # therefore covers the whole table, which is the expensive case.
    trailing = SEED_ANCHOR - datetime.timedelta(minutes=1)

    print(f"\n--- post-scoring accounting reads over {windows} windows ---")
    for name, call in (
        ("collect_incidents", lambda: collect_incidents(conn)),
        ("scored_window_counts", lambda: scored_window_counts(conn)),
        ("warmed_bucket_count", lambda: warmed_bucket_count(conn)),
        ("bucket_sample_counts", lambda: bucket_sample_counts(conn)),
        ("landed_row_total", lambda: landed_row_total(conn, trailing)),
        ("clean_event_time_hours", lambda: clean_event_time_hours(conn, trailing)),
    ):
        started = time.monotonic()
        try:
            call()
        except Exception as exc:  # noqa: BLE001 - a probe reports, it does not mask
            print(f"  {name}: raised {type(exc).__name__}: {exc}")
            conn.rollback()
            continue
        print(f"  {name}: {time.monotonic() - started:.2f}s")
        conn.rollback()


def project(aggregate_rate, span_weeks, detectors=3):
    """Hours of scoring for one replay phase at a given contiguous span."""
    windows = span_weeks * 7 * 24 * 60
    return (windows * detectors) / aggregate_rate / 3600.0


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--windows", type=int, default=20000,
                        help="consecutive one-minute windows to seed and score")
    parser.add_argument("--metrics-per-window", type=int, default=REAL_METRICS_PER_WINDOW,
                        help="column_metrics rows per window, matching what the Flink "
                             "job really writes, so the database is realistically sized")
    parser.add_argument("--settings", default="on",
                        help="comma-separated synchronous_commit values to measure. "
                             "Defaults to 'on' alone: a 20000-window probe already "
                             "measured 'off' at 1.00x, so it is not worth doubling a "
                             "long run to re-measure a settled non-effect")
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
    args = parser.parse_args()

    settings = [s.strip() for s in args.settings.split(",") if s.strip()]
    unknown = [s for s in settings if s not in ("on", "off", "local", "remote_write")]
    if unknown:
        print(f"FAIL: not valid synchronous_commit values: {unknown}")
        sys.exit(1)

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    rates = {}
    try:
        print(f"seeding {args.windows} one-minute windows from {SEED_ANCHOR}, "
              f"{args.metrics_per_window} column metrics each ...")
        seed(conn, args.windows, args.metrics_per_window)

        # ALTER SYSTEM refuses to run inside a transaction block, and the
        # detector connection is deliberately autocommit=False, so the
        # cluster-wide setting is driven from a separate admin session.
        # ALTER SYSTEM rather than SET: it has to survive the transactions
        # run_once opens and closes for every window.
        admin = psycopg.connect(conninfo, autocommit=True, connect_timeout=10)
        try:
            for setting in settings:
                reset_incidents(conn)
                with admin.cursor() as cur:
                    cur.execute(f"ALTER SYSTEM SET synchronous_commit = '{setting}'")
                    cur.execute("SELECT pg_reload_conf()")
                # Read it back on the connection that will actually do
                # the scoring - a reload the detector session hasn't
                # picked up would silently measure the wrong thing.
                # pg_reload_conf() only signals the backends; each
                # applies the change when it next checks for interrupts,
                # so this polls instead of reading once. The first
                # attempt lost that race by three milliseconds.
                deadline = time.monotonic() + RELOAD_TIMEOUT_SECONDS
                while True:
                    conn.rollback()
                    with conn.cursor() as cur:
                        cur.execute("SHOW synchronous_commit")
                        (effective,) = cur.fetchone()
                    conn.rollback()
                    if effective == setting:
                        break
                    if time.monotonic() > deadline:
                        print(f"FAIL: asked for synchronous_commit={setting}, but the "
                              f"scoring session still reports {effective} after "
                              f"{RELOAD_TIMEOUT_SECONDS}s")
                        sys.exit(1)
                    time.sleep(0.1)
                timings, counts = time_detectors(conn)
                rates[setting] = report(setting, timings, counts)
                time_accounting(conn, args.windows)

            with admin.cursor() as cur:
                cur.execute("ALTER SYSTEM RESET synchronous_commit")
                cur.execute("SELECT pg_reload_conf()")
        finally:
            admin.close()
    finally:
        conn.close()

    if "off" in rates and "on" in rates:
        print(f"\nsynchronous_commit=off is {rates['off'] / rates['on']:.2f}x "
              f"the throughput of on")
    print("\nprojected scoring hours for ONE replay phase (three detectors):")
    for setting, rate in rates.items():
        for span_weeks in (8, 10, 12):
            print(f"  synchronous_commit={setting}, {span_weeks:>2}w span: "
                  f"{project(rate, span_weeks):.2f}h")
    print("\nA full benchmark is TWO phases (clean + injected), so double the "
          "above and add ~1.2h/phase of replay. The CI ceiling is 6h.")


if __name__ == "__main__":
    main()
