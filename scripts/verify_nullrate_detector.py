#!/usr/bin/env python3
"""Real-data integration check for the null-rate detector, monitoring
passenger_count. Assumes scripts/verify_metrics_job.py has already
been run (2025-01 is fine). Never touches Kafka/Flink/replay_producer.py.

Checks, each computed independently of the detector under test (V7),
never unaccounted-for (V8):

  1. every real window_metrics row that has a matching column_metrics
     null_count row for this column has exactly one scored_windows row
  2. every scored null_rate is in [0, 1] - a real rate, computed
     independently as null_count/row_count straight from the source
     tables, not trusted from the detector's own output
  3. each touched bucket's baseline_state.observation_count matches an
     independent count straight from scored_windows
"""
import argparse
import os
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reliability.nullrate.config import config_for_column
from reliability.nullrate.run import run_once
from reliability.volume.run import to_utc_instant

COLUMN = "passenger_count"


def fail(msg):
    print(f"\nFAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def apply_sql_file(conn, path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def main():
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
    args = parser.parse_args()

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    config = config_for_column(COLUMN)

    print("=== Stage 1: apply incidents_schema.sql (fresh) ===")
    apply_sql_file(conn, "reliability/store/incidents_schema.sql")
    with conn.cursor() as cur:
        for table in ("scored_windows", "baseline_state", "detector_progress", "incidents"):
            cur.execute(f"DELETE FROM weir_incidents.{table} WHERE detector_name = %s", (config.detector_name,))
    conn.commit()
    print(f"PASS: stage 1 - schema applied, {config.detector_name!r} state cleared")

    print(f"\n=== Stage 2: independent ground truth for {COLUMN!r}'s real null rate ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_metrics.window_metrics wm "
            "JOIN weir_metrics.column_metrics cm "
            "  ON cm.window_start = wm.window_start AND cm.window_end = wm.window_end "
            "WHERE cm.column_name = %s AND cm.metric_name = 'null_count'",
            (COLUMN,),
        )
        (real_window_count,) = cur.fetchone()
    if real_window_count == 0:
        fail(f"no real window_metrics/column_metrics rows for {COLUMN!r} - "
             f"run scripts/verify_metrics_job.py first")
    print(f"real windows with a {COLUMN!r} null_count: {real_window_count}")

    statuses = run_once(conn, COLUMN, assume_no_more_arrivals=True)
    print(f"run.py processed {len(statuses)} window(s)")

    print("\n=== Stage 3: every real window is accounted for (V7/V8) ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_incidents.scored_windows WHERE detector_name = %s",
            (config.detector_name,),
        )
        (scored_windows_count,) = cur.fetchone()
    if scored_windows_count != real_window_count:
        fail(
            f"{real_window_count} real windows have a null_count for {COLUMN!r}, "
            f"but scored_windows has {scored_windows_count}"
        )
    print(f"PASS: stage 3 - all {real_window_count} real windows have exactly one scored_windows row")

    print("\n=== Stage 4: every scored null_rate matches an independently computed rate, and is in [0, 1] ===")
    # scored_windows.window_end is TIMESTAMPTZ (true UTC, converted by
    # run.py); window_metrics/column_metrics' is naive local TIMESTAMP.
    # Joining the two directly in SQL silently compares incompatible
    # representations and matches nothing - converted in Python instead
    # (DEFENSE.md #46's own reasoning, missed on the first draft of
    # this check: a raw SQL join here isn't the "second AT TIME ZONE
    # cast" that entry rejected, but it's the same class of mistake).
    with conn.cursor() as cur:
        cur.execute(
            "SELECT window_end, observed_value FROM weir_incidents.scored_windows "
            "WHERE detector_name = %s",
            (config.detector_name,),
        )
        scored_rows = cur.fetchall()

    with conn.cursor() as cur:
        cur.execute(
            "SELECT wm.window_end, cm.metric_value, wm.row_count "
            "FROM weir_metrics.window_metrics wm "
            "JOIN weir_metrics.column_metrics cm "
            "  ON cm.window_start = wm.window_start AND cm.window_end = wm.window_end "
            "WHERE cm.column_name = %s AND cm.metric_name = 'null_count'",
            (COLUMN,),
        )
        source_by_utc_end = {
            to_utc_instant(window_end_naive, config.timezone): (null_count, row_count)
            for window_end_naive, null_count, row_count in cur.fetchall()
        }

    mismatches = []
    for window_end_utc, scored_value in scored_rows:
        source = source_by_utc_end.get(window_end_utc)
        if source is None:
            mismatches.append((window_end_utc, scored_value, "no matching source row"))
            continue
        null_count, row_count = source
        expected_rate = null_count / row_count
        if not (0.0 <= scored_value <= 1.0) or abs(scored_value - expected_rate) > 1e-9:
            mismatches.append((window_end_utc, scored_value, expected_rate))
    if mismatches:
        fail(f"{len(mismatches)} scored null_rate value(s) don't match independent computation "
             f"or fall outside [0, 1]: {mismatches[:5]}")
    print(f"PASS: stage 4 - all {len(scored_rows)} scored null_rate values match independently, all in [0, 1]")

    print("\n=== Stage 5: baseline_state.observation_count matches an independent per-bucket count ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT bucket_weekday, bucket_hour, observation_count "
            "FROM weir_incidents.baseline_state WHERE detector_name = %s",
            (config.detector_name,),
        )
        baseline_rows = cur.fetchall()

    obs_mismatches = []
    for bucket_weekday, bucket_hour, observation_count in baseline_rows:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM weir_incidents.scored_windows "
                "WHERE detector_name = %s AND bucket_weekday = %s AND bucket_hour = %s "
                "AND status != 'skipped_late' AND status != 'dst_ambiguous_excluded'",
                (config.detector_name, bucket_weekday, bucket_hour),
            )
            (independent_count,) = cur.fetchone()
        if independent_count != observation_count:
            obs_mismatches.append((bucket_weekday, bucket_hour, observation_count, independent_count))

    if obs_mismatches:
        fail(f"baseline_state.observation_count mismatches an independent count for "
             f"{len(obs_mismatches)} bucket(s): {obs_mismatches}")
    print(f"PASS: stage 5 - all {len(baseline_rows)} touched buckets' observation_count match independently")

    conn.close()
    print("\n=== verify_nullrate_detector.py: all stages passed ===")


if __name__ == "__main__":
    main()
