#!/usr/bin/env python3
"""Real-data integration check for the freshness detector. Assumes
scripts/verify_metrics_job.py has already been run (2025-01 is fine -
this detector needs window_metrics.window_end/row_count only, not the
column_metrics stats 2024-11 was chosen for). Never touches
Kafka/Flink/replay_producer.py itself.

Checks, each computed independently of the detector under test (V7),
never unaccounted-for (V8):

  1. every real window_metrics row has a scored_windows row, except
     the one boundary row that's deliberately never scored (the very
     first real window ever, which has no prior window to compute a
     gap from - reliability/freshness/run.py's own documented case)
  2. every scored gap is >= 60 seconds - windows are 1 minute wide and
     non-overlapping, so no two distinct window_end values can be
     closer than that; a smaller value would mean a real bug in the
     gap computation, not real data
  3. each touched bucket's baseline_state.observation_count matches an
     independent count straight from scored_windows
"""
import argparse
import os
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reliability.freshness.config import DEFAULT_CONFIG
from reliability.freshness.run import run_once


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
    config = DEFAULT_CONFIG

    print("=== Stage 1: apply incidents_schema.sql (fresh) ===")
    apply_sql_file(conn, "reliability/store/incidents_schema.sql")
    with conn.cursor() as cur:
        for table in ("scored_windows", "baseline_state", "detector_progress", "incidents"):
            cur.execute(f"DELETE FROM weir_incidents.{table} WHERE detector_name = %s", (config.detector_name,))
    conn.commit()
    print("PASS: stage 1 - schema applied, freshness detector state cleared")

    print("\n=== Stage 2: run the detector against real window_metrics data ===")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM weir_metrics.window_metrics")
        (real_window_count,) = cur.fetchone()
    if real_window_count == 0:
        fail("weir_metrics.window_metrics is empty - run scripts/verify_metrics_job.py first")
    print(f"real window_metrics rows present: {real_window_count}")

    statuses = run_once(conn, config, assume_no_more_arrivals=True)
    print(f"run.py processed {len(statuses)} window(s)")

    print("\n=== Stage 3: every real window is accounted for, except the one un-scoreable boundary row (V7/V8) ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_incidents.scored_windows WHERE detector_name = %s",
            (config.detector_name,),
        )
        (scored_windows_count,) = cur.fetchone()
    expected_count = real_window_count - 1
    if scored_windows_count != expected_count:
        fail(
            f"window_metrics has {real_window_count} real rows (expected {expected_count} scored, "
            f"the first has no prior window) but scored_windows has {scored_windows_count}"
        )
    print(f"PASS: stage 3 - {scored_windows_count} of {real_window_count} real windows scored "
          f"(the first row correctly has no gap to score)")

    print("\n=== Stage 4: every scored gap is >= 60 seconds (windows are 1 minute wide, non-overlapping) ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT window_end, observed_value FROM weir_incidents.scored_windows "
            "WHERE detector_name = %s AND observed_value < 60.0",
            (config.detector_name,),
        )
        bad_rows = cur.fetchall()
    if bad_rows:
        fail(f"{len(bad_rows)} scored window(s) have a gap under 60 seconds - a real bug, not real data: {bad_rows[:5]}")
    print("PASS: stage 4 - no impossible (< 60s) gaps")

    print("\n=== Stage 5: baseline_state.observation_count matches an independent per-bucket count ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT bucket_weekday, bucket_hour, observation_count "
            "FROM weir_incidents.baseline_state WHERE detector_name = %s",
            (config.detector_name,),
        )
        baseline_rows = cur.fetchall()

    mismatches = []
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
            mismatches.append((bucket_weekday, bucket_hour, observation_count, independent_count))

    if mismatches:
        fail(f"baseline_state.observation_count mismatches an independent count for "
             f"{len(mismatches)} bucket(s): {mismatches}")
    print(f"PASS: stage 5 - all {len(baseline_rows)} touched buckets' observation_count match independently")

    conn.close()
    print("\n=== verify_freshness_detector.py: all stages passed ===")


if __name__ == "__main__":
    main()
