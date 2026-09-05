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
import sys
from pathlib import Path

import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reliability.nullrate.config import config_for_column
from reliability.nullrate.run import run_once

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
    parser.add_argument("--password", default="weir")
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
    with conn.cursor() as cur:
        cur.execute(
            "SELECT sw.window_start, sw.window_end, sw.observed_value, cm.metric_value, wm.row_count "
            "FROM weir_incidents.scored_windows sw "
            "JOIN weir_metrics.window_metrics wm "
            "  ON wm.window_start = sw.window_start AND wm.window_end = sw.window_end "
            "JOIN weir_metrics.column_metrics cm "
            "  ON cm.window_start = sw.window_start AND cm.window_end = sw.window_end "
            "  AND cm.column_name = %s AND cm.metric_name = 'null_count' "
            "WHERE sw.detector_name = %s",
            (COLUMN, config.detector_name),
        )
        joined_rows = cur.fetchall()

    mismatches = []
    for window_start, window_end, scored_value, null_count, row_count in joined_rows:
        expected_rate = null_count / row_count
        if not (0.0 <= scored_value <= 1.0) or abs(scored_value - expected_rate) > 1e-9:
            mismatches.append((window_start, window_end, scored_value, expected_rate))
    if mismatches:
        fail(f"{len(mismatches)} scored null_rate value(s) don't match independent computation "
             f"or fall outside [0, 1]: {mismatches[:5]}")
    print(f"PASS: stage 4 - all {len(joined_rows)} scored null_rate values match independently, all in [0, 1]")

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
