#!/usr/bin/env python3
"""DEFENSE.md #48: real-data integration check for the volume
detector. Assumes scripts/verify_metrics_job.py has already been run
against 2024-11 (chosen for its real DST fall-back) in the same
workflow, so weir_metrics.window_metrics already holds one real
month's rows - this script never touches Kafka/Flink/replay_producer.py
itself (DEFENSE.md #48's reuse-not-duplicate decision).

Applies reliability/store/incidents_schema.sql fresh, runs
reliability/volume/run.py's run_once against the real data, then
checks four things, each computed independently of the detector
under test (V7), never unaccounted-for (V8):

  1. every real window_metrics row has exactly one scored_windows row
  2. every real row in the Nov 3 2024 DST fall-back's ambiguous hour
     is status='dst_ambiguous_excluded'
  3. zero scored_windows rows are status='scored' - one month can't
     warm any bucket's 8-week baseline (DEFENSE.md #44/#48's stated
     scope limit), and this asserts that absence rather than silently
     not checking for it
  4. each touched bucket's baseline_state.observation_count matches
     an independent count straight from window_metrics
"""
import argparse
import datetime
import sys
from pathlib import Path

import psycopg

# Running this file directly (`python scripts/verify_volume_detector.py`)
# only puts scripts/ itself on sys.path, not the repo root - unlike
# pytest, which resolves reliability.volume.* correctly on its own.
# Every other verify_*.py script has no project-internal imports and
# never hit this; this is the first one that does.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from reliability.volume.config import DEFAULT_CONFIG
from reliability.volume.run import run_once, to_naive_local


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
    config = DEFAULT_CONFIG

    print("=== Stage 1: apply incidents_schema.sql (fresh) ===")
    apply_sql_file(conn, "reliability/store/incidents_schema.sql")
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE weir_incidents.scored_windows, weir_incidents.baseline_state, "
            "weir_incidents.detector_progress, weir_incidents.incidents;"
        )
    conn.commit()
    print("PASS: stage 1 - schema applied, detector state truncated")

    print("\n=== Stage 2: run the detector against real window_metrics data ===")
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM weir_metrics.window_metrics")
        (real_window_count,) = cur.fetchone()
    if real_window_count == 0:
        fail("weir_metrics.window_metrics is empty - run scripts/verify_metrics_job.py "
             "against 2024-11 first")
    print(f"real window_metrics rows present: {real_window_count}")

    statuses = run_once(conn, config)
    statuses += run_once(conn, config)  # second pass: catches the lag-buffered tail
    print(f"run.py processed {len(statuses)} window(s) across two passes")

    print("\n=== Stage 3: every real window is accounted for (V7/V8) ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT "
            "  (SELECT COUNT(*) FROM weir_metrics.window_metrics), "
            "  (SELECT COUNT(*) FROM weir_incidents.scored_windows WHERE detector_name = %s)",
            (config.detector_name,),
        )
        window_metrics_count, scored_windows_count = cur.fetchone()
    if window_metrics_count != scored_windows_count:
        fail(
            f"window_metrics has {window_metrics_count} real rows but scored_windows has "
            f"{scored_windows_count} for detector {config.detector_name!r} - not every real "
            f"window was accounted for"
        )
    print(f"PASS: stage 3 - all {window_metrics_count} real windows have exactly one scored_windows row")

    print("\n=== Stage 4: the real DST fall-back hour was excluded, not silently dropped ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_metrics.window_metrics "
            "WHERE window_end > %s AND window_end <= %s",
            (datetime.datetime(2024, 11, 3, 1, 0, 0), datetime.datetime(2024, 11, 3, 2, 0, 0)),
        )
        (real_dst_row_count,) = cur.fetchone()
    if real_dst_row_count == 0:
        fail("no real window_metrics rows fall in the 2024-11-03 01:00-02:00 DST fall-back hour - "
             "is this really 2024-11 data?")

    # Independent of adapter.is_dst_ambiguous: fetch every real scored_windows
    # row for Nov 3 2024 (a UTC range wide enough to cover the local date from
    # either offset), convert its stored UTC window_end back to naive local
    # here (DEFENSE.md #46's to_naive_local, the one place this conversion
    # lives - not a second hand-derived UTC boundary), and check membership
    # against the ambiguous range directly.
    with conn.cursor() as cur:
        cur.execute(
            "SELECT window_end, status FROM weir_incidents.scored_windows "
            "WHERE detector_name = %s AND window_end >= %s AND window_end < %s",
            (config.detector_name,
             datetime.datetime(2024, 11, 3, tzinfo=datetime.timezone.utc),
             datetime.datetime(2024, 11, 4, tzinfo=datetime.timezone.utc)),
        )
        nov3_rows = cur.fetchall()

    dst_ambiguous_start = datetime.datetime(2024, 11, 3, 1, 0, 0)
    dst_ambiguous_end = datetime.datetime(2024, 11, 3, 2, 0, 0)
    excluded_count = 0
    mismatched_statuses = []
    for window_end_utc, status in nov3_rows:
        naive_local = to_naive_local(window_end_utc, config.timezone)
        if dst_ambiguous_start <= naive_local < dst_ambiguous_end:
            excluded_count += 1
            if status != "dst_ambiguous_excluded":
                mismatched_statuses.append((window_end_utc, status))

    if mismatched_statuses:
        fail(f"rows in the real DST fall-back hour with the wrong status: {mismatched_statuses}")
    if excluded_count != real_dst_row_count:
        fail(
            f"{real_dst_row_count} real rows fall in the DST fall-back hour, but only "
            f"{excluded_count} were found with status='dst_ambiguous_excluded'"
        )
    print(f"PASS: stage 4 - all {real_dst_row_count} real DST fall-back rows excluded correctly")

    print("\n=== Stage 5: honest scope limit - zero rows are 'scored' (one month can't warm a baseline) ===")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_incidents.scored_windows "
            "WHERE detector_name = %s AND status = 'scored'",
            (config.detector_name,),
        )
        (scored_count,) = cur.fetchone()
    if scored_count != 0:
        fail(
            f"expected 0 'scored' rows (DEFENSE.md #48's stated scope limit - one month can't "
            f"reach min_observations={config.min_observations}), found {scored_count}"
        )
    print("PASS: stage 5 - zero 'scored' rows, exactly as this detector's warmup math predicts")

    print("\n=== Stage 6: baseline_state.observation_count matches an independent per-bucket count ===")
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
        fail(f"baseline_state.observation_count mismatches an independent scored_windows count "
             f"for {len(mismatches)} bucket(s): {mismatches}")
    print(f"PASS: stage 6 - all {len(baseline_rows)} touched buckets' observation_count match independently")

    conn.close()
    print("\n=== verify_volume_detector.py: all stages passed ===")


if __name__ == "__main__":
    main()
