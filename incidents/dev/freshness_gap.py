"""Dev trigger: writes one synthetic window_metrics row --gap-minutes
after the current MAX(window_end), so the freshness detector's
run.py has a real arrival gap to react to, without touching
Kafka/Flink/the replay producer. Same direct-write pattern as
incidents/dev/volume_drop.py - never imported by benchmarks/ (hard
rule 2).
"""
import argparse
import os
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from reliability.freshness.config import DEFAULT_CONFIG
from reliability.volume.adapter import to_local_bucket
from reliability.volume.run import to_utc_instant


def print_current_baseline(conn, window_end, config):
    window_end_utc = to_utc_instant(window_end, config.timezone)
    bucket_weekday, bucket_hour = to_local_bucket(window_end_utc, config.timezone)

    with conn.cursor() as cur:
        cur.execute(
            "SELECT observation_count, ewma_mean, ewma_mad FROM weir_incidents.baseline_state "
            "WHERE detector_name = %s AND bucket_weekday = %s AND bucket_hour = %s",
            (config.detector_name, bucket_weekday, bucket_hour),
        )
        row = cur.fetchone()

    print(f"target window_end (local): {window_end} -> bucket (weekday={bucket_weekday}, hour={bucket_hour})")
    if row is None:
        print("  baseline_state: no rows yet for this bucket (first-ever observation, insufficient_baseline)")
        return
    observation_count, ewma_mean, ewma_mad = row
    warm = observation_count >= config.min_observations
    print(
        f"  baseline_state: observation_count={observation_count} "
        f"(min_observations={config.min_observations}, warm={warm}), "
        f"ewma_mean={ewma_mean}s, ewma_mad={ewma_mad}"
    )


def insert_gap(conn, window_end, row_count):
    window_start = window_end - datetime.timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO weir_metrics.window_metrics (window_start, window_end, row_count) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (window_start, window_end) DO UPDATE SET row_count = EXCLUDED.row_count",
            (window_start, window_end, row_count),
        )
    conn.commit()
    print(f"wrote window_metrics row: window=[{window_start}, {window_end})")


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
    parser.add_argument(
        "--gap-minutes", type=float, required=True,
        help="Minutes after the current MAX(window_end) to place this synthetic window - "
             "the gap this creates is what the freshness detector will score.",
    )
    parser.add_argument("--row-count", type=float, default=1.0)
    args = parser.parse_args()

    import psycopg

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(window_end) FROM weir_metrics.window_metrics")
            (max_window_end,) = cur.fetchone()
        if max_window_end is None:
            raise SystemExit("weir_metrics.window_metrics is empty - nothing to create a gap after")

        window_end = max_window_end + datetime.timedelta(minutes=args.gap_minutes)
        print_current_baseline(conn, window_end, DEFAULT_CONFIG)
        insert_gap(conn, window_end, args.row_count)
        print(f"gap from previous window: {args.gap_minutes} minutes")
        print("Run reliability/freshness/run.py next to have the detector actually score this window.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
