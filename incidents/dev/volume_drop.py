"""Dev trigger: writes one synthetic low-row-count window straight
into weir_metrics.window_metrics so the volume detector's run.py has
something anomalous to react to, without touching Kafka/Flink/the
replay producer. Never imported by benchmarks/ (hard rule 2) - see
DEFENSE.md #47 for why this is a direct write, not a pipeline-level
fault injector, and why that's the right boundary for a fast dev
loop rather than a gap in benchmark coverage.

Reuses reliability/volume/run.py's normal read path unchanged: the
injected row looks like any other window_metrics row to the detector.
"""
import argparse
import datetime
import sys
from pathlib import Path

# Running this file directly (`python incidents/dev/volume_drop.py`) only
# puts incidents/dev/ itself on sys.path, not the repo root - confirmed by
# scripts/verify_volume_detector.py hitting exactly this on its first real
# dispatch (DEFENSE.md #48's addendum); fixed here before this script's own
# first real run hits the same ModuleNotFoundError.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from reliability.volume.adapter import to_local_bucket
from reliability.volume.config import DEFAULT_CONFIG
from reliability.volume.run import to_naive_local, to_utc_instant


def default_window_end(conn):
    """The next minute after MAX(window_end) currently in
    window_metrics, or the current UTC time (floored to the minute,
    converted to naive local) if the table is empty. Not a fixed
    timestamp - run.py's frontier only advances, so a fixed default
    would work once per fresh database and silently no-op after
    (DEFENSE.md #47)."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(window_end) FROM weir_metrics.window_metrics")
        (max_window_end,) = cur.fetchone()

    if max_window_end is not None:
        return max_window_end + datetime.timedelta(minutes=1)

    now_utc = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)
    return to_naive_local(now_utc, DEFAULT_CONFIG.timezone)


def print_current_baseline(conn, window_end, config):
    """V3: print what's actually there before writing, so a chosen
    --row-count's likely outcome isn't guesswork (DEFENSE.md #47)."""
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
        f"ewma_mean={ewma_mean}, ewma_mad={ewma_mad}"
    )
    if warm and ewma_mean is not None:
        from reliability.volume.detector import scale_floor

        scale = scale_floor(ewma_mean, ewma_mad, config)
        print(f"  scale_floor={scale} -> a row_count needs |row_count - {ewma_mean}| > {config.z_threshold * scale:.2f} to flag")


def insert_drop(conn, window_end, row_count):
    window_start = window_end - datetime.timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO weir_metrics.window_metrics (window_start, window_end, row_count) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (window_start, window_end) DO UPDATE SET row_count = EXCLUDED.row_count",
            (window_start, window_end, row_count),
        )
    conn.commit()
    print(f"wrote window_metrics row: window=[{window_start}, {window_end}) row_count={row_count}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    parser.add_argument(
        "--window-end", default=None,
        help="Naive America/New_York local time, e.g. '2025-01-15 08:31:00'. "
             "Defaults to the next minute after the latest real window_metrics row.",
    )
    parser.add_argument(
        "--row-count", type=float, required=True,
        help="The synthetic row_count to write. Check the printed baseline before choosing one.",
    )
    args = parser.parse_args()

    import psycopg

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    try:
        window_end = (
            datetime.datetime.strptime(args.window_end, "%Y-%m-%d %H:%M:%S")
            if args.window_end
            else default_window_end(conn)
        )
        print_current_baseline(conn, window_end, DEFAULT_CONFIG)
        insert_drop(conn, window_end, args.row_count)
        print("Run reliability/volume/run.py next to have the detector actually score this window.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
