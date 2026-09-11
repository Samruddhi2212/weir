"""Dev trigger: writes one synthetic window_metrics row plus a
matching column_metrics null_count row, so the null-rate detector's
run.py has a controlled null rate to react to, without touching
Kafka/Flink/the replay producer. Same direct-write pattern as
incidents/dev/volume_drop.py and freshness_gap.py.
"""
import argparse
import datetime
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from reliability.nullrate.config import config_for_column
from reliability.volume.adapter import to_local_bucket
from reliability.volume.run import to_naive_local, to_utc_instant


def print_current_baseline(conn, window_end, column_name):
    config = config_for_column(column_name)
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
        f"ewma_mean={ewma_mean} (rate), ewma_mad={ewma_mad}"
    )


def insert_spike(conn, window_end, column_name, row_count, null_count):
    window_start = window_end - datetime.timedelta(minutes=1)
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO weir_metrics.window_metrics (window_start, window_end, row_count) "
            "VALUES (%s, %s, %s) "
            "ON CONFLICT (window_start, window_end) DO UPDATE SET row_count = EXCLUDED.row_count",
            (window_start, window_end, row_count),
        )
        cur.execute(
            "INSERT INTO weir_metrics.column_metrics (window_start, window_end, column_name, metric_name, metric_value) "
            "VALUES (%s, %s, %s, 'null_count', %s) "
            "ON CONFLICT (window_start, window_end, column_name, metric_name) DO UPDATE SET metric_value = EXCLUDED.metric_value",
            (window_start, window_end, column_name, null_count),
        )
    conn.commit()
    print(f"wrote window=[{window_start}, {window_end}) row_count={row_count} "
          f"{column_name}.null_count={null_count} (rate={null_count / row_count:.3f})")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    parser.add_argument("--column", required=True, help="the column_metrics column_name to target")
    parser.add_argument("--row-count", type=float, default=100.0)
    parser.add_argument("--null-count", type=float, required=True,
                         help="Check the printed baseline before choosing one - null_count/row_count is the rate scored.")
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
        if max_window_end is not None:
            window_end = max_window_end + datetime.timedelta(minutes=1)
        else:
            now_utc = datetime.datetime.now(datetime.timezone.utc).replace(second=0, microsecond=0)
            window_end = to_naive_local(now_utc, config_for_column(args.column).timezone)
        print_current_baseline(conn, window_end, args.column)
        insert_spike(conn, window_end, args.column, args.row_count, args.null_count)
        print("Run reliability/nullrate/run.py --column <col> next to have the detector actually score this window.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
