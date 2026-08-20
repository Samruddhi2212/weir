#!/usr/bin/env python3
"""Part 2.1: time-compressed replay of a downloaded NYC TLC trip file into Kafka.

Reads a Yellow Taxi Parquet file (see download_tlc_data.py), sorts by
pickup time, and replays it to a Kafka topic with the wall-clock gap
between sends equal to each pair's real inter-arrival time divided by
--speed-factor - preserving the shape of the real arrival pattern
(rush-hour bursts, overnight lulls), not a flat constant rate.

Design decisions (dataset scope, synthetic trip key, time-compression
formula and its cap, zone IDs left unresolved, producer library reuse)
are recorded in DEFENSE.md #26, written before this file.
"""
import argparse
import datetime
import json
import sys
import time

import pyarrow.parquet as pq
from kafka import KafkaProducer

PICKUP_COLUMN = "tpep_pickup_datetime"


def _json_default(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return str(value)


def load_sorted_trips(parquet_path):
    table = pq.read_table(parquet_path)
    if PICKUP_COLUMN not in table.column_names:
        raise SystemExit(
            f"FAIL: {parquet_path} has no '{PICKUP_COLUMN}' column - found: {table.column_names}. "
            f"TLC's schema has changed across releases before; this file's isn't the one expected."
        )
    table = table.sort_by(PICKUP_COLUMN)
    return table.to_pylist()


def replay(rows, producer, topic, speed_factor, max_sleep, limit):
    if limit:
        rows = rows[:limit]
    if not rows:
        print("no rows to replay", file=sys.stderr)
        return 0

    sent = 0
    delivery_failures = []
    prev_pickup = None

    def on_error(key, exc):
        print(f"replay_producer.py: DELIVERY FAILED for {key}: {exc}", file=sys.stderr)
        delivery_failures.append({"key": key, "error": str(exc)})

    for i, row in enumerate(rows):
        pickup = row[PICKUP_COLUMN]
        if prev_pickup is not None:
            real_delta_seconds = max((pickup - prev_pickup).total_seconds(), 0.0)
            sleep_seconds = min(real_delta_seconds / speed_factor, max_sleep)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        prev_pickup = pickup

        trip_key = f"tlc-yellow-{i:08d}"
        payload = {k: v for k, v in row.items()}
        future = producer.send(topic, key=trip_key.encode("utf-8"), value=payload)
        future.add_errback(lambda exc, k=trip_key: on_error(k, exc))
        sent += 1
        if sent % 1000 == 0:
            print(f"replay_producer.py: sent {sent}/{len(rows)}", file=sys.stderr)

    producer.flush(timeout=60)
    if delivery_failures:
        print(f"replay_producer.py: {len(delivery_failures)} delivery failures", file=sys.stderr)
        print(json.dumps(delivery_failures), file=sys.stderr)
        raise SystemExit(1)
    return sent


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="path to a downloaded yellow_tripdata_*.parquet file")
    parser.add_argument("--bootstrap-server", required=True, help="host:port, e.g. localhost:9092")
    parser.add_argument("--topic", default="weir-tlc-trips")
    parser.add_argument("--speed-factor", type=float, default=3600.0,
                         help="real seconds compressed into 1 replayed second (default: 3600 = 1 real hour -> 1s)")
    parser.add_argument("--max-inter-arrival-sleep", type=float, default=5.0,
                         help="cap on wall-clock sleep between sends, seconds (default: 5)")
    parser.add_argument("--limit", type=int, default=0, help="replay only the first N trips (0 = all, default)")
    args = parser.parse_args()

    print(f"replay_producer.py: loading {args.input}", file=sys.stderr)
    rows = load_sorted_trips(args.input)
    print(f"replay_producer.py: {len(rows)} trips loaded, sorted by {PICKUP_COLUMN}", file=sys.stderr)

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=lambda v: json.dumps(v, default=_json_default).encode("utf-8"),
        acks="all",
    )
    try:
        sent = replay(rows, producer, args.topic, args.speed_factor, args.max_inter_arrival_sleep, args.limit)
    finally:
        producer.close(timeout=30)

    print(f"replay_producer.py: done, sent {sent} trips to {args.topic}", file=sys.stderr)


if __name__ == "__main__":
    main()
