#!/usr/bin/env python3
"""Part 2.2: measure the real lateness distribution from a sample replay.

Runs ingestion/replay/replay_producer.py against a fresh topic with
--shuffle-window enabled (bounded out-of-order injection - DEFENSE.md
#40; the naive, perfectly-sorted replay produces ~0 lateness by
construction and would tell us nothing), consumes the result in actual
arrival order, and reports:

  - the full lateness distribution (histogram, p50/p95/p99/p999, max)
  - a recommended watermark bound (p99 by default) and the EXACT
    measured percentage of events that would be dropped at that bound

Lateness definition (running-max-based, not wall-clock/broker-
timestamp-based) and why p99 is the recommended default rather than
p95 or p999 are in DEFENSE.md #40, written before this file. No
fabricated numbers (CLAUDE.md hard rule 1) - every number below comes
from this run, not a guess.
"""
import argparse
import datetime
import json
import os
import subprocess
import sys

from kafka import KafkaConsumer

REPLAY_PRODUCER = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "ingestion", "replay", "replay_producer.py"
)


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def recreate_topic(kafka_exec, topic, partitions=1):
    kafka_exec_list = kafka_exec.split()
    subprocess.run(
        kafka_exec_list + ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                            "--delete", "--topic", topic],
        capture_output=True,
    )
    result = subprocess.run(
        kafka_exec_list + ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                            "--create", "--topic", topic, "--partitions", str(partitions),
                            "--replication-factor", "1"],
        capture_output=True, text=True,
    )
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        fail(f"could not create topic {topic}")


def run_replay(args):
    cmd = [
        sys.executable, REPLAY_PRODUCER,
        "--input", args.input, "--bootstrap-server", args.bootstrap_server, "--topic", args.topic,
        "--limit", str(args.limit),
        # Fast, not testing compression ratio here - see DEFENSE.md #39
        # for why isolating unrelated properties from the thing under
        # test is the right call, applied here to a different pair.
        "--speed-factor", "1000000", "--max-inter-arrival-sleep", "0.01",
        "--shuffle-window", str(args.shuffle_window), "--shuffle-seed", str(args.shuffle_seed),
    ]
    print(f"--- {' '.join(cmd)} ---")
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        fail(f"replay_producer.py exited {result.returncode} - see output above")


def consume_in_arrival_order(bootstrap_server, topic, expected_count, timeout_ms=60000):
    """Single-partition topic: broker offset order IS actual arrival
    order, unambiguously - the ground truth this measurement needs,
    not the producer's own emission-log append order (DEFENSE.md #40)."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset="earliest",
        group_id=f"weir-lateness-{os.getpid()}-{id(topic)}",
        consumer_timeout_ms=timeout_ms,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    event_timestamps = []
    try:
        for record in consumer:
            ts_str = record.value.get("tpep_pickup_datetime")
            if ts_str is None:
                fail(f"message at offset {record.offset} has no tpep_pickup_datetime")
            event_timestamps.append(datetime.datetime.fromisoformat(ts_str))
            if len(event_timestamps) >= expected_count:
                break
    finally:
        consumer.close()
    return event_timestamps


def compute_lateness(event_timestamps_in_arrival_order):
    """lateness[i] = seconds by which event i's own timestamp trails the
    largest timestamp seen so far (0 for an event that IS the new max -
    an "on-time or leading" event). Standard operational definition for
    tuning a watermark bound - DEFENSE.md #40."""
    running_max = None
    lateness = []
    for ts in event_timestamps_in_arrival_order:
        if running_max is None or ts > running_max:
            running_max = ts
            lateness.append(0.0)
        else:
            lateness.append((running_max - ts).total_seconds())
    return lateness


def percentile(sorted_values, p):
    """Nearest-rank method: sorted ascending, 0-indexed rank
    ceil(p/100 * N) - 1, clamped to [0, N-1]. Simple and unambiguous -
    deliberately not statistics.quantiles(), whose interpolation method
    choice would need its own justification for no real benefit here."""
    n = len(sorted_values)
    if n == 0:
        return None
    import math
    rank = max(0, min(n - 1, math.ceil(p / 100.0 * n) - 1))
    return sorted_values[rank]


def build_histogram(values, num_buckets=10):
    if not values:
        return []
    lo, hi = min(values), max(values)
    if lo == hi:
        return [{"range": [lo, hi], "count": len(values)}]
    width = (hi - lo) / num_buckets
    buckets = [0] * num_buckets
    for v in values:
        b = min(num_buckets - 1, int((v - lo) / width))
        buckets[b] += 1
    return [
        {"range": [round(lo + i * width, 3), round(lo + (i + 1) * width, 3)], "count": buckets[i]}
        for i in range(num_buckets)
    ]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="path to a downloaded yellow_tripdata_*.parquet file")
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--topic", default="weir-tlc-lateness-sample")
    parser.add_argument(
        "--kafka-exec", default="docker compose exec -T kafka",
        help="command prefix to run kafka-topics.sh inside the kafka container",
    )
    parser.add_argument("--limit", type=int, default=2000, help="sample size (default: 2000 trips)")
    parser.add_argument("--shuffle-window", type=int, default=50,
                         help="bounded out-of-order window size (default: 50) - see DEFENSE.md #40")
    parser.add_argument("--shuffle-seed", type=int, default=42)
    parser.add_argument("--watermark-percentile", type=float, default=99.0,
                         help="which percentile to recommend as the watermark bound (default: p99)")
    parser.add_argument("--report-json", default=None)
    args = parser.parse_args()

    recreate_topic(args.kafka_exec, args.topic)
    run_replay(args)

    event_timestamps = consume_in_arrival_order(args.bootstrap_server, args.topic, args.limit)
    print(f"consumed {len(event_timestamps)} messages (expected {args.limit})")
    if len(event_timestamps) != args.limit:
        fail(f"expected {args.limit} landed messages, got {len(event_timestamps)}")

    lateness = compute_lateness(event_timestamps)
    sorted_lateness = sorted(lateness)

    p50 = percentile(sorted_lateness, 50)
    p95 = percentile(sorted_lateness, 95)
    p99 = percentile(sorted_lateness, 99)
    p999 = percentile(sorted_lateness, 99.9)
    max_lateness = sorted_lateness[-1]

    recommended_bound = percentile(sorted_lateness, args.watermark_percentile)
    dropped = sum(1 for v in lateness if v > recommended_bound)
    dropped_pct = 100.0 * dropped / len(lateness)

    report = {
        "sample_size": len(lateness),
        "shuffle_window": args.shuffle_window,
        "shuffle_seed": args.shuffle_seed,
        "p50_seconds": p50,
        "p95_seconds": p95,
        "p99_seconds": p99,
        "p999_seconds": p999,
        "max_seconds": max_lateness,
        "histogram": build_histogram(lateness),
        "recommended_watermark_bound_percentile": args.watermark_percentile,
        "recommended_watermark_bound_seconds": recommended_bound,
        "events_dropped_at_recommended_bound": dropped,
        "events_dropped_at_recommended_bound_pct": round(dropped_pct, 4),
    }

    print("")
    print("=== measure_lateness.py report ===")
    print(json.dumps(report, indent=2))
    if args.report_json:
        with open(args.report_json, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

    print("")
    print(
        f"RECOMMENDATION: watermark bound = {recommended_bound:.3f}s (p{args.watermark_percentile}) - "
        f"{dropped}/{len(lateness)} events ({dropped_pct:.3f}%) would have been dropped at this bound "
        f"in this sample."
    )


if __name__ == "__main__":
    main()
