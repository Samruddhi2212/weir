#!/usr/bin/env python3
"""Part 2.1 completion: verify replay_producer.py against live Kafka.

Three independent checks, each against its own fresh topic:

  timestamps   Every landed message's tpep_pickup_datetime exactly
               matches the source row at the same ordinal position -
               parsed datetime comparison (V1), not log text.
  ratio        The wall-clock-to-event-time ratio, measured from the
               broker's own message timestamps (not the producer's -
               measuring the thing under test with itself would be
               circular), matches --speed-factor within tolerance.
               Runs with the inter-arrival cap effectively disabled, so
               the simple ratio check is exactly correct for what it's
               checking (DEFENSE.md #39).
  resume-seam  Two separate replay_producer.py invocations - run 1,
               then a --resume-after-timestamp continuation - produce a
               combined emission log with zero duplicate trip_keys and
               zero gaps across the expected row-index range. Relies on
               load_sorted_trips' verified sort determinism (DEFENSE.md
               #39), not a new tie-breaking key.

Design decisions for all three are in DEFENSE.md #39, written before
this file.
"""
import argparse
import datetime
import json
import os
import subprocess
import sys
import tempfile
import time
import uuid

from kafka import KafkaConsumer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from replay_producer import PICKUP_COLUMN, load_sorted_trips  # noqa: E402

REPLAY_PRODUCER = os.path.join(os.path.dirname(os.path.abspath(__file__)), "replay_producer.py")


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def recreate_topic(kafka_bin_exec, topic, partitions=1):
    """kafka_bin_exec is a list like ['docker', 'compose', 'exec', '-T', 'kafka']."""
    subprocess.run(
        kafka_bin_exec + ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                           "--delete", "--topic", topic],
        capture_output=True,
    )
    result = subprocess.run(
        kafka_bin_exec + ["/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092",
                           "--create", "--topic", topic, "--partitions", str(partitions),
                           "--replication-factor", "1"],
        capture_output=True, text=True,
    )
    print("--- kafka-topics.sh --create ---")
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        fail(f"could not create topic {topic}")


def run_replay_producer(args_list):
    print(f"--- replay_producer.py {' '.join(args_list)} ---")
    result = subprocess.run(
        [sys.executable, REPLAY_PRODUCER] + args_list,
        capture_output=True, text=True, timeout=600,
    )
    print(result.stdout)
    print(result.stderr)
    if result.returncode != 0:
        fail(f"replay_producer.py exited {result.returncode} - see output above")
    return result


def consume_all(bootstrap_server, topic, expected_count, timeout_ms=60000):
    """Returns list of (key_str, value_dict, broker_timestamp_ms)."""
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_server],
        auto_offset_reset="earliest",
        group_id=f"weir-verify-{uuid.uuid4()}",
        consumer_timeout_ms=timeout_ms,
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    messages = []
    try:
        for record in consumer:
            messages.append((record.key, record.value, record.timestamp))
            if len(messages) >= expected_count:
                break
    finally:
        consumer.close()
    return messages


def row_index_from_key(key_str):
    # trip_key format is "tlc-yellow-{index:08d}" - see replay_producer.py.
    prefix = "tlc-yellow-"
    if not key_str.startswith(prefix):
        fail(f"unexpected trip key format: {key_str!r}")
    return int(key_str[len(prefix):])


def cmd_timestamps(args):
    print("=== Test: timestamps intact ===")
    rows = load_sorted_trips(args.input)
    recreate_topic(args.kafka_exec.split(), args.topic)

    run_replay_producer([
        "--input", args.input, "--bootstrap-server", args.bootstrap_server, "--topic", args.topic,
        "--limit", str(args.limit), "--speed-factor", "1000000", "--max-inter-arrival-sleep", "0.01",
    ])

    messages = consume_all(args.bootstrap_server, args.topic, args.limit)
    print(f"consumed {len(messages)} messages (expected {args.limit})")
    if len(messages) != args.limit:
        fail(f"expected {args.limit} landed messages, got {len(messages)}")

    mismatches = []
    for key, value, _ts in messages:
        idx = row_index_from_key(key)
        expected_ts = rows[idx][PICKUP_COLUMN]
        landed_ts_str = value.get(PICKUP_COLUMN)
        if landed_ts_str is None:
            mismatches.append({"row_index": idx, "error": f"missing {PICKUP_COLUMN} in payload"})
            continue
        landed_ts = datetime.datetime.fromisoformat(landed_ts_str)
        if landed_ts != expected_ts:
            mismatches.append({
                "row_index": idx, "expected": expected_ts.isoformat(), "landed": landed_ts_str,
            })

    print(f"checked {len(messages)} messages, {len(mismatches)} timestamp mismatches")
    if mismatches:
        print(json.dumps(mismatches[:20], indent=2))
        fail(f"{len(mismatches)} landed timestamps did not exactly match their source row")
    print("PASS: timestamps - all landed event timestamps exactly match source data")


def cmd_ratio(args):
    print("=== Test: time-compression ratio ===")
    rows = load_sorted_trips(args.input)
    recreate_topic(args.kafka_exec.split(), args.topic)

    run_replay_producer([
        "--input", args.input, "--bootstrap-server", args.bootstrap_server, "--topic", args.topic,
        "--limit", str(args.limit), "--speed-factor", str(args.speed_factor),
        # Effectively disabled - this test isolates the ratio from the
        # cap's own behavior on purpose (DEFENSE.md #39).
        "--max-inter-arrival-sleep", "999999",
    ])

    messages = consume_all(args.bootstrap_server, args.topic, args.limit)
    print(f"consumed {len(messages)} messages (expected {args.limit})")
    if len(messages) != args.limit:
        fail(f"expected {args.limit} landed messages, got {len(messages)}")

    broker_timestamps = [ts for _k, _v, ts in messages]
    wall_clock_elapsed = (max(broker_timestamps) - min(broker_timestamps)) / 1000.0
    event_time_span = (rows[args.limit - 1][PICKUP_COLUMN] - rows[0][PICKUP_COLUMN]).total_seconds()

    print(f"event_time_span={event_time_span:.3f}s wall_clock_elapsed={wall_clock_elapsed:.3f}s")
    if wall_clock_elapsed <= 0:
        fail("wall_clock_elapsed was <=0 - broker timestamps did not span a measurable interval")

    actual_ratio = event_time_span / wall_clock_elapsed
    relative_error = abs(actual_ratio - args.speed_factor) / args.speed_factor
    print(f"actual_ratio={actual_ratio:.2f} configured_speed_factor={args.speed_factor} "
          f"relative_error={relative_error:.3f} tolerance={args.tolerance}")

    if relative_error > args.tolerance:
        fail(
            f"measured ratio {actual_ratio:.2f} differs from configured speed-factor "
            f"{args.speed_factor} by {relative_error:.1%}, exceeding tolerance {args.tolerance:.1%}"
        )
    print("PASS: ratio - wall-clock-to-event-time ratio matches configured speed-factor")


def cmd_resume_seam(args):
    print("=== Test: resume-from-event-time-offset seam ===")
    rows = load_sorted_trips(args.input)
    recreate_topic(args.kafka_exec.split(), args.topic)

    n1 = args.run1_limit
    n2 = args.run2_limit
    if n1 + n2 > len(rows):
        fail(f"run1_limit + run2_limit ({n1 + n2}) exceeds available rows ({len(rows)})")

    resume_ts = rows[n1 - 1][PICKUP_COLUMN]
    ties_in_run1 = sum(1 for r in rows[:n1] if r[PICKUP_COLUMN] == resume_ts)
    print(f"run1: first {n1} rows. resume_ts={resume_ts.isoformat()} ties_in_run1={ties_in_run1}")

    log1 = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".jsonl").name
    log2 = tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".jsonl").name
    try:
        run_replay_producer([
            "--input", args.input, "--bootstrap-server", args.bootstrap_server, "--topic", args.topic,
            "--limit", str(n1), "--speed-factor", "1000000", "--max-inter-arrival-sleep", "0.01",
            "--emission-log", log1,
        ])
        run_replay_producer([
            "--input", args.input, "--bootstrap-server", args.bootstrap_server, "--topic", args.topic,
            "--limit", str(n2), "--speed-factor", "1000000", "--max-inter-arrival-sleep", "0.01",
            "--resume-after-timestamp", resume_ts.isoformat(), "--resume-skip-ties", str(ties_in_run1),
            "--emission-log", log2,
        ])

        with open(log1) as f:
            keys1 = [json.loads(line)["trip_key"] for line in f if line.strip()]
        with open(log2) as f:
            keys2 = [json.loads(line)["trip_key"] for line in f if line.strip()]
    finally:
        os.remove(log1)
        os.remove(log2)

    indices1 = set(row_index_from_key(k) for k in keys1)
    indices2 = set(row_index_from_key(k) for k in keys2)
    overlap = indices1 & indices2
    union = indices1 | indices2
    expected = set(range(n1 + n2))
    missing = expected - union
    unexpected = union - expected

    print(f"run1 landed: {len(indices1)} (expected {n1})")
    print(f"run2 landed: {len(indices2)} (expected {n2})")
    print(f"overlap (duplicates): {len(overlap)}")
    print(f"missing (gaps): {len(missing)}")
    print(f"unexpected (outside expected range): {len(unexpected)}")

    if len(indices1) != n1:
        fail(f"run 1 landed {len(indices1)} distinct row indices, expected {n1}")
    if len(indices2) != n2:
        fail(f"run 2 landed {len(indices2)} distinct row indices, expected {n2}")
    if overlap:
        fail(f"{len(overlap)} row indices landed in BOTH runs (duplicates at the seam): {sorted(overlap)[:20]}")
    if missing:
        fail(f"{len(missing)} row indices never landed in either run (gap at the seam): {sorted(missing)[:20]}")
    if unexpected:
        fail(f"{len(unexpected)} row indices landed outside the expected range: {sorted(unexpected)[:20]}")

    print("PASS: resume-seam - no gaps, no duplicates across the resume boundary")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--input", required=True, help="path to a downloaded yellow_tripdata_*.parquet file")
    parser.add_argument(
        "--kafka-exec", default="docker compose exec -T kafka",
        help="command prefix to run kafka-topics.sh inside the kafka container",
    )
    sub = parser.add_subparsers(dest="test", required=True)

    p_ts = sub.add_parser("timestamps")
    p_ts.add_argument("--topic", default="weir-tlc-verify-timestamps")
    p_ts.add_argument("--limit", type=int, default=500)

    p_ratio = sub.add_parser("ratio")
    p_ratio.add_argument("--topic", default="weir-tlc-verify-ratio")
    p_ratio.add_argument("--limit", type=int, default=1000)
    p_ratio.add_argument("--speed-factor", type=float, default=60.0)
    p_ratio.add_argument("--tolerance", type=float, default=0.25)

    p_seam = sub.add_parser("resume-seam")
    p_seam.add_argument("--topic", default="weir-tlc-verify-resume")
    p_seam.add_argument("--run1-limit", type=int, default=500)
    p_seam.add_argument("--run2-limit", type=int, default=500)

    args = parser.parse_args()
    {"timestamps": cmd_timestamps, "ratio": cmd_ratio, "resume-seam": cmd_resume_seam}[args.test](args)


if __name__ == "__main__":
    main()
