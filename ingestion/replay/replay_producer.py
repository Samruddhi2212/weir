#!/usr/bin/env python3
"""Part 2.1: time-compressed replay of a downloaded NYC TLC trip file into Kafka.

Reads a Yellow Taxi Parquet file (see download_tlc_data.py), sorts by
pickup time, and replays it to a Kafka topic with the wall-clock gap
between sends equal to each pair's real inter-arrival time divided by
--speed-factor - preserving the shape of the real arrival pattern
(rush-hour bursts, overnight lulls), not a flat constant rate.

Design decisions (dataset scope, synthetic trip key, time-compression
formula and its cap, zone IDs left unresolved, producer library reuse,
V5 re-audit, emission log, resume-seam determinism, ratio-test
isolation from the cap) are recorded in DEFENSE.md #38-#39, written
before this file.
"""
import argparse
import datetime
import json
import sys
import threading
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
    # Deterministic across separate process invocations, not just within
    # one run - required for --resume-after-timestamp's tie-counting to
    # be gap-free/duplicate-free (DEFENSE.md #39). Verified, not assumed:
    # pyarrow.Table.sort_by() is documented as a stable sort, and
    # pq.read_table() on the same file always reads rows in the same
    # on-disk physical order - stable sort of a deterministic input
    # order is deterministic output order, ties included.
    table = table.sort_by(PICKUP_COLUMN)
    rows = table.to_pylist()
    # Assigned here, against the full sorted sequence, before any
    # --limit slicing or --resume-after-timestamp filtering happens -
    # both of those produce a *subset* of rows, and deriving the trip
    # key from a loop-local position within that subset (as an earlier
    # version of this function did, via enumerate()) would reset to 0
    # for every subset, colliding across a resumed run's two separate
    # invocations instead of continuing the same key space. This index
    # is stable across the whole dataset regardless of what's sliced
    # out of it afterward.
    for i, row in enumerate(rows):
        row["_row_index"] = i
    return rows


def filter_resume(rows, resume_after_timestamp, resume_skip_ties):
    """Skips every row with pickup < resume_after_timestamp entirely,
    then skips the first resume_skip_ties rows with pickup ==
    resume_after_timestamp - the tie-safe seam described in DEFENSE.md
    #39. A no-op if resume_after_timestamp is None."""
    if resume_after_timestamp is None:
        return rows
    result = []
    ties_skipped = 0
    for row in rows:
        ts = row[PICKUP_COLUMN]
        if ts < resume_after_timestamp:
            continue
        if ts == resume_after_timestamp and ties_skipped < resume_skip_ties:
            ties_skipped += 1
            continue
        result.append(row)
    return result


def replay(rows, producer, topic, speed_factor, max_sleep, limit, emission_log_path=None):
    if limit:
        rows = rows[:limit]
    if not rows:
        print("no rows to replay", file=sys.stderr)
        return 0, 0

    if emission_log_path:
        open(emission_log_path, "w", encoding="utf-8").close()

    # V5 (CLAUDE.md Verification Standard): ground truth is the broker's
    # own delivery confirmation, not "we called send()". `attempted`
    # counts send() calls; `confirmed` counts only what the broker has
    # actually acked via on_success - the two are reported separately,
    # not conflated, so a partial-delivery run can't be misread as fully
    # replayed. Re-audited (DEFENSE.md #39), not just assumed compliant:
    # confirmed is driven exclusively by this callback, nothing else
    # increments it, and every log line calling it "attempted" is
    # honest about what it actually counts.
    attempted = 0
    confirmed = 0
    delivery_failures = []
    lock = threading.Lock()
    prev_pickup = None

    def on_success(key, event_ts_iso, metadata):
        nonlocal confirmed
        with lock:
            confirmed += 1
            if emission_log_path:
                entry = {
                    "trip_key": key,
                    "event_ts": event_ts_iso,
                    "partition": metadata.partition,
                    "offset": metadata.offset,
                }
                with open(emission_log_path, "a", encoding="utf-8") as f:
                    f.write(json.dumps(entry) + "\n")
                    f.flush()

    def on_error(key, exc):
        print(f"replay_producer.py: DELIVERY FAILED for {key}: {exc}", file=sys.stderr)
        with lock:
            delivery_failures.append({"key": key, "error": str(exc)})

    for row in rows:
        pickup = row[PICKUP_COLUMN]
        if prev_pickup is not None:
            real_delta_seconds = max((pickup - prev_pickup).total_seconds(), 0.0)
            sleep_seconds = min(real_delta_seconds / speed_factor, max_sleep)
            if sleep_seconds > 0:
                time.sleep(sleep_seconds)
        prev_pickup = pickup

        # _row_index is stable across the whole dataset (assigned in
        # load_sorted_trips, before any --limit/--resume slicing) - see
        # DEFENSE.md #39 for why the key must not be derived from a
        # loop-local position within whatever subset this call received.
        trip_key = f"tlc-yellow-{row['_row_index']:08d}"
        payload = {k: v for k, v in row.items() if k != "_row_index"}
        event_ts_iso = pickup.isoformat() if hasattr(pickup, "isoformat") else str(pickup)
        future = producer.send(topic, key=trip_key.encode("utf-8"), value=payload)
        future.add_callback(lambda md, k=trip_key, ts=event_ts_iso: on_success(k, ts, md))
        future.add_errback(lambda exc, k=trip_key: on_error(k, exc))
        attempted += 1
        if attempted % 1000 == 0:
            print(f"replay_producer.py: attempted {attempted}/{len(rows)}", file=sys.stderr)

    # flush() blocks until every buffered send's callback (success or
    # error) has actually fired - `confirmed` is final and accurate only
    # after this returns, not as soon as the loop above finishes queuing.
    producer.flush(timeout=60)
    if delivery_failures:
        print(f"replay_producer.py: {len(delivery_failures)} delivery failures", file=sys.stderr)
        print(json.dumps(delivery_failures), file=sys.stderr)
        raise SystemExit(1)
    return attempted, confirmed


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
    parser.add_argument(
        "--emission-log", default=None,
        help="optional path to write JSON-lines confirmed-delivery log (trip_key, event_ts, partition, offset)",
    )
    parser.add_argument(
        "--resume-after-timestamp", default=None,
        help="ISO8601 timestamp - skip every row with pickup time strictly before this (see --resume-skip-ties)",
    )
    parser.add_argument(
        "--resume-skip-ties", type=int, default=0,
        help="also skip this many rows with pickup time == --resume-after-timestamp - "
             "must equal how many such rows a prior run already sent, or the seam gaps/duplicates (DEFENSE.md #39)",
    )
    args = parser.parse_args()

    print(f"replay_producer.py: loading {args.input}", file=sys.stderr)
    rows = load_sorted_trips(args.input)
    print(f"replay_producer.py: {len(rows)} trips loaded, sorted by {PICKUP_COLUMN}", file=sys.stderr)

    if args.resume_after_timestamp:
        resume_ts = datetime.datetime.fromisoformat(args.resume_after_timestamp)
        rows = filter_resume(rows, resume_ts, args.resume_skip_ties)
        print(
            f"replay_producer.py: resuming after {resume_ts.isoformat()} "
            f"(skipping {args.resume_skip_ties} tie(s)) - {len(rows)} trips remain",
            file=sys.stderr,
        )

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=lambda v: json.dumps(v, default=_json_default).encode("utf-8"),
        acks="all",
    )
    try:
        attempted, confirmed = replay(
            rows, producer, args.topic, args.speed_factor, args.max_inter_arrival_sleep, args.limit,
            emission_log_path=args.emission_log,
        )
    finally:
        producer.close(timeout=30)

    print(
        f"replay_producer.py: done - attempted {attempted}, broker-confirmed {confirmed} trips to {args.topic}",
        file=sys.stderr,
    )
    if confirmed != attempted:
        # Reached only if delivery_failures was somehow empty despite a
        # count mismatch - shouldn't happen given flush() waits for every
        # callback, but a silent mismatch here would be exactly the kind
        # of unearned confidence V5 exists to rule out. Fail loudly
        # rather than let a smaller number quietly look like success.
        raise SystemExit(f"FAIL: attempted {attempted} but broker only confirmed {confirmed} - see stderr above")


if __name__ == "__main__":
    main()
