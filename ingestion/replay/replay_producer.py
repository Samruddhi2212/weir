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
isolation from the cap, bounded-window send-order shuffle for
out-of-order injection) are recorded in DEFENSE.md #38-#40, written
before this file.
"""
import argparse
import datetime
import json
import random
import sys
import threading
import time

import pyarrow as pa
import pyarrow.parquet as pq

PICKUP_COLUMN = "tpep_pickup_datetime"


def _json_default(value):
    if isinstance(value, (datetime.datetime, datetime.date)):
        return value.isoformat()
    return str(value)


def load_sorted_trips(parquet_path, preserve_input_order=False):
    """Sorted by pickup time with a fresh stable _row_index, unless
    preserve_input_order is set.

    preserve_input_order exists for the benchmark runner
    (benchmarks/run_benchmark.py): its scenarios express failures as
    transforms on the event stream, and two of them - an out-of-order
    flood and a duplicate storm - are defined by send order and by
    repeated keys. Re-sorting and re-indexing such a stream silently
    erases exactly what it was injecting, turning a scenario into a
    no-op that the benchmark would then score as a detector miss. With
    the flag, the file's physical row order IS the send order and an
    existing _row_index column is used as-is.
    """
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
    if not preserve_input_order:
        table = table.sort_by(PICKUP_COLUMN)
    rows = table.to_pylist()
    if preserve_input_order:
        if any("_row_index" not in row for row in rows):
            raise SystemExit(
                f"FAIL: {parquet_path} is missing a _row_index column, which "
                f"--preserve-input-order requires - the caller owns key identity in that mode."
            )
        return rows
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


def iter_sorted_trips(parquet_path, preserve_input_order=False, batch_size=100_000):
    """Yield rows as dicts in batches instead of materialising the file.

    Same ordering and _row_index semantics as load_sorted_trips - this is
    purely about memory. A 12-week contiguous slice is ~10.5M rows, and
    to_pylist() on that builds ~10GB of dicts, which OOM-killed the CI
    runner (exit 143) while Kafka, Postgres and Flink shared the same
    box. The Arrow table stays columnar; only batch_size rows are Python
    dicts at any moment.
    """
    table = pq.read_table(parquet_path)
    if PICKUP_COLUMN not in table.column_names:
        raise SystemExit(
            f"FAIL: {parquet_path} has no '{PICKUP_COLUMN}' column - found: {table.column_names}."
        )
    if preserve_input_order:
        if "_row_index" not in table.column_names:
            raise SystemExit(
                f"FAIL: {parquet_path} is missing a _row_index column, which "
                f"--preserve-input-order requires - the caller owns key identity in that mode."
            )
    else:
        table = table.sort_by(PICKUP_COLUMN)
        if "_row_index" in table.column_names:
            table = table.drop_columns(["_row_index"])
        table = table.append_column(
            "_row_index", pa.array(range(table.num_rows), type=pa.int64())
        )
    for batch in table.to_batches(max_chunksize=batch_size):
        for row in batch.to_pylist():
            yield row


def filter_resume(rows, resume_after_timestamp, resume_skip_ties):
    """Skips every row with pickup < resume_after_timestamp entirely,
    then skips the first resume_skip_ties rows with pickup ==
    resume_after_timestamp - the tie-safe seam described in DEFENSE.md
    #39. A no-op if resume_after_timestamp is None."""
    if resume_after_timestamp is None:
        return rows

    def generate():
        ties_skipped = 0
        for row in rows:
            ts = row[PICKUP_COLUMN]
            if ts < resume_after_timestamp:
                continue
            if ts == resume_after_timestamp and ties_skipped < resume_skip_ties:
                ties_skipped += 1
                continue
            yield row

    # Lazy, so a resumed run over a multi-million-row stream doesn't
    # rebuild it as a list. Callers that need a list can wrap it.
    return generate()


def build_send_order(n, window, seed):
    """Returns a list of indices into a chronologically-sorted sequence
    of length n: identity order if window <= 1, otherwise partitioned
    into non-overlapping windows of `window` consecutive indices, each
    shuffled independently. An event's send position can never move
    more than window-1 slots from its true chronological position - a
    deterministic bound, not an open-ended tail. See DEFENSE.md #40."""
    order = list(range(n))
    if window <= 1:
        return order
    rng = random.Random(seed)
    for start in range(0, n, window):
        block = order[start:start + window]
        rng.shuffle(block)
        order[start:start + window] = block
    return order


def replay(rows, producer, topic, speed_factor, max_sleep, limit, emission_log_path=None,
           shuffle_window=0, shuffle_seed=42):
    # The shuffle path needs random access within a window and is only
    # ever used with small --limit runs (measure_lateness, replay-verify),
    # so it keeps the original list behaviour byte-for-byte. Everything
    # else streams: the benchmark replays ~10.5M rows and materialising
    # them is a multi-GB OOM.
    streaming = shuffle_window <= 0 and not isinstance(rows, list)
    if not streaming:
        rows = list(rows)
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

    # Deltas are computed against TRUE chronological order, before any
    # shuffling of send order - each row's own pacing sleep is tied to
    # its real chronological predecessor regardless of when it actually
    # gets sent. Windows are non-overlapping and shuffling only permutes
    # within one, so the total of all sleeps - and therefore overall
    # replay duration and the compression ratio (DEFENSE.md #39's ratio
    # test) - is unchanged by turning shuffling on. See DEFENSE.md #40.
    def send_one(row, sleep_seconds):
        nonlocal attempted
        if sleep_seconds > 0:
            time.sleep(sleep_seconds)
        pickup = row[PICKUP_COLUMN]
        # _row_index is stable across the whole dataset (assigned in
        # load_sorted_trips/iter_sorted_trips, before any --limit/--resume
        # slicing) - see DEFENSE.md #39 for why the key must not be
        # derived from a loop-local position within whatever subset this
        # call received.
        trip_key = f"tlc-yellow-{row['_row_index']:08d}"
        payload = {k: v for k, v in row.items() if k != "_row_index"}
        event_ts_iso = pickup.isoformat() if hasattr(pickup, "isoformat") else str(pickup)
        future = producer.send(topic, key=trip_key.encode("utf-8"), value=payload)
        future.add_callback(lambda md, k=trip_key, ts=event_ts_iso: on_success(k, ts, md))
        future.add_errback(lambda exc, k=trip_key: on_error(k, exc))
        attempted += 1
        if attempted % 100000 == 0:
            print(f"replay_producer.py: attempted {attempted}", file=sys.stderr)

    if streaming:
        # One row in hand at a time. Pacing is identical to the list
        # path - each row's sleep is its gap from its true chronological
        # predecessor - it just doesn't need every gap precomputed.
        previous_pickup = None
        sent = 0
        for row in rows:
            if limit and sent >= limit:
                break
            pickup = row[PICKUP_COLUMN]
            delta = 0.0 if previous_pickup is None else max(
                (pickup - previous_pickup).total_seconds(), 0.0
            )
            send_one(row, min(delta / speed_factor, max_sleep))
            previous_pickup = pickup
            sent += 1
        if sent == 0:
            print("no rows to replay", file=sys.stderr)
            return 0, 0
    else:
        # Deltas are computed against TRUE chronological order, before any
        # shuffling of send order - each row's own pacing sleep is tied to
        # its real chronological predecessor regardless of when it actually
        # gets sent. Windows are non-overlapping and shuffling only permutes
        # within one, so the total of all sleeps - and therefore overall
        # replay duration and the compression ratio (DEFENSE.md #39's ratio
        # test) - is unchanged by turning shuffling on. See DEFENSE.md #40.
        deltas = [0.0] * len(rows)
        for i in range(1, len(rows)):
            deltas[i] = max(
                (rows[i][PICKUP_COLUMN] - rows[i - 1][PICKUP_COLUMN]).total_seconds(), 0.0
            )
        for idx in build_send_order(len(rows), shuffle_window, shuffle_seed):
            send_one(rows[idx], min(deltas[idx] / speed_factor, max_sleep))

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
        "--preserve-input-order", action="store_true",
        help="treat the file's physical row order as the send order and reuse its existing "
             "_row_index column, instead of sorting by pickup time and re-indexing. For "
             "pre-built streams whose order and repeated keys ARE the payload "
             "(benchmarks/run_benchmark.py's injected scenarios).",
    )
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
    parser.add_argument(
        "--shuffle-window", type=int, default=0,
        help="bounded out-of-order injection (DEFENSE.md #40): shuffle send order within non-overlapping "
             "windows of this many rows; event_ts is untouched, pacing stays tied to true chronological order. "
             "0 (default) = disabled, perfectly in-order replay.",
    )
    parser.add_argument(
        "--shuffle-seed", type=int, default=42,
        help="seed for --shuffle-window's shuffling - fixed by default so the injected disorder is "
             "reproducible, not a one-off random result (DEFENSE.md #40)",
    )
    args = parser.parse_args()

    print(f"replay_producer.py: loading {args.input}", file=sys.stderr)
    rows = iter_sorted_trips(args.input, preserve_input_order=args.preserve_input_order)
    ordering = "input order preserved" if args.preserve_input_order else f"sorted by {PICKUP_COLUMN}"
    print(f"replay_producer.py: streaming trips, {ordering}", file=sys.stderr)

    if args.resume_after_timestamp:
        resume_ts = datetime.datetime.fromisoformat(args.resume_after_timestamp)
        rows = filter_resume(rows, resume_ts, args.resume_skip_ties)
        print(
            f"replay_producer.py: resuming after {resume_ts.isoformat()} "
            f"(skipping {args.resume_skip_ties} tie(s))",
            file=sys.stderr,
        )

    # Imported here, not at module scope: load_sorted_trips/filter_resume
    # are pure functions over a parquet file, and importers that only need
    # those (benchmarks/run_benchmark.py, and its unit tests) shouldn't
    # need a Kafka driver installed to import this module at all.
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        value_serializer=lambda v: json.dumps(v, default=_json_default).encode("utf-8"),
        acks="all",
    )
    try:
        attempted, confirmed = replay(
            rows, producer, args.topic, args.speed_factor, args.max_inter_arrival_sleep, args.limit,
            emission_log_path=args.emission_log,
            shuffle_window=args.shuffle_window, shuffle_seed=args.shuffle_seed,
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
