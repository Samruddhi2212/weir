#!/usr/bin/env python3
"""Step 6 exactly-once validation: minimal continuous Kafka producer.

Sends keyed JSON events at a target rate. The emission log
(--emission-log) is the ground truth scripts/verify_exactly_once.py
compares Iceberg's contents against - see DEFENSE.md #25 for why it's
written only from a per-message delivery callback (confirmed broker ack),
never speculatively before a send, and why kafka-python (not
kafka-console-producer.sh) was chosen to make that possible at all.

Graceful shutdown on SIGTERM/SIGINT: stop producing new events, flush
in-flight sends (letting their callbacks fire and finish writing the
log), then exit. scripts/verify_recovery.sh relies on this to get a
clean, complete emission log rather than an arbitrarily truncated one.
"""
import argparse
import json
import signal
import sys
import threading
import time

from kafka import KafkaProducer

_stop_requested = threading.Event()
_log_lock = threading.Lock()
_delivery_failures = []


def _handle_stop_signal(signum, _frame):
    print(f"produce_events.py: received signal {signum}, stopping...", file=sys.stderr)
    _stop_requested.set()


def _on_success(log_path, event_key, event_ts_ms, record_metadata):
    entry = {
        "event_key": event_key,
        "event_ts": event_ts_ms,
        "partition": record_metadata.partition,
        "offset": record_metadata.offset,
    }
    with _log_lock:
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")
            f.flush()


def _on_error(event_key, exc):
    # A failed delivery must be visible, not silently absent from the
    # emission log - an absent entry would be indistinguishable from
    # "never attempted," hiding a real send failure as if it were normal
    # producer behavior.
    print(f"produce_events.py: DELIVERY FAILED for {event_key}: {exc}", file=sys.stderr)
    with _log_lock:
        _delivery_failures.append({"event_key": event_key, "error": str(exc)})


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bootstrap-server", required=True, help="host:port, e.g. localhost:9092")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--emission-log", required=True, help="path to write JSON-lines confirmed-delivery log")
    parser.add_argument("--rate", type=float, default=100.0, help="target events/sec (default: 100)")
    parser.add_argument("--max-events", type=int, default=0, help="stop after N events (0 = unbounded, default)")
    args = parser.parse_args()

    signal.signal(signal.SIGTERM, _handle_stop_signal)
    signal.signal(signal.SIGINT, _handle_stop_signal)

    # Truncate any log from a previous run - this run's emission log must
    # describe only this run's sends, not accumulate across invocations.
    open(args.emission_log, "w", encoding="utf-8").close()

    producer = KafkaProducer(
        bootstrap_servers=[args.bootstrap_server],
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks="all",
    )

    sleep_interval = 1.0 / args.rate if args.rate > 0 else 0
    sent_count = 0

    print(
        f"produce_events.py: producing to {args.topic} at ~{args.rate}/sec, "
        f"emission log at {args.emission_log}",
        file=sys.stderr,
    )

    try:
        i = 0
        while not _stop_requested.is_set():
            if args.max_events and i >= args.max_events:
                break
            event_key = f"evt-{i:08d}"
            event_ts_ms = int(time.time() * 1000)
            future = producer.send(args.topic, key=event_key, value={"event_key": event_key, "event_ts": event_ts_ms})
            future.add_callback(
                lambda md, k=event_key, ts=event_ts_ms: _on_success(args.emission_log, k, ts, md)
            )
            future.add_errback(lambda exc, k=event_key: _on_error(k, exc))
            sent_count += 1
            i += 1
            if sleep_interval:
                time.sleep(sleep_interval)
    finally:
        print("produce_events.py: flushing in-flight sends...", file=sys.stderr)
        producer.flush(timeout=30)
        producer.close(timeout=30)

    print(
        f"produce_events.py: done. attempted={sent_count} delivery_failures={len(_delivery_failures)}",
        file=sys.stderr,
    )
    if _delivery_failures:
        print(json.dumps(_delivery_failures), file=sys.stderr)
        sys.exit(1)
    sys.exit(0)


if __name__ == "__main__":
    main()
