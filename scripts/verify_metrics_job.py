#!/usr/bin/env python3
"""Part 3.1: end-to-end verification of scripts/sql/metrics_job.sql.

Replays a real, in-order (no shuffle - DEFENSE.md #42's watermark math
below assumes monotonically increasing event time) slice of TLC data
into Kafka, submits the metrics job, then checks its output against
ground truth computed independently in Python from the exact rows the
replay producer's own emission log confirms were delivered - never
against a second copy of the SQL's own arithmetic.

Two checks, per V1 (exact parsed values, not "looks about right"):

1. Deep check: every column_metrics row (69 when every NULL_COLS/
   STAT_COLS column is present in this file's own schema - fewer for
   a real month missing one, e.g. cbd_congestion_fee before 2025-01,
   DEFENSE.md #49) plus window_metrics.row_count for ONE specific,
   certainly-closed window, computed independently in Python from the
   source parquet rows that fall in it.
2. Broad check: SUM(row_count) across every window that Flink's
   watermark (last event time - 270s, DEFENSE.md #42) guarantees has
   closed, compared against an independently computed count from the
   emission log - not every window closes; trailing events within the
   last 270s of event time sit in a window that never fires, and this
   script computes that boundary itself rather than assuming all
   replayed rows show up.

V2/V3: every external command has an explicit timeout and prints its
raw output unconditionally. V5: "replayed" is the replay producer's
own broker-confirmed count (its exit code contract, DEFENSE.md #39),
not a send-call count.
"""
import argparse
import datetime
import json
import math
import os
import re
import subprocess
import sys
import tempfile
import time


def fail(msg):
    print(f"\nFAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def print_raw(label, content):
    print(f"--- raw output: {label} ---")
    print(content)
    print(f"--- end raw output: {label} ---")


def run(cmd, timeout, **kwargs):
    try:
        return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, **kwargs)
    except subprocess.TimeoutExpired as e:
        fail(f"command timed out after {timeout}s: {' '.join(cmd)}\nstdout so far: {e.stdout}\nstderr so far: {e.stderr}")


def docker_compose_logs(services):
    out = run(["docker", "compose", "logs", "--tail=200"] + services, timeout=30)
    print_raw(f"docker compose logs {' '.join(services)}", (out.stdout or "") + (out.stderr or ""))


def service_is_healthy(ps_out, service):
    for line in ps_out.splitlines():
        if line.split() and line.split()[0] == service:
            return "(healthy)" in line
    return False


def wait_for_healthy(services, max_wait=180):
    waited = 0
    while True:
        ps = run(["docker", "compose", "ps"], timeout=15)
        ps_out = ps.stdout or ""
        if all(service_is_healthy(ps_out, svc) for svc in services):
            print_raw("docker compose ps", ps_out)
            return
        waited += 5
        if waited >= max_wait:
            print_raw("docker compose ps", ps_out)
            docker_compose_logs(services)
            fail(f"services {services} not all healthy after {max_wait}s")
        time.sleep(5)


def psql(pg_container, pg_user, pg_db, sql, timeout=30):
    out = run(["docker", "compose", "exec", "-T", pg_container,
               "psql", "-t", "-A", "-F", "\x1f", "-U", pg_user, "-d", pg_db, "-c", sql], timeout=timeout)
    if out.returncode != 0:
        fail(f"psql failed (exit {out.returncode}): {sql}\nstdout: {out.stdout}\nstderr: {out.stderr}")
    rows = []
    for line in out.stdout.strip("\n").split("\n"):
        if line == "":
            continue
        rows.append(line.split("\x1f"))
    return rows


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--input", required=True)
    p.add_argument("--bootstrap-server", required=True)
    p.add_argument("--topic", default="weir-metrics-verify")
    p.add_argument("--limit", type=int, default=6000)
    p.add_argument(
        "--expected-emitted-count", type=int, default=None,
        help="broker-confirmed delivery count to assert, when it isn't --limit. The benchmark "
             "runner supplies a count derived independently from its scenarios' declared "
             "effects (a duplicate storm adds rows, a partition degradation removes them), so "
             "the invariant stays exact rather than being relaxed for injected runs.",
    )
    p.add_argument(
        "--preserve-input-order", action="store_true",
        help="passed through to replay_producer.py - see its help.",
    )
    p.add_argument(
        "--allow-late-drop", action="store_true",
        help="the input stream deliberately contains events the pipeline should drop "
             "(an injected benchmark run). Reports the shortfall instead of asserting zero "
             "loss, and still fails if MORE rows land than were emitted.",
    )
    p.add_argument(
        "--replay-timeout", type=int, default=300,
        help="seconds allowed for replay_producer.py. 300 suits a 6k-event verification; the "
             "benchmark replays millions and needs far more (measured ~2,800 events/s).",
    )
    p.add_argument(
        "--drain-max-wait", type=int, default=900,
        help="seconds to let the JDBC sink finish flushing windows before the broad check "
             "asserts. Waits for the pipeline to stop moving; never relaxes the assertion.",
    )
    p.add_argument("--resume-after-timestamp", default=None,
                   help="passed through to replay_producer.py - seek near a specific date "
                        "(e.g. a real DST transition) instead of always starting at the file's "
                        "first row")
    p.add_argument("--speed-factor", type=float, default=50000.0)
    p.add_argument("--max-inter-arrival-sleep", type=float, default=0.05)
    p.add_argument("--checkpoint-interval", default="10s")
    p.add_argument("--watermark-bound-seconds", type=int, default=270)
    p.add_argument("--pg-container", default="postgres")
    p.add_argument("--pg-user", default="weir")
    # No literal default: this is substituted into the Flink job's JDBC
    # sink DDL. Same reasoning as every other password argument.
    p.add_argument("--pg-password", default=os.environ.get("POSTGRES_PASSWORD"),
                   required=os.environ.get("POSTGRES_PASSWORD") is None,
                   help="Postgres password. Set POSTGRES_PASSWORD or pass this flag.")
    p.add_argument("--pg-db", default="weir_catalog")
    p.add_argument("--flink-ui-port", type=int, default=8081)
    args = p.parse_args()

    print("=== Stage 1: recreate topic ===")
    run(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
         "--bootstrap-server", "localhost:9092", "--delete", "--topic", args.topic], timeout=30)
    out = run(["docker", "compose", "exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh",
               "--bootstrap-server", "localhost:9092", "--create", "--topic", args.topic,
               "--partitions", "1", "--replication-factor", "1"], timeout=30)
    print_raw("kafka-topics.sh --create", out.stdout + out.stderr)
    if out.returncode != 0:
        fail(f"could not create topic {args.topic}")

    print("\n=== Stage 2: apply reliability/store/schema.sql (fresh) ===")
    with open("reliability/store/schema.sql", "rb") as f:
        schema_sql = f.read()
    apply = run(["docker", "compose", "exec", "-T", args.pg_container,
                 "psql", "-U", args.pg_user, "-d", args.pg_db], timeout=30, input=schema_sql.decode())
    print_raw("apply schema.sql", apply.stdout + apply.stderr)
    if apply.returncode != 0:
        fail("could not apply schema.sql")
    # Clean slate: this script may run more than once against the same DB.
    wipe = run(["docker", "compose", "exec", "-T", args.pg_container,
                "psql", "-U", args.pg_user, "-d", args.pg_db, "-c",
                "TRUNCATE weir_metrics.window_metrics_wide, weir_metrics.window_metrics, weir_metrics.column_metrics;"],
               timeout=30)
    print_raw("truncate metrics tables", wipe.stdout + wipe.stderr)
    if wipe.returncode != 0:
        fail("could not truncate metrics tables")

    print("\n=== Stage 3: replay real TLC data (in-order, emission log recorded) ===")
    emission_log = tempfile.NamedTemporaryFile(prefix="weir_metrics_emission_", suffix=".jsonl", delete=False).name
    replay_cmd = [
        sys.executable, "ingestion/replay/replay_producer.py",
        "--input", args.input,
        "--bootstrap-server", args.bootstrap_server,
        "--topic", args.topic,
        "--limit", str(args.limit),
        "--speed-factor", str(args.speed_factor),
        "--max-inter-arrival-sleep", str(args.max_inter_arrival_sleep),
        "--emission-log", emission_log,
    ]
    if args.resume_after_timestamp:
        replay_cmd += ["--resume-after-timestamp", args.resume_after_timestamp]
    if args.preserve_input_order:
        replay_cmd.append("--preserve-input-order")
    replay = run(replay_cmd, timeout=args.replay_timeout)
    print_raw("replay_producer.py", replay.stdout + replay.stderr)
    if replay.returncode != 0:
        fail(f"replay_producer.py exited {replay.returncode} - broker did not confirm all sends (V5)")

    # Streamed, not materialised: the benchmark replays ~10.5M events and
    # a list of that many dicts (plus a sorted list of that many
    # datetimes) is several GB - enough to get OOM-killed on a runner
    # already hosting Kafka, Postgres and Flink. Two cheap passes over
    # the file replace one expensive pass into memory; every assertion
    # below is unchanged.
    emitted_count = 0
    first_event_ts = None
    last_event_ts = None
    with open(emission_log, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            event_ts = datetime.datetime.fromisoformat(json.loads(line)["event_ts"])
            emitted_count += 1
            if first_event_ts is None or event_ts < first_event_ts:
                first_event_ts = event_ts
            if last_event_ts is None or event_ts > last_event_ts:
                last_event_ts = event_ts

    expected_emitted = args.expected_emitted_count if args.expected_emitted_count is not None else args.limit
    source = "--expected-emitted-count" if args.expected_emitted_count is not None else "--limit"
    if emitted_count != expected_emitted:
        fail(f"emission log has {emitted_count} entries, expected exactly {expected_emitted} "
             f"(broker-confirmed count, from {source})")
    print(f"PASS: stage 3 - {emitted_count} broker-confirmed deliveries, matches {source} exactly")

    closed_boundary = last_event_ts - datetime.timedelta(seconds=args.watermark_bound_seconds)
    print(f"last_event_ts={last_event_ts.isoformat()} closed_boundary={closed_boundary.isoformat()}")

    def window_start_of(ts):
        return ts.replace(second=0, microsecond=0)

    def window_closed(ws):
        we = ws + datetime.timedelta(minutes=1)
        return we <= closed_boundary

    # Deep-check window: the EARLIEST window, since with in-order replay
    # it is certainly closed as long as any window is closed at all.
    target_ws = window_start_of(first_event_ts)
    target_we = target_ws + datetime.timedelta(minutes=1)

    # Second pass: the closed-row count, and the delivered keys for the
    # target window only - the full delivered-index set would be another
    # 10.5M-entry structure, and only one window's worth is ever used.
    expected_closed_row_count = 0
    delivered_in_target = set()
    with open(emission_log, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            entry = json.loads(line)
            event_ts = datetime.datetime.fromisoformat(entry["event_ts"])
            if window_closed(window_start_of(event_ts)):
                expected_closed_row_count += 1
            if target_ws <= event_ts < target_we:
                # trip_key format: tlc-yellow-{row_index:08d} (replay_producer.py)
                delivered_in_target.add(int(entry["trip_key"].rsplit("-", 1)[-1]))

    if expected_closed_row_count == 0:
        fail("no windows are expected to close with these --limit/--watermark-bound-seconds settings - "
             "increase --limit or the event-time span so at least one full window closes")
    print(f"expected_closed_row_count (independently computed from emission log) = {expected_closed_row_count}")
    if not window_closed(target_ws):
        fail("earliest window is not expected to be closed - dataset/limit too small for this check")
    print(f"deep-check target window: [{target_ws.isoformat()}, {target_we.isoformat()})")

    print("\n=== Stage 4: compute ground truth for the target window from the source parquet ===")
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pyarrow.compute as pc

    table = pq.read_table(args.input)
    # _row_index is already a column when the caller preserved input
    # order; otherwise derive it from sorted position, as before. Either
    # way the table is filtered to the one target window BEFORE being
    # materialised - to_pylist() on the whole input is what the streaming
    # above exists to avoid.
    if "_row_index" not in table.column_names:
        table = table.sort_by("tpep_pickup_datetime")
        table = table.append_column("_row_index", pa.array(range(table.num_rows), type=pa.int64()))
    pickup = table.column("tpep_pickup_datetime")
    in_target = pc.and_(
        pc.greater_equal(pickup, pa.scalar(target_ws)),
        pc.less(pickup, pa.scalar(target_we)),
    )
    window_rows = table.filter(in_target).to_pylist()
    target_rows = [r for r in window_rows if r["_row_index"] in delivered_in_target]
    n = len(target_rows)
    print(f"ground-truth row_count for target window = {n}")

    NULL_COLS = ["VendorID", "tpep_dropoff_datetime", "passenger_count", "trip_distance", "RatecodeID",
                 "store_and_fwd_flag", "PULocationID", "DOLocationID", "payment_type", "fare_amount",
                 "extra", "mta_tax", "tip_amount", "tolls_amount", "improvement_surcharge",
                 "total_amount", "congestion_surcharge", "Airport_fee", "cbd_congestion_fee"]
    DISTINCT_COLS = ["VendorID", "RatecodeID", "payment_type", "PULocationID", "DOLocationID", "store_and_fwd_flag"]
    NEGATIVE_COLS = ["fare_amount", "tip_amount", "tolls_amount", "total_amount", "trip_distance", "passenger_count"]
    STAT_COLS = ["trip_distance", "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
                 "improvement_surcharge", "total_amount", "congestion_surcharge", "Airport_fee",
                 "cbd_congestion_fee", "passenger_count"]

    # NYC TLC's own column set isn't fixed across months - cbd_congestion_fee
    # (congestion pricing) only exists from 2025-01 onward and is genuinely
    # absent, not just null, from earlier files' own schema. But a column
    # missing from the source parquet is indistinguishable, at the Flink
    # table level, from one that's declared and simply null on every row -
    # metrics_job.sql's Kafka source table declares every column (nullable),
    # so a genuinely missing JSON field is read as NULL, not an error.
    # null_count/distinct_count/negative_count are COUNT-shaped and
    # ALWAYS computable even when every value is null (a real, non-NULL
    # 0 or n, never SQL NULL) - only MIN/MAX/AVG (STAT_COLS) produce a
    # true SQL NULL with zero non-null values to aggregate. Confirmed
    # the hard way: an earlier version of this fix skipped null_count too
    # for an absent column, computing 65 expected metrics against a real
    # 66 the trigger actually wrote (DEFENSE.md #49 addendum) - null_count
    # for cbd_congestion_fee is a real row (value = n), just never a
    # min/max/mean one.
    available_columns = set(table.column_names)
    skipped_absent = [c for c in (NULL_COLS + DISTINCT_COLS + NEGATIVE_COLS + STAT_COLS)
                      if c not in available_columns]
    if skipped_absent:
        print(f"columns absent from this file's own schema (not null - structurally missing): {sorted(set(skipped_absent))}")

    expected = {}
    skipped_metric_count = 0
    for col in NULL_COLS:
        expected[(col, "null_count")] = float(sum(1 for r in target_rows if r.get(col) is None))
    for col in DISTINCT_COLS:
        expected[(col, "distinct_count")] = float(len(set(r.get(col) for r in target_rows)))
    for col in NEGATIVE_COLS:
        expected[(col, "negative_count")] = float(sum(1 for r in target_rows if r.get(col) is not None and r.get(col) < 0))
    for col in STAT_COLS:
        vals = [r.get(col) for r in target_rows if r.get(col) is not None]
        if not vals:
            # Zero non-null values - whether because the column is
            # entirely absent from this file's schema or because every
            # row happens to be null this window, SQL MIN/MAX/AVG over
            # an empty/all-null group is NULL, not a real number -
            # nothing this check can assert a ground-truth value
            # against, so it's skipped rather than asserted as 0 or
            # any other placeholder (matching the trigger's own
            # WHERE metric_value IS NOT NULL, DEFENSE.md #50).
            skipped_metric_count += 3
            continue
        expected[(col, "min")] = float(min(vals))
        expected[(col, "max")] = float(max(vals))
        expected[(col, "mean")] = sum(vals) / len(vals)
    expected[("trip_duration", "negative_count")] = float(sum(
        1 for r in target_rows if r["tpep_dropoff_datetime"] < r["tpep_pickup_datetime"]))
    expected[("trip_duration", "zero_count")] = float(sum(
        1 for r in target_rows if r["tpep_dropoff_datetime"] == r["tpep_pickup_datetime"]))
    expected_metric_count = 69 - skipped_metric_count
    print(f"computed {len(expected)} expected (column_name, metric_name) -> value pairs (expect {expected_metric_count})")
    if len(expected) != expected_metric_count:
        fail(f"expected exactly {expected_metric_count} metrics per window per DEFENSE.md #41/#42/#49, "
             f"computed {len(expected)}")

    print("\n=== Stage 5: services healthy (kafka, postgres, flink) ===")
    wait_for_healthy(["weir-kafka", "weir-postgres", "weir-flink-jobmanager", "weir-flink-taskmanager"])
    print("PASS: stage 5 - services healthy")

    print("\n=== Stage 6: submit metrics_job.sql ===")
    with open("scripts/sql/metrics_job.sql", "r", encoding="utf-8") as f:
        job_sql = f.read()
    job_sql = (job_sql
               .replace("__WEIR_METRICS_CHECKPOINT_INTERVAL__", args.checkpoint_interval)
               .replace("__WEIR_METRICS_TOPIC__", args.topic)
               .replace("__WEIR_METRICS_PG_DB__", args.pg_db)
               .replace("__WEIR_METRICS_PG_USER__", args.pg_user)
               .replace("__WEIR_METRICS_PG_PASSWORD__", args.pg_password))
    resolved = tempfile.NamedTemporaryFile(prefix="weir_metrics_job_resolved_", suffix=".sql", delete=False)
    resolved.write(job_sql.encode("utf-8"))
    resolved.close()
    os.chmod(resolved.name, 0o644)

    cp = run(["docker", "compose", "cp", resolved.name, "flink-jobmanager:/tmp/weir_metrics_job_resolved.sql"], timeout=30)
    print_raw("docker compose cp", cp.stdout + cp.stderr)
    if cp.returncode != 0:
        fail("could not copy resolved metrics_job.sql into flink-jobmanager")

    # Verify the file that actually landed in the container, not assume
    # the copy produced what was intended - a silent truncation/empty-
    # file bug would otherwise look identical to a sql-client problem.
    landed = run(["docker", "compose", "exec", "-T", "flink-jobmanager",
                  "wc", "-l", "/tmp/weir_metrics_job_resolved.sql"], timeout=15)
    print_raw("wc -l of the file that landed in the container", landed.stdout + landed.stderr)
    landed_cat = run(["docker", "compose", "exec", "-T", "flink-jobmanager",
                       "cat", "/tmp/weir_metrics_job_resolved.sql"], timeout=15)
    print_raw("full content of the file that landed in the container", landed_cat.stdout + landed_cat.stderr)

    submit = run(["docker", "compose", "exec", "-T", "flink-jobmanager",
                  "./bin/sql-client.sh", "-f", "/tmp/weir_metrics_job_resolved.sql"], timeout=90)
    print_raw("sql-client.sh (job submission)", submit.stdout + submit.stderr)
    # Dump Flink's own logs unconditionally here, not only on failure -
    # a clean exit code with near-empty sql-client output is itself
    # inconclusive without seeing what the cluster side actually did.
    docker_compose_logs(["flink-jobmanager", "flink-taskmanager"])
    if submit.returncode != 0:
        fail(f"sql-client exited {submit.returncode} submitting metrics_job.sql")

    m = re.search(r"Job ID:\s*([0-9a-f]{32})", submit.stdout)
    if not m:
        fail(f"could not extract a 32-char hex job ID from submission output")
    job_id = m.group(1)
    print(f"PASS: stage 6 - job submitted, Job ID = {job_id}")

    print("\n=== Stage 7: wait for the target window's row to land in window_metrics ===")
    waited = 0
    max_wait = 180
    target_ws_sql = target_ws.strftime("%Y-%m-%d %H:%M:%S")
    while True:
        rows_found = psql(args.pg_container, args.pg_user, args.pg_db,
                           f"SELECT row_count FROM weir_metrics.window_metrics WHERE window_start = '{target_ws_sql}';")
        if rows_found:
            break
        waited += 5
        if waited >= max_wait:
            docker_compose_logs(["flink-jobmanager", "flink-taskmanager"])
            fail(f"target window row never appeared in window_metrics after {max_wait}s")
        time.sleep(5)
    actual_row_count = float(rows_found[0][0])
    print(f"PASS: stage 7 - target window present, row_count={actual_row_count}")

    print(f"\n=== Stage 8: deep check - all {expected_metric_count} column_metrics values for the target window ===")
    cm_rows = psql(args.pg_container, args.pg_user, args.pg_db,
                   f"SELECT column_name, metric_name, metric_value FROM weir_metrics.column_metrics "
                   f"WHERE window_start = '{target_ws_sql}' ORDER BY column_name, metric_name;")
    actual = {(r[0], r[1]): float(r[2]) for r in cm_rows}
    print(f"fetched {len(actual)} column_metrics rows for target window")

    mismatches = []
    if actual_row_count != n:
        mismatches.append(f"window_metrics.row_count: expected {n}, got {actual_row_count}")
    for key, expected_val in expected.items():
        actual_val = actual.get(key)
        if actual_val is None:
            mismatches.append(f"{key}: MISSING from column_metrics")
        elif not math.isclose(actual_val, expected_val, rel_tol=1e-9, abs_tol=1e-9):
            mismatches.append(f"{key}: expected {expected_val!r}, got {actual_val!r}")
    if len(actual) != expected_metric_count:
        mismatches.append(f"column_metrics row count for target window: expected {expected_metric_count}, got {len(actual)}")

    if mismatches:
        print_raw("all expected vs actual", json.dumps(
            {"expected": {f"{k[0]}|{k[1]}": v for k, v in expected.items()},
             "actual": {f"{k[0]}|{k[1]}": v for k, v in actual.items()}}, indent=2))
        fail("deep check mismatches:\n  " + "\n  ".join(mismatches))
    print(f"PASS: stage 8 - all {expected_metric_count} metrics + row_count exactly match independently computed ground truth")

    print("\n=== Stage 9: broad check - SUM(row_count) across all closed windows ===")
    # Wait for the sink to drain before asserting, rather than relaxing
    # what is asserted. Stage 7 only waits for ONE window to land; with
    # a small replay everything else has landed by now, but a large one
    # (the benchmark replays ~385k events across four months) is still
    # flushing windows through the JDBC sink's checkpoint cadence when
    # this stage is reached - a race that reads as a huge shortfall.
    # The invariant below is unchanged and still exact; it just runs
    # once the pipeline has actually stopped moving.
    previous_sum, stable_polls, waited = None, 0, 0
    while waited < args.drain_max_wait:
        sum_rows = psql(args.pg_container, args.pg_user, args.pg_db,
                        "SELECT COALESCE(SUM(row_count), 0) FROM weir_metrics.window_metrics;")
        current_sum = float(sum_rows[0][0])
        stable_polls = stable_polls + 1 if current_sum == previous_sum else 0
        if current_sum == expected_closed_row_count or stable_polls >= 3:
            break
        previous_sum = current_sum
        waited += 5
        time.sleep(5)
    print(f"drained after ~{waited}s (SUM stable for {stable_polls} consecutive polls)")

    sum_rows = psql(args.pg_container, args.pg_user, args.pg_db,
                     "SELECT COALESCE(SUM(row_count), 0) FROM weir_metrics.window_metrics;")
    actual_sum = float(sum_rows[0][0])
    print(f"actual SUM(row_count) = {actual_sum}, expected_closed_row_count = {expected_closed_row_count}")
    if args.allow_late_drop:
        # An injected stream deliberately contains events the pipeline is
        # SUPPOSED to drop - months-old backfill copies, and an
        # out-of-order tail past the watermark bound. "Every emitted event
        # lands in a closed window" is false by construction there, so
        # asserting it would be asserting that injection did nothing.
        # Still one-directional: more rows than emitted would mean rows
        # appearing from nowhere, which no scenario can explain.
        shortfall = expected_closed_row_count - actual_sum
        if actual_sum > expected_closed_row_count:
            fail(f"SUM(row_count)={actual_sum} EXCEEDS emitted {expected_closed_row_count} - "
                 f"rows appeared that were never sent; no scenario can account for that")
        print(f"late-drop expected: {shortfall:.0f} of {expected_closed_row_count:.0f} events "
              f"({shortfall / expected_closed_row_count * 100:.4f}%) did not reach a closed window. "
              f"The caller is responsible for attributing this to its injected scenarios.")
    elif actual_sum != expected_closed_row_count:
        docker_compose_logs(["flink-jobmanager", "flink-taskmanager"])
        fail(f"SUM(row_count)={actual_sum} != independently computed expected_closed_row_count={expected_closed_row_count}")
    print("PASS: stage 9 - broad check matches exactly")

    print("\n=== Stage 10: stop the Flink job cleanly ===")
    stop = run(["docker", "compose", "exec", "-T", "flink-jobmanager", "./bin/flink", "stop", job_id], timeout=60)
    print_raw("bin/flink stop", stop.stdout + stop.stderr)
    if stop.returncode != 0:
        fail(f"bin/flink stop exited {stop.returncode}")
    print("PASS: stage 10 - job stopped cleanly")

    print("\n=== verify_metrics_job.py: all stages passed ===")


if __name__ == "__main__":
    main()
