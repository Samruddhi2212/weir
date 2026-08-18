#!/usr/bin/env bash
# Step 6 exactly-once validation: destructive TaskManager-kill test.
# Separate from scripts/smoke_test.sh on purpose - this script kills a
# running container. Never wired into ci.yml's normal per-push gate; run
# it explicitly (see .github/workflows/exactly-once.yml, workflow_dispatch
# only) and, per the standing instruction after DEFENSE.md #20/#21, never
# trust a single run - run it repeatedly before treating a result as real.
#
# Sequence: start the pipeline (submit the Flink SQL job, start the
# producer), wait for >=3 completed checkpoints, SIGKILL the
# TaskManager mid-write, bring the container back (see DEFENSE.md #27 for
# why that's this script's job and not a docker-compose.yml restart:
# policy), wait for the job to recover, wait for >=3 MORE completed
# checkpoints, stop the producer and the job cleanly.
#
# Every assertion below compares an exact parsed value (a JSON field via
# a Python one-liner, or a validated regex-extracted field) - never a
# grep against mixed stdout/stderr. Audited specifically against the
# class of bug in DEFENSE.md #14 (`grep -qE '\b2\b'` matching a jar
# version string, not the row count it claimed to check) before writing
# any of these; see the header comment for verify_exactly_once.py, which
# was audited the same way.
#
# Every external command is wrapped in an explicit `timeout`, and every
# timeout (exit 124) dumps the relevant container logs before failing -
# see dump_logs_and_fail() below. Raw output is printed unconditionally,
# on success as well as failure - not just on failure the way
# smoke_test.sh does it - because not printing on success is exactly why
# this project couldn't retroactively check whether smoke test step 4
# ever really passed before this session's fixes (DEFENSE.md #18).
#
# Preconditions: `make up` (or `docker compose up -d --build`) already
# run, same as smoke_test.sh - AND `make seed` already run too. The
# SeaweedFS S3 identity + warehouse bucket setup (DEFENSE.md #24)
# currently lives inline in smoke_test.sh, not as a shared/importable
# step; this script deliberately doesn't duplicate that logic a second
# time, and requires `make seed` as an explicit precondition instead.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

FLINK_UI_PORT="${FLINK_JOBMANAGER_UI_PORT:-8081}"
KAFKA_PORT="${KAFKA_PORT:-9092}"
WEIR_EOS_TOPIC="${WEIR_EOS_TOPIC:-weir-eos-events}"
WEIR_EOS_CHECKPOINT_INTERVAL="${WEIR_EOS_CHECKPOINT_INTERVAL:-10s}"
WEIR_EOS_EVENTS_PER_SEC="${WEIR_EOS_EVENTS_PER_SEC:-100}"
WEIR_S3_ACCESS_KEY="${WEIR_S3_ACCESS_KEY:-admin}"
WEIR_S3_SECRET_KEY="${WEIR_S3_SECRET_KEY:-password123}"

EMISSION_LOG="$(pwd)/artifacts/eos_emission_log.jsonl"
mkdir -p "$(dirname "$EMISSION_LOG")"

PYTHON_BIN="$(command -v python3 || command -v python || true)"
if [ -z "$PYTHON_BIN" ]; then
  echo "FAIL: neither python3 nor python found on PATH"
  exit 1
fi

PRODUCER_PID=""
JOB_ID=""

# ------------------------------------------------------------
# Cleanup: runs on ANY exit (success, fail(), or an uncaught error) so a
# failed run never leaves an orphaned producer process or a still-running
# Flink job behind for the next run to trip over.
# ------------------------------------------------------------
cleanup() {
  if [ -n "$PRODUCER_PID" ] && kill -0 "$PRODUCER_PID" 2>/dev/null; then
    echo "cleanup: stopping producer (pid $PRODUCER_PID)"
    kill -TERM "$PRODUCER_PID" 2>/dev/null || true
    sleep 2
    kill -0 "$PRODUCER_PID" 2>/dev/null && kill -KILL "$PRODUCER_PID" 2>/dev/null || true
  fi
  if [ -n "$JOB_ID" ]; then
    echo "cleanup: best-effort cancel of job $JOB_ID (ignored if already stopped)"
    docker compose exec -T flink-jobmanager ./bin/flink cancel "$JOB_ID" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

fail() {
  echo ""
  echo "FAIL: $1"
  exit 1
}

dump_logs_and_fail() {
  local msg="$1"
  echo ""
  echo "--- container logs (flink-jobmanager, flink-taskmanager), tail 200 ---"
  docker compose logs --tail=200 flink-jobmanager flink-taskmanager 2>&1
  echo "--- end container logs ---"
  fail "$msg"
}

make_readable_tmp() {
  local f
  f="$(mktemp)"
  chmod 644 "$f"
  echo "$f"
}

# Usage: extract_json '<json string>' "<python expression using d>"
# Exits nonzero (via the python process) if the JSON itself doesn't parse
# or the expression raises - never returns a value silently invented from
# malformed input.
extract_json() {
  local json="$1" expr="$2"
  printf '%s' "$json" | "$PYTHON_BIN" -c "
import json, sys
try:
    d = json.load(sys.stdin)
    print($expr)
except Exception as e:
    print('WEIR_JSON_ERROR: ' + str(e), file=sys.stderr)
    sys.exit(1)
"
}

print_raw() {
  local label="$1" content="$2"
  echo "--- raw output: $label ---"
  echo "$content"
  echo "--- end raw output: $label ---"
}

# ------------------------------------------------------------
# Stage 1: every needed service healthy
# ------------------------------------------------------------
echo "=== Stage 1: services healthy ==="
EXPECTED_SERVICES="weir-kafka weir-seaweedfs weir-iceberg-rest weir-flink-jobmanager weir-flink-taskmanager"
WAITED=0
MAX_WAIT=180
while true; do
  PS_OUTPUT="$(docker compose ps)"
  all_healthy=1
  for svc in $EXPECTED_SERVICES; do
    line="$(echo "$PS_OUTPUT" | grep "^$svc[[:space:]]" || true)"
    if [ -z "$line" ] || ! echo "$line" | grep -q "(healthy)"; then
      all_healthy=0
      break
    fi
  done
  [ "$all_healthy" -eq 1 ] && break
  WAITED=$((WAITED + 5))
  if [ "$WAITED" -ge "$MAX_WAIT" ]; then
    print_raw "docker compose ps" "$PS_OUTPUT"
    dump_logs_and_fail "stage 1: not all of [$EXPECTED_SERVICES] healthy after ${MAX_WAIT}s"
  fi
  sleep 5
done
print_raw "docker compose ps" "$PS_OUTPUT"
echo "PASS: stage 1 - services healthy"

# ------------------------------------------------------------
# Stage 2: fresh, single-partition topic
# ------------------------------------------------------------
echo ""
echo "=== Stage 2: fresh topic $WEIR_EOS_TOPIC ==="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic "$WEIR_EOS_TOPIC" >/dev/null 2>&1 || true
CREATE_TOPIC_OUTPUT="$(docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic "$WEIR_EOS_TOPIC" --partitions 1 --replication-factor 1 2>&1)"
CREATE_TOPIC_EXIT=$?
print_raw "kafka-topics.sh --create" "$CREATE_TOPIC_OUTPUT"
[ "$CREATE_TOPIC_EXIT" -eq 0 ] || fail "stage 2: could not create topic $WEIR_EOS_TOPIC"
echo "PASS: stage 2 - topic created"

# ------------------------------------------------------------
# Stage 3: submit the Flink SQL job (detached)
# ------------------------------------------------------------
echo ""
echo "=== Stage 3: submit exactly-once job ==="
JOB_SQL_RESOLVED="$(make_readable_tmp)"
sed \
  -e "s|__WEIR_S3_ACCESS_KEY__|${WEIR_S3_ACCESS_KEY}|" \
  -e "s|__WEIR_S3_SECRET_KEY__|${WEIR_S3_SECRET_KEY}|" \
  -e "s|__WEIR_EOS_CHECKPOINT_INTERVAL__|${WEIR_EOS_CHECKPOINT_INTERVAL}|" \
  -e "s|__WEIR_EOS_TOPIC__|${WEIR_EOS_TOPIC}|" \
  scripts/sql/exactly_once_job.sql > "$JOB_SQL_RESOLVED"

docker compose cp "$JOB_SQL_RESOLVED" flink-jobmanager:/tmp/weir_exactly_once_job_resolved.sql \
  || fail "stage 3: could not copy resolved SQL script into flink-jobmanager"
rm -f "$JOB_SQL_RESOLVED"

SUBMIT_OUTPUT="$(timeout 90 docker compose exec -T flink-jobmanager ./bin/sql-client.sh -f /tmp/weir_exactly_once_job_resolved.sql 2>&1)"
SUBMIT_EXIT=$?
print_raw "sql-client.sh (job submission)" "$SUBMIT_OUTPUT"

if [ "$SUBMIT_EXIT" -eq 124 ]; then
  dump_logs_and_fail "stage 3: sql-client timed out after 90s submitting the job"
fi
[ "$SUBMIT_EXIT" -eq 0 ] || dump_logs_and_fail "stage 3: sql-client exited $SUBMIT_EXIT submitting the job"

# Flink job IDs are 32 lowercase hex characters - validated exactly, not
# just "found something that looked like an ID" (DEFENSE.md #14's lesson
# applied here too).
JOB_ID_CANDIDATE="$(echo "$SUBMIT_OUTPUT" | grep -oE 'Job ID: [0-9a-f]+' | head -1 | sed -E 's/Job ID: //')"
if ! echo "$JOB_ID_CANDIDATE" | grep -qE '^[0-9a-f]{32}$'; then
  fail "stage 3: could not extract a valid 32-char hex job ID from submission output (got: '$JOB_ID_CANDIDATE')"
fi
JOB_ID="$JOB_ID_CANDIDATE"
echo "PASS: stage 3 - job submitted, Job ID = $JOB_ID"

# ------------------------------------------------------------
# Stage 4: start the producer
# ------------------------------------------------------------
echo ""
echo "=== Stage 4: start producer (~${WEIR_EOS_EVENTS_PER_SEC}/sec) ==="
"$PYTHON_BIN" scripts/produce_events.py \
  --bootstrap-server "localhost:${KAFKA_PORT}" \
  --topic "$WEIR_EOS_TOPIC" \
  --emission-log "$EMISSION_LOG" \
  --rate "$WEIR_EOS_EVENTS_PER_SEC" \
  >artifacts/eos_producer.log 2>&1 &
PRODUCER_PID=$!
sleep 3
if ! kill -0 "$PRODUCER_PID" 2>/dev/null; then
  print_raw "producer log" "$(cat artifacts/eos_producer.log 2>/dev/null || true)"
  fail "stage 4: producer process (pid $PRODUCER_PID) died within 3s of starting"
fi
echo "PASS: stage 4 - producer running (pid $PRODUCER_PID)"

# ------------------------------------------------------------
# Stage 5: wait for >=3 completed checkpoints (baseline, pre-kill)
# ------------------------------------------------------------
wait_for_checkpoints() {
  local min_count="$1" budget="$2" waited=0 cp_json completed
  while true; do
    cp_json="$(curl -s --max-time 10 "http://localhost:${FLINK_UI_PORT}/jobs/${JOB_ID}/checkpoints")"
    completed="$(extract_json "$cp_json" "d['counts']['completed']")" || completed=""
    if [ -n "$completed" ] && [ "$completed" -ge "$min_count" ] 2>/dev/null; then
      echo "$completed"
      return 0
    fi
    waited=$((waited + 5))
    if [ "$waited" -ge "$budget" ]; then
      print_raw "final checkpoints JSON" "$cp_json"
      return 1
    fi
    sleep 5
  done
}

echo ""
echo "=== Stage 5: wait for >=3 completed checkpoints (baseline) ==="
BASELINE_CHECKPOINTS="$(wait_for_checkpoints 3 180)"
if [ $? -ne 0 ] || [ -z "$BASELINE_CHECKPOINTS" ]; then
  dump_logs_and_fail "stage 5: did not reach 3 completed checkpoints within 180s"
fi
echo "PASS: stage 5 - $BASELINE_CHECKPOINTS completed checkpoints observed (baseline)"

# ------------------------------------------------------------
# Stage 6: SIGKILL the TaskManager mid-write
# ------------------------------------------------------------
echo ""
echo "=== Stage 6: SIGKILL flink-taskmanager ==="
KILL_OUTPUT="$(docker compose kill -s SIGKILL flink-taskmanager 2>&1)"
KILL_EXIT=$?
print_raw "docker compose kill" "$KILL_OUTPUT"
[ "$KILL_EXIT" -eq 0 ] || fail "stage 6: docker compose kill exited $KILL_EXIT"
echo "PASS: stage 6 - flink-taskmanager killed"

# ------------------------------------------------------------
# Stage 7: bring the container back (see DEFENSE.md #27 - no restart:
# policy on this service, by design; this script does it explicitly)
# ------------------------------------------------------------
echo ""
echo "=== Stage 7: restart flink-taskmanager container ==="
START_OUTPUT="$(docker compose start flink-taskmanager 2>&1)"
START_EXIT=$?
print_raw "docker compose start" "$START_OUTPUT"
[ "$START_EXIT" -eq 0 ] || fail "stage 7: docker compose start exited $START_EXIT"

TM_WAITED=0
TM_MAX_WAIT=120
while true; do
  PS_OUTPUT="$(docker compose ps)"
  line="$(echo "$PS_OUTPUT" | grep "^weir-flink-taskmanager[[:space:]]" || true)"
  if [ -n "$line" ] && echo "$line" | grep -q "(healthy)"; then
    break
  fi
  TM_WAITED=$((TM_WAITED + 5))
  if [ "$TM_WAITED" -ge "$TM_MAX_WAIT" ]; then
    print_raw "docker compose ps" "$PS_OUTPUT"
    dump_logs_and_fail "stage 7: flink-taskmanager not healthy again after ${TM_MAX_WAIT}s"
  fi
  sleep 5
done
print_raw "docker compose ps" "$PS_OUTPUT"
echo "PASS: stage 7 - flink-taskmanager container healthy again (took ~${TM_WAITED}s)"

# ------------------------------------------------------------
# Stage 8: wait for the job to recover (RUNNING again)
# ------------------------------------------------------------
echo ""
echo "=== Stage 8: wait for job recovery ==="
JOB_WAITED=0
JOB_MAX_WAIT=300
while true; do
  JOB_JSON="$(curl -s --max-time 10 "http://localhost:${FLINK_UI_PORT}/jobs/${JOB_ID}")"
  STATE="$(extract_json "$JOB_JSON" "d['state']")" || STATE=""
  if [ "$STATE" = "RUNNING" ]; then
    break
  fi
  if [ "$STATE" = "FAILED" ] || [ "$STATE" = "CANCELED" ]; then
    print_raw "job status JSON" "$JOB_JSON"
    dump_logs_and_fail "stage 8: job entered terminal state '$STATE' instead of recovering - this is itself a finding, reported, not retried"
  fi
  JOB_WAITED=$((JOB_WAITED + 5))
  if [ "$JOB_WAITED" -ge "$JOB_MAX_WAIT" ]; then
    print_raw "last job status JSON" "$JOB_JSON"
    dump_logs_and_fail "stage 8: job did not return to RUNNING within ${JOB_MAX_WAIT}s (last observed state: '$STATE')"
  fi
  sleep 5
done
print_raw "job status JSON" "$JOB_JSON"
echo "PASS: stage 8 - job state is RUNNING again (took ~${JOB_WAITED}s)"

# ------------------------------------------------------------
# Stage 9: wait for >=3 MORE completed checkpoints after recovery
# ------------------------------------------------------------
echo ""
echo "=== Stage 9: wait for >=3 completed checkpoints after recovery ==="
POST_TARGET=$((BASELINE_CHECKPOINTS + 3))
POST_CHECKPOINTS="$(wait_for_checkpoints "$POST_TARGET" 180)"
if [ $? -ne 0 ] || [ -z "$POST_CHECKPOINTS" ]; then
  dump_logs_and_fail "stage 9: did not reach $POST_TARGET completed checkpoints within 180s of recovery"
fi
echo "PASS: stage 9 - $POST_CHECKPOINTS completed checkpoints observed (target was >=$POST_TARGET)"

# ------------------------------------------------------------
# Stage 10: stop the producer cleanly
# ------------------------------------------------------------
echo ""
echo "=== Stage 10: stop producer cleanly ==="
kill -TERM "$PRODUCER_PID"
STOP_WAITED=0
while kill -0 "$PRODUCER_PID" 2>/dev/null; do
  STOP_WAITED=$((STOP_WAITED + 1))
  if [ "$STOP_WAITED" -ge 30 ]; then
    print_raw "producer log" "$(cat artifacts/eos_producer.log 2>/dev/null || true)"
    kill -KILL "$PRODUCER_PID" 2>/dev/null || true
    fail "stage 10: producer did not exit within 30s of SIGTERM - force-killed, treated as a failure, not ignored"
  fi
  sleep 1
done
wait "$PRODUCER_PID" 2>/dev/null
PRODUCER_EXIT=$?
PRODUCER_PID=""
print_raw "producer log" "$(cat artifacts/eos_producer.log 2>/dev/null || true)"
[ "$PRODUCER_EXIT" -eq 0 ] || fail "stage 10: producer exited $PRODUCER_EXIT (delivery failures reported above) - see artifacts/eos_producer.log"
echo "PASS: stage 10 - producer stopped cleanly"

# ------------------------------------------------------------
# Stage 11: stop the Flink job cleanly (graceful stop, with savepoint)
# ------------------------------------------------------------
echo ""
echo "=== Stage 11: stop Flink job cleanly ==="
STOP_JOB_OUTPUT="$(timeout 60 docker compose exec -T flink-jobmanager ./bin/flink stop "$JOB_ID" 2>&1)"
STOP_JOB_EXIT=$?
print_raw "bin/flink stop" "$STOP_JOB_OUTPUT"
if [ "$STOP_JOB_EXIT" -eq 124 ]; then
  dump_logs_and_fail "stage 11: bin/flink stop timed out after 60s"
fi
[ "$STOP_JOB_EXIT" -eq 0 ] || dump_logs_and_fail "stage 11: bin/flink stop exited $STOP_JOB_EXIT"
JOB_ID=""
echo "PASS: stage 11 - job stopped cleanly"

echo ""
echo "=== verify_recovery.sh: all stages passed ==="
echo "emission log: $EMISSION_LOG"
echo "baseline completed checkpoints (pre-kill): $BASELINE_CHECKPOINTS"
echo "final completed checkpoints (post-recovery): $POST_CHECKPOINTS"
