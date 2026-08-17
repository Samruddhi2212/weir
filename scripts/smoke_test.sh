#!/usr/bin/env bash
# Smoke test covering steps 2-5 of the verification sequence:
#   2. docker compose ps - every service healthy, not merely running
#   3. Kafka round-trip - produce 10, consume 10, no Flink involved
#   4. Trivial Flink job - reads the Kafka topic, prints to stdout, no
#      Iceberg. Where a Kafka-connector/Flink version mismatch is expected
#      to surface (NoClassDefFoundError/NoSuchMethodError at runtime).
#   5. Write to Iceberg via the single catalog config, verify rows landed.
#
# Step 1 (make up --build) is a precondition - run it first. Step 6 (kill
# the TaskManager, exactly-once verification) is deliberately NOT here -
# destructive, lives in its own script.
#
# Verified against real Docker via CI, not just written to spec: all four
# steps pass as of this writing (four fixes were needed to get here - see
# DEFENSE.md #10-13). Every failure message below still dumps full
# output, since any of this can still break in ways not yet seen on a
# future change.
#
# SeaweedFS is started in docker-compose.yml with no -s3.config identity
# file, so it has no credentials of its own configured to check against.
# Step 5's WEIR_S3_ACCESS_KEY/WEIR_S3_SECRET_KEY (see .env.example) were
# accepted by SeaweedFS in CI with their local-dev defaults - confirmed,
# not assumed. That's SeaweedFS's default-permissive behavior when no
# identity file is configured, not something this smoke test enforces.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

# KAFKA_GROUP/ICEBERG_CATALOG/ICEBERG_DB/ICEBERG_TABLE were removed from
# here - they only ever fed the step 4/5 SQL heredocs, which are now
# committed static files in scripts/sql/ instead (see DEFENSE.md #10).
# Those files hardcode the matching literal values directly.
TEST_TOPIC="weir-smoke-test"

fail() {
  echo ""
  echo "FAIL: $1"
  exit 1
}

# The Flink image runs as non-root USER flink (see docker/flink/
# Dockerfile). A file created by mktemp defaults to mode 600 (owner-only),
# and `docker compose cp` preserves that mode bit-for-bit into the
# container - the flink user then can't read it, regardless of who ends
# up owning it after the copy. See DEFENSE.md #10. Any script-generated
# file that needs to cross the host->container boundary into this image
# should be created via this helper, not raw mktemp.
#
# Currently unused by steps 2-5 below: step 4/5's SQL turned out to be
# static, not templated, so it moved to committed files in scripts/sql/,
# bind-mounted read-only, instead of being generated at runtime at all.
# Kept for whatever gets added later that does need a runtime-generated
# file (e.g. step 6's exactly-once verification script).
make_readable_tmp() {
  local f
  f="$(mktemp)"
  chmod 644 "$f"
  echo "$f"
}

# ------------------------------------------------------------
# Step 2: every service healthy, not merely running
# ------------------------------------------------------------
echo "=== Step 2: docker compose ps - every service healthy ==="

EXPECTED_SERVICES="weir-kafka weir-postgres weir-seaweedfs weir-flink-jobmanager weir-flink-taskmanager"
MAX_WAIT_SECONDS=180
WAITED=0

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
  if [ "$WAITED" -ge "$MAX_WAIT_SECONDS" ]; then
    fail "step 2: not all of [$EXPECTED_SERVICES] healthy after ${MAX_WAIT_SECONDS}s. Full 'docker compose ps':
$PS_OUTPUT"
  fi
  sleep 5
done
echo "PASS: step 2 - all 5 services healthy"

# ------------------------------------------------------------
# Step 3: Kafka round-trip, no Flink involved
# ------------------------------------------------------------
echo ""
echo "=== Step 3: Kafka round-trip (produce 10, consume 10) ==="

# Bare script names are not resolvable here - /opt/kafka/bin is not on
# PATH in apache/kafka:4.1.0 (confirmed via CI debug job: `which
# kafka-broker-api-versions.sh` returned exit=1 even though the script
# exists and works fine by absolute path). Same fix as the kafka
# healthcheck in docker-compose.yml.
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic "$TEST_TOPIC" >/dev/null 2>&1 || true

docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic "$TEST_TOPIC" --partitions 1 --replication-factor 1 \
  || fail "step 3: could not create topic $TEST_TOPIC"

seq 1 10 | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 --topic "$TEST_TOPIC" \
  || fail "step 3: producer failed"

RECEIVED="$(docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic "$TEST_TOPIC" \
  --from-beginning --max-messages 10 --timeout-ms 15000 2>/dev/null \
  | wc -l | tr -d '[:space:]')"

[ "$RECEIVED" = "10" ] || fail "step 3: expected 10 messages, got $RECEIVED"
echo "PASS: step 3 - Kafka round-trip (10/10 messages)"

# ------------------------------------------------------------
# Step 4: trivial Flink job reads Kafka, no Iceberg
# ------------------------------------------------------------
echo ""
echo "=== Step 4: Flink reads the Kafka topic, no Iceberg ==="
echo "(this is where a Kafka-connector/Flink version mismatch is expected"
echo " to surface - see docker/flink/Dockerfile's known-risk note)"

# SQL is static (see DEFENSE.md #10) - committed at scripts/sql/
# smoke_step4.sql, bind-mounted read-only at /opt/weir/sql. No temp file,
# no docker compose cp, no host->container permissions to get wrong.
STEP4_OUTPUT="$(docker compose exec -T flink-jobmanager ./bin/sql-client.sh -f /opt/weir/sql/smoke_step4.sql 2>&1)"
STEP4_EXIT=$?

if echo "$STEP4_OUTPUT" | grep -qE 'NoClassDefFoundError|NoSuchMethodError|ClassNotFoundException|Could not find any factory'; then
  fail "step 4: Kafka connector / Flink classpath error. Full output:
$STEP4_OUTPUT"
fi
[ "$STEP4_EXIT" -eq 0 ] || fail "step 4: sql-client exited $STEP4_EXIT. Full output:
$STEP4_OUTPUT"

# Exact count of the sentinel marker, not "any output at all" (that was
# nearly tautological - sql-client prints substantial banner/log output
# whether or not the query actually returned anything). LIMIT 5 means
# exactly 5 is the only correct answer; 0 means the query errored or
# returned nothing without tripping the exit-code/error-pattern checks
# above, and must fail here instead of passing silently (see DEFENSE.md
# #14).
STEP4_MATCHED="$(echo "$STEP4_OUTPUT" | grep -oE 'WEIR_MSG=[0-9]+' | wc -l | tr -d '[:space:]')"
[ "$STEP4_MATCHED" = "5" ] || fail "step 4: expected 5 tagged messages, got $STEP4_MATCHED. Full output:
$STEP4_OUTPUT"
echo "PASS: step 4 - Flink read from Kafka, no classpath errors"

# ------------------------------------------------------------
# Step 5: write to Iceberg via the single catalog config
# ------------------------------------------------------------
echo ""
echo "=== Step 5: write to Iceberg, verify rows landed ==="
echo "(SeaweedFS has no configured S3 credentials - see script header)"

# scripts/sql/smoke_step5.sql is static and committed (DEFENSE.md #10),
# containing no credential values at all - only the sentinel tokens
# __WEIR_S3_ACCESS_KEY__/__WEIR_S3_SECRET_KEY__. It's bind-mounted
# read-only, so the substituted copy can't be written back to that path;
# it goes to a make_readable_tmp() file instead (world-readable, so the
# non-root flink user can read it after docker compose cp - see
# DEFENSE.md #10/#11) and gets copied in fresh each run.
WEIR_S3_ACCESS_KEY="${WEIR_S3_ACCESS_KEY:-admin}"
WEIR_S3_SECRET_KEY="${WEIR_S3_SECRET_KEY:-***REMOVED***}"

STEP5_SQL_RESOLVED="$(make_readable_tmp)"
# | as sed delimiter, not /, since s3a:// paths already use / - a
# credential value containing | would still break this, but these are
# local-dev defaults, not expected to contain shell/sed metacharacters.
sed \
  -e "s|__WEIR_S3_ACCESS_KEY__|${WEIR_S3_ACCESS_KEY}|" \
  -e "s|__WEIR_S3_SECRET_KEY__|${WEIR_S3_SECRET_KEY}|" \
  scripts/sql/smoke_step5.sql > "$STEP5_SQL_RESOLVED"

docker compose cp "$STEP5_SQL_RESOLVED" flink-jobmanager:/tmp/weir_smoke_step5_resolved.sql \
  || fail "step 5: could not copy resolved SQL script into flink-jobmanager"
rm -f "$STEP5_SQL_RESOLVED"

STEP5_OUTPUT="$(docker compose exec -T flink-jobmanager ./bin/sql-client.sh -f /tmp/weir_smoke_step5_resolved.sql 2>&1)"
STEP5_EXIT=$?

# This list is a diagnostic aid, not the actual safety net - the
# exit-code check below is what genuinely catches failures (confirmed:
# an "Unknown catalog-type" DDL error was caught by the exit-code check
# alone, this list didn't recognize it and had to be extended after the
# fact - see DEFENSE.md #14). Kept and extended anyway for a clearer
# failure message than a bare nonzero exit code would give.
if echo "$STEP5_OUTPUT" | grep -qE 'AccessDenied|403 Forbidden|Connection refused|ClassNotFoundException|NoClassDefFoundError|SQLException|UnsupportedOperationException'; then
  fail "step 5: catalog/S3/JDBC error writing to Iceberg. Full output:
$STEP5_OUTPUT"
fi
[ "$STEP5_EXIT" -eq 0 ] || fail "step 5: sql-client exited $STEP5_EXIT. Full output:
$STEP5_OUTPUT"

# Exact comparison against a parsed row count, not "does '2' appear
# anywhere" - that previously matched bare digits inside jar version
# strings like "flink-table-api-java-uber-2.1.0.jar", which appear in
# essentially any Flink stack trace, error or not (see DEFENSE.md #14).
# An errored/empty query must fail here, not pass because some unrelated
# "2" happened to be in the output.
STEP5_PARSED_COUNT="$(echo "$STEP5_OUTPUT" | grep -oE 'WEIR_ROW_COUNT=[0-9]+' | head -1 | sed -E 's/WEIR_ROW_COUNT=//')"
if [ -z "$STEP5_PARSED_COUNT" ]; then
  fail "step 5: query produced no WEIR_ROW_COUNT marker at all - an
errored or empty query must fail, not pass silently. Full output:
$STEP5_OUTPUT"
fi
if [ "$STEP5_PARSED_COUNT" != "2" ]; then
  fail "step 5: expected row count 2, got '$STEP5_PARSED_COUNT'. Full output:
$STEP5_OUTPUT"
fi
echo "PASS: step 5 - wrote 2 rows to Iceberg, count verified ($STEP5_PARSED_COUNT)"

echo ""
echo "=== Steps 2-5 all passed ==="
