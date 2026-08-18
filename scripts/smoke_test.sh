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
# file. Earlier revisions of this comment claimed that meant SeaweedFS was
# "default-permissive" and accepted any access key/secret pair - that
# claim was never actually backed by a signed S3 write reaching SeaweedFS;
# it was inferred from an earlier catalog-type=jdbc-era run in which
# catalog creation talked to Postgres directly and never touched S3 at
# all. Confirmed since to be flatly wrong: with no identity configured,
# SeaweedFS rejects every *signed* S3 request outright with `S3Exception:
# Signed request requires setting up SeaweedFS S3 authentication` (see
# DEFENSE.md #24) - the AWS SDK v2 clients both Flink's S3FileIO and
# iceberg-rest use always sign their requests. The setup step below
# configures a real identity via `weed shell`'s `s3.configure`, matching
# WEIR_S3_ACCESS_KEY/WEIR_S3_SECRET_KEY, before step 5 ever runs.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

# KAFKA_GROUP/ICEBERG_CATALOG/ICEBERG_DB/ICEBERG_TABLE were removed from
# here - they only ever fed the step 4/5 SQL heredocs, which are now
# committed static files in scripts/sql/ instead (see DEFENSE.md #10).
# Those files hardcode the matching literal values directly.
TEST_TOPIC="weir-smoke-test"

# Read here (not just in step 5) since the new SeaweedFS setup step below
# also needs them - see DEFENSE.md #24.
WEIR_S3_ACCESS_KEY="${WEIR_S3_ACCESS_KEY:-admin}"
WEIR_S3_SECRET_KEY="${WEIR_S3_SECRET_KEY:-password123}"

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

EXPECTED_SERVICES="weir-kafka weir-postgres weir-seaweedfs weir-iceberg-rest weir-flink-jobmanager weir-flink-taskmanager"
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
# Setup: SeaweedFS S3 identity + warehouse bucket
# ------------------------------------------------------------
# Not one of the numbered verification steps - this is precondition setup,
# same role as `make up` itself, just not possible to fold into the image
# or docker-compose.yml's `command:` cleanly (see DEFENSE.md #24 for why
# that was tried and reverted). Runs every invocation, not just the first:
# `s3.configure -apply` creates-or-updates the identity idempotently, and
# CI never ran `make seed` before this (the warehouse bucket has never
# actually existed in any CI run to date - a second, latent gap this
# closes at the same time as the auth one).
echo ""
echo "=== Setup: configure SeaweedFS S3 identity, create warehouse bucket ==="

docker compose exec -T seaweedfs sh -c \
  "echo 's3.configure -user=weir -access_key=${WEIR_S3_ACCESS_KEY} -secret_key=${WEIR_S3_SECRET_KEY} -actions=Admin,Read,Write,List,Tagging -apply' | weed shell" \
  || fail "setup: could not configure SeaweedFS S3 identity"

# No idempotency guarantee found for s3.bucket.create in SeaweedFS's docs
# or source (unlike s3.configure, which its own source describes as
# create-or-update) - `|| true` so a second run against an already-seeded
# stack doesn't fail here, same lenient treatment as step 3's topic-delete
# line below.
docker compose exec -T seaweedfs sh -c \
  'echo "s3.bucket.create -name weir-warehouse" | weed shell' || true

echo "PASS: setup - SeaweedFS identity configured, warehouse bucket present"

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
#
# Wrapped in `timeout` - a real CI run hung here (or in step 5) for the
# full 20-minute job cap with zero output, cancelled by GitHub Actions
# rather than failing with a message (see DEFENSE.md #17). Without an
# external bound, an unresponsive sql-client has no way to report
# anything at all; this trades "silent 20-minute cancellation" for a
# fast, clear failure message.
STEP4_OUTPUT="$(timeout 90 docker compose exec -T flink-jobmanager ./bin/sql-client.sh -f /opt/weir/sql/smoke_step4.sql 2>&1)"
STEP4_EXIT=$?

if [ "$STEP4_EXIT" -eq 124 ]; then
  fail "step 4: sql-client timed out after 90s (no output captured before
the timeout - see DEFENSE.md #17)."
fi

if echo "$STEP4_OUTPUT" | grep -qE 'NoClassDefFoundError|NoSuchMethodError|ClassNotFoundException|Could not find any factory'; then
  fail "step 4: Kafka connector / Flink classpath error. Full output:
$STEP4_OUTPUT"
fi
[ "$STEP4_EXIT" -eq 0 ] || fail "step 4: sql-client exited $STEP4_EXIT. Full output:
$STEP4_OUTPUT"

# Exact count of the sentinel marker, not "any output at all" (that was
# nearly tautological - sql-client prints substantial banner/log output
# whether or not the query actually returned anything). The source is
# scan.bounded.mode-bounded to exactly offsets 0-9 (see smoke_step4.sql),
# so 10 is the only correct answer; 0 means the query errored or returned
# nothing without tripping the exit-code/error-pattern checks above, and
# must fail here instead of passing silently (see DEFENSE.md #14).
STEP4_MATCHED="$(echo "$STEP4_OUTPUT" | grep -oE 'WEIR_MSG=[0-9]+' | wc -l | tr -d '[:space:]')"
[ "$STEP4_MATCHED" = "10" ] || fail "step 4: expected 10 tagged messages, got $STEP4_MATCHED. Full output:
$STEP4_OUTPUT"
echo "PASS: step 4 - Flink read from Kafka, no classpath errors"

# The step 4 diagnostic (LIMIT-based read, longer timeout) that lived
# here has been removed. It answered its question - the LIMIT approach
# genuinely hangs, confirmed at 180s, not just slow - and removing it
# turned out to matter for a second reason: `timeout` only kills the
# local `docker compose exec` client, not the remote Flink job, so the
# hung diagnostic query likely kept running in the cluster as a zombie
# job after being "killed" locally, right before step 5 submitted its
# own job. See DEFENSE.md #20.

# ------------------------------------------------------------
# Step 5: write to Iceberg via the single catalog config
# ------------------------------------------------------------
echo ""
echo "=== Step 5: write to Iceberg, verify rows landed ==="
echo "(using the SeaweedFS identity configured in the setup step above)"

# scripts/sql/smoke_step5.sql is static and committed (DEFENSE.md #10),
# containing no credential values at all - only the sentinel tokens
# __WEIR_S3_ACCESS_KEY__/__WEIR_S3_SECRET_KEY__. It's bind-mounted
# read-only, so the substituted copy can't be written back to that path;
# it goes to a make_readable_tmp() file instead (world-readable, so the
# non-root flink user can read it after docker compose cp - see
# DEFENSE.md #10/#11) and gets copied in fresh each run.
# WEIR_S3_ACCESS_KEY/WEIR_S3_SECRET_KEY are set near the top of this
# script - the setup step above needs them too, not just this one.

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

STEP5_OUTPUT="$(timeout 90 docker compose exec -T flink-jobmanager ./bin/sql-client.sh -f /tmp/weir_smoke_step5_resolved.sql 2>&1)"
STEP5_EXIT=$?

if [ "$STEP5_EXIT" -eq 124 ]; then
  fail "step 5: sql-client timed out after 90s (no output captured before
the timeout - see DEFENSE.md #17)."
fi

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
