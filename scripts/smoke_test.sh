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
# NEVER RUN AGAINST REAL DOCKER: written to spec, not runtime-verified -
# no Docker available in the environment this was written in. Expect to
# need fixes on first real run; every failure message below dumps full
# output specifically so that fixing is possible without re-deriving
# what happened.
#
# Known unverified risk (step 5): SeaweedFS is started in docker-
# compose.yml with no -s3.config identity file, so it has no configured
# access key/secret. The credentials below are placeholders. If step 5
# fails on auth, that's a real gap this smoke test found, not a bug in
# the smoke test itself.

set -uo pipefail
cd "$(git rev-parse --show-toplevel)"

TEST_TOPIC="weir-smoke-test"
KAFKA_GROUP="weir-smoke-consumer"
ICEBERG_CATALOG="weir_smoke_catalog"
ICEBERG_DB="smoke_test"
ICEBERG_TABLE="smoke_iceberg_table"

fail() {
  echo ""
  echo "FAIL: $1"
  exit 1
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

STEP4_SQL="$(mktemp)"
cat > "$STEP4_SQL" <<SQL
CREATE TABLE IF NOT EXISTS smoke_kafka_source (
  message STRING
) WITH (
  'connector' = 'kafka',
  'topic' = '${TEST_TOPIC}',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = '${KAFKA_GROUP}',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

SELECT * FROM smoke_kafka_source LIMIT 5;
SQL

docker compose cp "$STEP4_SQL" flink-jobmanager:/tmp/weir_smoke_step4.sql \
  || fail "step 4: could not copy SQL script into flink-jobmanager"
rm -f "$STEP4_SQL"

STEP4_OUTPUT="$(docker compose exec -T flink-jobmanager ./bin/sql-client.sh -f /tmp/weir_smoke_step4.sql 2>&1)"
STEP4_EXIT=$?

if echo "$STEP4_OUTPUT" | grep -qE 'NoClassDefFoundError|NoSuchMethodError|ClassNotFoundException|Could not find any factory'; then
  fail "step 4: Kafka connector / Flink classpath error. Full output:
$STEP4_OUTPUT"
fi
[ "$STEP4_EXIT" -eq 0 ] || fail "step 4: sql-client exited $STEP4_EXIT. Full output:
$STEP4_OUTPUT"
[ -n "$STEP4_OUTPUT" ] || fail "step 4: sql-client produced no output at all"
echo "PASS: step 4 - Flink read from Kafka, no classpath errors"

# ------------------------------------------------------------
# Step 5: write to Iceberg via the single catalog config
# ------------------------------------------------------------
echo ""
echo "=== Step 5: write to Iceberg, verify rows landed ==="
echo "(SeaweedFS has no configured S3 credentials - see script header)"

STEP5_SQL="$(mktemp)"
cat > "$STEP5_SQL" <<SQL
SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS ${ICEBERG_CATALOG} WITH (
  'type' = 'iceberg',
  'catalog-type' = 'jdbc',
  'uri' = 'jdbc:postgresql://postgres:5432/weir_catalog',
  'jdbc.user' = 'weir',
  'jdbc.password' = 'weir',
  'warehouse' = 's3a://weir-warehouse/warehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true',
  's3.access-key-id' = 'admin',
  's3.secret-access-key' = '***REMOVED***'
);

USE CATALOG ${ICEBERG_CATALOG};
CREATE DATABASE IF NOT EXISTS ${ICEBERG_DB};
USE ${ICEBERG_DB};
DROP TABLE IF EXISTS ${ICEBERG_TABLE};
CREATE TABLE ${ICEBERG_TABLE} (message STRING) WITH ('format-version' = '2');
INSERT INTO ${ICEBERG_TABLE} VALUES ('smoke-row-1'), ('smoke-row-2');
SELECT COUNT(*) AS row_count FROM ${ICEBERG_TABLE};
SQL

docker compose cp "$STEP5_SQL" flink-jobmanager:/tmp/weir_smoke_step5.sql \
  || fail "step 5: could not copy SQL script into flink-jobmanager"
rm -f "$STEP5_SQL"

STEP5_OUTPUT="$(docker compose exec -T flink-jobmanager ./bin/sql-client.sh -f /tmp/weir_smoke_step5.sql 2>&1)"
STEP5_EXIT=$?

if echo "$STEP5_OUTPUT" | grep -qE 'AccessDenied|403 Forbidden|Connection refused|ClassNotFoundException|NoClassDefFoundError|SQLException'; then
  fail "step 5: catalog/S3/JDBC error writing to Iceberg. Full output:
$STEP5_OUTPUT"
fi
[ "$STEP5_EXIT" -eq 0 ] || fail "step 5: sql-client exited $STEP5_EXIT. Full output:
$STEP5_OUTPUT"
if ! echo "$STEP5_OUTPUT" | grep -qE '\b2\b'; then
  fail "step 5: could not confirm row_count=2 in output. Full output:
$STEP5_OUTPUT"
fi
echo "PASS: step 5 - wrote 2 rows to Iceberg, count verified"

echo ""
echo "=== Steps 2-5 all passed ==="
