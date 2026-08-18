-- Smoke test step 4: trivial Flink job reads the Kafka topic, no Iceberg.
-- Where a Kafka-connector/Flink version mismatch is expected to surface -
-- see docker/flink/Dockerfile's known-risk note.
--
-- Topic and group id are plain literals, not templated: this file is
-- static (see DEFENSE.md #10) and must match TEST_TOPIC/KAFKA_GROUP in
-- scripts/smoke_test.sh if either ever changes.
--
-- result-mode is required, not optional, when running a SELECT via
-- `sql-client.sh -f` (non-interactive) - without it, Flink throws
-- SqlExecutionException: "In non-interactive mode, it only supports to
-- use TABLEAU as value of sql-client.execution.result-mode". Confirmed
-- by hitting this directly in CI (see DEFENSE.md #16).
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- scan.bounded.mode=specific-offsets, not LIMIT against an unbounded
-- streaming source: relying on LIMIT to terminate a continuous Kafka
-- read hung indefinitely under TABLEAU result-mode (see DEFENSE.md #18).
-- specific-offsets was chosen over the more common latest-offset bounded
-- mode specifically to avoid a known, confirmed open bug (FLINK-34470:
-- transactional-producer control records can make latest-offset's
-- stopping-offset calculation hang indefinitely) - our producer isn't
-- transactional, but specific-offsets has no such dynamic negotiation at
-- all, so there's nothing to verify there instead of assuming safety.
-- offset:10 means "stop before offset 10", i.e. read offsets 0-9 - all
-- 10 messages step 3 actually produced.
CREATE TABLE IF NOT EXISTS smoke_kafka_source (
  message STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'weir-smoke-test',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'weir-smoke-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'scan.bounded.mode' = 'specific-offsets',
  'scan.bounded.specific-offsets' = 'partition:0,offset:10',
  'format' = 'raw'
);

-- Sentinel-prefixed, not a bare SELECT *: smoke_test.sh greps for the
-- exact literal 'WEIR_MSG=' prefix and counts matches. A bare value here
-- (e.g. just the digit) would risk matching unrelated digits elsewhere in
-- sql-client's own output (jar version strings like "...-2.1.0.jar"
-- contain bare digits too) - see DEFENSE.md #14 for why this matters.
-- No LIMIT - the bounded source above already stops after all 10 messages.
SELECT CONCAT('WEIR_MSG=', message) AS tagged_message FROM smoke_kafka_source;
