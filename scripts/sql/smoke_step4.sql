-- Smoke test step 4: trivial Flink job reads the Kafka topic, no Iceberg.
-- Where a Kafka-connector/Flink version mismatch is expected to surface -
-- see docker/flink/Dockerfile's known-risk note.
--
-- Topic and group id are plain literals, not templated: this file is
-- static (see DEFENSE.md #10) and must match TEST_TOPIC/KAFKA_GROUP in
-- scripts/smoke_test.sh if either ever changes.

CREATE TABLE IF NOT EXISTS smoke_kafka_source (
  message STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'weir-smoke-test',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'weir-smoke-consumer',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

-- Sentinel-prefixed, not a bare SELECT *: smoke_test.sh greps for the
-- exact literal 'WEIR_MSG=' prefix and counts matches. A bare value here
-- (e.g. just the digit) would risk matching unrelated digits elsewhere in
-- sql-client's own output (jar version strings like "...-2.1.0.jar"
-- contain bare digits too) - see DEFENSE.md #14 for why this matters.
SELECT CONCAT('WEIR_MSG=', message) AS tagged_message FROM smoke_kafka_source LIMIT 5;
