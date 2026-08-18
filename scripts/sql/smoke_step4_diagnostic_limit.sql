-- DIAGNOSTIC ONLY - not part of the gating smoke test, see DEFENSE.md
-- #18. Reproduces the original approach (LIMIT against an unbounded
-- streaming Kafka source, no scan.bounded.mode) under TABLEAU result-mode
-- to learn whether it merely takes longer to terminate than the 90s the
-- gating check uses, or hangs indefinitely regardless of timeout length.
-- Its outcome is reported but does not fail the smoke test - the real,
-- gating query is scripts/sql/smoke_step4.sql, using scan.bounded.mode
-- instead of LIMIT.

SET 'sql-client.execution.result-mode' = 'TABLEAU';

CREATE TABLE IF NOT EXISTS smoke_kafka_source_diag (
  message STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'weir-smoke-test',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'weir-smoke-consumer-diag',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'raw'
);

SELECT CONCAT('WEIR_MSG=', message) AS tagged_message FROM smoke_kafka_source_diag LIMIT 5;
