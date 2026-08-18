-- Step 6 exactly-once validation: continuous Kafka -> Iceberg job.
-- Submitted via `sql-client.sh -f`, same mechanism as smoke_step4.sql/
-- smoke_step5.sql, but detached (no `table.dml-sync`) - this job is
-- meant to keep running so scripts/verify_recovery.sh can kill the
-- TaskManager mid-write and watch it recover, not finish and return.
--
-- Written only after DEFENSE.md #19 (checkpoint-barrier/Iceberg-commit
-- explanation) and #25-#28 (producer, orphan-detection, TaskManager
-- kill mechanism, checkpoint-interval override design) were written -
-- per the standing project rule, the explanation comes before the code.
--
-- Credentials and the checkpoint-interval override are sentinel tokens,
-- substituted by scripts/verify_recovery.sh before this file ever
-- reaches the container - this committed file never contains a real or
-- placeholder-literal value for either. Same reasoning as
-- smoke_step5.sql (DEFENSE.md #10/#12/#24).

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

-- Session-scoped, not cluster-wide - never touches WEIR_CHECKPOINT_
-- INTERVAL's 60s default (DEFENSE.md #1) that other jobs still run
-- under. min-pause must move with the interval, or it silently caps the
-- real cadence below what the interval override claims - see DEFENSE.md
-- #28 for why both are set here, not just the first one.
SET 'execution.checkpointing.interval' = '__WEIR_EOS_CHECKPOINT_INTERVAL__';
SET 'execution.checkpointing.min-pause' = '0s';

CREATE CATALOG IF NOT EXISTS weir_eos_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'rest',
  'uri' = 'http://iceberg-rest:8181',
  'warehouse' = 's3a://weir-warehouse/warehouse',
  'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
  's3.endpoint' = 'http://seaweedfs:8333',
  's3.path-style-access' = 'true',
  's3.access-key-id' = '__WEIR_S3_ACCESS_KEY__',
  's3.secret-access-key' = '__WEIR_S3_SECRET_KEY__'
);

USE CATALOG weir_eos_catalog;
CREATE DATABASE IF NOT EXISTS eos_test;
USE eos_test;
DROP TABLE IF EXISTS eos_events;
CREATE TABLE eos_events (
  event_key STRING,
  event_ts BIGINT
) WITH ('format-version' = '2');

CREATE TABLE eos_kafka_source (
  event_key STRING,
  event_ts BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = '__WEIR_EOS_TOPIC__',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'weir-eos-verify',
  'format' = 'json',
  -- Single partition (see DEFENSE.md #28's scope note).
  'scan.startup.mode' = 'earliest-offset',
  -- Continuous partition discovery, not one-time enumeration - added
  -- after this job reached FINISHED on its own within ~2s against a
  -- genuinely empty topic (0 records read, job-type STREAMING, not
  -- crashed or hung - a real voluntary completion). scripts/
  -- verify_recovery.sh was also reordered to start the producer before
  -- this job is ever submitted, removing the empty-topic condition
  -- outright; this setting is a second, complementary line of defense,
  -- not fully proven to be the actual mechanism on its own. See
  -- DEFENSE.md #32 - reported honestly as not fully root-caused yet.
  'scan.topic-partition-discovery.interval' = '10s'
);

-- Detached: no `SET 'table.dml-sync' = 'true';` here, unlike
-- smoke_step5.sql. That job was a one-shot batch INSERT that needed the
-- SELECT after it to wait; this one is an unbounded streaming INSERT
-- that must keep running in the background after this script returns,
-- for scripts/verify_recovery.sh to observe, kill against, and stop
-- explicitly later via `bin/flink stop`.
INSERT INTO eos_events SELECT event_key, event_ts FROM eos_kafka_source;
