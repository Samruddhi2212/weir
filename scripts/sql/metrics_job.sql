-- Part 3.1: continuous Kafka -> windowed-metrics Flink job.
-- Submitted via `sql-client.sh -f`, same mechanism as exactly_once_job.sql
-- - detached (no `table.dml-sync`), meant to keep running so
-- scripts/verify_metrics_job.sh can replay real TLC data through it and
-- poll Postgres for the resulting rows, not finish and return.
--
-- Design (wide-staging-table + Postgres-trigger mechanism, watermark
-- bound, JSON timestamp format, deferred lateness columns) is recorded in
-- DEFENSE.md #42, written before this file, per D1. The metric list
-- itself (what's computed and why) is DEFENSE.md #41.
--
-- `weir_tlc_trips_source` is created under Flink's default catalog,
-- before any Iceberg catalog switch - same ordering reason as
-- exactly_once_job.sql's eos_kafka_source (DEFENSE.md #33): creating a
-- Kafka-connector table while an Iceberg catalog is active silently
-- creates an empty Iceberg table instead of a Kafka source.
--
-- Column names are backtick-quoted throughout to preserve the exact
-- mixed case (`VendorID`, `PULocationID`, `DOLocationID`, `RatecodeID`,
-- `Airport_fee`) that replay_producer.py's JSON payload actually uses -
-- confirmed against a real sample payload, not assumed from reading the
-- producer's code alone (DEFENSE.md #42).

SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'execution.checkpointing.interval' = '__WEIR_METRICS_CHECKPOINT_INTERVAL__';
SET 'execution.checkpointing.min-pause' = '0s';
-- The verify_metrics_job.py test topic has 1 partition; this job's
-- default parallelism is 2 (unset). The unassigned source subtask is
-- idle forever and, by default, its watermark never advances past
-- its initial value - since the merged watermark is the MINIMUM
-- across all parallel source subtasks, that one idle subtask holds
-- every window open forever, even though the other subtask has real
-- data flowing through it and checkpoints keep completing normally.
-- Found the hard way (checkpoints completed steadily for 180s, no
-- window ever fired) - confirmed against Flink's own table-config
-- docs before applying (default is 0ms = disabled).
SET 'table.exec.source.idle-timeout' = '5s';

CREATE TABLE weir_tlc_trips_source (
  `VendorID` INT,
  `tpep_pickup_datetime` TIMESTAMP(3),
  `tpep_dropoff_datetime` TIMESTAMP(3),
  `passenger_count` BIGINT,
  `trip_distance` DOUBLE,
  `RatecodeID` BIGINT,
  `store_and_fwd_flag` STRING,
  `PULocationID` INT,
  `DOLocationID` INT,
  `payment_type` BIGINT,
  `fare_amount` DOUBLE,
  `extra` DOUBLE,
  `mta_tax` DOUBLE,
  `tip_amount` DOUBLE,
  `tolls_amount` DOUBLE,
  `improvement_surcharge` DOUBLE,
  `total_amount` DOUBLE,
  `congestion_surcharge` DOUBLE,
  `Airport_fee` DOUBLE,
  `cbd_congestion_fee` DOUBLE,
  -- DEFENSE.md #40's measured p99 bound (270s), not a fresh number -
  -- the "20/2000 (1.0%) dropped as too-late" cost already quantified
  -- there is this watermark's real, known cost.
  -- SECOND with no explicit precision defaults to SECOND(2) (max 2
  -- digits) in Calcite's interval-literal grammar, which Flink SQL
  -- uses as-is - 270 needs SECOND(3). Found the hard way: the first
  -- real CI run failed with "Interval field value 270 exceeds
  -- precision of SECOND(2) field", confirmed against Calcite's own
  -- SqlIntervalQualifier docs before fixing, not guessed.
  WATERMARK FOR `tpep_pickup_datetime` AS `tpep_pickup_datetime` - INTERVAL '270' SECOND(3)
) WITH (
  'connector' = 'kafka',
  'topic' = '__WEIR_METRICS_TOPIC__',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'weir-metrics-job',
  'format' = 'json',
  -- DEFENSE.md #42: replay_producer.py serializes timestamps via
  -- Python's datetime.isoformat() ("2024-12-31T20:47:55"), the 'T'-
  -- separated form Flink's JSON format calls 'ISO-8601' - confirmed
  -- against Flink's own docs, not assumed; the default ('SQL') expects
  -- a space separator instead and would fail to parse this.
  'json.timestamp-format.standard' = 'ISO-8601',
  'scan.startup.mode' = 'earliest-offset',
  'scan.topic-partition-discovery.interval' = '10s'
);

CREATE TABLE weir_metrics_wide_sink (
  window_start TIMESTAMP(3),
  window_end TIMESTAMP(3),
  row_count BIGINT,
  vendorid_nulls BIGINT,
  tpep_dropoff_datetime_nulls BIGINT,
  passenger_count_nulls BIGINT,
  trip_distance_nulls BIGINT,
  ratecodeid_nulls BIGINT,
  store_and_fwd_flag_nulls BIGINT,
  pulocationid_nulls BIGINT,
  dolocationid_nulls BIGINT,
  payment_type_nulls BIGINT,
  fare_amount_nulls BIGINT,
  extra_nulls BIGINT,
  mta_tax_nulls BIGINT,
  tip_amount_nulls BIGINT,
  tolls_amount_nulls BIGINT,
  improvement_surcharge_nulls BIGINT,
  total_amount_nulls BIGINT,
  congestion_surcharge_nulls BIGINT,
  airport_fee_nulls BIGINT,
  cbd_congestion_fee_nulls BIGINT,
  vendorid_distinct BIGINT,
  ratecodeid_distinct BIGINT,
  payment_type_distinct BIGINT,
  pulocationid_distinct BIGINT,
  dolocationid_distinct BIGINT,
  store_and_fwd_flag_distinct BIGINT,
  fare_amount_negative BIGINT,
  tip_amount_negative BIGINT,
  tolls_amount_negative BIGINT,
  total_amount_negative BIGINT,
  trip_distance_negative BIGINT,
  passenger_count_negative BIGINT,
  trip_distance_min DOUBLE,
  trip_distance_max DOUBLE,
  trip_distance_mean DOUBLE,
  fare_amount_min DOUBLE,
  fare_amount_max DOUBLE,
  fare_amount_mean DOUBLE,
  extra_min DOUBLE,
  extra_max DOUBLE,
  extra_mean DOUBLE,
  mta_tax_min DOUBLE,
  mta_tax_max DOUBLE,
  mta_tax_mean DOUBLE,
  tip_amount_min DOUBLE,
  tip_amount_max DOUBLE,
  tip_amount_mean DOUBLE,
  tolls_amount_min DOUBLE,
  tolls_amount_max DOUBLE,
  tolls_amount_mean DOUBLE,
  improvement_surcharge_min DOUBLE,
  improvement_surcharge_max DOUBLE,
  improvement_surcharge_mean DOUBLE,
  total_amount_min DOUBLE,
  total_amount_max DOUBLE,
  total_amount_mean DOUBLE,
  congestion_surcharge_min DOUBLE,
  congestion_surcharge_max DOUBLE,
  congestion_surcharge_mean DOUBLE,
  airport_fee_min DOUBLE,
  airport_fee_max DOUBLE,
  airport_fee_mean DOUBLE,
  cbd_congestion_fee_min DOUBLE,
  cbd_congestion_fee_max DOUBLE,
  cbd_congestion_fee_mean DOUBLE,
  passenger_count_min DOUBLE,
  passenger_count_max DOUBLE,
  passenger_count_mean DOUBLE,
  trip_duration_negative_count BIGINT,
  trip_duration_zero_count BIGINT,
  PRIMARY KEY (window_start, window_end) NOT ENFORCED
) WITH (
  'connector' = 'jdbc',
  -- A PRIMARY KEY on this DDL is what makes this connector's writes
  -- upsert (INSERT ... ON CONFLICT DO UPDATE), not append-only -
  -- confirmed against Flink's JDBC connector docs, not assumed
  -- (DEFENSE.md #42) - so a job restart re-processing a window from
  -- its last checkpoint overwrites rather than duplicates.
  'url' = 'jdbc:postgresql://postgres:5432/__WEIR_METRICS_PG_DB__',
  'table-name' = 'weir_metrics.window_metrics_wide',
  'username' = '__WEIR_METRICS_PG_USER__',
  'password' = '__WEIR_METRICS_PG_PASSWORD__'  -- pragma: allowlist secret
);

INSERT INTO weir_metrics_wide_sink
SELECT
  window_start,
  window_end,
  COUNT(*) AS row_count,
  COUNT(*) - COUNT(`VendorID`) AS vendorid_nulls,
  COUNT(*) - COUNT(`tpep_dropoff_datetime`) AS tpep_dropoff_datetime_nulls,
  COUNT(*) - COUNT(`passenger_count`) AS passenger_count_nulls,
  COUNT(*) - COUNT(`trip_distance`) AS trip_distance_nulls,
  COUNT(*) - COUNT(`RatecodeID`) AS ratecodeid_nulls,
  COUNT(*) - COUNT(`store_and_fwd_flag`) AS store_and_fwd_flag_nulls,
  COUNT(*) - COUNT(`PULocationID`) AS pulocationid_nulls,
  COUNT(*) - COUNT(`DOLocationID`) AS dolocationid_nulls,
  COUNT(*) - COUNT(`payment_type`) AS payment_type_nulls,
  COUNT(*) - COUNT(`fare_amount`) AS fare_amount_nulls,
  COUNT(*) - COUNT(`extra`) AS extra_nulls,
  COUNT(*) - COUNT(`mta_tax`) AS mta_tax_nulls,
  COUNT(*) - COUNT(`tip_amount`) AS tip_amount_nulls,
  COUNT(*) - COUNT(`tolls_amount`) AS tolls_amount_nulls,
  COUNT(*) - COUNT(`improvement_surcharge`) AS improvement_surcharge_nulls,
  COUNT(*) - COUNT(`total_amount`) AS total_amount_nulls,
  COUNT(*) - COUNT(`congestion_surcharge`) AS congestion_surcharge_nulls,
  COUNT(*) - COUNT(`Airport_fee`) AS airport_fee_nulls,
  COUNT(*) - COUNT(`cbd_congestion_fee`) AS cbd_congestion_fee_nulls,
  COUNT(DISTINCT `VendorID`) AS vendorid_distinct,
  COUNT(DISTINCT `RatecodeID`) AS ratecodeid_distinct,
  COUNT(DISTINCT `payment_type`) AS payment_type_distinct,
  COUNT(DISTINCT `PULocationID`) AS pulocationid_distinct,
  COUNT(DISTINCT `DOLocationID`) AS dolocationid_distinct,
  COUNT(DISTINCT `store_and_fwd_flag`) AS store_and_fwd_flag_distinct,
  SUM(CASE WHEN `fare_amount` < 0 THEN 1 ELSE 0 END) AS fare_amount_negative,
  SUM(CASE WHEN `tip_amount` < 0 THEN 1 ELSE 0 END) AS tip_amount_negative,
  SUM(CASE WHEN `tolls_amount` < 0 THEN 1 ELSE 0 END) AS tolls_amount_negative,
  SUM(CASE WHEN `total_amount` < 0 THEN 1 ELSE 0 END) AS total_amount_negative,
  SUM(CASE WHEN `trip_distance` < 0 THEN 1 ELSE 0 END) AS trip_distance_negative,
  SUM(CASE WHEN `passenger_count` < 0 THEN 1 ELSE 0 END) AS passenger_count_negative,
  MIN(`trip_distance`) AS trip_distance_min,
  MAX(`trip_distance`) AS trip_distance_max,
  AVG(`trip_distance`) AS trip_distance_mean,
  MIN(`fare_amount`) AS fare_amount_min,
  MAX(`fare_amount`) AS fare_amount_max,
  AVG(`fare_amount`) AS fare_amount_mean,
  MIN(`extra`) AS extra_min,
  MAX(`extra`) AS extra_max,
  AVG(`extra`) AS extra_mean,
  MIN(`mta_tax`) AS mta_tax_min,
  MAX(`mta_tax`) AS mta_tax_max,
  AVG(`mta_tax`) AS mta_tax_mean,
  MIN(`tip_amount`) AS tip_amount_min,
  MAX(`tip_amount`) AS tip_amount_max,
  AVG(`tip_amount`) AS tip_amount_mean,
  MIN(`tolls_amount`) AS tolls_amount_min,
  MAX(`tolls_amount`) AS tolls_amount_max,
  AVG(`tolls_amount`) AS tolls_amount_mean,
  MIN(`improvement_surcharge`) AS improvement_surcharge_min,
  MAX(`improvement_surcharge`) AS improvement_surcharge_max,
  AVG(`improvement_surcharge`) AS improvement_surcharge_mean,
  MIN(`total_amount`) AS total_amount_min,
  MAX(`total_amount`) AS total_amount_max,
  AVG(`total_amount`) AS total_amount_mean,
  MIN(`congestion_surcharge`) AS congestion_surcharge_min,
  MAX(`congestion_surcharge`) AS congestion_surcharge_max,
  AVG(`congestion_surcharge`) AS congestion_surcharge_mean,
  MIN(`Airport_fee`) AS airport_fee_min,
  MAX(`Airport_fee`) AS airport_fee_max,
  AVG(`Airport_fee`) AS airport_fee_mean,
  MIN(`cbd_congestion_fee`) AS cbd_congestion_fee_min,
  MAX(`cbd_congestion_fee`) AS cbd_congestion_fee_max,
  AVG(`cbd_congestion_fee`) AS cbd_congestion_fee_mean,
  MIN(`passenger_count`) AS passenger_count_min,
  MAX(`passenger_count`) AS passenger_count_max,
  -- CAST before AVG, not after: passenger_count is BIGINT (the only
  -- integer column in this SELECT's means), and Flink's AVG over an
  -- integer type returns that integer type, truncating. That shipped a
  -- silently wrong mean - 154/107 stored as 1.0, 13/6 stored as 2.0 -
  -- while MIN/MAX on the same column stayed correct because they never
  -- divide. Every other *_mean here is already DOUBLE.
  AVG(CAST(`passenger_count` AS DOUBLE)) AS passenger_count_mean,
  SUM(CASE WHEN `tpep_dropoff_datetime` < `tpep_pickup_datetime` THEN 1 ELSE 0 END) AS trip_duration_negative_count,
  SUM(CASE WHEN `tpep_dropoff_datetime` = `tpep_pickup_datetime` THEN 1 ELSE 0 END) AS trip_duration_zero_count
FROM TABLE(
  TUMBLE(TABLE weir_tlc_trips_source, DESCRIPTOR(`tpep_pickup_datetime`), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end;
