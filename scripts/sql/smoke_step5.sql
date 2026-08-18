-- Smoke test step 5: write to Iceberg via the single catalog config,
-- verify rows landed. Catalog/db/table names are plain literals, not
-- templated: this file is static (see DEFENSE.md #10) and must match
-- ICEBERG_CATALOG/ICEBERG_DB/ICEBERG_TABLE in scripts/smoke_test.sh if
-- any of them ever change.
--
-- S3 credential values are NOT here - see DEFENSE.md and README on why.
-- __WEIR_S3_ACCESS_KEY__ / __WEIR_S3_SECRET_KEY__ are substituted by
-- scripts/smoke_test.sh (via WEIR_S3_ACCESS_KEY/WEIR_S3_SECRET_KEY env
-- vars, see .env.example) into a temp copy before this file ever reaches
-- the container - this static file, as committed, never contains a real
-- or even placeholder-literal credential value. SeaweedFS in docker-
-- compose.yml has no -s3.config identity file baked in; smoke_test.sh's
-- setup step configures the matching identity live, via `weed shell`,
-- before this file ever runs (see DEFENSE.md #24) - if this fails on
-- auth, that's a real finding, not a bug in this file.
--
-- catalog-type is 'rest', not 'jdbc': Iceberg's FlinkCatalogFactory never
-- supports 'jdbc' at all - confirmed from its own source, and confirmed
-- the hard way, 5/5 reproducible CI failures against the identical
-- config (see DEFENSE.md #21). The catalog server itself still stores
-- its metadata in Postgres via JDBC (see the iceberg-rest service's own
-- CATALOG_URI in docker-compose.yml) - that part didn't change. What
-- changed is that Flink now talks to it over the REST protocol instead
-- of trying to open a JDBC catalog connection directly, which was never
-- a supported combination for Flink specifically.

SET 'execution.runtime-mode' = 'batch';
-- Required, not optional, for the final SELECT below when run via
-- `sql-client.sh -f` (non-interactive) - see smoke_step4.sql's comment
-- and DEFENSE.md #16.
SET 'sql-client.execution.result-mode' = 'TABLEAU';
-- By default SQL Client submits INSERT as a detached job and moves on
-- immediately - "successfully submitted to the cluster" means submitted,
-- not finished. Without this, the SELECT below can (and did, in CI) run
-- before the INSERT's job has actually committed any rows, reading 0
-- back deterministically rather than 2 - not a flaky race, a guaranteed
-- one, since -f runs every statement back-to-back with no delay at all.
-- Confirmed against Flink's own SQL Client docs (table.dml-sync). See
-- DEFENSE.md #24.
SET 'table.dml-sync' = 'true';

CREATE CATALOG IF NOT EXISTS weir_smoke_catalog WITH (
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

USE CATALOG weir_smoke_catalog;
CREATE DATABASE IF NOT EXISTS smoke_test;
USE smoke_test;
DROP TABLE IF EXISTS smoke_iceberg_table;
CREATE TABLE smoke_iceberg_table (message STRING) WITH ('format-version' = '2');
INSERT INTO smoke_iceberg_table VALUES ('smoke-row-1'), ('smoke-row-2');
-- Sentinel-prefixed, not a bare COUNT(*): smoke_test.sh parses the exact
-- literal 'WEIR_ROW_COUNT=' prefix and compares the number after it to
-- "2" exactly. A bare digit here previously matched ANY "2" anywhere in
-- the whole captured output, including inside jar version strings like
-- "flink-table-api-java-uber-2.1.0.jar" - which is how a genuinely
-- broken query once "passed" (see DEFENSE.md #14).
SELECT CONCAT('WEIR_ROW_COUNT=', CAST(COUNT(*) AS STRING)) AS row_count_marker FROM smoke_iceberg_table;
