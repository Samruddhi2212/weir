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
-- compose.yml has no -s3.config identity file, so it has no configured
-- access key/secret of its own to match against - if this fails on auth,
-- that's a real finding, not a bug in this file.

SET 'execution.runtime-mode' = 'batch';

CREATE CATALOG IF NOT EXISTS weir_smoke_catalog WITH (
  'type' = 'iceberg',
  'catalog-type' = 'jdbc',
  'uri' = 'jdbc:postgresql://postgres:5432/weir_catalog',
  'jdbc.user' = 'weir',
  'jdbc.password' = 'weir',
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
SELECT COUNT(*) AS row_count FROM smoke_iceberg_table;
