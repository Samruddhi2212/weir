-- Reads back eos_events' committed rows and every file $all_data_files
-- has ever referenced (any snapshot, not just the current one - see
-- DEFENSE.md #26), for scripts/verify_exactly_once.py to diff against
-- the producer's emission log and the object store's actual contents.
-- Batch mode: a one-shot read of whatever's currently committed, not a
-- continuous query - unlike exactly_once_job.sql, which stays running.
--
-- 'table.display.max-column-width' raised from TABLEAU mode's
-- documented 30-character default (confirmed via Flink's own SQL
-- Client docs and FLINK-30025, not assumed) to 500. S3 URIs in
-- file_path are routinely longer than 30 characters; a silently
-- truncated path wouldn't match anything in the Filer's actual on-disk
-- listing, manufacturing false orphans out of nothing but a display
-- setting nobody looked at. See DEFENSE.md #29 - the same class of "a
-- setting nobody stated explicitly quietly determines the result" as
-- DEFENSE.md #16 and #28.
--
-- Sentinel-prefixed markers (WEIR_ROW=/WEIR_FILE=), not a bare TABLEAU
-- dump parsed by table position - same reasoning as smoke_step5.sql and
-- DEFENSE.md #14: exact, anchored extraction, not "whatever happens to
-- be on this line."

SET 'execution.runtime-mode' = 'batch';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'table.display.max-column-width' = '500';

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
USE eos_test;

SELECT CONCAT('WEIR_ROW=', event_key, ',', CAST(event_ts AS STRING)) AS row_marker FROM eos_events;
SELECT CONCAT('WEIR_FILE=', file_path) AS file_marker FROM eos_events$all_data_files;
