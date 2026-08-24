-- Part 3.1: metrics store schema. Design rationale in DEFENSE.md #41,
-- written before this file, per D1.
--
-- Isolated in its own schema (weir_metrics), never `public`, where
-- Iceberg's own JDBC catalog (see docker-compose.yml's `iceberg-rest`
-- service, CATALOG_URI pointed at this same `weir_catalog` database)
-- keeps its own tables. Deliberately not verified against Iceberg's
-- exact table names and avoided instead - a version bump to Iceberg's
-- JDBC catalog implementation could add or rename tables in `public`
-- without this project's knowledge, and a schema-level separation is a
-- stronger, longer-lived guarantee than a one-time check of names that
-- could drift. Idempotent (IF NOT EXISTS throughout) - safe to apply on
-- every job start, not just once.

CREATE SCHEMA IF NOT EXISTS weir_metrics;

-- One row per window. Lateness columns are nullable: they depend on
-- the watermark/late-data mechanism (DEFENSE.md #40) being wired into
-- whatever job populates this table, which is a separate, not-yet-built
-- step from this schema itself.
CREATE TABLE IF NOT EXISTS weir_metrics.window_metrics (
    window_start          TIMESTAMP NOT NULL,
    window_end            TIMESTAMP NOT NULL,
    row_count             BIGINT NOT NULL,
    late_event_count       BIGINT,
    lateness_p50_seconds   DOUBLE PRECISION,
    lateness_p99_seconds   DOUBLE PRECISION,
    lateness_max_seconds   DOUBLE PRECISION,
    PRIMARY KEY (window_start, window_end)
);

-- One row per window per source column per metric. metric_value is
-- always a raw count or a raw statistic (min/max/mean) - never a
-- pre-divided rate. Rates are computed at query time against
-- window_metrics.row_count (DEFENSE.md #41's "one stored ground truth
-- per fact" reasoning).
CREATE TABLE IF NOT EXISTS weir_metrics.column_metrics (
    window_start   TIMESTAMP NOT NULL,
    window_end     TIMESTAMP NOT NULL,
    column_name    TEXT NOT NULL,
    metric_name    TEXT NOT NULL,
    metric_value   DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (window_start, window_end, column_name, metric_name)
);

-- The access pattern a detector needs: one column's one metric's time
-- series across windows, not "everything about window Y" (that's the
-- primary key's job, for the write path / point lookups).
CREATE INDEX IF NOT EXISTS column_metrics_series_lookup
    ON weir_metrics.column_metrics (column_name, metric_name, window_start);
