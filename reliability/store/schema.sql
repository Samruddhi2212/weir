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

-- Part 3.1 mechanism, DEFENSE.md #42: the Flink job (scripts/sql/
-- metrics_job.sql) writes ONE wide row per window here via its JDBC
-- sink's upsert mode (this table's PRIMARY KEY makes that an upsert,
-- not append-only) - plain, standard TUMBLE/GROUP BY/aggregate-
-- function SQL, nothing exotic. The AFTER INSERT OR UPDATE trigger
-- below reshapes each wide row into window_metrics + column_metrics,
-- using Postgres's own well-established UNNEST(ARRAY[...]), not
-- Flink's less-battle-tested equivalent - #42's rejected-alternative
-- reasoning.
CREATE TABLE IF NOT EXISTS weir_metrics.window_metrics_wide (
    window_start TIMESTAMP NOT NULL,
    window_end   TIMESTAMP NOT NULL,
    row_count    BIGINT NOT NULL,
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
    trip_distance_min DOUBLE PRECISION,
    trip_distance_max DOUBLE PRECISION,
    trip_distance_mean DOUBLE PRECISION,
    fare_amount_min DOUBLE PRECISION,
    fare_amount_max DOUBLE PRECISION,
    fare_amount_mean DOUBLE PRECISION,
    extra_min DOUBLE PRECISION,
    extra_max DOUBLE PRECISION,
    extra_mean DOUBLE PRECISION,
    mta_tax_min DOUBLE PRECISION,
    mta_tax_max DOUBLE PRECISION,
    mta_tax_mean DOUBLE PRECISION,
    tip_amount_min DOUBLE PRECISION,
    tip_amount_max DOUBLE PRECISION,
    tip_amount_mean DOUBLE PRECISION,
    tolls_amount_min DOUBLE PRECISION,
    tolls_amount_max DOUBLE PRECISION,
    tolls_amount_mean DOUBLE PRECISION,
    improvement_surcharge_min DOUBLE PRECISION,
    improvement_surcharge_max DOUBLE PRECISION,
    improvement_surcharge_mean DOUBLE PRECISION,
    total_amount_min DOUBLE PRECISION,
    total_amount_max DOUBLE PRECISION,
    total_amount_mean DOUBLE PRECISION,
    congestion_surcharge_min DOUBLE PRECISION,
    congestion_surcharge_max DOUBLE PRECISION,
    congestion_surcharge_mean DOUBLE PRECISION,
    airport_fee_min DOUBLE PRECISION,
    airport_fee_max DOUBLE PRECISION,
    airport_fee_mean DOUBLE PRECISION,
    cbd_congestion_fee_min DOUBLE PRECISION,
    cbd_congestion_fee_max DOUBLE PRECISION,
    cbd_congestion_fee_mean DOUBLE PRECISION,
    passenger_count_min DOUBLE PRECISION,
    passenger_count_max DOUBLE PRECISION,
    passenger_count_mean DOUBLE PRECISION,
    trip_duration_negative_count BIGINT,
    trip_duration_zero_count BIGINT,
    PRIMARY KEY (window_start, window_end)
);

-- Reshapes one wide row into window_metrics (fixed columns; the
-- lateness_* columns are left NULL here - DEFENSE.md #42, that needs
-- Flink's side-output/DataStream-level late-data handling, not plain
-- SQL, and is a separate, not-yet-built step) plus 69 column_metrics
-- rows via UNNEST(ARRAY[...]). ON CONFLICT DO UPDATE on both target
-- tables so re-processing the same window (a job restart replaying
-- from its last checkpoint, for instance) overwrites rather than
-- duplicates or errors.
CREATE OR REPLACE FUNCTION weir_metrics.fan_out_window_metrics_wide()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO weir_metrics.window_metrics (window_start, window_end, row_count)
    VALUES (NEW.window_start, NEW.window_end, NEW.row_count)
    ON CONFLICT (window_start, window_end) DO UPDATE SET row_count = EXCLUDED.row_count;

    INSERT INTO weir_metrics.column_metrics (window_start, window_end, column_name, metric_name, metric_value)
    SELECT NEW.window_start, NEW.window_end, m.column_name, m.metric_name, m.metric_value
    FROM UNNEST(ARRAY[
        ROW('VendorID', 'null_count', NEW.vendorid_nulls::double precision),
        ROW('tpep_dropoff_datetime', 'null_count', NEW.tpep_dropoff_datetime_nulls::double precision),
        ROW('passenger_count', 'null_count', NEW.passenger_count_nulls::double precision),
        ROW('trip_distance', 'null_count', NEW.trip_distance_nulls::double precision),
        ROW('RatecodeID', 'null_count', NEW.ratecodeid_nulls::double precision),
        ROW('store_and_fwd_flag', 'null_count', NEW.store_and_fwd_flag_nulls::double precision),
        ROW('PULocationID', 'null_count', NEW.pulocationid_nulls::double precision),
        ROW('DOLocationID', 'null_count', NEW.dolocationid_nulls::double precision),
        ROW('payment_type', 'null_count', NEW.payment_type_nulls::double precision),
        ROW('fare_amount', 'null_count', NEW.fare_amount_nulls::double precision),
        ROW('extra', 'null_count', NEW.extra_nulls::double precision),
        ROW('mta_tax', 'null_count', NEW.mta_tax_nulls::double precision),
        ROW('tip_amount', 'null_count', NEW.tip_amount_nulls::double precision),
        ROW('tolls_amount', 'null_count', NEW.tolls_amount_nulls::double precision),
        ROW('improvement_surcharge', 'null_count', NEW.improvement_surcharge_nulls::double precision),
        ROW('total_amount', 'null_count', NEW.total_amount_nulls::double precision),
        ROW('congestion_surcharge', 'null_count', NEW.congestion_surcharge_nulls::double precision),
        ROW('Airport_fee', 'null_count', NEW.airport_fee_nulls::double precision),
        ROW('cbd_congestion_fee', 'null_count', NEW.cbd_congestion_fee_nulls::double precision),
        ROW('VendorID', 'distinct_count', NEW.vendorid_distinct::double precision),
        ROW('RatecodeID', 'distinct_count', NEW.ratecodeid_distinct::double precision),
        ROW('payment_type', 'distinct_count', NEW.payment_type_distinct::double precision),
        ROW('PULocationID', 'distinct_count', NEW.pulocationid_distinct::double precision),
        ROW('DOLocationID', 'distinct_count', NEW.dolocationid_distinct::double precision),
        ROW('store_and_fwd_flag', 'distinct_count', NEW.store_and_fwd_flag_distinct::double precision),
        ROW('fare_amount', 'negative_count', NEW.fare_amount_negative::double precision),
        ROW('tip_amount', 'negative_count', NEW.tip_amount_negative::double precision),
        ROW('tolls_amount', 'negative_count', NEW.tolls_amount_negative::double precision),
        ROW('total_amount', 'negative_count', NEW.total_amount_negative::double precision),
        ROW('trip_distance', 'negative_count', NEW.trip_distance_negative::double precision),
        ROW('passenger_count', 'negative_count', NEW.passenger_count_negative::double precision),
        ROW('trip_distance', 'min', NEW.trip_distance_min),
        ROW('trip_distance', 'max', NEW.trip_distance_max),
        ROW('trip_distance', 'mean', NEW.trip_distance_mean),
        ROW('fare_amount', 'min', NEW.fare_amount_min),
        ROW('fare_amount', 'max', NEW.fare_amount_max),
        ROW('fare_amount', 'mean', NEW.fare_amount_mean),
        ROW('extra', 'min', NEW.extra_min),
        ROW('extra', 'max', NEW.extra_max),
        ROW('extra', 'mean', NEW.extra_mean),
        ROW('mta_tax', 'min', NEW.mta_tax_min),
        ROW('mta_tax', 'max', NEW.mta_tax_max),
        ROW('mta_tax', 'mean', NEW.mta_tax_mean),
        ROW('tip_amount', 'min', NEW.tip_amount_min),
        ROW('tip_amount', 'max', NEW.tip_amount_max),
        ROW('tip_amount', 'mean', NEW.tip_amount_mean),
        ROW('tolls_amount', 'min', NEW.tolls_amount_min),
        ROW('tolls_amount', 'max', NEW.tolls_amount_max),
        ROW('tolls_amount', 'mean', NEW.tolls_amount_mean),
        ROW('improvement_surcharge', 'min', NEW.improvement_surcharge_min),
        ROW('improvement_surcharge', 'max', NEW.improvement_surcharge_max),
        ROW('improvement_surcharge', 'mean', NEW.improvement_surcharge_mean),
        ROW('total_amount', 'min', NEW.total_amount_min),
        ROW('total_amount', 'max', NEW.total_amount_max),
        ROW('total_amount', 'mean', NEW.total_amount_mean),
        ROW('congestion_surcharge', 'min', NEW.congestion_surcharge_min),
        ROW('congestion_surcharge', 'max', NEW.congestion_surcharge_max),
        ROW('congestion_surcharge', 'mean', NEW.congestion_surcharge_mean),
        ROW('Airport_fee', 'min', NEW.airport_fee_min),
        ROW('Airport_fee', 'max', NEW.airport_fee_max),
        ROW('Airport_fee', 'mean', NEW.airport_fee_mean),
        ROW('cbd_congestion_fee', 'min', NEW.cbd_congestion_fee_min),
        ROW('cbd_congestion_fee', 'max', NEW.cbd_congestion_fee_max),
        ROW('cbd_congestion_fee', 'mean', NEW.cbd_congestion_fee_mean),
        ROW('passenger_count', 'min', NEW.passenger_count_min),
        ROW('passenger_count', 'max', NEW.passenger_count_max),
        ROW('passenger_count', 'mean', NEW.passenger_count_mean),
        ROW('trip_duration', 'negative_count', NEW.trip_duration_negative_count::double precision),
        ROW('trip_duration', 'zero_count', NEW.trip_duration_zero_count::double precision)
    ]) AS m(column_name text, metric_name text, metric_value double precision)
    ON CONFLICT (window_start, window_end, column_name, metric_name) DO UPDATE SET metric_value = EXCLUDED.metric_value;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS window_metrics_wide_fan_out ON weir_metrics.window_metrics_wide;
CREATE TRIGGER window_metrics_wide_fan_out
    AFTER INSERT OR UPDATE ON weir_metrics.window_metrics_wide
    FOR EACH ROW
    EXECUTE FUNCTION weir_metrics.fan_out_window_metrics_wide();
