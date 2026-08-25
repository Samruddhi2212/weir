-- Part 4.1: the volume detector's incident/baseline schema.
-- Design rationale, the three-table baseline/scoring/incident
-- separation, the idempotency mechanism, the timezone handling, and
-- the EWMA-of-mean-absolute-deviation correction (not a true
-- median/MAD) are all in DEFENSE.md #44/#45, written before this
-- file, per D1.
--
-- Separate from reliability/store/schema.sql's weir_metrics schema on
-- purpose: weir_metrics holds what the Flink metrics job computes
-- (Part 3.1); weir_incidents holds what detectors (this one, and
-- future ones - freshness, nullrate, schema-drift) find when reading
-- from weir_metrics. Idempotent (IF NOT EXISTS throughout, matching
-- schema.sql's own convention) - safe to apply on every detector
-- process start, not just once.

CREATE SCHEMA IF NOT EXISTS weir_incidents;

-- Warmed EWMA/MAD baseline state, one row per (detector, weekday,
-- hour) bucket - weekday/hour are America/New_York local civil time
-- (DEFENSE.md #45: rush-hour/overnight seasonality is a local-time
-- phenomenon; a UTC bucket would smear it across the EST/EDT
-- boundary and shift whenever DST changes). The ONLY table a
-- sensitivity sweep over thresholds never touches - varying alpha
-- still requires a full rewarm (docs/FUTURE_WORK.md).
CREATE TABLE IF NOT EXISTS weir_incidents.baseline_state (
    detector_name      TEXT NOT NULL,
    bucket_weekday     SMALLINT NOT NULL,  -- 0=Mon..6=Sun, America/New_York
    bucket_hour        SMALLINT NOT NULL,  -- 0-23, America/New_York
    observation_count  BIGINT NOT NULL DEFAULT 0,
    ewma_mean          DOUBLE PRECISION,
    -- EWMA of MEAN absolute deviation from ewma_mean - an EWMA-
    -- recursive approximation, not a true median/MAD (DEFENSE.md
    -- #45; an exact-MAD variant would need a retained sorted sample
    -- per bucket, defeating the O(1)-state point of this design).
    -- Rescaled to sigma-equivalent units at scoring time via
    -- sqrt(pi/2) ~= 1.2533 (the MeanAD-to-sigma constant - NOT MAD's
    -- 1.4826, confirmed independently, not assumed) - this column
    -- stores the raw tracked quantity, not the rescaled one.
    ewma_mad           DOUBLE PRECISION,
    alpha              DOUBLE PRECISION NOT NULL,
    config_hash        TEXT NOT NULL,
    -- NOT NULL with a real sentinel, not nullable - see
    -- detector_progress below for why the epoch specifically
    -- eliminates a NULL-comparison ambiguity rather than requiring
    -- application code to special-case it.
    last_window_end    TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00+00'::timestamptz,
    PRIMARY KEY (detector_name, bucket_weekday, bucket_hour),
    CONSTRAINT baseline_state_weekday_range CHECK (bucket_weekday BETWEEN 0 AND 6),
    CONSTRAINT baseline_state_hour_range CHECK (bucket_hour BETWEEN 0 AND 23)
);

-- Fast idempotent resume point - O(1) lookup, not a scan of
-- scored_windows for its max. The FOR UPDATE row lock taken on this
-- row (in application code, not visible in DDL) is what makes the
-- "is this window behind the frontier" check-and-branch atomic
-- against concurrent runs of the same detector_name.
--
-- last_processed_window_end is NOT NULL, defaulted to the Unix epoch
-- (1970-01-01T00:00:00Z) - deliberately not nullable, and deliberately
-- NOT Postgres's own 'infinity'/'-infinity' timestamp special values.
-- In SQL's three-valued logic, `candidate.window_end <= NULL`
-- evaluates to NULL (neither branch), and in ordinary application
-- code a comparison against a Python None would raise outright -
-- either way, "what happens on the very first window a detector ever
-- processes" would be an unstated edge case, not a decision.
-- '-infinity' was the first choice and was wrong: confirmed against
-- psycopg 3's own documentation (not assumed) that, unlike psycopg2,
-- it raises DataError by default when reading '-infinity'/'infinity'
-- timestamptz values back into Python, rather than mapping them to
-- datetime.min/max - the very first SELECT of this column from the
-- approved psycopg client would have crashed. The epoch is an
-- ordinary, ISO-representable instant psycopg handles with no special
-- casing at all, and serves the identical structural purpose here:
-- every real TLC window_end (Oct 2024 onward) is trivially greater
-- than 1970, so "is this window behind the frontier" has exactly one
-- correct answer (no) on a brand-new detector, with no separate
-- NULL-handling branch anywhere in the comparison logic. A detector
-- registers itself (INSERT ... ON CONFLICT DO NOTHING) before its
-- first read cycle, relying on this column default rather than an
-- application-level literal, so the sentinel exists even if
-- registration code is ever called in a different order.
CREATE TABLE IF NOT EXISTS weir_incidents.detector_progress (
    detector_name             TEXT PRIMARY KEY,
    last_processed_window_end TIMESTAMPTZ NOT NULL DEFAULT '1970-01-01T00:00:00+00'::timestamptz
);

-- Append-only log of every window that cleared the ordering/lag
-- check - one row per window, always, regardless of outcome
-- (DEFENSE.md #45's "8-week warmup is an accounting hole otherwise").
-- PRIMARY KEY is the hard correctness backstop against ever double-
-- applying a window to the EWMA, independent of detector_progress.
-- Decoupled from any particular threshold decision: a sensitivity
-- sweep recomputes score/flag at a different threshold straight from
-- observed_value/baseline_mean_at_time/baseline_scale_at_time, never
-- re-reading weir_metrics or re-running the EWMA recursion.
CREATE TABLE IF NOT EXISTS weir_incidents.scored_windows (
    detector_name           TEXT NOT NULL,
    window_start            TIMESTAMPTZ NOT NULL,
    window_end              TIMESTAMPTZ NOT NULL,
    bucket_weekday          SMALLINT NOT NULL,
    bucket_hour             SMALLINT NOT NULL,
    -- 'scored': baseline was ALREADY warm enough (checked against
    --   this bucket's observation_count BEFORE this window's own
    --   update), so a score was computed against the pre-update
    --   ewma_mean/ewma_mad - then this window's own EWMA update is
    --   applied, same as any other processed window.
    -- 'insufficient_baseline': baseline was not yet warm enough at
    --   the time of this window - no score computed - but this
    --   window's EWMA update IS still applied to baseline_state
    --   (observation_count increments regardless of status; this is
    --   the only way warmup ever progresses at all - a bucket that
    --   never updated until it was already warm could never become
    --   warm in the first place).
    -- 'skipped_late': arrived behind detector_progress's frontier -
    --   the ONE status that does NOT update baseline_state at all -
    --   counted per V8, not dropped.
    status                  TEXT NOT NULL,
    observed_value          DOUBLE PRECISION NOT NULL,
    baseline_mean_at_time   DOUBLE PRECISION,
    baseline_scale_at_time  DOUBLE PRECISION,
    score                   DOUBLE PRECISION,
    scored_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (detector_name, window_start, window_end),
    CONSTRAINT scored_windows_status_valid
        CHECK (status IN ('scored', 'insufficient_baseline', 'skipped_late')),
    CONSTRAINT scored_windows_weekday_range CHECK (bucket_weekday BETWEEN 0 AND 6),
    CONSTRAINT scored_windows_hour_range CHECK (bucket_hour BETWEEN 0 AND 23),
    -- Biconditional, not one-directional: status='scored' REQUIRES all
    -- three NOT NULL, and any other status REQUIRES all three NULL -
    -- an earlier draft of this constraint only checked the second half
    -- and would have silently allowed a 'scored' row with a NULL
    -- score to pass. Enforced at the database level, not left as a
    -- comment application code is trusted to honor.
    CONSTRAINT scored_windows_null_contract CHECK (
        (status = 'scored'
            AND baseline_mean_at_time IS NOT NULL
            AND baseline_scale_at_time IS NOT NULL
            AND score IS NOT NULL)
        OR
        (status <> 'scored'
            AND baseline_mean_at_time IS NULL
            AND baseline_scale_at_time IS NULL
            AND score IS NULL)
    )
);
CREATE INDEX IF NOT EXISTS scored_windows_by_bucket
    ON weir_incidents.scored_windows (detector_name, bucket_weekday, bucket_hour, window_start);
CREATE INDEX IF NOT EXISTS scored_windows_by_status
    ON weir_incidents.scored_windows (detector_name, status);

-- Only rows where score crossed the CURRENTLY CONFIGURED threshold -
-- the smaller, actionable table (what an alerting system would
-- consume), not every scored window. Continuous score stored on the
-- row, never a boolean.
CREATE TABLE IF NOT EXISTS weir_incidents.incidents (
    id              BIGSERIAL PRIMARY KEY,
    detector_name   TEXT NOT NULL,
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    bucket_weekday  SMALLINT,
    bucket_hour     SMALLINT,
    observed_value  DOUBLE PRECISION NOT NULL,
    baseline_mean   DOUBLE PRECISION,
    baseline_scale  DOUBLE PRECISION,
    score           DOUBLE PRECISION NOT NULL,
    detected_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    details         JSONB,
    UNIQUE (detector_name, window_start, window_end),
    -- Nullable here (unlike scored_windows/baseline_state) - a future
    -- non-bucketed detector type may write incidents without a
    -- weekday/hour concept at all - but WHEN present, still in range.
    CONSTRAINT incidents_weekday_range CHECK (bucket_weekday IS NULL OR bucket_weekday BETWEEN 0 AND 6),
    CONSTRAINT incidents_hour_range CHECK (bucket_hour IS NULL OR bucket_hour BETWEEN 0 AND 23)
);
CREATE INDEX IF NOT EXISTS incidents_by_detector_window
    ON weir_incidents.incidents (detector_name, window_start);
