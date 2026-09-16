"""Part 4.1: Postgres reading/writing adapter for the volume detector.

Wraps the transactional read-score-write loop from DEFENSE.md #45.
The ordering/frontier decision (is_behind_frontier) and the bucket-
derivation logic (to_local_bucket, is_dst_ambiguous) are pure
functions, kept separate from the psycopg-using I/O around them, so
they're unit-testable without a live database - the same reason
detector.py's scoring logic is I/O-free.
"""
from zoneinfo import ZoneInfo

from ingestion.replay.load_pickup_timestamps import DST_FALLBACK_AMBIGUOUS_HOURS
from reliability.volume.detector import BucketState, update_and_score


def to_local_bucket(utc_instant, timezone_name):
    """Converts a UTC instant to (weekday, hour) in the given
    timezone's local civil time - weekday 0=Mon..6=Sun, matching
    weir_incidents' own documented column range (DEFENSE.md #45:
    rush-hour/overnight seasonality is a local-time phenomenon, not
    UTC)."""
    local_dt = utc_instant.astimezone(ZoneInfo(timezone_name))
    return local_dt.weekday(), local_dt.hour


def is_dst_ambiguous(utc_instant, timezone_name):
    """True if this instant's local civil time falls in a DST fall-
    back's ambiguous hour (DEFENSE.md #44/#45) - reuses the exact same
    table ingestion/replay/load_pickup_timestamps.py already
    maintains, rather than a second, independently-drifting copy of
    the same fact."""
    local_naive = utc_instant.astimezone(ZoneInfo(timezone_name)).replace(tzinfo=None)
    return any(start <= local_naive < end for start, end in DST_FALLBACK_AMBIGUOUS_HOURS)


def is_behind_frontier(window_end, last_processed_window_end):
    """True if this window arrived behind the detector's already-
    advanced frontier - i.e. it must be recorded 'skipped_late', never
    applied to baseline_state. On a freshly registered detector,
    last_processed_window_end is the epoch sentinel (DEFENSE.md #45),
    so every real window_end (Oct 2024 onward) is trivially NOT behind
    it - the comparison has exactly one correct answer on a brand-new
    detector, with no separate NULL-handling branch anywhere."""
    return window_end <= last_processed_window_end


def release_snapshot(conn):
    """End the read-only transaction a fetch opened, before the caller
    starts its per-window write transactions.

    psycopg opens a transaction on the first execute against a
    non-autocommit connection, and conn.transaction() nests as a
    SAVEPOINT when one is already open. Leaving a fetch's transaction
    open therefore silently turned process_window's "one window, one
    transaction" (DEFENSE.md #45) into a savepoint inside a single
    transaction that never committed. Postgres caches 64
    subtransactions per backend and spills to the pg_subtrans SLRU past
    that, so the cost of each window grew with the number of windows
    already done.

    Measured, not theorised: run 34885328884 scored at ~172/s for the
    first few thousand windows and ~14/s by 50000, and reset to ~172/s
    at every detector boundary - which is exactly where the transaction
    was reopened. Nothing about the scores themselves changed, only how
    long they took and whether a mid-run crash could actually be
    resumed.

    rollback, not commit: the transaction being ended only ever read,
    and fetch_eligible_windows documents its result as an efficiency
    filter whose staleness is harmless.
    """
    conn.rollback()
    # Self-verifying on purpose. The fix is one line and its absence is
    # invisible - the previous version produced correct scores, just
    # slowly and without the crash-resumability process_window claims -
    # so a regression would not show up as a failing assertion anywhere
    # else.
    status = conn.info.transaction_status.name
    if status != "IDLE":
        raise RuntimeError(
            f"connection reports transaction_status={status} after rollback, not IDLE; "
            f"process_window would nest as a SAVEPOINT rather than open its own "
            f"transaction, which is the regression this function exists to prevent"
        )


def process_window(conn, window_start, window_end, observed_value, config):
    """One window, one transaction (DEFENSE.md #45): FOR UPDATE lock
    on detector_progress, branch on is_behind_frontier, update
    baseline_state (skipped only for 'skipped_late'), insert
    scored_windows, insert incidents if flagged, advance
    detector_progress. All in the same transaction - a crash partway
    rolls back cleanly and the window is simply reprocessed next run,
    exactly once, since scored_windows' PRIMARY KEY rejects a genuine
    double-attempt outright.

    Returns the status written ('scored', 'insufficient_baseline', or
    'skipped_late').
    """
    bucket_weekday, bucket_hour = to_local_bucket(window_end, config.timezone)

    with conn.transaction():
        with conn.cursor() as cur:
            # Checked before the frontier lock is even taken - a DST-
            # ambiguous window is excluded regardless of arrival order,
            # so there's no need to serialize this decision against
            # concurrent runs the way the frontier check does.
            if is_dst_ambiguous(window_end, config.timezone):
                cur.execute(
                    "INSERT INTO weir_incidents.scored_windows "
                    "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, "
                    " status, observed_value) "
                    "VALUES (%s, %s, %s, %s, %s, 'dst_ambiguous_excluded', %s)",
                    (config.detector_name, window_start, window_end, bucket_weekday, bucket_hour, observed_value),
                )
                return "dst_ambiguous_excluded"

            cur.execute(
                "SELECT last_processed_window_end FROM weir_incidents.detector_progress "
                "WHERE detector_name = %s FOR UPDATE",
                (config.detector_name,),
            )
            row = cur.fetchone()
            last_processed_window_end = row[0]

            if is_behind_frontier(window_end, last_processed_window_end):
                cur.execute(
                    "INSERT INTO weir_incidents.scored_windows "
                    "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, "
                    " status, observed_value) "
                    "VALUES (%s, %s, %s, %s, %s, 'skipped_late', %s)",
                    (config.detector_name, window_start, window_end, bucket_weekday, bucket_hour, observed_value),
                )
                return "skipped_late"

            cur.execute(
                "SELECT observation_count, ewma_mean, ewma_mad, config_hash "
                "FROM weir_incidents.baseline_state "
                "WHERE detector_name = %s AND bucket_weekday = %s AND bucket_hour = %s FOR UPDATE",
                (config.detector_name, bucket_weekday, bucket_hour),
            )
            bucket_row = cur.fetchone()
            if bucket_row is None:
                state = BucketState(observation_count=0, ewma_mean=None, ewma_mad=None)
            else:
                obs_count, ewma_mean, ewma_mad, stored_hash = bucket_row
                if stored_hash != config.config_hash:
                    raise RuntimeError(
                        f"config_hash mismatch for bucket ({bucket_weekday},{bucket_hour}): "
                        f"stored={stored_hash!r}, current={config.config_hash!r} - this baseline "
                        f"was built under a different config; a rewarm decision is required, "
                        f"not a silent continue (DEFENSE.md #45)"
                    )
                state = BucketState(observation_count=obs_count, ewma_mean=ewma_mean, ewma_mad=ewma_mad)

            result = update_and_score(state, observed_value, config)

            cur.execute(
                "INSERT INTO weir_incidents.baseline_state "
                "(detector_name, bucket_weekday, bucket_hour, observation_count, ewma_mean, ewma_mad, "
                " alpha, config_hash, last_window_end) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                "ON CONFLICT (detector_name, bucket_weekday, bucket_hour) DO UPDATE SET "
                "  observation_count = EXCLUDED.observation_count, "
                "  ewma_mean = EXCLUDED.ewma_mean, "
                "  ewma_mad = EXCLUDED.ewma_mad, "
                "  last_window_end = EXCLUDED.last_window_end",
                (config.detector_name, bucket_weekday, bucket_hour,
                 result.new_state.observation_count, result.new_state.ewma_mean, result.new_state.ewma_mad,
                 config.alpha, config.config_hash, window_end),
            )

            cur.execute(
                "INSERT INTO weir_incidents.scored_windows "
                "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, status, "
                " observed_value, baseline_mean_at_time, baseline_scale_at_time, score) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                (config.detector_name, window_start, window_end, bucket_weekday, bucket_hour, result.status,
                 observed_value, result.baseline_mean_at_time, result.baseline_scale_at_time, result.score),
            )

            if result.status == "scored" and result.is_incident:
                cur.execute(
                    "INSERT INTO weir_incidents.incidents "
                    "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, observed_value, "
                    " baseline_mean, baseline_scale, score) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) "
                    "ON CONFLICT (detector_name, window_start, window_end) DO NOTHING",
                    (config.detector_name, window_start, window_end, bucket_weekday, bucket_hour, observed_value,
                     result.baseline_mean_at_time, result.baseline_scale_at_time, result.score),
                )

            cur.execute(
                "UPDATE weir_incidents.detector_progress SET last_processed_window_end = %s "
                "WHERE detector_name = %s",
                (window_end, config.detector_name),
            )

    return result.status


def register_detector(conn, detector_name):
    """Idempotent - relies on detector_progress's column DEFAULT
    (the epoch sentinel) rather than an application-level literal, so
    the sentinel exists even if this is called in an unexpected order
    relative to the first process_window call (DEFENSE.md #45)."""
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO weir_incidents.detector_progress (detector_name) VALUES (%s) "
            "ON CONFLICT (detector_name) DO NOTHING",
            (detector_name,),
        )
    conn.commit()
