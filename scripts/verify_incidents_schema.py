#!/usr/bin/env python3
"""Part 4.1: verify reliability/store/incidents_schema.sql actually
behaves the way DEFENSE.md #44/#45 claims, not just that it applies
without a syntax error.

V1: every check below asserts an exact outcome (a specific exception
class, an exact returned value) - never "did something happen."

Three kinds of checks:
  1. Idempotency - applying the schema twice does not error (matching
     reliability/store/schema.sql's own established pattern).
  2. Every CHECK constraint actually rejects the specific bad write it
     exists to reject - a constraint nobody tests is a comment
     (V4/testing-the-thing-itself discipline already used for the
     metrics store schema).
  3. The first-window bootstrap: a freshly registered detector's
     last_processed_window_end sentinel ('-infinity') compares as
     "not behind the frontier" against any real window_end, with no
     separate NULL-handling branch anywhere.
"""
import argparse
import datetime
import sys

import psycopg
from psycopg import errors as pg_errors


def fail(msg):
    print(f"\nFAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def apply_sql_file(conn, path):
    with open(path, "r", encoding="utf-8") as f:
        sql = f.read()
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()


def expect_check_violation(conn, label, sql, params=(), expected_constraint=None):
    """Runs sql expecting a CheckViolation; rolls back either way so
    the failed attempt never persists. Fails loudly if no exception,
    the wrong exception class, or (when expected_constraint is given)
    the wrong constraint fired - a test row that happens to violate
    two constraints at once would otherwise "pass" for the wrong
    reason, exactly as a first draft of this script's very first
    check did (rejected by scored_windows_null_contract while
    claiming to test scored_windows_status_valid)."""
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.rollback()
        fail(f"{label}: expected a CheckViolation, but the INSERT succeeded")
    except pg_errors.CheckViolation as e:
        conn.rollback()
        actual_constraint = e.diag.constraint_name
        if expected_constraint is not None and actual_constraint != expected_constraint:
            fail(
                f"{label}: expected constraint '{expected_constraint}' to fire, "
                f"but '{actual_constraint}' fired instead - this test row violates "
                f"more than one constraint and isn't isolating the one it claims to"
            )
        print(f"PASS: {label} - rejected as expected ({actual_constraint})")
    except Exception as e:
        conn.rollback()
        fail(f"{label}: expected CheckViolation, got {type(e).__name__}: {e}")


def expect_success(conn, label, sql, params=()):
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
        conn.commit()
        print(f"PASS: {label} - accepted as expected")
    except Exception as e:
        conn.rollback()
        fail(f"{label}: expected success, got {type(e).__name__}: {e}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    args = parser.parse_args()

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)

    print("=== Stage 1: apply schema.sql, then incidents_schema.sql twice (idempotency) ===")
    apply_sql_file(conn, "reliability/store/schema.sql")
    apply_sql_file(conn, "reliability/store/incidents_schema.sql")
    apply_sql_file(conn, "reliability/store/incidents_schema.sql")
    print("PASS: stage 1 - applied twice without error")

    # Clean slate for the checks below - this script may run more than once.
    with conn.cursor() as cur:
        cur.execute("TRUNCATE weir_incidents.scored_windows, weir_incidents.baseline_state, "
                    "weir_incidents.detector_progress, weir_incidents.incidents;")
    conn.commit()

    print("\n=== Stage 2: each CHECK constraint actually rejects the bad write it exists for ===")

    ws = datetime.datetime(2025, 1, 6, 15, 0, 0, tzinfo=datetime.timezone.utc)
    we = ws + datetime.timedelta(minutes=1)

    def insert_scored_windows_sql():
        return (
            "INSERT INTO weir_incidents.scored_windows "
            "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, status, "
            " observed_value, baseline_mean_at_time, baseline_scale_at_time, score) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        )

    expect_check_violation(
        conn, "scored_windows_status_valid rejects an invalid status",
        insert_scored_windows_sql(),
        # baseline_mean_at_time/baseline_scale_at_time/score all NULL -
        # satisfies null_contract's "not scored -> all NULL" branch on
        # its own, so ONLY status_valid is what this row can violate.
        ("volume", ws, we, 0, 15, "bogus_status", 100.0, None, None, None),
        expected_constraint="scored_windows_status_valid",
    )

    expect_check_violation(
        conn, "scored_windows_null_contract rejects status='scored' with a NULL score",
        insert_scored_windows_sql(),
        ("volume", ws, we, 0, 15, "scored", 100.0, 90.0, 5.0, None),
        expected_constraint="scored_windows_null_contract",
    )

    expect_check_violation(
        conn, "scored_windows_null_contract rejects status='insufficient_baseline' with a non-NULL score "
              "(the exact gap the first draft's one-directional CHECK left open)",
        insert_scored_windows_sql(),
        ("volume", ws, we, 0, 15, "insufficient_baseline", 100.0, None, None, 2.0),
        expected_constraint="scored_windows_null_contract",
    )

    expect_check_violation(
        conn, "scored_windows_weekday_range rejects bucket_weekday=7",
        insert_scored_windows_sql(),
        ("volume", ws, we, 7, 15, "insufficient_baseline", 100.0, None, None, None),
        expected_constraint="scored_windows_weekday_range",
    )

    expect_check_violation(
        conn, "scored_windows_hour_range rejects bucket_hour=24",
        insert_scored_windows_sql(),
        ("volume", ws, we, 0, 24, "insufficient_baseline", 100.0, None, None, None),
        expected_constraint="scored_windows_hour_range",
    )

    expect_check_violation(
        conn, "baseline_state_weekday_range rejects bucket_weekday=-1",
        "INSERT INTO weir_incidents.baseline_state "
        "(detector_name, bucket_weekday, bucket_hour, alpha, config_hash) VALUES (%s, %s, %s, %s, %s)",
        ("volume", -1, 15, 0.001, "deadbeef"),
        expected_constraint="baseline_state_weekday_range",
    )

    expect_check_violation(
        conn, "baseline_state_hour_range rejects bucket_hour=24",
        "INSERT INTO weir_incidents.baseline_state "
        "(detector_name, bucket_weekday, bucket_hour, alpha, config_hash) VALUES (%s, %s, %s, %s, %s)",
        ("volume", 0, 24, 0.001, "deadbeef"),
        expected_constraint="baseline_state_hour_range",
    )

    expect_check_violation(
        conn, "incidents_weekday_range rejects bucket_weekday=7 even though the column is nullable",
        "INSERT INTO weir_incidents.incidents "
        "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, observed_value, score) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        ("volume", ws, we, 7, 15, 100.0, 4.0),
        expected_constraint="incidents_weekday_range",
    )

    expect_check_violation(
        conn, "incidents_hour_range rejects bucket_hour=99 even though the column is nullable",
        "INSERT INTO weir_incidents.incidents "
        "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, observed_value, score) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        ("volume", ws, we, 0, 99, 100.0, 4.0),
        expected_constraint="incidents_hour_range",
    )

    expect_success(
        conn, "incidents accepts NULL bucket_weekday/bucket_hour (non-bucketed detector types)",
        "INSERT INTO weir_incidents.incidents "
        "(detector_name, window_start, window_end, bucket_weekday, bucket_hour, observed_value, score) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s)",
        ("some_future_detector", ws, we, None, None, 100.0, 4.0),
    )

    expect_success(
        conn, "scored_windows accepts a well-formed 'scored' row",
        insert_scored_windows_sql(),
        ("volume", ws, we, 0, 15, "scored", 100.0, 90.0, 5.0, 2.0),
    )

    print("\n=== Stage 3: first-window bootstrap - no NULL-handling branch needed ===")
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO weir_incidents.detector_progress (detector_name) VALUES (%s) "
            "ON CONFLICT (detector_name) DO NOTHING",
            ("volume_bootstrap_test",),
        )
        cur.execute(
            "SELECT last_processed_window_end FROM weir_incidents.detector_progress "
            "WHERE detector_name = %s",
            ("volume_bootstrap_test",),
        )
        (sentinel,) = cur.fetchone()
    conn.commit()
    expected_sentinel = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
    if sentinel != expected_sentinel:
        # Exact match, not "close enough" - this is precisely the
        # value the schema comment promises, and the whole reason the
        # epoch replaced '-infinity' (which psycopg 3 can't even read
        # back without raising DataError, confirmed via its own docs
        # before this schema shipped - DEFENSE.md #45's addendum).
        fail(f"expected the epoch sentinel {expected_sentinel!r}, got {sentinel!r}")
    print(f"PASS: registered detector's default last_processed_window_end = {sentinel} (exact match)")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT %s::timestamptz > last_processed_window_end FROM weir_incidents.detector_progress "
            "WHERE detector_name = %s",
            (ws, "volume_bootstrap_test"),
        )
        (first_window_is_not_behind_frontier,) = cur.fetchone()
    if first_window_is_not_behind_frontier is not True:
        fail(
            f"a real window_end ({ws}) should compare as 'not behind the frontier' against "
            f"the fresh sentinel, got {first_window_is_not_behind_frontier!r}"
        )
    print("PASS: stage 3 - first window on a brand-new detector is never 'skipped_late'")

    with conn.cursor() as cur:
        cur.execute("DELETE FROM weir_incidents.detector_progress WHERE detector_name = %s",
                    ("volume_bootstrap_test",))
    conn.commit()

    conn.close()
    print("\n=== verify_incidents_schema.py: all stages passed ===")


if __name__ == "__main__":
    main()
