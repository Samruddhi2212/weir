"""Kill a detector mid-loop; assert it resumes from the last committed window.

`adapter.process_window` has always documented "one window, one
transaction... a crash partway rolls back cleanly and the window is
simply reprocessed next run, exactly once" (DEFENSE.md #45). That
guarantee was false for as long as it was written down: the fetch left
its transaction open, so every window nested as a SAVEPOINT inside one
transaction that never committed, and a crash would have discarded the
entire run (DEFENSE.md #55). It was found through a performance
investigation, not a correctness one, because the scores were right the
whole time.

So this test exists for the lesson rather than the bug: an untested
guarantee is a claim, not a property. It is written to fail against the
pre-fix code - with everything inside one uncommitted transaction, a
SIGKILL leaves *zero* committed windows, and the first assertion below
catches exactly that.

Isolation: a test-only `detector_name` means `scored_windows`,
`baseline_state` and `detector_progress` are untouched for the real
detectors, and seeding far-future windows plus setting this detector's
own frontier just before them means no other test's `window_metrics`
rows are eligible. Nothing here is truncated that another test reads.
"""
import datetime
import os
import shutil
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

psycopg = pytest.importorskip("psycopg")

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

from reliability.volume.config import DEFAULT_CONFIG  # noqa: E402
from reliability.volume.run import run_once, to_utc_instant  # noqa: E402

TEST_DETECTOR = "volume_crash_resumability_test"
# January, so America/New_York is unambiguously EST - a DST fall-back
# hour would be excluded as dst_ambiguous_excluded and muddy the counts.
# Far future, so no other test's window_metrics rows sort after these.
SEED_START = datetime.datetime(2030, 1, 7, 0, 0, 0)
SEEDED_WINDOWS = 3000
# Enough committed windows to prove real progress, few enough that the
# kill lands well before the loop finishes.
KILL_AFTER_WINDOWS = 300
APP_NAME = "weir-crash-resumability-driver"

CONFIG = replace(DEFAULT_CONFIG, detector_name=TEST_DETECTOR)

CONNINFO = (
    f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
    f"port={os.environ.get('POSTGRES_PORT', '5432')} "
    f"dbname={os.environ.get('POSTGRES_DB', 'weir_catalog')} "
    f"user={os.environ.get('POSTGRES_USER', 'weir')} "
    f"password={os.environ.get('POSTGRES_PASSWORD', 'weir')}"
)

# Runs the detector in its own process so it can be killed abruptly. An
# in-process exception would unwind cleanly and prove nothing about a
# crash.
DRIVER = """
import sys
from dataclasses import replace
sys.path.insert(0, {repo!r})
import psycopg
from reliability.volume.config import DEFAULT_CONFIG
from reliability.volume.run import run_once

config = replace(DEFAULT_CONFIG, detector_name={detector!r})
conn = psycopg.connect({conninfo!r} + " application_name={app}", autocommit=False)
run_once(conn, config)
"""


def _apply_schemas():
    if shutil.which("docker") is None:
        pytest.skip("docker is not on PATH; this test needs the compose Postgres")
    for sql in ("reliability/store/schema.sql", "reliability/store/incidents_schema.sql"):
        with open(REPO_ROOT / sql, "rb") as handle:
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", "postgres",
                 "psql", "-v", "ON_ERROR_STOP=1", "-U", "weir", "-d", "weir_catalog"],
                cwd=REPO_ROOT, stdin=handle, capture_output=True, text=True, check=False,
            )
        assert result.returncode == 0, (
            f"applying {sql} failed (exit {result.returncode}).\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


def _seed(conn):
    """SEEDED_WINDOWS consecutive one-minute windows, plus a frontier for
    this detector placed just before them."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM weir_metrics.window_metrics WHERE window_start >= %s",
            (SEED_START,),
        )
        for table in ("scored_windows", "baseline_state", "incidents", "detector_progress"):
            cur.execute(
                f"DELETE FROM weir_incidents.{table} WHERE detector_name = %s",
                (TEST_DETECTOR,),
            )
        with cur.copy(
            "COPY weir_metrics.window_metrics (window_start, window_end, row_count) FROM STDIN"
        ) as copy:
            for i in range(SEEDED_WINDOWS):
                start = SEED_START + datetime.timedelta(minutes=i)
                copy.write_row((start, start + datetime.timedelta(minutes=1), 1000))
        # register_detector relies on the column DEFAULT (the epoch
        # sentinel), which would make every pre-existing window_metrics
        # row eligible too. Pinning the frontier to just before the
        # seeded range is what keeps this test's counts exact.
        cur.execute(
            "INSERT INTO weir_incidents.detector_progress "
            "(detector_name, last_processed_window_end) VALUES (%s, %s) "
            "ON CONFLICT (detector_name) DO UPDATE SET "
            "last_processed_window_end = EXCLUDED.last_processed_window_end",
            (TEST_DETECTOR, to_utc_instant(SEED_START, CONFIG.timezone)),
        )


def _expected_eligible():
    """The lag buffer holds back the trailing max_lag_seconds of windows."""
    return SEEDED_WINDOWS - int(CONFIG.max_lag_seconds // 60)


def _scored(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*), COUNT(DISTINCT window_end), MAX(window_end) "
            "FROM weir_incidents.scored_windows WHERE detector_name = %s",
            (TEST_DETECTOR,),
        )
        return cur.fetchone()


def _frontier(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_processed_window_end FROM weir_incidents.detector_progress "
            "WHERE detector_name = %s",
            (TEST_DETECTOR,),
        )
        return cur.fetchone()[0]


def _wait_for_backend_to_clear(observer, timeout=30.0):
    """Postgres rolls back a killed session's transaction when it notices
    the socket is gone. Reading before that happens would race the
    cleanup and could also block on locks the dead backend still holds."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        with observer.cursor() as cur:
            cur.execute(
                "SELECT COUNT(*) FROM pg_stat_activity WHERE application_name = %s",
                (APP_NAME,),
            )
            (alive,) = cur.fetchone()
        if alive == 0:
            return
        time.sleep(0.2)
    pytest.fail(f"the killed driver's backend was still present after {timeout}s")


def test_detector_resumes_from_the_last_committed_window():
    _apply_schemas()

    observer = psycopg.connect(CONNINFO, autocommit=True, connect_timeout=10)
    try:
        _seed(observer)
        expected_total = _expected_eligible()

        driver = subprocess.Popen(
            [sys.executable, "-u", "-c", DRIVER.format(
                repo=str(REPO_ROOT), detector=TEST_DETECTOR,
                conninfo=CONNINFO, app=APP_NAME)],
            cwd=REPO_ROOT, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            deadline = time.monotonic() + 120
            while True:
                if driver.poll() is not None:
                    out, err = driver.communicate()
                    pytest.fail(
                        "the driver exited before it could be killed - it either "
                        f"crashed or finished all {expected_total} windows too fast "
                        f"to interrupt.\nstdout:\n{out}\nstderr:\n{err}"
                    )
                committed, _, _ = _scored(observer)
                if committed >= KILL_AFTER_WINDOWS:
                    break
                if time.monotonic() > deadline:
                    pytest.fail(
                        f"only {committed} windows committed within 120s; expected to "
                        f"reach {KILL_AFTER_WINDOWS}"
                    )
                time.sleep(0.05)
        finally:
            driver.kill()
        driver.wait(timeout=30)
        _wait_for_backend_to_clear(observer)

        at_crash, distinct_at_crash, max_at_crash = _scored(observer)

        # The assertion the pre-fix code fails. With every window a
        # SAVEPOINT inside one uncommitted transaction, a kill discards
        # all of them and this is 0.
        assert at_crash > 0, (
            "a killed detector left no committed windows at all, so nothing was "
            "durable per window - this is what a nested-SAVEPOINT scoring loop "
            "looks like (DEFENSE.md #55)"
        )
        assert at_crash < expected_total, (
            f"the detector finished all {expected_total} windows before the kill "
            f"landed, so this run proves nothing about resumability"
        )
        assert at_crash == distinct_at_crash, (
            f"{at_crash} scored rows but only {distinct_at_crash} distinct window_ends - "
            f"a window was scored more than once"
        )

        # The frontier and the scored rows advance in the same
        # transaction, so a crash can never leave them disagreeing.
        assert _frontier(observer) == max_at_crash, (
            f"detector_progress says {_frontier(observer)} but the last committed "
            f"scored window is {max_at_crash}; a crash split the two apart, so "
            f"they are not in one transaction"
        )

        # Resume. Exactly the remainder, no gaps, no repeats.
        resumed = psycopg.connect(CONNINFO, autocommit=False, connect_timeout=10)
        try:
            statuses = run_once(resumed, CONFIG)
        finally:
            resumed.close()

        assert len(statuses) == expected_total - at_crash, (
            f"resumed run processed {len(statuses)} windows; expected exactly the "
            f"{expected_total - at_crash} left after the crash, which is what "
            f"'resumes from the last committed window' means"
        )

        final, distinct_final, _ = _scored(observer)
        assert final == expected_total, (
            f"{final} windows scored across both runs, expected {expected_total}"
        )
        assert distinct_final == expected_total, (
            f"{final} scored rows but {distinct_final} distinct window_ends after "
            f"resuming - the crash caused a window to be scored twice"
        )

        # No separate gap check: the detector only ever scores windows the
        # fetch returned, so "exactly expected_total rows, all distinct"
        # already means every eligible window was scored exactly once.
    finally:
        with observer.cursor() as cur:
            cur.execute(
                "DELETE FROM weir_metrics.window_metrics WHERE window_start >= %s",
                (SEED_START,),
            )
            for table in ("scored_windows", "baseline_state", "incidents",
                          "detector_progress"):
                cur.execute(
                    f"DELETE FROM weir_incidents.{table} WHERE detector_name = %s",
                    (TEST_DETECTOR,),
                )
        observer.close()
