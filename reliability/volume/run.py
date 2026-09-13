"""Part 4.2: single-pass runner for the volume detector - DEFENSE.md #46.

One poll: fetch weir_metrics.window_metrics rows not yet safely past
detector_progress's frontier, convert each from window_metrics' own
naive America/New_York local-civil-time timestamp into the true UTC
instant adapter.process_window requires, and process them through it
in ascending window_end order. Not a daemon - scheduling repeated
polls is a deployment concern, deferred with the rest of Sprint 1's
deployment scope (docs/FUTURE_WORK.md).
"""
import argparse
import datetime
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

# Running this file directly (`python reliability/volume/run.py`), its
# own documented entry point, only puts reliability/volume/ itself on
# sys.path, not the repo root - the same class of bug already fixed in
# verify_volume_detector.py and incidents/dev/volume_drop.py
# (DEFENSE.md #48/#47 addenda), caught here proactively before this
# file's own first direct invocation hit it too. Harmless no-op when
# this module is imported normally instead (pytest, or another script
# that already put the repo root on sys.path).
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from reliability.volume.adapter import process_window, register_detector
from reliability.volume.config import DEFAULT_CONFIG


def to_utc_instant(naive_local_dt, timezone_name):
    """Attaches timezone_name to a naive local-civil-time datetime and
    converts to a true UTC instant. `.replace()`, not `.astimezone()`
    alone - naive_local_dt already IS local wall-clock time, not UTC
    misread as local (DEFENSE.md #46)."""
    return naive_local_dt.replace(tzinfo=ZoneInfo(timezone_name)).astimezone(datetime.timezone.utc)


def to_naive_local(utc_instant, timezone_name):
    """Inverse of to_utc_instant - used only to translate
    detector_progress's UTC frontier into window_metrics' own naive
    local terms for the same-type SQL filter in fetch_eligible_windows."""
    return utc_instant.astimezone(ZoneInfo(timezone_name)).replace(tzinfo=None)


def fetch_eligible_windows(conn, config, assume_no_more_arrivals=False):
    """Rows from window_metrics behind detector_progress's frontier,
    filtered by the lag buffer, ascending by window_end (DEFENSE.md
    #45 point 2 / #46). Purely an efficiency filter in the live-
    polling case - a coarse or slightly-wrong result here just defers
    a row to the next run, never processes one incorrectly.

    assume_no_more_arrivals=True skips the lag-buffer filter entirely
    (DEFENSE.md #51) - only correct when the caller genuinely knows no
    further writes are coming for this range, e.g. verifying against
    an already-fully-loaded historical month. The default (False)
    preserves live-polling's write-order protection unchanged."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT last_processed_window_end FROM weir_incidents.detector_progress "
            "WHERE detector_name = %s",
            (config.detector_name,),
        )
        row = cur.fetchone()
        if row is None:
            raise RuntimeError(
                f"detector {config.detector_name!r} is not registered - call register_detector first"
            )
        frontier_naive_local = to_naive_local(row[0], config.timezone)

        cur.execute(
            "SELECT window_start, window_end, row_count FROM weir_metrics.window_metrics "
            "WHERE window_end > %s ORDER BY window_end ASC",
            (frontier_naive_local,),
        )
        rows = cur.fetchall()

    if not rows or assume_no_more_arrivals:
        return rows

    max_window_end_seen = max(r[1] for r in rows)
    lag_buffer = datetime.timedelta(seconds=config.max_lag_seconds)
    return [r for r in rows if r[1] <= max_window_end_seen - lag_buffer]


def run_once(conn, config=DEFAULT_CONFIG, assume_no_more_arrivals=False, progress=None):
    """Registers the detector if needed, then processes every
    currently eligible window in order. Returns the list of statuses
    written, one per window processed. See fetch_eligible_windows for
    assume_no_more_arrivals.

    progress, if given, is called as progress(done, total) after each
    window. Purely observational - a live poll processes a handful of
    windows and needs nothing, but the benchmark processes six figures
    of them in one call, where a silent loop is indistinguishable from
    a hung one."""
    register_detector(conn, config.detector_name)

    eligible = fetch_eligible_windows(
        conn, config, assume_no_more_arrivals=assume_no_more_arrivals
    )
    statuses = []
    for window_start_naive, window_end_naive, row_count in eligible:
        window_start_utc = to_utc_instant(window_start_naive, config.timezone)
        window_end_utc = to_utc_instant(window_end_naive, config.timezone)
        status = process_window(conn, window_start_utc, window_end_utc, float(row_count), config)
        statuses.append(status)
        if progress is not None:
            progress(len(statuses), len(eligible))
    return statuses


def main():
    # Local import, not module-level: keeps the pure functions above
    # (and fetch_eligible_windows/run_once, which only need a conn
    # object, never psycopg itself) importable/testable without the
    # psycopg dependency, matching adapter.py's own I/O-free design.
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    parser.add_argument(
        "--assume-no-more-arrivals", action="store_true",
        help="Skip the lag buffer entirely - only correct against an already-fully-loaded, "
             "static dataset (e.g. a historical backfill), never a live stream (DEFENSE.md #51).",
    )
    args = parser.parse_args()

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )
    conn = psycopg.connect(conninfo, autocommit=False, connect_timeout=10)
    try:
        statuses = run_once(conn, assume_no_more_arrivals=args.assume_no_more_arrivals)
        print(f"processed {len(statuses)} window(s): {statuses}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
