"""Unit tests for reliability/volume/adapter.py's pure functions.

to_local_bucket, is_dst_ambiguous, and is_behind_frontier are kept
I/O-free specifically so they're testable without a live Postgres
connection (adapter.py's own module docstring) - only process_window
and register_detector touch a connection, and aren't covered here.

All instants below are tz-aware UTC datetimes, not naive ones -
datetime.astimezone() on a naive value assumes the *system's* local
zone rather than UTC, which would make to_local_bucket's result
depend on the machine running the test.
"""
import datetime

from reliability.volume.adapter import is_behind_frontier, is_dst_ambiguous, to_local_bucket

UTC = datetime.timezone.utc
NY = "America/New_York"


def utc(*args):
    return datetime.datetime(*args, tzinfo=UTC)


def test_to_local_bucket_converts_utc_to_ny_weekday_and_hour():
    # 2024-10-07 is a Monday. Noon UTC is 08:00 EDT (UTC-4) that day.
    weekday, hour = to_local_bucket(utc(2024, 10, 7, 12, 0, 0), NY)

    assert weekday == 0  # Monday
    assert hour == 8


def test_to_local_bucket_crosses_midnight_into_next_local_day():
    # 2024-10-08 02:00 UTC is 2024-10-07 22:00 EDT - still Monday
    # locally, a day behind the UTC calendar date.
    weekday, hour = to_local_bucket(utc(2024, 10, 8, 2, 0, 0), NY)

    assert weekday == 0
    assert hour == 22


def test_is_dst_ambiguous_true_on_both_occurrences_of_the_repeated_hour():
    # DST fall-back, 2024-11-03: local clocks go from 02:00 EDT back to
    # 01:00 EST, so 01:00-02:00 local happens twice. 05:30 UTC is still
    # EDT (pre-fallback); 06:30 UTC is already EST (post-fallback) -
    # both land on local 01:30, the ambiguous hour, from opposite sides
    # of the transition instant (06:00 UTC).
    assert is_dst_ambiguous(utc(2024, 11, 3, 5, 30, 0), NY) is True
    assert is_dst_ambiguous(utc(2024, 11, 3, 6, 30, 0), NY) is True


def test_is_dst_ambiguous_false_just_outside_the_repeated_hour():
    # 00:30 EDT (before the ambiguous hour starts) and 02:30 EST (after
    # the fallback has fully completed) are each unambiguous.
    assert is_dst_ambiguous(utc(2024, 11, 3, 4, 30, 0), NY) is False
    assert is_dst_ambiguous(utc(2024, 11, 3, 7, 30, 0), NY) is False


def test_is_behind_frontier_true_when_at_or_before_frontier():
    frontier = utc(2024, 10, 15, 0, 0, 0)

    assert is_behind_frontier(frontier, frontier) is True
    assert is_behind_frontier(frontier - datetime.timedelta(minutes=1), frontier) is True


def test_is_behind_frontier_false_when_after_frontier():
    frontier = utc(2024, 10, 15, 0, 0, 0)

    assert is_behind_frontier(frontier + datetime.timedelta(minutes=1), frontier) is False


def test_is_behind_frontier_false_against_epoch_sentinel():
    # detector_progress.last_processed_window_end defaults to the Unix
    # epoch on a freshly registered detector (DEFENSE.md #45) - every
    # real TLC window_end (Oct 2024 onward) must clear it.
    epoch_sentinel = datetime.datetime(1970, 1, 1, tzinfo=UTC)

    assert is_behind_frontier(utc(2024, 10, 1, 0, 0, 0), epoch_sentinel) is False
