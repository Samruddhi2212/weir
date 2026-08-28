"""Unit tests for reliability/volume/run.py's pure timezone-boundary
functions (DEFENSE.md #46). fetch_eligible_windows/run_once need a
live Postgres connection and aren't covered here - only the
naive-local <-> UTC conversion, which is the part most likely to be
silently wrong in a way nothing else would catch.
"""
import datetime

from reliability.volume.adapter import is_dst_ambiguous
from reliability.volume.run import to_naive_local, to_utc_instant

NY = "America/New_York"


def naive(*args):
    return datetime.datetime(*args)


def test_to_utc_instant_during_edt():
    # 2024-10-07 08:00 is EDT (UTC-4).
    result = to_utc_instant(naive(2024, 10, 7, 8, 0, 0), NY)

    assert result == datetime.datetime(2024, 10, 7, 12, 0, 0, tzinfo=datetime.timezone.utc)


def test_to_utc_instant_during_est():
    # 2024-12-07 08:00 is EST (UTC-5) - after the fall-back has
    # completed, a different offset than the EDT case above.
    result = to_utc_instant(naive(2024, 12, 7, 8, 0, 0), NY)

    assert result == datetime.datetime(2024, 12, 7, 13, 0, 0, tzinfo=datetime.timezone.utc)


def test_to_naive_local_is_the_inverse_of_to_utc_instant():
    original = naive(2024, 10, 7, 8, 0, 0)

    round_tripped = to_naive_local(to_utc_instant(original, NY), NY)

    assert round_tripped == original


def test_to_utc_instant_on_dst_fallback_ambiguous_hour_still_gets_excluded():
    # 2024-11-03 01:30 local happens twice (fall-back). to_utc_instant
    # must produce SOME well-defined UTC instant either way - it isn't
    # required to pick the "objectively correct" one, only to land
    # somewhere adapter.is_dst_ambiguous will still catch on the
    # round trip (DEFENSE.md #46's whole point).
    ambiguous_local = naive(2024, 11, 3, 1, 30, 0)

    utc_instant = to_utc_instant(ambiguous_local, NY)

    assert is_dst_ambiguous(utc_instant, NY) is True
    assert to_naive_local(utc_instant, NY) == ambiguous_local


def test_to_utc_instant_just_outside_the_ambiguous_hour_is_unambiguous():
    before = to_utc_instant(naive(2024, 11, 3, 0, 30, 0), NY)
    after = to_utc_instant(naive(2024, 11, 3, 2, 30, 0), NY)

    assert is_dst_ambiguous(before, NY) is False
    assert is_dst_ambiguous(after, NY) is False
