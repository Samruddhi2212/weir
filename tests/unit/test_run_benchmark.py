"""Unit tests for benchmarks/run_benchmark.py's scoring logic.

Attribution, latency, prediction and the summary are pure functions over
plain data, so they're tested without Kafka, Flink or Postgres. The
error paths matter as much as the happy ones here: the runner is
required to treat a missing measurement as an error rather than a zero,
and a rate with no denominator as an error rather than 0%.
"""
import datetime

import pytest

from benchmarks.run_benchmark import (
    attribute,
    percentile,
    predict_emitted_count,
    render_markdown,
    summarise,
)
from incidents.benchmark.catalog import DuplicateEventStorm, PartitionDegradation, SlowVolumeDecline
from incidents.benchmark.scenario import PICKUP_COLUMN
from reliability.volume.config import DEFAULT_CONFIG as VOLUME_CONFIG
from reliability.volume.run import to_utc_instant

BASE = datetime.datetime(2025, 1, 6, 0, 0, 0)
SLICE_END = BASE + datetime.timedelta(days=1)


def events(count, step_seconds=60):
    return [
        {"_row_index": i, PICKUP_COLUMN: BASE + datetime.timedelta(seconds=i * step_seconds)}
        for i in range(count)
    ]


def incident(detector_name, event_time, score=9.0):
    return {
        "detector_name": detector_name,
        "window_start": to_utc_instant(event_time - datetime.timedelta(minutes=1), VOLUME_CONFIG.timezone),
        "window_end": to_utc_instant(event_time, VOLUME_CONFIG.timezone),
        "observed_value": 1.0,
        "score": score,
    }


def test_predicted_count_catches_a_scenario_that_does_nothing():
    # A duplicate storm declares it adds copies; if inject() silently did
    # nothing the prediction would no longer match, which is exactly the
    # no-op bug class this check exists for.
    source = events(120)
    storm = DuplicateEventStorm(BASE, BASE + datetime.timedelta(hours=1), copies=3)

    predicted = predict_emitted_count([storm], source)
    actual = len(storm.inject(source))

    assert predicted == actual
    assert predicted > len(source)


def test_predicted_count_accounts_for_drops_and_additions_together():
    source = events(240)
    storm = DuplicateEventStorm(BASE, BASE + datetime.timedelta(hours=1), copies=2)
    degraded = PartitionDegradation(BASE + datetime.timedelta(hours=2),
                                     BASE + datetime.timedelta(hours=3), num_partitions=3)

    predicted = predict_emitted_count([storm, degraded], source)

    # Non-overlapping spans, so the two effects are independent and the
    # prediction is exact rather than approximate.
    from incidents.benchmark.scenario import compose
    assert predicted == len(compose([storm, degraded], source))


def test_attribution_requires_both_the_right_detector_and_the_right_window():
    scenario = PartitionDegradation(BASE + datetime.timedelta(hours=1),
                                     BASE + datetime.timedelta(hours=2))
    inside = incident("volume", BASE + datetime.timedelta(hours=1, minutes=30))
    wrong_detector = incident("freshness", BASE + datetime.timedelta(hours=1, minutes=30))
    outside = incident("volume", BASE + datetime.timedelta(hours=5))

    results, unattributed = attribute([scenario], [inside, wrong_detector, outside], SLICE_END)

    assert results[0]["detected"] is True
    assert results[0]["matching_incident_count"] == 1
    # The other two aren't silently dropped - they're reported.
    assert len(unattributed) == 2


def test_latency_is_measured_from_the_first_attributable_incident():
    starts_at = BASE + datetime.timedelta(hours=1)
    scenario = PartitionDegradation(starts_at, BASE + datetime.timedelta(hours=3))
    late = incident("volume", starts_at + datetime.timedelta(minutes=50))
    early = incident("volume", starts_at + datetime.timedelta(minutes=10))

    results, _ = attribute([scenario], [late, early], SLICE_END)

    assert results[0]["detection_latency_seconds"] == 600.0


def test_undetected_scenario_reports_no_latency_rather_than_zero():
    scenario = PartitionDegradation(BASE, BASE + datetime.timedelta(hours=1))

    results, _ = attribute([scenario], [], SLICE_END)

    assert results[0]["detected"] is False
    assert results[0]["detection_latency_seconds"] is None


def test_expected_miss_still_appears_in_the_results():
    miss = SlowVolumeDecline(BASE, BASE + datetime.timedelta(weeks=4), weekly_decline_fraction=0.05)

    results, _ = attribute([miss], [], SLICE_END)

    assert len(results) == 1
    assert results[0]["expected_detector"] is None
    assert results[0]["detected"] is False


def test_percentile_is_nearest_rank():
    assert percentile([10, 20, 30, 40], 0.95) == 40
    assert percentile([10, 20, 30, 40], 0.5) == 20
    assert percentile([], 0.95) is None


def test_zero_scored_windows_is_an_error_not_a_zero_percent_fp_rate():
    results, _ = attribute([], [], SLICE_END)

    with pytest.raises(SystemExit):
        summarise(results, clean_incidents=[], clean_scored=0, clean_hours=1.0, unattributed=[])


def test_detected_scenario_without_a_latency_is_an_error():
    broken = [{
        "name": "broken", "expected_detector": "volume", "detected": True,
        "detection_latency_seconds": None,
    }]

    with pytest.raises(SystemExit):
        summarise(broken, clean_incidents=[], clean_scored=100, clean_hours=1.0, unattributed=[])


def test_summary_reports_exact_fractions_alongside_rates():
    scenario = PartitionDegradation(BASE, BASE + datetime.timedelta(hours=2))
    miss = SlowVolumeDecline(BASE, BASE + datetime.timedelta(weeks=4), weekly_decline_fraction=0.05)
    results, _ = attribute(
        [scenario, miss], [incident("volume", BASE + datetime.timedelta(minutes=30))], SLICE_END
    )

    summary = summarise(results, clean_incidents=[incident("volume", BASE)], clean_scored=200,
                        clean_hours=3.5, unattributed=[])

    assert summary["detection_rate_fraction"] == "1/2"
    assert summary["detection_rate"] == 0.5
    assert summary["false_positive_rate_fraction"] == "1/200"
    assert summary["false_positive_rate"] == 0.005
    assert summary["clean_event_time_hours"] == 3.5
    # Targeted-only rate is reported separately so the expected misses
    # in the denominator can't be mistaken for detector weakness.
    assert summary["targeted_detection_rate_fraction"] == "1/1"


def test_markdown_labels_expected_misses_and_does_not_round_rates_away():
    scenario = PartitionDegradation(BASE, BASE + datetime.timedelta(hours=2))
    miss = SlowVolumeDecline(BASE, BASE + datetime.timedelta(weeks=4), weekly_decline_fraction=0.05)
    results, _ = attribute(
        [scenario, miss], [incident("volume", BASE + datetime.timedelta(minutes=30))], SLICE_END
    )
    # 3/7 is 42.857142...% - a rate that rounding would flatter.
    summary = summarise(results, clean_incidents=[], clean_scored=7, clean_hours=1.0, unattributed=[])
    summary["detection_rate"] = 3 / 7
    summary["detection_rate_fraction"] = "3/7"

    table = render_markdown(results, summary)

    assert "| volume_partition_degradation | yes | 1800s |" in table
    assert "no (expected miss)" in table
    assert "3/7" in table
    assert "42.857" in table, "the rate was rounded away"
