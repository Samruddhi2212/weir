"""Unit tests for the benchmark failure catalog (Part 5.1).

Pure event-stream transforms, so every scenario is testable against
synthetic events with no Kafka, Flink, or Postgres - the same reason
detector.py's scoring logic is kept I/O-free.

Two properties matter across the whole catalog, not just per scenario:
inject() must not mutate the caller's events (composition and re-use
both depend on it), and every scenario must be deterministic (a
benchmark that can't be reproduced can't be published).
"""
import datetime

import pytest

from incidents.benchmark.catalog import (
    CATALOG,
    EXPECTED_MISSES,
    MEASURED_LATENESS_QUANTILES,
    DuplicateEventStorm,
    GradualDelay,
    NullRateCreep,
    OutOfOrderFlood,
    PartitionDegradation,
    SlowVolumeDecline,
    UpstreamBackfillReplay,
)
from incidents.benchmark.scenario import PICKUP_COLUMN, Severity, compose, simulated_partition

BASE = datetime.datetime(2025, 1, 6, 0, 0, 0)


def events(count, start=BASE, step_seconds=60, **fields):
    return [
        {
            "_row_index": i,
            PICKUP_COLUMN: start + datetime.timedelta(seconds=i * step_seconds),
            "passenger_count": 1,
            **fields,
        }
        for i in range(count)
    ]


def all_scenarios():
    """One configured instance of every catalog entry."""
    span_end = BASE + datetime.timedelta(hours=4)
    return [
        PartitionDegradation(BASE, span_end),
        GradualDelay(BASE, span_end, max_delay_seconds=600),
        NullRateCreep(BASE, span_end, column="passenger_count", start_rate=0.01, end_rate=0.5),
        DuplicateEventStorm(BASE, span_end, copies=3),
        OutOfOrderFlood(BASE, span_end),
        UpstreamBackfillReplay(
            BASE + datetime.timedelta(hours=2),
            backfill_age=datetime.timedelta(hours=2),
            backfill_span=datetime.timedelta(minutes=30),
        ),
        SlowVolumeDecline(BASE, BASE + datetime.timedelta(weeks=6), weekly_decline_fraction=0.05),
    ]


def test_catalog_covers_every_scenario_class():
    assert len(CATALOG) == 7
    assert len(all_scenarios()) == len(CATALOG)
    assert {type(s) for s in all_scenarios()} == set(CATALOG)


def test_expected_misses_are_exactly_the_four_untargeted_failures():
    assert {s.name for s in EXPECTED_MISSES} == {
        "duplicate_event_storm",
        "out_of_order_flood",
        "upstream_backfill_replay",
        "slow_volume_decline",
    }


def test_every_scenario_declares_its_ground_truth():
    for scenario in all_scenarios():
        truth = scenario.ground_truth()
        assert truth["name"]
        assert truth["starts_at"] == scenario.starts_at
        assert truth["affected_dataset"]
        assert truth["observable_in"]
        assert truth["severity"] in {s.value for s in Severity}


def test_nullrate_creep_is_not_an_expected_miss():
    # Its expected_detector is resolved per instance; a class-level None
    # would have silently filed a targeted failure as an expected miss.
    assert NullRateCreep not in EXPECTED_MISSES
    scenario = NullRateCreep(BASE, BASE + datetime.timedelta(hours=4),
                              column="passenger_count", start_rate=0.0, end_rate=0.4)
    assert scenario.expected_detector == "null_rate_passenger_count"


@pytest.mark.parametrize("scenario", all_scenarios(), ids=lambda s: s.name)
def test_inject_does_not_mutate_the_caller_events(scenario):
    original = events(400)
    snapshot = [dict(e) for e in original]

    scenario.inject(original)

    assert original == snapshot


@pytest.mark.parametrize("scenario", all_scenarios(), ids=lambda s: s.name)
def test_inject_is_deterministic(scenario):
    first = scenario.inject(events(400))
    second = scenario.inject(events(400))

    assert first == second


def test_partition_degradation_drops_only_one_partition_and_leaves_the_rest():
    scenario = PartitionDegradation(BASE, BASE + datetime.timedelta(hours=1),
                                     num_partitions=3, degraded_partition=1)
    source = events(600)

    kept = scenario.inject(source)

    dropped = [e for e in source if e not in kept]
    assert dropped, "nothing was dropped - the scenario did nothing"
    assert all(simulated_partition(e, 3) == 1 for e in dropped)
    # Everything outside the degraded partition survives untouched.
    assert [e for e in source if simulated_partition(e, 3) != 1] == \
           [e for e in kept if simulated_partition(e, 3) != 1]


def test_partition_degradation_spares_events_outside_its_span():
    scenario = PartitionDegradation(BASE + datetime.timedelta(hours=1), None, num_partitions=3)
    source = events(120)  # two hours at one event/minute

    kept = scenario.inject(source)

    before = [e for e in source if e[PICKUP_COLUMN] < scenario.starts_at]
    assert all(e in kept for e in before)


def test_gradual_delay_ramps_and_drops_only_past_the_watermark_bound():
    scenario = GradualDelay(BASE, BASE + datetime.timedelta(hours=4), max_delay_seconds=600)

    early = scenario.delay_for(BASE + datetime.timedelta(minutes=6))
    late = scenario.delay_for(BASE + datetime.timedelta(hours=3))
    assert datetime.timedelta(0) < early < late <= datetime.timedelta(seconds=600)

    source = events(300)
    kept = scenario.inject(source)

    # Under the 270s bound the failure is invisible by design: those
    # events survive untouched, with their event times unchanged.
    for event in source:
        if scenario.delay_for(event[PICKUP_COLUMN]).total_seconds() <= 270:
            assert event in kept
    # Past the bound they're dropped, which is what thins the windows.
    dropped = [e for e in source if e not in kept]
    assert dropped, "the ramp never crossed the watermark bound - scenario did nothing"
    assert all(scenario.delay_for(e[PICKUP_COLUMN]).total_seconds() > 270 for e in dropped)


def test_gradual_delay_under_the_bound_is_a_deliberate_no_op():
    # A ramp that never crosses the bound changes nothing downstream -
    # stated as a property, not left as a surprise in a benchmark result.
    scenario = GradualDelay(BASE, BASE + datetime.timedelta(hours=4), max_delay_seconds=60)
    source = events(300)

    assert scenario.inject(source) == source


def test_nullrate_creep_rate_rises_and_touches_only_its_column():
    scenario = NullRateCreep(BASE, BASE + datetime.timedelta(hours=4),
                              column="passenger_count", start_rate=0.0, end_rate=1.0)
    source = events(240, trip_distance=3.0)

    out = scenario.inject(source)

    first_hour = [e for e in out if e[PICKUP_COLUMN] < BASE + datetime.timedelta(hours=1)]
    last_hour = [e for e in out if e[PICKUP_COLUMN] >= BASE + datetime.timedelta(hours=3)]
    nulls = lambda batch: sum(1 for e in batch if e["passenger_count"] is None)
    assert nulls(first_hour) < nulls(last_hour)
    assert all(e["trip_distance"] == 3.0 for e in out), "an unrelated column was modified"


def test_duplicate_storm_multiplies_only_inside_its_span():
    scenario = DuplicateEventStorm(BASE, BASE + datetime.timedelta(hours=1), copies=3)
    source = events(120)

    out = scenario.inject(source)

    in_span = [e for e in source if e[PICKUP_COLUMN] < BASE + datetime.timedelta(hours=1)]
    assert len(out) == len(source) + 2 * len(in_span)
    # Duplicates carry the same key: that is what makes them duplicates
    # rather than new traffic.
    keys = [e["_row_index"] for e in out]
    assert keys.count(in_span[0]["_row_index"]) == 3


def test_out_of_order_flood_uses_only_the_measured_quantile_values():
    scenario = OutOfOrderFlood(BASE, BASE + datetime.timedelta(hours=2))
    source = events(2000)

    measured = {seconds for _, seconds in MEASURED_LATENESS_QUANTILES}
    drawn = {scenario.lateness_seconds(e) for e in source}

    assert drawn, "no lateness was drawn"
    assert drawn <= measured, f"invented a lateness value not in DEFENSE.md #40: {drawn - measured}"


def test_out_of_order_flood_reorders_without_losing_events():
    scenario = OutOfOrderFlood(BASE, BASE + datetime.timedelta(hours=2))
    source = events(500)

    out = scenario.inject(source)

    assert len(out) == len(source)
    assert sorted(e["_row_index"] for e in out) == sorted(e["_row_index"] for e in source)


def test_backfill_replays_old_event_times_into_the_current_stream():
    starts_at = BASE + datetime.timedelta(hours=2)
    scenario = UpstreamBackfillReplay(
        starts_at,
        backfill_age=datetime.timedelta(hours=2),
        backfill_span=datetime.timedelta(minutes=30),
    )
    source = events(240)

    out = scenario.inject(source)

    # 30 one-minute events fall in the replayed half-hour.
    assert len(out) == len(source) + 30
    # The copies sit in send order where the failure starts - after the
    # 120 originals that precede starts_at - while still carrying their
    # original, now-stale event times. Located by position, not by
    # timestamp: their timestamps are old, which is the whole point.
    replayed = out[120:150]
    assert all(e[PICKUP_COLUMN] < starts_at - scenario.backfill_age + scenario.backfill_span
               for e in replayed)
    assert out[150][PICKUP_COLUMN] >= starts_at, "the live stream should resume after the splice"


def test_slow_decline_drops_more_as_weeks_pass():
    scenario = SlowVolumeDecline(BASE, BASE + datetime.timedelta(weeks=8),
                                  weekly_decline_fraction=0.1)

    assert scenario.dropped_fraction_at(BASE) == 0.0
    week_one = scenario.dropped_fraction_at(BASE + datetime.timedelta(weeks=1))
    week_five = scenario.dropped_fraction_at(BASE + datetime.timedelta(weeks=5))
    assert week_one == pytest.approx(0.1)
    assert week_five == pytest.approx(0.5)

    # Clamped at total loss rather than running past 1.0 - checked
    # inside the span, since past ends_at the decline has stopped and
    # the stream is healthy again (0.0), not maximally degraded.
    long_run = SlowVolumeDecline(BASE, BASE + datetime.timedelta(weeks=20),
                                  weekly_decline_fraction=0.1)
    assert long_run.dropped_fraction_at(BASE + datetime.timedelta(weeks=15)) == 1.0
    assert long_run.dropped_fraction_at(BASE + datetime.timedelta(weeks=25)) == 0.0


def test_scenarios_compose_in_one_replay():
    span_end = BASE + datetime.timedelta(hours=2)
    storm = DuplicateEventStorm(BASE, span_end, copies=2)
    degradation = PartitionDegradation(BASE, span_end, num_partitions=3, degraded_partition=0)
    source = events(300)

    composed = compose([storm, degradation], source)

    # Both applied: inflated by the storm, then thinned by the partition loss.
    assert len(composed) > len(degradation.inject(source))
    assert len(composed) < len(storm.inject(source))


def test_composition_order_is_observable_not_incidental():
    # Key-determined drops and duplications commute, so those pairs
    # compose in any order. A globally re-sorting scenario does not: it
    # erases a positional splice. That's the case the runner has to
    # record its composition order for.
    backfill = UpstreamBackfillReplay(
        BASE + datetime.timedelta(hours=2),
        backfill_age=datetime.timedelta(hours=2),
        backfill_span=datetime.timedelta(minutes=30),
    )
    flood = OutOfOrderFlood(BASE + datetime.timedelta(hours=1), BASE + datetime.timedelta(hours=3))
    source = events(240)

    backfill_then_flood = compose([backfill, flood], source)
    flood_then_backfill = compose([flood, backfill], source)

    assert len(backfill_then_flood) == len(flood_then_backfill)
    # Splicing first, then re-sorting, scatters the replayed copies back
    # to their old chronological position; splicing last leaves them
    # sitting where the backfill actually delivered them.
    assert backfill_then_flood != flood_then_backfill
    assert all(e[PICKUP_COLUMN] < backfill.starts_at for e in flood_then_backfill[120:150])


def test_scenarios_reject_impossible_configuration():
    with pytest.raises(ValueError):
        PartitionDegradation(BASE, BASE - datetime.timedelta(hours=1))
    with pytest.raises(ValueError):
        GradualDelay(BASE, BASE + datetime.timedelta(hours=1), max_delay_seconds=0)
    with pytest.raises(ValueError):
        NullRateCreep(BASE, BASE + datetime.timedelta(hours=1), column="passenger_count",
                       start_rate=0.5, end_rate=0.1)
    with pytest.raises(ValueError):
        DuplicateEventStorm(BASE, BASE + datetime.timedelta(hours=1), copies=1)
    with pytest.raises(ValueError):
        SlowVolumeDecline(BASE, BASE + datetime.timedelta(weeks=4), weekly_decline_fraction=1.5)
