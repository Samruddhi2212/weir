"""Part 5.1: the failure catalog.

Two groups, both of which belong in the published results table:

1. Variants the detectors were NOT designed against. `incidents/dev/`
   already covers each detector's designed-for shape (sudden drop,
   complete gap, sudden spike); repeating those here would measure how
   well each detector catches the case it was built from, which is not
   a measurement worth publishing. Each variant below is the same
   failure *class* in a shape the detector's design never considered.

2. Failures no detector targets at all - expected misses. They are in
   the catalog precisely so the results table reports them as misses
   rather than quietly omitting them. An honest detection rate needs
   the denominator to include what this system cannot see.

Per hard rule 3, nothing here is permitted to drive a detector change:
if a detector misses one of these, it misses, and the miss is reported.
"""
import datetime

from incidents.benchmark.scenario import (
    PICKUP_COLUMN,
    Scenario,
    Severity,
    simulated_partition,
    stable_uniform,
)

# Measured, not modelled: scripts/measure_lateness.py through live Kafka
# (N=2000, --shuffle-window 50, --shuffle-seed 42) against the real
# January 2025 dataset, run 32672957098 - DEFENSE.md #40. Five real
# quantile points, used as a step ladder. The step assignment BETWEEN
# these points is a stated construction, not a measurement; the points
# themselves are not invented (hard rule 1).
MEASURED_LATENESS_QUANTILES = (
    (0.50, 12.0),
    (0.95, 41.0),
    (0.99, 270.0),
    (0.999, 11309.0),
    (1.0, 11601.0),
)

# metrics_job.sql's watermark bound (DEFENSE.md #40/#42). Lateness past
# this is dropped by Flink rather than absorbed into its window - the
# boundary that decides whether a delay is observable downstream at all.
WATERMARK_BOUND_SECONDS = 270


class PartitionDegradation(Scenario):
    """Volume variant: one Kafka partition degrades, the rest stay healthy.

    Designed-for case (incidents/dev/volume_drop.py) is a sudden drop of
    the whole stream. This is the partial, structurally-shaped version:
    a fixed ~1/N of traffic disappears - 1/3 at this project's
    KAFKA_NUM_PARTITIONS=3 - while two thirds of the stream looks
    completely normal. Harder than the designed-for case because the
    surviving volume is still plausible for the bucket, and the loss is
    keyed rather than random, so it doesn't average out.
    """

    name = "volume_partition_degradation"
    affected_column = None
    observable_in = "weir_metrics.window_metrics.row_count"
    severity = Severity.HIGH
    expected_detector = "volume"

    def __init__(self, starts_at, ends_at=None, num_partitions=3, degraded_partition=0,
                 drop_fraction=1.0):
        super().__init__(starts_at, ends_at)
        if not 0.0 < drop_fraction <= 1.0:
            raise ValueError(f"drop_fraction must be in (0, 1], got {drop_fraction!r}")
        if not 0 <= degraded_partition < num_partitions:
            raise ValueError(
                f"degraded_partition {degraded_partition} outside 0..{num_partitions - 1}"
            )
        self.num_partitions = num_partitions
        self.degraded_partition = degraded_partition
        self.drop_fraction = drop_fraction

    def inject(self, events):
        kept = []
        for event in events:
            degraded = (
                self.covers(event[PICKUP_COLUMN])
                and simulated_partition(event, self.num_partitions) == self.degraded_partition
                and stable_uniform(event, "partition_drop") < self.drop_fraction
            )
            if not degraded:
                kept.append(event)
        return kept


class GradualDelay(Scenario):
    """Freshness variant: arrival delay ramps up instead of stopping dead.

    Designed-for case (incidents/dev/freshness_gap.py) is a complete
    gap - data stops, then resumes. Here delay grows linearly from zero
    to max_delay_seconds across the span, which is what a degrading
    upstream actually looks like before it fails outright.

    Event times are untouched - the failure is when data arrives, not
    what it claims. Two consequences, both worth stating because the
    first one makes a whole implementation approach useless:

    A monotonically increasing delay reorders nothing. `t + delay(t)` is
    strictly increasing whenever delay rises smoothly, so "sort by
    virtual send time" returns the original order exactly - a first
    version of this scenario did that and was a silent no-op. Genuine
    reordering needs delay *variance*, which is OutOfOrderFlood's job,
    not a ramp's.

    What a ramp actually does downstream is drop data. Under the
    watermark bound (270s, DEFENSE.md #40) Flink still assigns each
    event to its correct window and window_metrics looks *identical* -
    the failure is invisible by design. Once the ramp crosses the bound,
    late events are dropped, thinning and then erasing windows, which is
    what widens the arrival gaps the freshness detector reads. So this
    models the observable consequence: events whose ramped delay exceeds
    the bound don't make it. max_delay_seconds must exceed
    WATERMARK_BOUND_SECONDS for the scenario to do anything at all.
    """

    name = "freshness_gradual_delay"
    affected_column = None
    observable_in = "weir_metrics.window_metrics.window_end"
    severity = Severity.HIGH
    expected_detector = "freshness"

    def __init__(self, starts_at, ends_at, max_delay_seconds):
        super().__init__(starts_at, ends_at)
        if max_delay_seconds <= 0:
            raise ValueError(f"max_delay_seconds must be positive, got {max_delay_seconds!r}")
        self.max_delay_seconds = max_delay_seconds

    def delay_for(self, event_time):
        if not self.covers(event_time):
            return datetime.timedelta(0)
        return datetime.timedelta(seconds=self.max_delay_seconds * self.progress(event_time))

    def inject(self, events):
        return [
            event for event in events
            if self.delay_for(event[PICKUP_COLUMN]).total_seconds() <= WATERMARK_BOUND_SECONDS
        ]


class NullRateCreep(Scenario):
    """Null-rate variant: the rate creeps up over hours, never spikes.

    Designed-for case (incidents/dev/nullrate_spike.py) is a sudden jump
    in one window. A creep is harder for two separate reasons worth
    keeping distinct when reading results: within an hour the rate is
    nearly flat, so there's no step change to catch; and across hours
    each (weekday, hour) bucket is a *different* baseline, so the early
    hours present a barely-elevated rate to their own bucket and the
    late hours a strongly-elevated one. Partial detection - later hours
    flagged, earlier ones missed - is the expected shape here, not a
    clean hit or a clean miss.
    """

    name = "nullrate_slow_creep"
    observable_in = "weir_metrics.column_metrics.null_count"
    severity = Severity.MEDIUM
    # Template at class level, resolved to the concrete detector name
    # per instance in __init__ - this is NOT an expected miss, and a
    # class-level None here would put it in EXPECTED_MISSES.
    expected_detector = "null_rate_<column>"

    def __init__(self, starts_at, ends_at, column, start_rate, end_rate):
        super().__init__(starts_at, ends_at)
        for label, rate in (("start_rate", start_rate), ("end_rate", end_rate)):
            if not 0.0 <= rate <= 1.0:
                raise ValueError(f"{label} must be in [0, 1], got {rate!r}")
        if end_rate <= start_rate:
            raise ValueError(f"end_rate {end_rate!r} must exceed start_rate {start_rate!r} for a creep")
        self.affected_column = column
        self.expected_detector = f"null_rate_{column}"
        self.start_rate = start_rate
        self.end_rate = end_rate

    def rate_at(self, event_time):
        if not self.covers(event_time):
            return 0.0
        return self.start_rate + (self.end_rate - self.start_rate) * self.progress(event_time)

    def inject(self, events):
        out = []
        for event in events:
            rate = self.rate_at(event[PICKUP_COLUMN])
            if rate and stable_uniform(event, "null_creep") < rate:
                event = dict(event)
                event[self.affected_column] = None
            out.append(event)
        return out


class DuplicateEventStorm(Scenario):
    """Expected miss: the same events are delivered several times.

    Same key, same event time, same payload - exactly what an upstream
    retry storm or a misconfigured at-least-once producer emits. Nothing
    in this system identifies duplicates: there is no uniqueness or
    dedup detector, and the pipeline's exactly-once guarantee covers
    failure-retry inside Flink, not distinct produce calls carrying the
    same key.

    The volume detector may *incidentally* flag the inflated row_count,
    since it scores |z| in both directions. That is not detection of
    this failure: it reports "volume is unusually high", not "these
    records are duplicates", and an operator reading it would look for
    a traffic surge. Scored as a miss for that reason.
    """

    name = "duplicate_event_storm"
    affected_column = None
    observable_in = "weir_metrics.window_metrics.row_count"
    severity = Severity.MEDIUM
    expected_detector = None

    def __init__(self, starts_at, ends_at, copies=3):
        super().__init__(starts_at, ends_at)
        if copies < 2:
            raise ValueError(f"copies must be at least 2 to be a duplicate, got {copies!r}")
        self.copies = copies

    def inject(self, events):
        out = []
        for event in events:
            out.append(event)
            if self.covers(event[PICKUP_COLUMN]):
                out.extend(dict(event) for _ in range(self.copies - 1))
        return out


class OutOfOrderFlood(Scenario):
    """Expected miss: a burst of badly out-of-order arrivals.

    Lateness is drawn from the real measured distribution
    (MEASURED_LATENESS_QUANTILES, DEFENSE.md #40) rather than an
    invented one, so the flood's shape is the shape this pipeline has
    actually been observed to produce: half the events within 12s, but a
    0.1% tail out past three hours, driven by how unevenly real trips
    arrive overnight versus at rush hour.

    Expected miss because no detector targets ordering or event delay -
    that is the deferred event-delay drift detector (docs/FUTURE_WORK.md),
    blocked on the lateness columns never being populated. What this
    scenario does produce downstream is silent loss: at the measured p99
    bound, DEFENSE.md #40 recorded 1.0% of events dropped as too-late.
    """

    name = "out_of_order_flood"
    affected_column = None
    observable_in = "weir_metrics.window_metrics.row_count"
    severity = Severity.MEDIUM
    expected_detector = None

    def __init__(self, starts_at, ends_at):
        super().__init__(starts_at, ends_at)

    def lateness_seconds(self, event):
        draw = stable_uniform(event, "lateness")
        for quantile, seconds in MEASURED_LATENESS_QUANTILES:
            if draw < quantile:
                return seconds
        return MEASURED_LATENESS_QUANTILES[-1][1]

    def inject(self, events):
        def send_time(event):
            event_time = event[PICKUP_COLUMN]
            if not self.covers(event_time):
                return event_time
            return event_time + datetime.timedelta(seconds=self.lateness_seconds(event))

        return sorted(events, key=send_time)


class UpstreamBackfillReplay(Scenario):
    """Expected miss: an upstream job replays an old span of data.

    Real records, real keys, their ORIGINAL old event times, re-sent at
    the current position in the stream - what a backfill or a corrected
    re-export actually looks like from downstream.

    Expected miss, but for a more interesting reason than "no detector
    looks": these land far behind the watermark, so Flink drops them,
    and any that do reach a detector land behind its frontier and are
    recorded `skipped_late` (DEFENSE.md #45). They are *accounted* for,
    per V8 - visible in scored_windows, never silently dropped - just
    never flagged as an incident. A benchmark that reported this as an
    unexplained miss would be hiding the fact that the system saw them
    and classified them.
    """

    name = "upstream_backfill_replay"
    affected_column = None
    observable_in = "weir_incidents.scored_windows.status"
    severity = Severity.LOW
    expected_detector = None

    def __init__(self, starts_at, backfill_age, backfill_span):
        super().__init__(starts_at, ends_at=None)
        if backfill_age.total_seconds() <= 0:
            raise ValueError(f"backfill_age must be positive, got {backfill_age!r}")
        if backfill_span.total_seconds() <= 0:
            raise ValueError(f"backfill_span must be positive, got {backfill_span!r}")
        self.backfill_age = backfill_age
        self.backfill_span = backfill_span

    def inject(self, events):
        replay_from = self.starts_at - self.backfill_age
        replay_until = replay_from + self.backfill_span
        replayed = [
            dict(event) for event in events
            if replay_from <= event[PICKUP_COLUMN] < replay_until
        ]

        out = []
        spliced = False
        for event in events:
            if not spliced and event[PICKUP_COLUMN] >= self.starts_at:
                out.extend(replayed)
                spliced = True
            out.append(event)
        if not spliced:
            out.extend(replayed)
        return out


class SlowVolumeDecline(Scenario):
    """Expected miss: volume declines slowly enough that the baseline follows it.

    The EWMA's decay is derived from half_life_weeks=8 (DEFENSE.md #44),
    so a change spread over a comparable or longer horizon is absorbed
    into the baseline rather than scored against it: every window looks
    normal relative to a mean that has been quietly sliding down with
    it. This is the structural cost of the design choice #44 made -
    it names the tradeoff as "reacting slower to genuine regime shifts"
    and rejects a flat rolling average partly for the opposite problem.

    Worth carrying the highest severity of the expected misses: it is
    sustained, silent, real data loss that by construction never alerts,
    and the longer it runs the better it hides.
    """

    name = "slow_volume_decline"
    affected_column = None
    observable_in = "weir_metrics.window_metrics.row_count"
    severity = Severity.HIGH
    expected_detector = None

    def __init__(self, starts_at, ends_at, weekly_decline_fraction):
        super().__init__(starts_at, ends_at)
        if not 0.0 < weekly_decline_fraction < 1.0:
            raise ValueError(
                f"weekly_decline_fraction must be in (0, 1), got {weekly_decline_fraction!r}"
            )
        self.weekly_decline_fraction = weekly_decline_fraction

    def dropped_fraction_at(self, event_time):
        """Cumulative, linear in elapsed weeks - stated plainly rather
        than compounded, so the scenario's own parameter reads directly
        as 'this much of the stream is gone per week'."""
        if not self.covers(event_time):
            return 0.0
        weeks = (event_time - self.starts_at).total_seconds() / (7 * 24 * 3600)
        return min(self.weekly_decline_fraction * weeks, 1.0)

    def inject(self, events):
        kept = []
        for event in events:
            fraction = self.dropped_fraction_at(event[PICKUP_COLUMN])
            if fraction and stable_uniform(event, "decline") < fraction:
                continue
            kept.append(event)
        return kept


# Every scenario class, for the runner to enumerate. Classes, not
# instances: ground-truth timestamps are chosen against whatever slice
# is actually being replayed, never fixed here.
CATALOG = (
    PartitionDegradation,
    GradualDelay,
    NullRateCreep,
    DuplicateEventStorm,
    OutOfOrderFlood,
    UpstreamBackfillReplay,
    SlowVolumeDecline,
)

EXPECTED_MISSES = tuple(s for s in CATALOG if s.expected_detector is None)
