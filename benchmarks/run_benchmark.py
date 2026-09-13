#!/usr/bin/env python3
"""Part 5.2: the benchmark runner.

Two replays of real NYC TLC data through the real pipeline:

  clean    - zero injections. Every incident any detector raises here is
             a false positive by construction: nothing was injected, so
             there was nothing to find. This is the FP denominator, and
             it runs against real data with its real oddities rather
             than a synthetic stream chosen to be quiet.
  injected - the same slice with incidents/benchmark/'s catalog composed
             into it. Produces TP/FN per scenario and detection latency.

Ground truth is the catalog's own declared starts_at/ends_at, fixed
before either replay and never derived from detector output (V7). The
expected emitted-row count is likewise predicted from each scenario's
declared effect and asserted against what the transform actually
produced - which is what catches a scenario that silently does nothing.

Latency is measured in EVENT TIME: first attributable incident's
window_end minus the scenario's ground-truth start. Wall-clock latency
would be meaningless here - the replay is time-compressed (--speed-factor
defaults to 50000x), so a wall-clock number would measure the compression
ratio, not the detector.

Hard rule 3 applies to everything here: no detector is tuned against
these scenarios. If a detector misses, it misses and the miss is
reported. Rates are printed as exact fractions alongside percentages -
nothing is rounded away, and no scenario is excluded from the
denominator, including the four expected misses.

assume_no_more_arrivals is deliberately NOT used (DEFENSE.md #51): it
skips the lag buffer, which would make every window score the instant
it appears and turn detection latency into batch-processing speed.
Instead a synthetic trailing window is appended past the real data so
the buffer clears against something real, then excluded from every
count.
"""
import argparse
import contextlib
import datetime
import json
import statistics
import subprocess
import sys
import time
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from incidents.benchmark.catalog import (
    DuplicateEventStorm,
    GradualDelay,
    NullRateCreep,
    OutOfOrderFlood,
    PartitionDegradation,
    SlowVolumeDecline,
    UpstreamBackfillReplay,
)
from incidents.benchmark.scenario import PICKUP_COLUMN, compose
from ingestion.replay.load_pickup_timestamps import plausible_window
from reliability.freshness.config import DEFAULT_CONFIG as FRESHNESS_CONFIG
from reliability.freshness.run import run_once as run_freshness
from reliability.nullrate.config import config_for_column
from reliability.nullrate.run import run_once as run_nullrate
from reliability.volume.config import DEFAULT_CONFIG as VOLUME_CONFIG
from reliability.volume.run import run_once as run_volume
from reliability.volume.run import to_naive_local, to_utc_instant

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
NULLRATE_COLUMN = "passenger_count"

# A bucket gains one weekly sample per calendar week, so the span in
# weeks IS the per-bucket sample count. min_observations (480 windows =
# 8 weekly occurrences x 60 one-minute windows, DEFENSE.md #44) is the
# floor; 10 weeks clears it with room to inject afterwards. The full
# 17.4-week dataset would give ~17 samples but costs ~5.4h/run against a
# 6h CI ceiling, and the 20-30 samples robust statistics prefers is not
# reachable from this dataset at any tractable runtime - stated as a
# limitation rather than implied away.
DEFAULT_SPAN_WEEKS = 12
WINDOWS_PER_WEEKLY_SAMPLE = 60
# Past the real data by more than max_lag_seconds so the lag buffer
# clears every real window without assume_no_more_arrivals.
TRAILING_WINDOW_OFFSET = datetime.timedelta(seconds=900)
VALID_STATUSES = (
    "scored", "insufficient_baseline", "skipped_late", "dst_ambiguous_excluded",
)


def fail(msg):
    print(f"\nFAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def connect(args):
    # Local import, matching reliability/volume/run.py: the scoring,
    # attribution and summary logic above is pure and unit-tested
    # without a live database, and a module-level psycopg would make the
    # whole test module unimportable wherever the driver isn't installed.
    import psycopg

    return psycopg.connect(
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}",
        autocommit=False, connect_timeout=10,
    )


# --------------------------------------------------------------------
# Ground truth: scenarios placed against the real slice, before any run
# --------------------------------------------------------------------

def load_contiguous(paths, span_weeks, starts_at=None):
    """A contiguous event-time slice as an Arrow table - deliberately NOT
    Python dicts.

    Replaces the earlier (weekday, hour) band sampling, which was a
    mistake: band sampling leaves ~42-hour gaps between occurrences, and
    the freshness detector's entire signal IS the gap between consecutive
    windows, so its baseline learned those artificial gaps as normal and
    the detector became unmeasurable by construction.

    Returns Arrow because a contiguous 12-week slice is ~10.5M rows, and
    to_pylist() on that builds ~10GB of dicts and gets the CI runner
    OOM-killed (exit 143) while Kafka, Postgres and Flink share the same
    16GB box. Only the injection region is ever materialised - see
    build_injected_table.

    Implausible timestamps are dropped BEFORE anchoring: this dataset's
    earliest pickup is a 2009 clock error, so anchoring on a raw min()
    put the span in 2009 and selected one event. Reuses
    load_pickup_timestamps' already-verified plausible window.
    """
    tables = []
    for path in paths:
        table = pq.read_table(path)
        if PICKUP_COLUMN not in table.column_names:
            fail(f"{path} has no {PICKUP_COLUMN} column")
        tables.append(table)
    combined = pa.concat_tables(tables, promote_options="default").sort_by(PICKUP_COLUMN)

    window_start, window_end = plausible_window([str(path) for path in paths])
    column = combined.column(PICKUP_COLUMN)
    plausible = pc.and_(
        pc.greater_equal(column, pa.scalar(window_start)),
        pc.less(column, pa.scalar(window_end)),
    )
    dropped = combined.num_rows - pc.sum(pc.cast(plausible, pa.int64())).as_py()
    if dropped:
        print(f"  dropped {dropped} implausible timestamps outside {window_start} .. {window_end}")
    combined = combined.filter(plausible)

    column = combined.column(PICKUP_COLUMN)
    first_event = starts_at or pc.min(column).as_py()
    last_event = first_event + datetime.timedelta(weeks=span_weeks)
    sliced = combined.filter(pc.and_(
        pc.greater_equal(column, pa.scalar(first_event)),
        pc.less(column, pa.scalar(last_event)),
    ))
    print(f"  contiguous slice {first_event} .. {last_event} "
          f"({span_weeks} weeks): {sliced.num_rows} rows")
    if sliced.num_rows == 0:
        fail(f"no events in the contiguous span {first_event} .. {last_event}")

    # Stable global keys, assigned once over the whole slice so they stay
    # unique across the warmup passthrough and the injected region.
    return sliced.append_column("_row_index", pa.array(range(sliced.num_rows), type=pa.int64()))


def table_bounds(table):
    column = table.column(PICKUP_COLUMN)
    return pc.min(column).as_py(), pc.max(column).as_py()


def rows_between(table, start, end):
    """Materialise only a bounded slice as dicts."""
    column = table.column(PICKUP_COLUMN)
    mask = pc.and_(pc.greater_equal(column, pa.scalar(start)), pc.less(column, pa.scalar(end)))
    return table.filter(mask).to_pylist()


def build_injected_table(table, scenarios, injection_start):
    """Apply the catalog without materialising the warmup region.

    Every scenario only touches events inside its own declared span, and
    all spans sit after injection_start - with one exception, the
    backfill, whose source window is deliberately in warmup. That slice
    is materialised on its own and handed to the scenario directly, so
    the multi-million-row warmup never becomes Python dicts.
    """
    column = table.column(PICKUP_COLUMN)
    warmup = table.filter(pc.less(column, pa.scalar(injection_start)))
    injection_rows = table.filter(pc.greater_equal(column, pa.scalar(injection_start))).to_pylist()
    print(f"  materialised {len(injection_rows)} injection-region rows "
          f"({warmup.num_rows} warmup rows passed through as Arrow)")

    for scenario in scenarios:
        if isinstance(scenario, UpstreamBackfillReplay):
            replay_from = scenario.starts_at - scenario.backfill_age
            scenario.source_events = rows_between(
                table, replay_from, replay_from + scenario.backfill_span
            )
            print(f"  backfill source: {len(scenario.source_events)} rows from {replay_from}")

    injected_rows = compose(scenarios, injection_rows)
    injected_table = pa.Table.from_pylist(injected_rows, schema=table.schema)
    return pa.concat_tables([warmup, injected_table]), len(injected_rows), warmup.num_rows


def build_scenarios(first, last, warmup_fraction):
    """Place every catalog scenario in the injection portion of the
    slice. Injections must land AFTER the warmup portion - a detector
    with an unwarmed baseline cannot score anything, so an injection
    during warmup would be scored as a detector miss when it is really
    an experiment-design error.
    """
    span = last - first
    injection_start = first + span * warmup_fraction

    # A bucket only becomes scoreable once it has min_observations
    # windows, gained at WINDOWS_PER_WEEKLY_SAMPLE per calendar week.
    # If injections start before that, every one of them scores
    # 'insufficient_baseline' and misses by construction - which would
    # read as detector failure rather than an experiment that never
    # tested anything. Checked rather than assumed.
    warmup_weeks = (span * warmup_fraction).total_seconds() / (7 * 24 * 3600)
    warmup_observations = warmup_weeks * WINDOWS_PER_WEEKLY_SAMPLE
    if warmup_observations < VOLUME_CONFIG.min_observations:
        fail(
            f"warmup covers {warmup_weeks:.1f} weeks = ~{warmup_observations:.0f} observations "
            f"per bucket, below min_observations ({VOLUME_CONFIG.min_observations}). Every "
            f"injection would land on an unwarmed baseline and miss by construction. Raise "
            f"--span-weeks or --warmup-fraction so warmup covers at least "
            f"{VOLUME_CONFIG.min_observations / WINDOWS_PER_WEEKLY_SAMPLE:.0f} weeks."
        )
    print(f"  warmup: {warmup_weeks:.1f} weeks (~{warmup_observations:.0f} observations/bucket, "
          f"floor {VOLUME_CONFIG.min_observations}); injections begin {injection_start}")
    injection_span = last - injection_start
    if injection_span.total_seconds() <= 0:
        fail(f"warmup_fraction {warmup_fraction} leaves no room to inject anything")

    def at(offset_fraction):
        return injection_start + injection_span * offset_fraction

    return [
        PartitionDegradation(at(0.00), at(0.12)),
        GradualDelay(at(0.14), at(0.28), max_delay_seconds=900),
        NullRateCreep(at(0.30), at(0.44), column=NULLRATE_COLUMN,
                      start_rate=0.01, end_rate=0.60),
        DuplicateEventStorm(at(0.46), at(0.56), copies=3),
        OutOfOrderFlood(at(0.58), at(0.70)),
        # backfill_age reaches back past injection_start into the warmup
        # region on purpose. Replaying genuinely old, already-processed
        # data is the realistic shape of this failure, and it keeps the
        # source window clear of every injected span - an earlier
        # 0.5*injection_span landed inside GradualDelay's window, so the
        # events it meant to replay had already been dropped by the time
        # it ran (caught by the declared-effect prediction, not by
        # reading the code).
        UpstreamBackfillReplay(
            at(0.72),
            backfill_age=injection_span * 0.9,
            backfill_span=injection_span * 0.06,
        ),
        SlowVolumeDecline(at(0.80), last, weekly_decline_fraction=0.05),
    ]


def assert_spans_disjoint(scenarios):
    """No two scenarios may overlap in event time.

    Two reasons, and the second is the one that survives any change to
    how counts are predicted: overlapping spans make the independent
    expected-count prediction inexact (a dropping scenario changes what
    a later one sees), and they make attribution ambiguous - an incident
    inside an overlap could belong to either scenario, so there would be
    no honest way to score it.
    """
    spans = []
    for scenario in scenarios:
        spans.append((scenario.name, scenario.starts_at, scenario.ends_at))
        if isinstance(scenario, UpstreamBackfillReplay):
            source_start = scenario.starts_at - scenario.backfill_age
            spans.append((f"{scenario.name}(source)", source_start,
                          source_start + scenario.backfill_span))

    overlaps = []
    for i, (name_a, start_a, end_a) in enumerate(spans):
        for name_b, start_b, end_b in spans[i + 1:]:
            if start_a < (end_b or start_b) and start_b < (end_a or start_a):
                overlaps.append(f"{name_a} overlaps {name_b}")
    if overlaps:
        fail("benchmark scenario spans overlap, which makes attribution ambiguous: "
             + "; ".join(overlaps))


def count_between(table, start, end):
    column = table.column(PICKUP_COLUMN)
    mask = pc.and_(pc.greater_equal(column, pa.scalar(start)), pc.less(column, pa.scalar(end)))
    return pc.sum(pc.cast(mask, pa.int64())).as_py() or 0


def predict_emitted_count(scenarios, table, slice_end):
    """Independently predict how many rows the injected stream should
    carry, from each scenario's DECLARED effect - never by measuring the
    transformed output, which would assert a transform against itself.

    Counted over the Arrow table so the prediction costs no memory. The
    dropping scenarios are evaluated against only their own span, which
    is exactly the rule they declare, computed a second way.
    """
    total = table.num_rows
    for scenario in scenarios:
        span_end = scenario.ends_at or slice_end
        if isinstance(scenario, DuplicateEventStorm):
            total += count_between(table, scenario.starts_at, span_end) * (scenario.copies - 1)
        elif isinstance(scenario, UpstreamBackfillReplay):
            replay_from = scenario.starts_at - scenario.backfill_age
            total += count_between(table, replay_from, replay_from + scenario.backfill_span)
        elif isinstance(scenario, (PartitionDegradation, GradualDelay, SlowVolumeDecline)):
            in_span = rows_between(table, scenario.starts_at, span_end)
            total -= len(in_span) - len(scenario.inject(in_span))
    return total


def peak_rss_mb():
    """Peak resident memory for this process, so the headroom against the
    runner's ceiling is reported rather than rediscovered by an OOM kill
    on some later, larger run."""
    try:
        import resource
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0
    except (ImportError, AttributeError):
        return None


# --------------------------------------------------------------------
# Replay + detection
# --------------------------------------------------------------------

def max_explainable_loss(scenarios, table, slice_end):
    """Upper bound on events the pipeline may legitimately fail to land.

    Only two scenarios can cause the pipeline to drop data: the backfill
    replays months-old copies far past the watermark, and the
    out-of-order flood makes part of its span arrive late. Bounding by
    "every event those two touch" deliberately over-counts rather than
    predicting Flink's exact watermark arithmetic - asserting a precise
    drop count would be asserting a model of Flink, not measuring it.
    What this catches is loss appearing anywhere it cannot be explained,
    which is the V8 property that matters.
    """
    bound = 0
    for scenario in scenarios:
        if isinstance(scenario, UpstreamBackfillReplay):
            replay_from = scenario.starts_at - scenario.backfill_age
            bound += count_between(table, replay_from, replay_from + scenario.backfill_span)
        elif isinstance(scenario, OutOfOrderFlood):
            bound += count_between(table, scenario.starts_at, scenario.ends_at or slice_end)
    return bound


def landed_row_total(conn, trailing_window_end):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE(SUM(row_count), 0) FROM weir_metrics.window_metrics "
            "WHERE window_end <> %s",
            (trailing_window_end,),
        )
        return float(cur.fetchone()[0])


def populate(args, stream_path, expected_count, label, allow_late_drop=False):
    """Reuse scripts/verify_metrics_job.py as the single population
    path, rather than a second orchestration codepath to keep in sync.
    Its deep checks still run; only the emitted-count expectation is
    parameterised, so the invariant stays exact for injected runs."""
    cmd = [
        sys.executable, "-u", "scripts/verify_metrics_job.py",
        "--input", str(stream_path),
        "--bootstrap-server", args.bootstrap_server,
        "--limit", str(expected_count),
        "--expected-emitted-count", str(expected_count),
        "--preserve-input-order",
        "--replay-timeout", str(args.replay_timeout),
    ]
    if allow_late_drop:
        cmd.append("--allow-late-drop")
    print(f"\n--- populating [{label}] via verify_metrics_job.py ({expected_count} events) ---")
    result = subprocess.run(cmd, cwd=REPO_ROOT, timeout=args.populate_timeout)
    if result.returncode != 0:
        fail(f"[{label}] population failed (verify_metrics_job.py exited {result.returncode})")


def reset_detector_state(conn):
    with conn.cursor() as cur:
        cur.execute(
            "TRUNCATE weir_incidents.scored_windows, weir_incidents.baseline_state, "
            "weir_incidents.detector_progress, weir_incidents.incidents;"
        )
    conn.commit()


def append_trailing_window(conn):
    """One synthetic window past the real data so the lag buffer clears
    every real window - the alternative to assume_no_more_arrivals,
    which would invalidate the latency measurement (DEFENSE.md #51).
    Returned so every downstream count can exclude it explicitly."""
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(window_end) FROM weir_metrics.window_metrics")
        (real_max,) = cur.fetchone()
        if real_max is None:
            fail("weir_metrics.window_metrics is empty after population")
        window_end = real_max + TRAILING_WINDOW_OFFSET
        window_start = window_end - datetime.timedelta(minutes=1)
        cur.execute(
            "INSERT INTO weir_metrics.window_metrics (window_start, window_end, row_count) "
            "VALUES (%s, %s, %s) ON CONFLICT (window_start, window_end) DO NOTHING",
            (window_start, window_end, 1),
        )
    conn.commit()
    return window_end


PROGRESS_INTERVAL_SECONDS = 30.0


@contextlib.contextmanager
def timed(label, what):
    """Wall-clock one step and print it. Printed on the way in as well
    as out, so a step that never finishes is still identifiable."""
    print(f"  [{label}] {what} ...", flush=True)
    started = time.monotonic()
    try:
        yield
    finally:
        print(f"  [{label}] {what}: {time.monotonic() - started:.1f}s", flush=True)


def progress_reporter(label, detector_name):
    """A throttled progress(done, total) callback for run_once.

    A contiguous span puts six figures of windows through one run_once
    call, each in its own transaction. Run 34757520031 spent 4h36m in
    that loop without printing anything and was killed at the CI
    ceiling with no way to tell a slow run from a hung one. This prints
    a rate and an ETA often enough to answer that, rarely enough not to
    drown the log."""
    state = {"last": time.monotonic(), "started": time.monotonic()}

    def report(done, total):
        now = time.monotonic()
        if now - state["last"] < PROGRESS_INTERVAL_SECONDS and done != total:
            return
        state["last"] = now
        elapsed = now - state["started"]
        rate = done / elapsed if elapsed else 0.0
        remaining = (total - done) / rate if rate else float("nan")
        print(f"  [{label}] {detector_name}: {done}/{total} windows, "
              f"{rate:.1f}/s, ~{remaining / 60:.1f} min left", flush=True)

    return report


def run_detectors(conn, label=""):
    """Every detector, lag buffer intact."""
    return {
        "volume": run_volume(
            conn, VOLUME_CONFIG, progress=progress_reporter(label, "volume")),
        "freshness": run_freshness(
            conn, FRESHNESS_CONFIG, progress=progress_reporter(label, "freshness")),
        f"null_rate_{NULLRATE_COLUMN}": run_nullrate(
            conn, NULLRATE_COLUMN,
            progress=progress_reporter(label, f"null_rate_{NULLRATE_COLUMN}")),
    }


def detector_names():
    return ["volume", "freshness", f"null_rate_{NULLRATE_COLUMN}"]


def collect_incidents(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT detector_name, window_start, window_end, observed_value, score "
            "FROM weir_incidents.incidents ORDER BY window_end ASC"
        )
        return [
            {
                "detector_name": r[0],
                "window_start": r[1],
                "window_end": r[2],
                "observed_value": float(r[3]),
                "score": float(r[4]),
            }
            for r in cur.fetchall()
        ]


def scored_window_counts(conn):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT detector_name, status, COUNT(*) FROM weir_incidents.scored_windows "
            "GROUP BY detector_name, status"
        )
        return [(r[0], r[1], r[2]) for r in cur.fetchall()]


def warmed_bucket_count(conn):
    """How many buckets actually reached min_observations. Zero means no
    detector could score anything, which makes both the detection rate
    and the FP rate meaningless rather than zero."""
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_incidents.baseline_state WHERE observation_count >= %s",
            (VOLUME_CONFIG.min_observations,),
        )
        return cur.fetchone()[0]


def bucket_sample_counts(conn):
    """Actual observation_count per bucket at scoring time, per detector.

    Reported rather than assumed: min_observations is a floor, not
    evidence that the baselines are mature. observation_count counts
    one-minute windows; the statistically meaningful unit is distinct
    weekly occurrences (the 60 windows inside one hour are autocorrelated
    - DEFENSE.md #44), so weekly samples are reported alongside.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT detector_name, COUNT(*), MIN(observation_count), "
            "       MAX(observation_count), "
            "       PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY observation_count) "
            "FROM weir_incidents.baseline_state GROUP BY detector_name ORDER BY detector_name"
        )
        stats = {}
        for name, buckets, low, high, median in cur.fetchall():
            stats[name] = {
                "buckets": buckets,
                "observation_count_min": int(low),
                "observation_count_median": float(median),
                "observation_count_max": int(high),
                "weekly_samples_min": int(low) / WINDOWS_PER_WEEKLY_SAMPLE,
                "weekly_samples_median": float(median) / WINDOWS_PER_WEEKLY_SAMPLE,
            }
        return stats


def clean_event_time_hours(conn, trailing_window_end):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT MIN(window_start), MAX(window_end) FROM weir_metrics.window_metrics "
            "WHERE window_end <> %s",
            (trailing_window_end,),
        )
        first, last = cur.fetchone()
    if first is None:
        fail("no real windows present when measuring covered event-time hours")
    return (last - first).total_seconds() / 3600.0


# --------------------------------------------------------------------
# V8: everything accounted for, explained, asserted
# --------------------------------------------------------------------

def assert_accounting(conn, trailing_window_end, label):
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_metrics.window_metrics WHERE window_end <> %s",
            (trailing_window_end,),
        )
        (real_windows,) = cur.fetchone()

    unexplained = []
    for detector_name, status, count in scored_window_counts(conn):
        if status not in VALID_STATUSES:
            unexplained.append(f"{detector_name}: unknown status {status!r} ({count} rows)")
    if unexplained:
        fail(f"[{label}] scored_windows contains statuses nothing accounts for: {unexplained}")

    with conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM weir_incidents.scored_windows WHERE window_end = %s",
            (to_utc_instant(trailing_window_end, VOLUME_CONFIG.timezone),),
        )
        (trailing_scored,) = cur.fetchone()
    if trailing_scored:
        fail(f"[{label}] the synthetic trailing window was scored ({trailing_scored} rows) - "
             f"it exists only to clear the lag buffer and must never enter a measurement")

    print(f"PASS: [{label}] accounting - {real_windows} real windows, "
          f"every scored_windows row carries a known status, trailing window excluded")
    return real_windows


# --------------------------------------------------------------------
# Scoring
# --------------------------------------------------------------------

def scoring_trace(conn, scenario, slice_end):
    """Every scored_windows row inside a scenario's declared span, with the
    exact numbers the detector used.

    No new instrumentation: scored_windows already persists
    observed_value, baseline_mean_at_time, baseline_scale_at_time and
    score for every window, precisely so a threshold sweep can re-score
    without re-running (DEFENSE.md #45). A miss therefore has a
    checkable cause rather than a candidate explanation - either the
    windows were never scored (a different bug), or they were scored and
    the score never crossed the threshold, and these rows say by how much.
    """
    if scenario.expected_detector is None:
        return None
    span_end = scenario.ends_at or slice_end
    with conn.cursor() as cur:
        cur.execute(
            "SELECT window_end, status, observed_value, baseline_mean_at_time, "
            "       baseline_scale_at_time, score "
            "FROM weir_incidents.scored_windows "
            "WHERE detector_name = %s AND window_end >= %s AND window_end < %s "
            "ORDER BY window_end ASC",
            (scenario.expected_detector,
             to_utc_instant(scenario.starts_at, VOLUME_CONFIG.timezone),
             to_utc_instant(span_end, VOLUME_CONFIG.timezone)),
        )
        rows = cur.fetchall()

    statuses = {}
    scored = []
    for window_end, status, observed, mean, scale, score in rows:
        statuses[status] = statuses.get(status, 0) + 1
        if score is not None:
            scored.append({
                "window_end": window_end, "observed_value": float(observed),
                "baseline_mean": float(mean), "baseline_scale": float(scale),
                "score": float(score),
            })

    ranked = sorted(scored, key=lambda r: abs(r["score"]), reverse=True)
    return {
        "detector": scenario.expected_detector,
        "windows_in_span": len(rows),
        "status_counts": statuses,
        "windows_scored": len(scored),
        "threshold": VOLUME_CONFIG.z_threshold,
        "max_abs_score": abs(ranked[0]["score"]) if ranked else None,
        "closest_to_threshold": ranked[:20],
    }


def attribute(scenarios, incidents, slice_end):
    """Match incidents to scenarios on ground truth alone: the incident's
    detector must be the one the scenario declared, and its window_end
    (converted back to the event-time domain) must fall inside the
    scenario's declared span. No tolerance padding - padding would be a
    tuning knob on the result."""
    results = []
    attributed = set()
    for scenario in scenarios:
        span_end = scenario.ends_at or slice_end
        matches = []
        for index, incident in enumerate(incidents):
            if incident["detector_name"] != scenario.expected_detector:
                continue
            event_time = to_naive_local(incident["window_end"], VOLUME_CONFIG.timezone)
            if scenario.starts_at <= event_time < span_end:
                matches.append((index, event_time))
        detected = bool(matches)
        latency_seconds = None
        if detected:
            first_index, first_event_time = min(matches, key=lambda m: m[1])
            latency_seconds = (first_event_time - scenario.starts_at).total_seconds()
            attributed.update(index for index, _ in matches)
        results.append({
            **scenario.ground_truth(),
            "detected": detected,
            "detection_latency_seconds": latency_seconds,
            "matching_incident_count": len(matches),
        })
    unattributed = [incidents[i] for i in range(len(incidents)) if i not in attributed]
    return results, unattributed


def percentile(values, fraction):
    """Nearest-rank, stated explicitly so the number is reproducible."""
    if not values:
        return None
    ordered = sorted(values)
    rank = max(1, min(len(ordered), int(-(-fraction * len(ordered) // 1))))
    return ordered[rank - 1]


def summarise(scenario_results, clean_incidents, clean_scored, clean_hours, unattributed,
              warmed_buckets=None, span_weeks=None, bucket_samples=None):
    detected = [r for r in scenario_results if r["detected"]]
    targeted = [r for r in scenario_results if r["expected_detector"] is not None]
    targeted_detected = [r for r in targeted if r["detected"]]
    latencies = [r["detection_latency_seconds"] for r in detected]

    missing = [r["name"] for r in detected if r["detection_latency_seconds"] is None]
    if missing:
        fail(f"detected scenarios with no latency measurement: {missing} - "
             f"a missing measurement is an error, not a zero")

    if clean_scored == 0:
        fail("the clean replay scored zero windows, so the false-positive rate has no "
             "denominator. That is an unwarmed baseline, not a 0% false-positive rate - "
             "replay a span long enough to warm at least one bucket.")

    samples = bucket_samples or {}
    weekly = [d["weekly_samples_median"] for d in samples.values()]

    return {
        "warmed_buckets": warmed_buckets,
        "peak_rss_mb": peak_rss_mb(),
        "span_weeks": span_weeks,
        "bucket_samples": samples,
        "weekly_samples_median": min(weekly) if weekly else None,
        "scenarios_total": len(scenario_results),
        "scenarios_detected": len(detected),
        "detection_rate": len(detected) / len(scenario_results),
        "detection_rate_fraction": f"{len(detected)}/{len(scenario_results)}",
        "targeted_scenarios_total": len(targeted),
        "targeted_scenarios_detected": len(targeted_detected),
        "targeted_detection_rate": (len(targeted_detected) / len(targeted)) if targeted else None,
        "targeted_detection_rate_fraction": f"{len(targeted_detected)}/{len(targeted)}",
        "false_positives": len(clean_incidents),
        "clean_scored_windows": clean_scored,
        "false_positive_rate": len(clean_incidents) / clean_scored,
        "false_positive_rate_fraction": f"{len(clean_incidents)}/{clean_scored}",
        "clean_event_time_hours": clean_hours,
        "detection_latency_median_seconds": statistics.median(latencies) if latencies else None,
        "detection_latency_p95_seconds": percentile(latencies, 0.95),
        "unattributed_injected_incidents": len(unattributed),
    }


# --------------------------------------------------------------------
# Reporting
# --------------------------------------------------------------------

def render_markdown(scenario_results, summary):
    lines = ["| scenario | detected | latency |", "|---|---|---|"]
    for r in scenario_results:
        if r["detected"]:
            detected = "yes"
            latency = f"{r['detection_latency_seconds']:.0f}s"
        else:
            detected = "no (expected miss)" if r["expected_detector"] is None else "no"
            latency = "-"
        lines.append(f"| {r['name']} | {detected} | {latency} |")

    def pct(value):
        return "n/a" if value is None else f"{value * 100:.4f}%"

    def secs(value):
        return "n/a (no detections)" if value is None else f"{value:.0f}s"

    lines += [
        "",
        "**Summary**",
        "",
        f"- detection rate: {summary['detection_rate_fraction']} "
        f"({pct(summary['detection_rate'])}), all scenarios including expected misses",
        f"- detection rate, targeted scenarios only: "
        f"{summary['targeted_detection_rate_fraction']} ({pct(summary['targeted_detection_rate'])})",
        f"- false-positive rate: {summary['false_positive_rate_fraction']} "
        f"({pct(summary['false_positive_rate'])}) of scored windows in the clean replay",
        f"- detection latency: median {secs(summary['detection_latency_median_seconds'])}, "
        f"p95 {secs(summary['detection_latency_p95_seconds'])} (event time, nearest-rank)",
        f"- scenarios: {summary['scenarios_total']}",
        f"- warmed buckets: {summary['warmed_buckets']} over a contiguous "
        f"{summary['span_weeks']}-week span",
        f"- weekly samples per bucket (median, lowest detector): "
        f"{summary['weekly_samples_median']}",
        f"- peak resident memory: {summary['peak_rss_mb']} MB"
        if summary.get("peak_rss_mb") else "- peak resident memory: unavailable on this platform",
        f"- clean event-time hours covered: {summary['clean_event_time_hours']:.2f}",

        f"- unattributed incidents in the injected replay: "
        f"{summary['unattributed_injected_incidents']}",
    ]
    return "\n".join(lines)


def json_safe(value):
    if isinstance(value, datetime.datetime):
        return value.isoformat()
    raise TypeError(f"not JSON-serialisable: {type(value).__name__}")


def write_results(payload):
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = RESULTS_DIR / f"{stamp}.json"
    path.write_text(json.dumps(payload, indent=2, default=json_safe), encoding="utf-8")
    return path


# --------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, nargs="+",
                        help="real TLC parquet file(s) to replay - several months, since a bucket "
                             "gains one weekly sample per calendar week")
    parser.add_argument("--span-start", default=None,
                        help="ISO event-time to anchor the contiguous span (default: the first "
                             "plausible event). Explicit is better for reproducing a run.")
    parser.add_argument("--span-weeks", type=float, default=DEFAULT_SPAN_WEEKS,
                        help=f"contiguous event-time span to replay, in weeks (default "
                             f"{DEFAULT_SPAN_WEEKS}). This IS the per-bucket weekly sample count; "
                             f"it must exceed the 8-week min_observations floor with room to "
                             f"inject after warmup.")
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    parser.add_argument("--limit", type=int, default=0,
                        help="replay only the first N real trips (0 = all)")
    parser.add_argument("--warmup-fraction", type=float, default=0.75,
                        help="fraction of the slice reserved for baseline warmup before any "
                             "injection - an injection during warmup would be scored as a "
                             "detector miss when it is an experiment-design error")
    parser.add_argument("--populate-timeout", type=int, default=3600)
    parser.add_argument("--replay-timeout", type=int, default=7200,
                        help="seconds allowed for each replay; measured throughput is "
                             "~2,800 events/s, so millions of events need hours")
    args = parser.parse_args()

    print(f"=== Stage 1: load a contiguous {args.span_weeks}-week slice ===")
    span_start = datetime.datetime.fromisoformat(args.span_start) if args.span_start else None
    clean_table = load_contiguous(args.input, args.span_weeks, starts_at=span_start)
    slice_start, slice_end = table_bounds(clean_table)
    print(f"{clean_table.num_rows} real trips, event time {slice_start} .. {slice_end}")

    print("\n=== Stage 2: fix ground truth before either replay (V7) ===")
    scenarios = build_scenarios(slice_start, slice_end, args.warmup_fraction)
    assert_spans_disjoint(scenarios)
    for scenario in scenarios:
        print(f"  {scenario.name}: {scenario.starts_at} .. {scenario.ends_at} "
              f"-> expects {scenario.expected_detector or 'MISS (no detector targets it)'}")

    injection_start = min(s.starts_at for s in scenarios)
    injected_table, injected_rows, warmup_rows = build_injected_table(
        clean_table, scenarios, injection_start
    )
    predicted = predict_emitted_count(scenarios, clean_table, slice_end)

    # Seam assertion (V7/V8): rows now take two paths - Arrow passthrough
    # for warmup, materialised dicts for the injection region - and a
    # silent drop at that seam would look exactly like a detection
    # result. Both the total and the split are checked, so a loss on
    # either side of the seam is caught rather than averaging out.
    if injected_table.num_rows != predicted:
        fail(f"injected stream has {injected_table.num_rows} rows but the scenarios' declared "
             f"effects predict {predicted} - a scenario is not doing what it declares, or rows "
             f"were lost at the Arrow/dict seam")
    if warmup_rows + injected_rows != injected_table.num_rows:
        fail(f"seam accounting is inconsistent: {warmup_rows} warmup + {injected_rows} injected "
             f"!= {injected_table.num_rows} written")
    expected_warmup = count_between(clean_table, slice_start, injection_start)
    if warmup_rows != expected_warmup:
        fail(f"the warmup passthrough carried {warmup_rows} rows but the slice holds "
             f"{expected_warmup} before {injection_start} - rows were lost passing through Arrow")
    print(f"PASS: injected stream is {injected_table.num_rows} rows "
          f"({warmup_rows} warmup passthrough + {injected_rows} injected), matching the "
          f"independent prediction from declared effects")

    with connect(args) as conn:
        phases = {}
        for label, table, expected in (
            ("clean", clean_table, clean_table.num_rows),
            ("injected", injected_table, predicted),
        ):
            stream_path = REPO_ROOT / f"benchmarks/results/_stream_{label}.parquet"
            pq.write_table(table, stream_path)
            # Only the injected stream deliberately contains events the
            # pipeline is supposed to drop; the clean run keeps the exact
            # zero-loss invariant, which is what makes it a usable FP
            # denominator in the first place.
            populate(args, stream_path, expected, label, allow_late_drop=(label == "injected"))

            # Every post-populate step is timed. Run 34757520031 spent
            # 4h36m somewhere in this block and died at the CI ceiling
            # without narrowing it down; a measured probe later put
            # scoring alone at well under that, so which step actually
            # dominates is an open question this answers directly
            # rather than by inference.
            with timed(label, "reset detector state"):
                reset_detector_state(conn)
            with timed(label, "append trailing window"):
                trailing = append_trailing_window(conn)
            with timed(label, "score all detectors"):
                statuses = run_detectors(conn, label)
            with timed(label, "assert accounting"):
                real_windows = assert_accounting(conn, trailing, label)

            with timed(label, "collect incidents"):
                incidents = collect_incidents(conn)
            with timed(label, "bucket sample counts"):
                bucket_samples = bucket_sample_counts(conn)
            with timed(label, "landed row total"):
                landed_rows = landed_row_total(conn, trailing)
            with timed(label, "scored window counts"):
                scored_windows = sum(
                    c for _, status, c in scored_window_counts(conn) if status == "scored"
                )
            with timed(label, "warmed bucket count"):
                warmed_buckets = warmed_bucket_count(conn)
            with timed(label, "clean event-time hours"):
                event_time_hours = clean_event_time_hours(conn, trailing)

            phases[label] = {
                "incidents": incidents,
                "bucket_samples": bucket_samples,
                "landed_rows": landed_rows,
                "scored_windows": scored_windows,
                "real_windows": real_windows,
                "warmed_buckets": warmed_buckets,
                "event_time_hours": event_time_hours,
                "statuses": {k: len(v) for k, v in statuses.items()},
            }
            stream_path.unlink(missing_ok=True)
            # Release this phase's read locks before the next phase's
            # population runs. These reads are autocommit=False, so they
            # leave the session idle in transaction holding ACCESS SHARE
            # on window_metrics - which blocks the next phase's TRUNCATE
            # (ACCESS EXCLUSIVE) until it times out. Everything above is
            # already materialised into Python, and every write already
            # committed, so there is nothing here to lose.
            conn.rollback()

        clean, injected = phases["clean"], phases["injected"]
        print(f"\nclean: {clean['warmed_buckets']} warmed buckets, "
              f"{clean['scored_windows']} scored windows, {len(clean['incidents'])} incidents")

        if clean["warmed_buckets"] == 0:
            fail("no bucket reached min_observations in the clean replay, so no detector could "
                 "score anything. Detection rate and FP rate would both be structurally zero "
                 "and would mean nothing - replay a longer span.")

        # V8: the injected run legitimately loses data (ancient backfill
        # copies, the out-of-order tail), but that loss has to be
        # attributable to a scenario that declares it - not merely
        # tolerated because injection was involved.
        injected_shortfall = predicted - injected["landed_rows"]
        loss_bound = max_explainable_loss(scenarios, clean_table, slice_end)
        print(f"injected replay lost {injected_shortfall:.0f} of {predicted} events; "
              f"at most {loss_bound} are explainable by the scenarios that declare data loss")
        if injected_shortfall > loss_bound:
            fail(f"injected replay lost {injected_shortfall:.0f} events but only {loss_bound} "
                 f"are attributable to scenarios declaring loss - the rest is unexplained")

        traces = {s.name: scoring_trace(conn, s, slice_end) for s in scenarios}
        for name, trace in traces.items():
            if trace is None:
                continue
            print(f"  {name}: {trace['windows_in_span']} windows in span, "
                  f"{trace['windows_scored']} scored, statuses={trace['status_counts']}, "
                  f"max|score|={trace['max_abs_score']} vs threshold {trace['threshold']}")

        scenario_results, unattributed = attribute(scenarios, injected["incidents"], slice_end)
        summary = summarise(
            scenario_results, clean["incidents"], clean["scored_windows"],
            clean["event_time_hours"], unattributed,
            warmed_buckets=clean["warmed_buckets"],
            span_weeks=args.span_weeks, bucket_samples=clean["bucket_samples"],
        )

    report = render_markdown(scenario_results, summary)
    print("\n" + report)

    path = write_results({
        "run_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "input": str(args.input),
        "slice": {"start": slice_start, "end": slice_end, "events": clean_table.num_rows},
        "peak_rss_mb": peak_rss_mb(),
        "warmup_fraction": args.warmup_fraction,
        "span_weeks": args.span_weeks,
        "composition_order": [s.name for s in scenarios],
        "summary": summary,
        "scenarios": scenario_results,
        "scoring_traces": traces,
        "clean_phase": {k: v for k, v in clean.items() if k != "incidents"},
        "injected_phase": {k: v for k, v in injected.items() if k != "incidents"},
        "false_positive_incidents": clean["incidents"],
        "unattributed_injected_incidents": unattributed,
    })
    print(f"\nraw results: {path}")


if __name__ == "__main__":
    main()
