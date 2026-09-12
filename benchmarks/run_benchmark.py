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
import datetime
import json
import statistics
import subprocess
import sys
from pathlib import Path

import psycopg
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
from ingestion.replay.replay_producer import load_sorted_trips
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

# (weekday, hour) bands, weekday 0=Mon. Warming one bucket needs
# min_observations (480) one-minute windows in that bucket, i.e. ~8
# weekly occurrences of its hour - so a contiguous short slice can warm
# nothing, while a band replayed across months warms that bucket cheaply.
#
# Several bands, not one: a detection or false-positive rate computed
# over a single (weekday, hour) is a rate over one baseline, not a
# denominator worth publishing. These four span genuinely different
# traffic regimes, so the FP rate reflects varied baseline conditions -
# dense rush hour, near-empty overnight, and weekend daytime behave
# nothing alike in this dataset (DEFENSE.md #38/#40).
DEFAULT_BANDS = (
    (0, 8),    # Monday 08:00 - weekday morning rush
    (4, 18),   # Friday 18:00 - weekday evening peak
    (2, 3),    # Wednesday 03:00 - weekday overnight lull
    (5, 13),   # Saturday 13:00 - weekend daytime
)
# A gap wider than this between consecutive real windows marks the start
# of a new band occurrence rather than a real arrival gap - an artifact
# of band filtering, reported separately rather than silently counted as
# detector noise.
BAND_BOUNDARY_GAP_SECONDS = 120
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
    return psycopg.connect(
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}",
        autocommit=False, connect_timeout=10,
    )


# --------------------------------------------------------------------
# Ground truth: scenarios placed against the real slice, before any run
# --------------------------------------------------------------------

def load_band_filtered(paths, bands):
    """Load only the events falling in the chosen (weekday, hour) bands.

    Filtered per file with pyarrow before materialising to Python, not
    after: the four months are millions of rows and a full to_pylist()
    would be gigabytes of dicts, while the bands are a low-single-digit
    percentage of them.

    Schemas differ across months - cbd_congestion_fee only exists from
    2025-01 (DEFENSE.md #49) - so concat promotes, filling the older
    months' missing column with nulls rather than refusing to combine.
    _row_index is assigned once, after the combined sort, so keys are
    unique and stable across the whole replayed stream.
    """
    filtered = []
    for path in paths:
        table = pq.read_table(path)
        if PICKUP_COLUMN not in table.column_names:
            fail(f"{path} has no {PICKUP_COLUMN} column")
        column = table.column(PICKUP_COLUMN)
        weekday = pc.day_of_week(column, count_from_zero=True, week_start=1)
        hour = pc.hour(column)
        mask = None
        for band_weekday, band_hour in bands:
            in_band = pc.and_(pc.equal(weekday, band_weekday), pc.equal(hour, band_hour))
            mask = in_band if mask is None else pc.or_(mask, in_band)
        kept = table.filter(mask)
        print(f"  {Path(path).name}: {kept.num_rows} of {table.num_rows} rows in band")
        filtered.append(kept)

    combined = pa.concat_tables(filtered, promote_options="default").sort_by(PICKUP_COLUMN)
    rows = combined.to_pylist()
    for index, row in enumerate(rows):
        row["_row_index"] = index
    return rows


def parse_bands(raw):
    if not raw:
        return list(DEFAULT_BANDS)
    bands = []
    for item in raw:
        try:
            weekday, hour = (int(part) for part in item.split(":"))
        except ValueError:
            fail(f"--band expects weekday:hour (0=Mon), got {item!r}")
        if not (0 <= weekday <= 6 and 0 <= hour <= 23):
            fail(f"--band out of range: {item!r}")
        bands.append((weekday, hour))
    return bands


def build_scenarios(events, warmup_fraction):
    """Place every catalog scenario in the injection portion of the
    slice. Injections must land AFTER the warmup portion - a detector
    with an unwarmed baseline cannot score anything, so an injection
    during warmup would be scored as a detector miss when it is really
    an experiment-design error.
    """
    first, last = events[0][PICKUP_COLUMN], events[-1][PICKUP_COLUMN]
    span = last - first
    injection_start = first + span * warmup_fraction
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
        UpstreamBackfillReplay(
            at(0.72),
            backfill_age=injection_span * 0.5,
            backfill_span=injection_span * 0.06,
        ),
        SlowVolumeDecline(at(0.80), last, weekly_decline_fraction=0.05),
    ]


def predict_emitted_count(scenarios, clean_events):
    """Independently predict how many rows the injected stream should
    carry, from each scenario's declared effect - NOT by measuring the
    transformed list (that would assert a transform against itself).

    Only the two scenarios that change cardinality are counted here;
    reordering and mutation preserve it. Drops are counted by asking
    each scenario which events it would remove, which is the same
    declared rule the injection uses, evaluated separately.
    """
    total = len(clean_events)
    for scenario in scenarios:
        if isinstance(scenario, DuplicateEventStorm):
            in_span = sum(1 for e in clean_events if scenario.covers(e[PICKUP_COLUMN]))
            total += in_span * (scenario.copies - 1)
        elif isinstance(scenario, UpstreamBackfillReplay):
            replay_from = scenario.starts_at - scenario.backfill_age
            replay_until = replay_from + scenario.backfill_span
            total += sum(1 for e in clean_events if replay_from <= e[PICKUP_COLUMN] < replay_until)
        elif isinstance(scenario, (PartitionDegradation, GradualDelay, SlowVolumeDecline)):
            total -= len(clean_events) - len(scenario.inject(clean_events))
    return total


# --------------------------------------------------------------------
# Replay + detection
# --------------------------------------------------------------------

def write_stream(events, path):
    """Write the exact send order, _row_index included - replayed with
    --preserve-input-order so neither is re-derived."""
    pq.write_table(pa.Table.from_pylist(events), path)
    return path


def populate(args, stream_path, expected_count, label):
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
    ]
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


def run_detectors(conn):
    """Every detector, lag buffer intact."""
    return {
        "volume": run_volume(conn, VOLUME_CONFIG),
        "freshness": run_freshness(conn, FRESHNESS_CONFIG),
        f"null_rate_{NULLRATE_COLUMN}": run_nullrate(conn, NULLRATE_COLUMN),
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


def band_boundary_window_ends(conn, trailing_window_end):
    """window_end values that begin a new band occurrence.

    Band filtering leaves a gap of days between one band occurrence and
    the next, and the freshness detector reads exactly those gaps. Those
    are an artifact of how this benchmark samples the data, not a real
    arrival failure - measured and reported separately so a reader can
    tell experiment design apart from detector noise. Not excluded: the
    incidents still count, they're just also broken out.
    """
    with conn.cursor() as cur:
        cur.execute(
            "SELECT window_end FROM weir_metrics.window_metrics "
            "WHERE window_end <> %s ORDER BY window_end ASC",
            (trailing_window_end,),
        )
        ends = [r[0] for r in cur.fetchall()]

    boundaries = set()
    for previous, current in zip(ends, ends[1:]):
        if (current - previous).total_seconds() > BAND_BOUNDARY_GAP_SECONDS:
            boundaries.add(to_utc_instant(current, VOLUME_CONFIG.timezone))
    return boundaries


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
              warmed_buckets=None, bands=None, boundary_window_ends=frozenset()):
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

    at_boundary = [i for i in clean_incidents if i["window_end"] in boundary_window_ends]

    return {
        "warmed_buckets": warmed_buckets,
        "bands": [f"{weekday}:{hour}" for weekday, hour in (bands or [])],
        "band_count": len(bands or []),
        "false_positives_at_band_boundaries": len(at_boundary),
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
        f"- warmed buckets: {summary['warmed_buckets']} across {summary['band_count']} "
        f"(weekday:hour) bands {', '.join(summary['bands'])}",
        f"- clean event-time hours covered: {summary['clean_event_time_hours']:.2f}",
        f"- of those false positives, at a band boundary (an artifact of band sampling, "
        f"not a real arrival gap): {summary['false_positives_at_band_boundaries']}",
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
                        help="real TLC parquet file(s) to replay - several months, since warming "
                             "one bucket needs ~8 weekly occurrences of its hour")
    parser.add_argument("--band", action="append", default=None,
                        help="weekday:hour band to replay (0=Mon), repeatable. Defaults to four "
                             "bands spanning different traffic regimes.")
    parser.add_argument("--bootstrap-server", required=True)
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    parser.add_argument("--limit", type=int, default=0,
                        help="replay only the first N real trips (0 = all)")
    parser.add_argument("--warmup-fraction", type=float, default=0.6,
                        help="fraction of the slice reserved for baseline warmup before any "
                             "injection - an injection during warmup would be scored as a "
                             "detector miss when it is an experiment-design error")
    parser.add_argument("--populate-timeout", type=int, default=3600)
    args = parser.parse_args()

    bands = parse_bands(args.band)
    print(f"=== Stage 1: load the real slice, bands {bands} ===")
    clean_events = load_band_filtered(args.input, bands)
    if args.limit:
        clean_events = clean_events[:args.limit]
    if not clean_events:
        fail(f"no events matched bands {bands} in {args.input}")
    slice_start = clean_events[0][PICKUP_COLUMN]
    slice_end = clean_events[-1][PICKUP_COLUMN]
    print(f"{len(clean_events)} real trips, event time {slice_start} .. {slice_end}")

    print("\n=== Stage 2: fix ground truth before either replay (V7) ===")
    scenarios = build_scenarios(clean_events, args.warmup_fraction)
    for scenario in scenarios:
        print(f"  {scenario.name}: {scenario.starts_at} .. {scenario.ends_at} "
              f"-> expects {scenario.expected_detector or 'MISS (no detector targets it)'}")

    injected_events = compose(scenarios, clean_events)
    predicted = predict_emitted_count(scenarios, clean_events)
    if len(injected_events) != predicted:
        fail(f"injected stream has {len(injected_events)} events but the scenarios' declared "
             f"effects predict {predicted} - a scenario is not doing what it declares")
    print(f"PASS: injected stream is {len(injected_events)} events, matching the independent "
          f"prediction from declared effects")

    with connect(args) as conn:
        phases = {}
        for label, events, expected in (
            ("clean", clean_events, len(clean_events)),
            ("injected", injected_events, predicted),
        ):
            stream_path = REPO_ROOT / f"benchmarks/results/_stream_{label}.parquet"
            write_stream(events, stream_path)
            populate(args, stream_path, expected, label)

            reset_detector_state(conn)
            trailing = append_trailing_window(conn)
            statuses = run_detectors(conn)
            real_windows = assert_accounting(conn, trailing, label)

            phases[label] = {
                "incidents": collect_incidents(conn),
                "band_boundaries": band_boundary_window_ends(conn, trailing),
                "scored_windows": sum(
                    c for _, status, c in scored_window_counts(conn) if status == "scored"
                ),
                "real_windows": real_windows,
                "warmed_buckets": warmed_bucket_count(conn),
                "event_time_hours": clean_event_time_hours(conn, trailing),
                "statuses": {k: len(v) for k, v in statuses.items()},
            }
            stream_path.unlink(missing_ok=True)

        clean, injected = phases["clean"], phases["injected"]
        print(f"\nclean: {clean['warmed_buckets']} warmed buckets, "
              f"{clean['scored_windows']} scored windows, {len(clean['incidents'])} incidents")

        if clean["warmed_buckets"] == 0:
            fail("no bucket reached min_observations in the clean replay, so no detector could "
                 "score anything. Detection rate and FP rate would both be structurally zero "
                 "and would mean nothing - replay a longer span.")

        scenario_results, unattributed = attribute(scenarios, injected["incidents"], slice_end)
        summary = summarise(
            scenario_results, clean["incidents"], clean["scored_windows"],
            clean["event_time_hours"], unattributed,
            warmed_buckets=clean["warmed_buckets"], bands=bands,
            boundary_window_ends=clean["band_boundaries"],
        )

    report = render_markdown(scenario_results, summary)
    print("\n" + report)

    path = write_results({
        "run_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "input": str(args.input),
        "slice": {"start": slice_start, "end": slice_end, "events": len(clean_events)},
        "warmup_fraction": args.warmup_fraction,
        "composition_order": [s.name for s in scenarios],
        "summary": summary,
        "scenarios": scenario_results,
        "clean_phase": {k: v for k, v in clean.items() if k not in ("incidents", "band_boundaries")},
        "injected_phase": {k: v for k, v in injected.items() if k not in ("incidents", "band_boundaries")},
        "false_positive_incidents": clean["incidents"],
        "unattributed_injected_incidents": unattributed,
    })
    print(f"\nraw results: {path}")


if __name__ == "__main__":
    main()
