#!/usr/bin/env python3
"""Regenerate the README benchmark table from benchmarks/results/*.json.

This is the positive guarantee behind the negative check in
scripts/check_no_metrics_in_markdown.sh (see DEFENSE.md #9): the README's
benchmark numbers are generated from the newest actual result file, not
typed by hand, so there's nothing for a human (or an assistant) to
fabricate.

"Newest" is the lexicographically greatest filename in
benchmarks/results/*.json - this assumes result filenames are timestamp-
prefixed (e.g. 2026-08-17T120000Z_run.json) so that sort order is
chronological. That convention isn't enforced anywhere yet, since
benchmarks/run_benchmark.py doesn't exist yet; whichever script writes
results needs to follow it. mtime and git-log timestamps were considered
and rejected: mtime isn't reliable after a fresh CI checkout (checkout
resets file times), and git-log depth depends on CI's fetch-depth.

Until the first real benchmark run, there are no files in
benchmarks/results/, and the block reads "No benchmark run yet."

Usage:
    python scripts/sync_benchmark_readme.py            # write README.md
    python scripts/sync_benchmark_readme.py --check     # exit 1 if stale
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
README = REPO_ROOT / "README.md"
BEGIN_MARKER = "<!-- BENCHMARK:BEGIN -->"
END_MARKER = "<!-- BENCHMARK:END -->"
NO_RUN_TEXT = "No benchmark run yet."


def newest_result_file() -> Path | None:
    files = sorted(RESULTS_DIR.glob("*.json"))
    return files[-1] if files else None


def render_block(result_file: Path | None) -> str:
    if result_file is None:
        return NO_RUN_TEXT

    data = json.loads(result_file.read_text(encoding="utf-8"))
    run_at = data.get("run_at", "unknown")
    scenarios = data.get("scenarios", [])
    summary = data.get("summary", {})

    def pct(value: object) -> str:
        # Four decimals, never rounded to something flattering: 3/7 reads
        # 42.8571%, not 43%.
        return f"{value * 100:.4f}%" if isinstance(value, (int, float)) else "n/a"

    def seconds(value: object) -> str:
        # "n/a" where nothing was measured - never 0, which would read as
        # "instant" rather than "not measured".
        return f"{value:.0f}s" if isinstance(value, (int, float)) else "n/a"

    lines = [
        f"Run: `{result_file.name}` ({run_at})",
        "",
        "| Scenario | Detected | Latency (event time) |",
        "|---|---|---|",
    ]
    for entry in scenarios:
        name = entry.get("name", "unknown")
        if entry.get("detected"):
            detected = "yes"
        elif entry.get("expected_detector") is None:
            detected = "no (expected miss)"
        else:
            detected = "no"
        lines.append(f"| {name} | {detected} | {seconds(entry.get('detection_latency_seconds'))} |")

    lines += [
        "",
        "| Measure | Value |",
        "|---|---|",
        f"| Scenarios flagged, all (incl. expected misses) | "
        f"{summary.get('detection_rate_fraction', 'n/a')} ({pct(summary.get('detection_rate'))}) |",
        f"| Scenarios flagged, targeted only | "
        f"{summary.get('targeted_detection_rate_fraction', 'n/a')} "
        f"({pct(summary.get('targeted_detection_rate'))}) |",
        f"| Spurious incidents per scored window, clean replay | "
        f"{summary.get('false_positive_rate_fraction', 'n/a')} "
        f"({pct(summary.get('false_positive_rate'))}) |",
        f"| Weekly samples per bucket (median, lowest detector) | "
        f"{summary.get('weekly_samples_median', 'n/a')} |",
        f"| Median / p95 time to flag (event time) | "
        f"{seconds(summary.get('detection_latency_median_seconds'))} / "
        f"{seconds(summary.get('detection_latency_p95_seconds'))} |",
        f"| Warmed buckets over a contiguous {summary.get('span_weeks', 'n/a')}-week span | "
        f"{summary.get('warmed_buckets', 'n/a')} |",
        f"| Clean event-time hours covered | "
        f"{summary.get('clean_event_time_hours', 0):.2f} |"
        if isinstance(summary.get("clean_event_time_hours"), (int, float))
        else "| Clean event-time hours covered | n/a |",
        f"| Unattributed incidents, injected replay | "
        f"{summary.get('unattributed_injected_incidents', 'n/a')} |",
    ]

    spread = render_spread(sorted(RESULTS_DIR.glob("*.json")))
    if spread:
        lines += ["", spread]

    return "\n".join(lines)


def render_spread(result_files: list[Path]) -> str:
    """Cross-run spread, generated rather than typed.

    A single run cannot say whether a figure is stable, and CLAUDE.md V4
    is explicit that one green run means nothing. Where every run agrees
    this says so; where they don't it prints the range rather than a
    central value that would hide it.
    """
    if len(result_files) < 2:
        return ""

    runs = [json.loads(f.read_text(encoding="utf-8")) for f in result_files]
    summaries = [r.get("summary", {}) for r in runs]
    count = len(runs)

    def agreed(key: str) -> str | None:
        values = {json.dumps(s.get(key)) for s in summaries}
        return json.loads(next(iter(values))) if len(values) == 1 else None

    def spread_of(key: str) -> str:
        values = [s.get(key) for s in summaries if isinstance(s.get(key), (int, float))]
        if not values:
            return "n/a"
        low, high = min(values), max(values)
        if low == high:
            return f"{low:.0f} in all {count}"
        ratio = f", {high / low:.1f}x" if low else ""
        return f"{low:.0f} .. {high:.0f}{ratio}"

    def stable(key: str, label: str) -> str:
        value = agreed(key)
        return f"| {label} | {value} in all {count} |" if value is not None else \
               f"| {label} | varies: {spread_of(key)} |"

    lines = [
        f"Across {count} runs of the same input and code:",
        "",
        "| Measure | Across runs |",
        "|---|---|",
        stable("detection_rate_fraction", "Scenarios flagged, all"),
        stable("targeted_detection_rate_fraction", "Scenarios flagged, targeted only"),
        stable("false_positive_rate_fraction", "Spurious incidents per scored window"),
        stable("warmed_buckets", "Warmed buckets"),
        f"| Median time to flag (event time) | {spread_of('detection_latency_median_seconds')} |",
        f"| Unattributed incidents, injected replay | "
        f"{spread_of('unattributed_injected_incidents')} |",
    ]
    return "\n".join(lines)


def replace_block(readme_text: str, new_block: str) -> str:
    begin_idx = readme_text.find(BEGIN_MARKER)
    end_idx = readme_text.find(END_MARKER)
    if begin_idx == -1 or end_idx == -1:
        msg = f"README.md is missing {BEGIN_MARKER} / {END_MARKER} markers"
        raise SystemExit(msg)
    before = readme_text[: begin_idx + len(BEGIN_MARKER)]
    after = readme_text[end_idx:]
    return f"{before}\n{new_block}\n{after}"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--check",
        action="store_true",
        help="exit 1 if the committed block doesn't match a fresh regeneration",
    )
    args = parser.parse_args()

    new_block = render_block(newest_result_file())
    current_text = README.read_text(encoding="utf-8")
    updated_text = replace_block(current_text, new_block)

    if args.check:
        if updated_text != current_text:
            print("README.md benchmark block is stale.")
            print("Run: python scripts/sync_benchmark_readme.py")
            sys.exit(1)
        print("README.md benchmark block is up to date.")
        return

    README.write_text(updated_text, encoding="utf-8")
    result = newest_result_file()
    print(
        f"README.md benchmark block updated from {result.name if result else 'no result file'}."
    )


if __name__ == "__main__":
    main()
