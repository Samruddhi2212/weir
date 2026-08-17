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

    lines = [
        f"Run: `{result_file.name}` ({run_at})",
        "",
        "| Scenario | Detection Rate | False Positive Rate | Detection Latency (ms) |",
        "|---|---|---|---|",
    ]
    for s in scenarios:
        name = s.get("scenario", "unknown")
        detection_rate = s.get("detection_rate")
        fp_rate = s.get("false_positive_rate")
        latency_ms = s.get("detection_latency_ms")
        dr = (
            f"{detection_rate * 100:.1f}%"
            if isinstance(detection_rate, (int, float))
            else "n/a"
        )
        fp = f"{fp_rate * 100:.1f}%" if isinstance(fp_rate, (int, float)) else "n/a"
        lat = str(latency_ms) if latency_ms is not None else "n/a"
        lines.append(f"| {name} | {dr} | {fp} | {lat} |")

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
