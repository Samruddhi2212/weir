#!/usr/bin/env bash
# Fails if a tracked markdown file outside benchmarks/results/ states a
# detection-performance metric - "detection rate", "false positive rate"
# / "false-positive rate", "detection latency", or "throughput" -
# followed by a number. Narrowed from an earlier broad percentage/latency
# scan, which false-positived on legitimate config values (DEFENSE.md #1's
# "checkpoint interval: 60s"). See DEFENSE.md #9.
#
# This is a cheap backstop, not the real guarantee: a fabricated number
# that avoids these exact phrases would slip past it. The real guarantee
# is scripts/sync_benchmark_readme.py, which generates the README's
# benchmark numbers directly from benchmarks/results/*.json instead of
# letting anyone type them by hand.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

pattern='(detection rate|false[-[:space:]]positive rate|detection latency|throughput)[^0-9]{0,40}[0-9]'

violations=0
while IFS= read -r f; do
  hits=$(grep -nEi "$pattern" "$f" || true)
  if [ -n "$hits" ]; then
    echo "$f:"
    echo "$hits"
    violations=1
  fi
done < <(git ls-files '*.md' | grep -v '^benchmarks/results/')

exit "$violations"
