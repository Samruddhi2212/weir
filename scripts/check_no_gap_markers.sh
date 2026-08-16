#!/usr/bin/env bash
# CLAUDE.md C1: no MISSING_VALIDATION / MISSING_TEST / TODO / FIXME markers
# left in shipped code. Scans source/config file types only - markdown docs
# that discuss this rule (CLAUDE.md, DEFENSE.md, docs/*.md) are excluded on
# purpose, since they legitimately name these markers as concepts.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

matches=$(git grep -nIE 'MISSING_VALIDATION|MISSING_TEST|TODO|FIXME' -- \
  '*.py' '*.java' '*.sql' '*.yaml' '*.yml' '*.ts' '*.tsx' '*.js' '*.jsx' \
  '*.sh' '*.go' '*.rs' \
  2>/dev/null | grep -v '^scripts/check_no_gap_markers\.sh:' || true)

if [ -n "$matches" ]; then
  echo "C1 violation: gap markers found in shipped code:"
  echo "$matches"
  exit 1
fi

exit 0
