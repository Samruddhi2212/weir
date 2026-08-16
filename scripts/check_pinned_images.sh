#!/usr/bin/env bash
# CLAUDE.md C4: every Docker image pinned to an explicit, non-latest tag.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

file="docker-compose.yml"
[ -f "$file" ] || exit 0

violations=0
while IFS= read -r line; do
  value=$(echo "$line" | sed -E 's/^[[:space:]]*image:[[:space:]]*//' | tr -d '"'"'"'')
  if [[ "$value" == *:latest ]] || [[ "$value" != *:* ]]; then
    echo "C4 violation: unpinned image -> $line"
    violations=1
  fi
done < <(grep -E '^[[:space:]]*image:[[:space:]]*' "$file")

exit "$violations"
