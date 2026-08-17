#!/usr/bin/env bash
# CLAUDE.md C4: every Docker image pinned to an explicit, non-latest tag
# AND a content digest (@sha256:...), not the tag alone.
#
# A tag alone is mutable - the same tag can be repushed with different
# content later, so tag-only pinning doesn't actually guarantee
# reproducibility. This was tightened after a real, still-partially-open
# investigation into a step-5 inconsistency between two otherwise-
# identical CI runs, where a mutable base-image tag was a live hypothesis
# that digest-pinning was meant to rule out. See DEFENSE.md #15.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

violations=0

check_line() {
  local line="$1" value="$2" source="$3"
  if [[ "$value" == *:latest ]]; then
    echo "C4 violation: :latest in $source -> $line"
    violations=1
    return
  fi
  if [[ "$value" != *:* ]]; then
    echo "C4 violation: no tag in $source -> $line"
    violations=1
    return
  fi
  # weir/* images are built locally (docker-compose.yml's build: for that
  # service), not pulled - they have no stable external digest to pin
  # against. Their reproducibility comes from the pinned base image and
  # pinned jar versions inside the Dockerfile that builds them, not from
  # a digest on the resulting local image itself.
  if [[ "$value" == weir/* ]]; then
    return
  fi
  if [[ "$value" != *@sha256:* ]]; then
    echo "C4 violation: tag without digest (@sha256:...) in $source -> $line"
    violations=1
  fi
}

if [ -f "docker-compose.yml" ]; then
  while IFS= read -r line; do
    value=$(echo "$line" | sed -E 's/^[[:space:]]*image:[[:space:]]*//' | tr -d '"'"'"'')
    check_line "$line" "$value" "docker-compose.yml"
  done < <(grep -E '^[[:space:]]*image:[[:space:]]*' docker-compose.yml)
fi

while IFS= read -r dockerfile; do
  while IFS= read -r line; do
    value=$(echo "$line" | sed -E 's/^FROM[[:space:]]+//i')
    check_line "$line" "$value" "$dockerfile"
  done < <(grep -iE '^FROM[[:space:]]+' "$dockerfile")
done < <(git ls-files '*Dockerfile*')

exit "$violations"
