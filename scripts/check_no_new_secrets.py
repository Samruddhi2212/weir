#!/usr/bin/env python3
"""Fail if detect-secrets finds anything not already in .secrets.baseline.

CLAUDE.md S2. This exists instead of calling detect-secrets directly
because both of its own entry points assert slightly the wrong thing:

  - `detect-secrets scan --baseline X` rewrites X in place, reports the
    baseline's own stored hashes as findings, and records path separators
    for whichever OS ran it - a baseline written on Windows never matches
    a Linux runner.
  - `detect-secrets-hook --baseline X` rewrites X whenever bookkeeping
    drifts (line numbers, its filter list) and exits non-zero asking for
    the update to be committed. It did exactly that on a Linux runner
    while exiting 0 on Windows for the same tree, so the check was
    platform-dependent for reasons unrelated to secrets.

What actually matters is whether a secret exists that nobody has audited.
So this compares (path, hashed_secret) pairs and ignores line numbers,
filter lists, timestamps and path separators. A finding that moved to a
different line is not news; a finding that did not exist before is.

Inline `pragma: allowlist secret` comments still work - the scan applies
its own allowlist filter before anything reaches here.
"""
import json
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
BASELINE = REPO_ROOT / ".secrets.baseline"


def normalise(path):
    return path.replace("\\", "/")


def pairs(results):
    return {
        (normalise(filename), finding["hashed_secret"])
        for filename, findings in results.items()
        for finding in findings
    }


def main():
    if not BASELINE.exists():
        print(f"FAIL: {BASELINE.name} is missing - nothing to compare against")
        return 1

    scan = subprocess.run(
        [sys.executable, "-m", "detect_secrets", "scan"],
        cwd=REPO_ROOT, capture_output=True, text=True, timeout=600,
    )
    if scan.returncode != 0:
        print(f"FAIL: detect-secrets scan exited {scan.returncode}")
        print(scan.stderr)
        return 1

    fresh = json.loads(scan.stdout).get("results", {})
    baseline = json.loads(BASELINE.read_text()).get("results", {})

    audited = pairs(baseline)
    # The baseline stores hashes of real findings, so scanning it finds
    # them again. detect-secrets' own newer filter excludes it; not every
    # baseline carries that filter, so exclude it here unconditionally.
    found = {p for p in pairs(fresh) if p[0] != BASELINE.name}

    new = sorted(found - audited)
    if new:
        print(f"FAIL: {len(new)} secret finding(s) not in {BASELINE.name}:")
        for filename, digest in new:
            print(f"  {filename}  (sha1 {digest[:12]}...)")
        print("")
        print("If it is a real secret, remove it and rotate it. If it is a")
        print("false positive, either add an inline `pragma: allowlist secret`")
        print("comment on that line, or audit it into the baseline:")
        print("  detect-secrets scan --baseline .secrets.baseline")
        print("  detect-secrets audit .secrets.baseline")
        return 1

    # V3: report on success too, so a passing run is auditable later.
    print(f"PASS: {len(found)} finding(s), all present in {BASELINE.name}")
    stale = sorted(audited - found)
    if stale:
        print(f"note: {len(stale)} baseline entr(ies) no longer found - "
              f"harmless, but the baseline could be regenerated:")
        for filename, digest in stale:
            print(f"  {filename}  (sha1 {digest[:12]}...)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
