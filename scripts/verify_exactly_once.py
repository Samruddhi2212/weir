#!/usr/bin/env python3
"""Step 6 exactly-once validation: compare Iceberg's committed contents
against the producer's emission log and the object store's actual files.

Reports, without adjusting anything to make the numbers look clean (per
explicit instruction - duplicates and gaps are findings, not failures):
  - events emitted (confirmed-delivered, from the emission log)
  - events landed (rows actually read back from the Iceberg table)
  - duplicates, by event_key
  - gaps: emitted keys never found in Iceberg at all
  - orphaned data files: physically present under the table's warehouse
    path but not referenced by any snapshot, past or present (see
    DEFENSE.md #19 for what "orphaned" means here and #26 for how this
    script finds them)

Every comparison below is against an exact parsed value (a regex-
anchored sentinel extraction, or a JSON field from the Filer's HTTP API)
- never a loose match against mixed/decorated output. Audited against
the DEFENSE.md #14 class of bug (a check that "passes" without having
verified anything) before being written this way.

This script's own exit code reflects whether it managed to produce a
report at all (0 = yes, whatever the report says; 1 = a tooling failure
- couldn't reach Flink, couldn't parse its output, couldn't read the
emission log). Duplicates or gaps in the report are not treated as this
script's own failure.
"""
import argparse
import http.client
import json
import os
import re
import subprocess
import sys
import tempfile
from collections import Counter
from urllib.parse import quote


def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    sys.exit(1)


def dump_compose_logs(services):
    print("--- container logs (on failure), tail 200 ---", file=sys.stderr)
    subprocess.run(
        ["docker", "compose", "logs", "--tail=200", *services],
        check=False,
    )
    print("--- end container logs ---", file=sys.stderr)


def read_emission_log(path):
    """Returns (emitted_keys: set, per_key_count: Counter, total_lines: int)."""
    if not os.path.exists(path):
        fail(f"emission log not found: {path}")
    per_key_count = Counter()
    total_lines = 0
    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue
            total_lines += 1
            try:
                entry = json.loads(line)
            except json.JSONDecodeError as e:
                fail(f"emission log line {line_no} is not valid JSON: {e}")
            if "event_key" not in entry:
                fail(f"emission log line {line_no} has no event_key field")
            per_key_count[entry["event_key"]] += 1
    return set(per_key_count.keys()), per_key_count, total_lines


def run_sql_dump(access_key, secret_key, timeout_sec):
    """Runs scripts/sql/exactly_once_verify.sql via sql-client.sh, returns raw stdout+stderr."""
    with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".sql") as tmp:
        resolved_path = tmp.name
        with open("scripts/sql/exactly_once_verify.sql", "r", encoding="utf-8") as src:
            content = src.read()
        content = content.replace("__WEIR_S3_ACCESS_KEY__", access_key)
        content = content.replace("__WEIR_S3_SECRET_KEY__", secret_key)
        tmp.write(content)
    os.chmod(resolved_path, 0o644)

    try:
        container_path = "/tmp/weir_exactly_once_verify_resolved.sql"
        cp = subprocess.run(
            ["docker", "compose", "cp", resolved_path, f"flink-jobmanager:{container_path}"],
            capture_output=True, text=True,
        )
        if cp.returncode != 0:
            print(cp.stdout)
            print(cp.stderr, file=sys.stderr)
            fail("could not copy exactly_once_verify.sql into flink-jobmanager")

        try:
            result = subprocess.run(
                ["docker", "compose", "exec", "-T", "flink-jobmanager",
                 "./bin/sql-client.sh", "-f", container_path],
                capture_output=True, text=True, timeout=timeout_sec,
            )
        except subprocess.TimeoutExpired as e:
            # text=True guarantees str, not bytes, here.
            print("--- raw output before timeout ---")
            print(e.stdout or "")
            dump_compose_logs(["flink-jobmanager", "flink-taskmanager"])
            fail(f"sql-client.sh timed out after {timeout_sec}s reading back eos_events")

        raw = result.stdout + result.stderr
        print("--- raw output: exactly_once_verify.sql ---")
        print(raw)
        print("--- end raw output: exactly_once_verify.sql ---")

        if result.returncode != 0:
            dump_compose_logs(["flink-jobmanager", "flink-taskmanager"])
            fail(f"sql-client.sh exited {result.returncode} reading back eos_events")
        return raw
    finally:
        os.remove(resolved_path)


def parse_rows(raw_output):
    """Returns (landed_rows: list[(event_key, event_ts)], per_key_count: Counter)."""
    rows = re.findall(r"WEIR_ROW=([^,\s]+),(-?\d+)", raw_output)
    per_key_count = Counter(key for key, _ts in rows)
    return rows, per_key_count


def parse_referenced_files(raw_output):
    """Returns set of file_path strings from $all_data_files."""
    return set(re.findall(r"WEIR_FILE=(\S+)", raw_output))


def _filer_get_json(host, port, dir_path, last_file_name=None):
    """dir_path is quoted component-wise (it's a real filesystem path, may
    contain characters http.client won't send verbatim); last_file_name,
    if given, is a separate, independently-quoted query parameter - the
    two must not be quoted together as one string, or the '?'/'=' query
    syntax itself gets percent-encoded into garbage."""
    quoted_dir = quote(dir_path, safe="/")
    request_path = quoted_dir if quoted_dir.endswith("/") else quoted_dir + "/"
    if last_file_name:
        request_path = f"{request_path}?lastFileName={quote(last_file_name, safe='')}"
    conn = http.client.HTTPConnection(host, port, timeout=15)
    try:
        conn.request("GET", request_path, headers={"Accept": "application/json"})
        resp = conn.getresponse()
        body = resp.read()
        if resp.status == 404:
            return None
        if resp.status != 200:
            fail(f"SeaweedFS Filer returned HTTP {resp.status} for {dir_path}: {body[:500]!r}")
        return json.loads(body)
    finally:
        conn.close()


def walk_filer_files(host, port, root_path):
    """Recursively lists every file (not directory) under root_path via
    SeaweedFS's Filer JSON HTTP API. A file is distinguished from a
    directory by the presence of a "chunks" key, per DEFENSE.md #26 -
    an inferred heuristic, not exhaustively verified against every
    SeaweedFS edge case; flagged here again at the point of use, not
    just in DEFENSE.md."""
    files = set()
    stack = [root_path]
    while stack:
        current = stack.pop()
        last_file_name = None
        while True:
            data = _filer_get_json(host, port, current, last_file_name)
            if data is None:
                break
            entries = data.get("Entries") or []
            for entry in entries:
                full_path = entry.get("FullPath", "")
                if "chunks" in entry:
                    files.add(full_path)
                else:
                    stack.append(full_path)
            if not data.get("ShouldDisplayLoadMore"):
                break
            last_file_name = data.get("LastFileName", "")
            if not last_file_name:
                break
    return files


def normalize_warehouse_path(p):
    """Iceberg's file_path (from $all_data_files) is an s3a:// URI;
    SeaweedFS Filer's FullPath is a /buckets/<bucket>/... path. Same
    underlying object, two different string schemes for the same bucket
    root - both are stripped down to the path relative to the bucket
    root before comparing, or every real file would spuriously look
    orphaned simply because the two prefixes never textually match."""
    p = re.sub(r"^s3a://weir-warehouse/?", "", p)
    p = re.sub(r"^/buckets/weir-warehouse/?", "", p)
    return p.strip("/")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--emission-log", required=True)
    parser.add_argument("--s3-access-key", default=os.environ.get("WEIR_S3_ACCESS_KEY", "admin"))
    parser.add_argument("--s3-secret-key", default=os.environ.get("WEIR_S3_SECRET_KEY", "password123"))
    parser.add_argument("--filer-host", default="localhost")
    parser.add_argument("--filer-port", type=int, default=int(os.environ.get("SEAWEEDFS_FILER_PORT", "8888")))
    parser.add_argument(
        "--warehouse-path", default="/buckets/weir-warehouse/warehouse/eos_test/eos_events/data",
        # Scoped to the data/ subdirectory specifically, not the whole
        # table directory - $all_data_files (parse_referenced_files)
        # only ever enumerates the data layer, never Iceberg's own
        # metadata.json/manifest/manifest-list files under metadata/.
        # Walking the whole table directory and diffing against a
        # data-only reference set made every metadata file look
        # "orphaned" in this script's first real run - not a finding,
        # a scope bug in the comparison itself. See DEFENSE.md #36.
        help="Filer-namespace path to the eos_events table's data/ directory (not the whole table dir)",
    )
    parser.add_argument("--sql-timeout", type=int, default=90)
    parser.add_argument("--report-json", default=None, help="optional path to also write the report as JSON")
    args = parser.parse_args()

    emission_log_abs = os.path.abspath(args.emission_log)
    report_json_abs = os.path.abspath(args.report_json) if args.report_json else None

    repo_root = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True, check=True,
    ).stdout.strip()
    os.chdir(repo_root)

    emitted_keys, emitted_per_key, emitted_lines = read_emission_log(emission_log_abs)
    if len(emitted_keys) != emitted_lines:
        # The producer's own key sequence is monotonic and unique - a
        # duplicate key in the EMISSION log itself (not Iceberg's output)
        # would mean the producer double-logged a confirmed delivery,
        # which would invalidate this run's ground truth. Reported, not
        # silently deduped away.
        dup_in_log = {k: c for k, c in emitted_per_key.items() if c > 1}
        fail(
            f"emission log has {emitted_lines} lines but only {len(emitted_keys)} distinct "
            f"event_key values - the ground truth itself is inconsistent: {dup_in_log}"
        )

    raw = run_sql_dump(args.s3_access_key, args.s3_secret_key, args.sql_timeout)
    landed_rows, landed_per_key = parse_rows(raw)
    referenced_files = parse_referenced_files(raw)

    landed_count = len(landed_rows)
    landed_keys = set(landed_per_key.keys())
    duplicates = {k: c for k, c in landed_per_key.items() if c > 1}
    gaps = emitted_keys - landed_keys
    unexpected = landed_keys - emitted_keys  # landed but never emitted this run - also a real finding

    actual_files_raw = walk_filer_files(args.filer_host, args.filer_port, args.warehouse_path)
    actual_files = {normalize_warehouse_path(p) for p in actual_files_raw}
    referenced_files = {normalize_warehouse_path(p) for p in referenced_files}
    orphaned_files = actual_files - referenced_files

    report = {
        "events_emitted": len(emitted_keys),
        "events_landed_total_rows": landed_count,
        "events_landed_distinct_keys": len(landed_keys),
        "duplicate_keys": duplicates,
        "duplicate_key_count": len(duplicates),
        "gap_keys": sorted(gaps),
        "gap_count": len(gaps),
        "unexpected_keys_not_in_emission_log": sorted(unexpected),
        "referenced_file_count": len(referenced_files),
        "actual_file_count": len(actual_files),
        "orphaned_files": sorted(orphaned_files),
        "orphaned_file_count": len(orphaned_files),
    }

    print("")
    print("=== verify_exactly_once.py report ===")
    print(json.dumps(report, indent=2))

    if report_json_abs:
        with open(report_json_abs, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)

    print("")
    if report["gap_count"] == 0 and report["duplicate_key_count"] == 0:
        print("VERDICT: no gaps, no duplicates observed this run - consistent with exactly-once.")
    elif report["gap_count"] > 0:
        print(
            f"VERDICT: {report['gap_count']} gap(s) observed - events confirmed delivered to Kafka "
            f"never landed in Iceberg at all. This is data loss, not a duplicate-tolerance question."
        )
    else:
        print(
            f"VERDICT: {report['duplicate_key_count']} duplicate key(s) observed, 0 gaps - "
            f"consistent with at-least-once, not exactly-once, for this run. See DEFENSE.md for "
            f"the corresponding entry and the recommended downstream-dedup response."
        )
    if report["orphaned_file_count"] > 0:
        print(
            f"NOTE: {report['orphaned_file_count']} orphaned data file(s) found - wasted storage from "
            f"an interrupted commit window (see DEFENSE.md #19), not itself a correctness problem."
        )

    sys.exit(0)


if __name__ == "__main__":
    main()
