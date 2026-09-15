#!/usr/bin/env python3
"""Sample pg_stat_activity, pg_locks and insert counters on an interval.

Exists to settle one specific open question. Benchmark run 34757520031
spent 4h36m in the block that scores the clean phase, but a probe measured
scoring at 75.8 window-scorings/s, which accounts for roughly a third of
that. Two hypotheses - fsync cost and shared-buffer pressure - were
measured and both came back flat, and reading the code has not found the
rest. So this observes the database directly instead:

  - if a lock wait is responsible, ungranted rows in pg_locks show it
    directly, with the blocking pid and the relation named;
  - if Postgres is idle the whole time, the database is eliminated
    entirely and the time is going somewhere outside it.

Either outcome is a fact rather than another hypothesis, which is the
point. Read-only: this only ever SELECTs from catalog views.

Full detail goes to --out on every sample. stdout stays quiet apart from
a periodic heartbeat and anything actually notable (an ungranted lock, a
connection failure), so this can run alongside the benchmark without
burying its output.
"""
import argparse
import datetime
import sys
import time

# Backends other than our own that are worth naming in the heartbeat.
INTERESTING_STATES = ("active", "idle in transaction", "idle in transaction (aborted)")

# Insert counters double as an independent progress signal: pg_stat's
# cumulative n_tup_ins is far cheaper than COUNT(*) on a growing table,
# and a flat counter alongside an active backend is exactly the shape a
# stall would take.
COUNTER_TABLES = (
    ("weir_incidents", "scored_windows"),
    ("weir_incidents", "incidents"),
    ("weir_metrics", "window_metrics"),
    ("weir_metrics", "column_metrics"),
)

ACTIVITY_SQL = """
SELECT pid, state, wait_event_type, wait_event,
       EXTRACT(EPOCH FROM (now() - xact_start)),
       EXTRACT(EPOCH FROM (now() - state_change)),
       left(regexp_replace(coalesce(query, ''), '\\s+', ' ', 'g'), 200)
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()
ORDER BY pid
"""

# Ungranted locks, each paired with whatever is blocking it. This is the
# query that answers the question if the answer is "a lock wait".
BLOCKED_SQL = """
SELECT w.pid,
       coalesce(c.relname, '(non-relation)'),
       w.locktype, w.mode,
       pg_blocking_pids(w.pid),
       left(regexp_replace(coalesce(a.query, ''), '\\s+', ' ', 'g'), 200)
FROM pg_locks w
LEFT JOIN pg_class c ON c.oid = w.relation
LEFT JOIN pg_stat_activity a ON a.pid = w.pid
WHERE NOT w.granted
ORDER BY w.pid
"""

COUNTER_SQL = """
SELECT schemaname, relname, n_tup_ins, n_live_tup
FROM pg_stat_user_tables
WHERE (schemaname, relname) IN %s
ORDER BY schemaname, relname
"""


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def sample(conn):
    """One sample. Returns (activity rows, blocked rows, counter dict)."""
    with conn.cursor() as cur:
        cur.execute(ACTIVITY_SQL)
        activity = cur.fetchall()
        cur.execute(BLOCKED_SQL)
        blocked = cur.fetchall()
        cur.execute(COUNTER_SQL, (COUNTER_TABLES,))
        counters = {f"{s}.{r}": (ins, live) for s, r, ins, live in cur.fetchall()}
    return activity, blocked, counters


def write_sample(out, stamp, activity, blocked, counters):
    out.write(f"=== {stamp} ===\n")
    for name, (ins, live) in sorted(counters.items()):
        out.write(f"  counter {name}: n_tup_ins={ins} n_live_tup={live}\n")
    for pid, state, wet, we, xact_age, state_age, query in activity:
        xact = f"{xact_age:.1f}s" if xact_age is not None else "-"
        held = f"{state_age:.1f}s" if state_age is not None else "-"
        out.write(f"  pid {pid} state={state!r} wait={wet}/{we} "
                  f"xact_age={xact} in_state={held} query={query!r}\n")
    for pid, relname, locktype, mode, blockers, query in blocked:
        out.write(f"  BLOCKED pid {pid} wants {mode} on {relname} ({locktype}), "
                  f"blocked by {blockers}, query={query!r}\n")
    if not activity:
        out.write("  (no other backends connected)\n")
    out.flush()


def main():
    import psycopg

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", required=True, help="file to append full samples to")
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--heartbeat", type=float, default=60.0,
                        help="seconds between one-line stdout summaries")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=5432)
    parser.add_argument("--dbname", default="weir_catalog")
    parser.add_argument("--user", default="weir")
    parser.add_argument("--password", default="weir")
    args = parser.parse_args()

    conninfo = (
        f"host={args.host} port={args.port} dbname={args.dbname} "
        f"user={args.user} password={args.password}"
    )

    conn = None
    last_heartbeat = 0.0
    # Reported once rather than every 5s - an unresolved lock wait would
    # otherwise print hundreds of identical lines.
    reported_blocks = set()
    samples = 0

    with open(args.out, "a", buffering=1) as out:
        out.write(f"=== sampler started {now()}, every {args.interval}s ===\n")
        print(f"[pg-sampler] started, sampling every {args.interval}s -> {args.out}",
              flush=True)
        while True:
            try:
                if conn is None or conn.closed:
                    conn = psycopg.connect(conninfo, autocommit=True, connect_timeout=10)
                stamp = now()
                activity, blocked, counters = sample(conn)
                samples += 1
                write_sample(out, stamp, activity, blocked, counters)

                for pid, relname, locktype, mode, blockers, query in blocked:
                    key = (pid, relname, mode)
                    if key not in reported_blocks:
                        reported_blocks.add(key)
                        print(f"[pg-sampler] LOCK WAIT: pid {pid} wants {mode} on "
                              f"{relname} ({locktype}), blocked by {blockers}",
                              flush=True)

                if time.monotonic() - last_heartbeat >= args.heartbeat:
                    last_heartbeat = time.monotonic()
                    busy = [f"{pid}:{state}/{wet or '-'}"
                            for pid, state, wet, _, _, _, _ in activity
                            if state in INTERESTING_STATES]
                    inserts = counters.get("weir_incidents.scored_windows", (0, 0))[0]
                    print(f"[pg-sampler] {stamp} samples={samples} "
                          f"scored_windows_inserts={inserts} "
                          f"blocked={len(blocked)} busy={busy or 'none'}", flush=True)
            except KeyboardInterrupt:
                break
            except Exception as exc:  # noqa: BLE001 - a sampler must outlive a blip
                out.write(f"=== {now()} sampler error: {type(exc).__name__}: {exc} ===\n")
                print(f"[pg-sampler] error: {type(exc).__name__}: {exc}", flush=True)
                if conn is not None and not conn.closed:
                    conn.close()
                conn = None
            time.sleep(args.interval)

    if conn is not None and not conn.closed:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
