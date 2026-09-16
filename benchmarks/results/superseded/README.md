# Superseded results

These five runs used **band sampling** - four `(weekday, hour)` bands
rather than continuous time - to warm baselines cheaply. They are kept
because history stays intact (CLAUDE.md rule 11), not because they are
comparable to the published result.

They are in a subdirectory specifically so `scripts/sync_benchmark_readme.py`
cannot see them: it globs `benchmarks/results/*.json`, non-recursively, and
its cross-run spread would otherwise average two incompatible
methodologies into one number.

Two of their figures were artifacts of the sampling rather than statements
about the detectors, which is why they were not published:

- the freshness detector was structurally unmeasurable, because band
  sampling leaves ~42-hour gaps between band occurrences and the
  freshness signal *is* the gap between windows;
- time-to-flag was quantised to band occurrences, so detection could only
  ever land in a sampled hour.

The published run replays contiguous time and has neither problem.
