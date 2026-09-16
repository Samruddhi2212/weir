# 60-second demo

Shows the volume detector flagging an injected drop, end to end, through
the real pipeline path: `weir_metrics` → detector → `weir_incidents`.

The recorded form of this walkthrough is `scripts/demo.sh`, and
`.github/workflows/demo-gif.yml` runs it against a live stack to produce
the GIF in the README. Keeping the two together is deliberate: this page
previously described a sequence that could not actually produce an
incident, and nothing caught it until the recording failed.

**What this is and isn't.** This is a demonstration of the *mechanism*,
not evidence of detection quality. Step 2 seeds a warm baseline directly
so the recording fits in a minute — a detector with an unwarmed baseline
correctly returns `insufficient_baseline` and flags nothing, and warming
one honestly takes ~8 weeks of data per bucket (which is what
`benchmarks/run_benchmark.py` does, and why it takes half an hour). The
seeded values are a fixture. The published numbers are in the README's
benchmark block and come only from real runs.

## Before recording

The stack must already be up — don't record the image pull.

```bash
cd weir
docker compose up -d kafka postgres
docker compose exec -T postgres psql -U weir -d weir_catalog < reliability/store/schema.sql
docker compose exec -T postgres psql -U weir -d weir_catalog < reliability/store/incidents_schema.sql
pip install -r reliability/volume/requirements.txt
```

## The recording (6 commands, ~60s)

**1 — There are no incidents yet.**

```bash
docker compose exec -T postgres psql -U weir -d weir_catalog \
  -c "SELECT count(*) AS incidents FROM weir_incidents.incidents;"
```

**2 — Seed a warm baseline for one bucket (the fixture).**

Monday 08:00, mean 500 rows/window, tight spread. `config_hash` is read
from the config rather than typed, because the detector rejects a
baseline whose hash doesn't match its own config — see DEFENSE.md #45.

```bash
docker compose exec -T postgres psql -U weir -d weir_catalog -c "
INSERT INTO weir_incidents.baseline_state
  (detector_name, bucket_weekday, bucket_hour, observation_count,
   ewma_mean, ewma_mad, alpha, config_hash)
VALUES ('volume', 0, 8, 1000, 500, 12,
  $(python -c 'from reliability.volume.config import DEFAULT_CONFIG as c; print(c.alpha)'),
  '$(python -c 'from reliability.volume.config import DEFAULT_CONFIG as c; print(c.config_hash)')')
ON CONFLICT (detector_name, bucket_weekday, bucket_hour) DO UPDATE
  SET observation_count = EXCLUDED.observation_count,
      ewma_mean = EXCLUDED.ewma_mean, ewma_mad = EXCLUDED.ewma_mad;"
```

**3 — Inject the drop.** The trigger prints the live baseline and the
threshold a row count has to cross before writing anything, so the
recording shows *why* the next step fires rather than just that it did.

```bash
python incidents/dev/volume_drop.py --window-end "2025-01-06 08:31:00" --row-count 40
```

**4 — Inject a later window so the lag buffer clears.**

`fetch_eligible_windows` holds back the newest `max_lag_seconds` (300s) of
windows, because in a live stream the most recent windows may still be
receiving late data. With only one window in the table, that window *is*
the newest, so it is held back and the detector processes nothing. A later
window is what makes 08:31 eligible — the same synthetic-trailing-window
trick `benchmarks/run_benchmark.py` uses for exactly this reason
(DEFENSE.md #51), rather than switching the buffer off.

500 rows is normal for this bucket, so this window flags nothing itself.

```bash
python incidents/dev/volume_drop.py --window-end "2025-01-06 08:37:00" --row-count 500
```

**5 — Run the detector.** No `--assume-no-more-arrivals`: the lag buffer
stays on, exactly as it would in a real run.

```bash
python reliability/volume/run.py
```

**6 — The incident.**

```bash
docker compose exec -T postgres psql -U weir -d weir_catalog -c "
SELECT window_end, observed_value AS rows_seen, baseline_mean,
       round(score::numeric, 2) AS score
FROM weir_incidents.incidents WHERE detector_name = 'volume';"
```

Expected: one row, `rows_seen` 40 against a baseline mean of 500, score
far past the 3.5 threshold.

## If step 6 shows nothing

Check `scored_windows` for the same window — the status says which path
it took, and each is a real behaviour rather than a failure:

```bash
docker compose exec -T postgres psql -U weir -d weir_catalog -c "
SELECT window_end, status, observed_value FROM weir_incidents.scored_windows
ORDER BY window_end DESC LIMIT 5;"
```

- `insufficient_baseline` — step 2 didn't take; `observation_count` must
  be at least `min_observations` (480).
- `skipped_late` — the window is behind `detector_progress`'s frontier.
  Pick a later `--window-end`, or clear the detector's progress row.
- nothing at all — step 4 didn't take. The window is still inside the lag
  buffer's trailing `max_lag_seconds`, so inject one more than 300s after
  it. Omitting step 4 is what made an earlier version of this walkthrough
  impossible to complete: with a single window in the table it is always
  the newest, and the newest is always held back.
