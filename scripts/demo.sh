#!/usr/bin/env bash
# The recorded form of docs/demo.md. Kept as a script rather than typed
# live so the GIF in the README is reproducible and so the two cannot
# drift: if a step here breaks, the workflow that records it fails.
#
# What this shows is the *mechanism* - metrics -> detector -> incident -
# not detection quality. Step 2 seeds a warm baseline as a fixture,
# because warming one honestly takes ~8 weeks of data per bucket. The
# published numbers come only from benchmarks/run_benchmark.py. That
# caveat is on screen in the recording too, not just in this comment.
set -euo pipefail

PAUSE="${DEMO_PAUSE:-1.6}"
PSQL=(docker compose exec -T postgres psql -U weir -d weir_catalog)

say() { printf '\033[1;36m%s\033[0m\n' "$*"; }
note() { printf '\033[2m%s\033[0m\n' "$*"; }
step() {
    printf '\n\033[1;33m$ %s\033[0m\n' "$*"
    sleep "$PAUSE"
}

say "Weir - volume detector, injected drop, end to end"
note "weir_metrics -> detector -> weir_incidents. Mechanism, not detection quality:"
note "step 2 seeds a warm baseline as a fixture. Real numbers are in the README."
sleep "$PAUSE"

step "psql -c 'SELECT count(*) AS incidents FROM weir_incidents.incidents;'"
"${PSQL[@]}" -c "SELECT count(*) AS incidents FROM weir_incidents.incidents;"
sleep "$PAUSE"

ALPHA=$(python -c 'from reliability.volume.config import DEFAULT_CONFIG as c; print(c.alpha)')
CONFIG_HASH=$(python -c 'from reliability.volume.config import DEFAULT_CONFIG as c; print(c.config_hash)')

step "seed a warm baseline for Monday 08:00 - mean 500, tight spread (fixture)"
note "config_hash is read from the config, never typed: the detector rejects a"
note "baseline whose hash disagrees with its own config (DEFENSE.md #45)."
"${PSQL[@]}" -c "
INSERT INTO weir_incidents.baseline_state
  (detector_name, bucket_weekday, bucket_hour, observation_count,
   ewma_mean, ewma_mad, alpha, config_hash)
VALUES ('volume', 0, 8, 1000, 500, 12, ${ALPHA}, '${CONFIG_HASH}')
ON CONFLICT (detector_name, bucket_weekday, bucket_hour) DO UPDATE
  SET observation_count = EXCLUDED.observation_count,
      ewma_mean = EXCLUDED.ewma_mean, ewma_mad = EXCLUDED.ewma_mad;"
sleep "$PAUSE"

step "python incidents/dev/volume_drop.py \
    --window-end '2025-01-06 08:31:00' --row-count 40"
note "the trigger prints the live baseline and the threshold a row count must"
note "cross, so the next step shows why it fires - not just that it did."
python incidents/dev/volume_drop.py --window-end "2025-01-06 08:31:00" --row-count 40
sleep "$PAUSE"

step "python incidents/dev/volume_drop.py \
    --window-end '2025-01-06 08:37:00' --row-count 500"
note "the lag buffer holds back the newest max_lag_seconds of windows, so"
note "08:31 is only eligible once a later window exists (DEFENSE.md #51)."
note "500 rows is normal for this bucket, so this one flags nothing itself."
python incidents/dev/volume_drop.py --window-end "2025-01-06 08:37:00" --row-count 500
sleep "$PAUSE"

step "python reliability/volume/run.py"
note "no --assume-no-more-arrivals: the lag buffer stays on, as in a real run."
python reliability/volume/run.py
sleep "$PAUSE"

step "psql -c 'SELECT ... FROM weir_incidents.incidents'"
"${PSQL[@]}" -c "
SELECT window_end, observed_value AS rows_seen, baseline_mean,
       round(score::numeric, 2) AS score
FROM weir_incidents.incidents WHERE detector_name = 'volume';"
sleep "$PAUSE"

# Fails the recording rather than shipping a GIF of a demo that did
# nothing - the failure modes and what each means are in docs/demo.md.
FOUND=$("${PSQL[@]}" -t -A -c \
    "SELECT count(*) FROM weir_incidents.incidents WHERE detector_name = 'volume';")
if [ "$FOUND" -lt 1 ]; then
    echo "FAIL: no incident was raised, so there is nothing to demonstrate"
    "${PSQL[@]}" -c "SELECT window_end, status, observed_value
                     FROM weir_incidents.scored_windows
                     ORDER BY window_end DESC LIMIT 5;"
    exit 1
fi

say "40 rows against a baseline of 500 - flagged, past the 3.5 threshold."
sleep 2

# asciinema exits 0 whether or not the command it recorded succeeded, so
# set -e alone cannot fail the workflow - it reported success over a
# recording that was nothing but a traceback. This sentinel is what the
# workflow actually checks.
: > demo-ok
