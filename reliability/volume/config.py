"""Part 4.1: the volume detector's ONE config file - every tunable
parameter lives here, none scattered through the detector's other
modules. A plain Python module (not YAML) so this needs no new
dependency beyond the stdlib. Design rationale for every parameter:
DEFENSE.md #44/#45.
"""
import hashlib
import json
from dataclasses import dataclass


@dataclass(frozen=True)
class VolumeDetectorConfig:
    detector_name: str = "volume"

    # Bucketing - rush-hour/overnight seasonality is a local-time
    # phenomenon, not UTC (DEFENSE.md #45): a UTC bucket would smear
    # it across the EST/EDT boundary and shift whenever DST changes.
    timezone: str = "America/New_York"
    # 1-minute windows x 1 hour = 60 occurrences of a bucket per week.
    # Tied to metrics_job.sql's TUMBLE window size (DEFENSE.md #41);
    # must be updated if that window size ever changes.
    windows_per_bucket_occurrence: int = 60

    # Baseline warmup and decay, both measured in distinct weekly
    # occurrences of a bucket, not raw per-window updates - the
    # statistically meaningful sample size for a stable scale estimate
    # is "distinct weeks," which are autocorrelated within one hour's
    # 60 per-minute updates (DEFENSE.md #44). 8 weeks is a floor
    # against the worst cold-start noise, not a claim of full
    # maturity - published false-positive rates should read as an
    # upper bound (DEFENSE.md #44/README.md).
    half_life_weeks: float = 8.0
    min_baseline_weeks: float = 8.0

    # Scoring: reuses Iglewicz & Hoaglin's (1993) modified-z-score
    # threshold convention, but NOT their formula as-is - this
    # detector tracks an EWMA-of-mean-absolute-deviation (MeanAD), not
    # a true median/MAD, since an O(1) streaming update can't produce
    # a true median or MAD at all (DEFENSE.md #45).
    z_threshold: float = 3.5
    # sqrt(pi/2) ~= 1.2533 - the MeanAD-to-sigma constant for a normal
    # distribution, confirmed independently (not MAD's 1.4826, which
    # is calibrated for a true median-based MAD instead). Reusing
    # 0.6745/MAD's constant here would make the score's true
    # sigma-equivalent threshold ~2.96, not the 3.5 the code would
    # claim (DEFENSE.md #45).
    mad_to_sigma_constant: float = 1.2533141373155003
    # Absolute floor under the Poisson-noise floor (DEFENSE.md #44) -
    # row counts are integers; a sub-1 difference is meaningless.
    poisson_floor_min_count: float = 1.0

    # Ordering / lateness buffer against Postgres write-order lateness
    # (JDBC sink flush/checkpoint cadence) - NOT Kafka-level event-time
    # lateness, which metrics_job.sql's own watermark already resolves
    # before a row ever reaches weir_metrics (DEFENSE.md #40/#42/#45's
    # explicit distinction between the two).
    max_lag_seconds: int = 300

    @property
    def alpha(self):
        """EWMA decay - derived from half_life_weeks, not set
        directly, so the two can never silently drift apart."""
        total_updates_for_half_life = self.half_life_weeks * self.windows_per_bucket_occurrence
        return 1 - 0.5 ** (1 / total_updates_for_half_life)

    @property
    def min_observations(self):
        """Raw per-window update count corresponding to
        min_baseline_weeks - derived, not set directly."""
        return round(self.min_baseline_weeks * self.windows_per_bucket_occurrence)

    @property
    def config_hash(self):
        """Hash of only the fields that affect baseline_state's own
        trajectory - NOT z_threshold/max_lag_seconds, which affect
        scoring/ordering decisions but never the EWMA itself (DEFENSE.md
        #45's sweep-tractability design: varying those doesn't
        invalidate an already-warmed baseline). Stored per-bucket-row
        in baseline_state and checked on every update - a mismatch
        against a differently-configured baseline fails loudly rather
        than silently blending two decay rates."""
        baseline_relevant = {
            "detector_name": self.detector_name,
            "timezone": self.timezone,
            "windows_per_bucket_occurrence": self.windows_per_bucket_occurrence,
            "half_life_weeks": self.half_life_weeks,
        }
        canonical = json.dumps(baseline_relevant, sort_keys=True)
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


DEFAULT_CONFIG = VolumeDetectorConfig()
