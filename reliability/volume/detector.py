"""Part 4.1: the volume detector's core scoring logic.

Deliberately pure functions with no I/O - Postgres reading/writing
lives in adapter.py. Keeping this module I/O-free is what makes the
five unit-test cases (normal variation, true anomaly, legitimate
seasonal spike, MAD=0, insufficient_baseline) trivial to write without
a live database.

Statistical note, stated plainly rather than implied away (DEFENSE.md
#45): this tracks an EWMA-of-mean-absolute-deviation (MeanAD), an
EWMA-recursive approximation - not Iglewicz & Hoaglin's (1993) true
median/MAD-based modified z-score, even though it reuses their 3.5
threshold convention and z-score shape. An exact-MAD variant would
need a retained sorted historical sample per bucket, which is exactly
the O(1)-state tradeoff this streaming design exists to avoid. The
scale is rescaled by sqrt(pi/2) ~= 1.2533 (the MeanAD-to-sigma
constant for a normal distribution), not MAD's 1.4826, so the
resulting score is genuinely in sigma-equivalent units.
"""
import math
from dataclasses import dataclass
from typing import Optional


@dataclass
class BucketState:
    """Mirrors weir_incidents.baseline_state's per-bucket columns.
    ewma_mean/ewma_mad are None only for a bucket that has never been
    updated at all (observation_count == 0)."""

    observation_count: int
    ewma_mean: Optional[float]
    ewma_mad: Optional[float]


@dataclass
class ScoreResult:
    status: str  # 'scored' | 'insufficient_baseline'
    observed_value: float
    new_state: BucketState
    baseline_mean_at_time: Optional[float] = None
    baseline_scale_at_time: Optional[float] = None
    score: Optional[float] = None
    threshold_used: Optional[float] = None  # config.z_threshold, only set when status='scored'

    @property
    def is_incident(self):
        return self.status == "scored" and abs(self.score) > self.threshold_used


def update_and_score(state: BucketState, observed_value: float, config) -> ScoreResult:
    """Applies one window's observed_value to a bucket's EWMA state
    and, if the baseline was ALREADY warm enough (checked against
    observation_count BEFORE this window's own update - DEFENSE.md
    #45's fix to incidents_schema.sql's own comment), computes a score
    against the PRE-update mean/scale. Matches the reference
    ewma_ad.py's own "score against yesterday's state, then update"
    ordering (DEFENSE.md #44), just re-keyed by bucket instead of
    device_id and using a MeanAD-based scale instead of variance.

    Always returns a new_state - every status except 'skipped_late'
    (an ordering/lateness outcome, decided by adapter.py before this
    function is ever called - DEFENSE.md #45) updates baseline_state,
    which is the only way a bucket's warmup ever progresses at all.
    """
    was_warm_enough = state.observation_count >= config.min_observations

    if state.ewma_mean is None:
        # First-ever observation for this bucket - nothing to compare
        # against yet, so there's no meaningful deviation either.
        new_mean = observed_value
        new_mad = 0.0
    else:
        new_mean = config.alpha * observed_value + (1 - config.alpha) * state.ewma_mean
        # Deviation from the NEW mean, not the old one - matching the
        # reference algorithm's own convention (`reading.x - new_ewma`,
        # not `reading.x - state.ewma`, DEFENSE.md #44).
        abs_deviation = abs(observed_value - new_mean)
        new_mad = config.alpha * abs_deviation + (1 - config.alpha) * state.ewma_mad

    new_state = BucketState(
        observation_count=state.observation_count + 1,
        ewma_mean=new_mean,
        ewma_mad=new_mad,
    )

    if not was_warm_enough:
        return ScoreResult(
            status="insufficient_baseline",
            observed_value=observed_value,
            new_state=new_state,
        )

    scale = scale_floor(state.ewma_mean, state.ewma_mad, config)
    score = (observed_value - state.ewma_mean) / scale

    return ScoreResult(
        status="scored",
        observed_value=observed_value,
        new_state=new_state,
        baseline_mean_at_time=state.ewma_mean,
        baseline_scale_at_time=scale,
        score=score,
        threshold_used=config.z_threshold,
    )


def scale_floor(ewma_mean, ewma_mad, config):
    """The floored, sigma-equivalent scale used as the z-score's
    denominator. Three terms, largest wins (DEFENSE.md #44/#45):

    1. `mad_to_sigma_constant * ewma_mad` - the actual tracked
       dispersion, rescaled from MeanAD to sigma-equivalent units.
       Zero when historical values are identical (MAD=0 case) - this
       is exactly why it can't be trusted alone.
    2. `sqrt(ewma_mean)` - a Poisson-noise floor. Row count is a count
       of arrivals; under a simple homogeneous-arrivals assumption,
       variance ~= mean, so sqrt(mean) is a principled lower bound on
       plausible noise for a bucket of that magnitude - self-scaling,
       not an arbitrary constant.
    3. `poisson_floor_min_count` - an absolute floor under BOTH of the
       above, for the zero-mean edge case where sqrt(ewma_mean) would
       itself be 0. Row counts are integers; a sub-1 difference is
       meaningless.
    """
    mad_scale = config.mad_to_sigma_constant * (ewma_mad or 0.0)
    poisson_scale = math.sqrt(ewma_mean) if ewma_mean and ewma_mean > 0 else 0.0
    return max(mad_scale, poisson_scale, config.poisson_floor_min_count)
