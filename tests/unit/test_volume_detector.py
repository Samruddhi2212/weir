"""Unit tests for reliability/volume/detector.py's pure scoring logic.

Five cases promised by detector.py's own module docstring: normal
variation, true anomaly, legitimate seasonal spike, MAD=0, and
insufficient_baseline. No live database needed - detector.py is
deliberately I/O-free (DEFENSE.md #44/#45) so BucketState/config are
passed in directly here.

A local SimpleConfig, not VolumeDetectorConfig, supplies alpha/
min_observations directly rather than through half_life_weeks/
min_baseline_weeks math - these tests are about update_and_score's
scoring logic, not config.py's derivation of its inputs.
"""
from dataclasses import dataclass

from reliability.volume.detector import BucketState, scale_floor, update_and_score


@dataclass
class SimpleConfig:
    alpha: float = 0.1
    min_observations: int = 10
    z_threshold: float = 3.5
    mad_to_sigma_constant: float = 1.2533141373155003
    poisson_floor_min_count: float = 1.0


WARM = SimpleConfig()


def warm_state(ewma_mean, ewma_mad, observation_count=None):
    return BucketState(
        observation_count=observation_count if observation_count is not None else WARM.min_observations,
        ewma_mean=ewma_mean,
        ewma_mad=ewma_mad,
    )


def test_normal_variation_scores_but_is_not_incident():
    state = warm_state(ewma_mean=100.0, ewma_mad=5.0)

    result = update_and_score(state, observed_value=102.0, config=WARM)

    assert result.status == "scored"
    assert result.is_incident is False
    assert result.baseline_mean_at_time == 100.0
    assert result.threshold_used == WARM.z_threshold


def test_true_anomaly_is_flagged_as_incident():
    state = warm_state(ewma_mean=100.0, ewma_mad=5.0)

    result = update_and_score(state, observed_value=250.0, config=WARM)

    assert result.status == "scored"
    assert result.is_incident is True
    assert result.score > WARM.z_threshold


def test_legitimate_seasonal_spike_stays_under_threshold():
    # A real, sizeable jump relative to this bucket's own historical
    # spread - well above test_normal_variation's delta - but still
    # short of z_threshold, i.e. explained by the bucket's own tracked
    # dispersion rather than flagged as an incident.
    state = warm_state(ewma_mean=100.0, ewma_mad=10.0)

    result = update_and_score(state, observed_value=130.0, config=WARM)

    assert result.status == "scored"
    assert result.is_incident is False
    assert 0 < result.score < WARM.z_threshold


def test_mad_zero_falls_back_to_poisson_floor_not_division_by_zero():
    # A bucket whose history has been perfectly identical every time
    # (ewma_mad == 0.0) - scale_floor must not divide by zero, and the
    # Poisson-noise floor (sqrt(ewma_mean)), not the zeroed MAD term,
    # must be what's actually used.
    state = warm_state(ewma_mean=100.0, ewma_mad=0.0)

    result = update_and_score(state, observed_value=105.0, config=WARM)

    assert result.status == "scored"
    expected_scale = max(0.0, (100.0**0.5), WARM.poisson_floor_min_count)
    assert result.baseline_scale_at_time == expected_scale
    assert result.score == (105.0 - 100.0) / expected_scale


def test_scale_floor_uses_absolute_floor_at_zero_mean():
    # ewma_mean == 0 zeroes both the MAD term's context and
    # sqrt(ewma_mean) - only poisson_floor_min_count is left standing.
    assert scale_floor(ewma_mean=0.0, ewma_mad=0.0, config=WARM) == WARM.poisson_floor_min_count


def test_insufficient_baseline_returns_no_score():
    state = warm_state(ewma_mean=100.0, ewma_mad=5.0, observation_count=WARM.min_observations - 1)

    result = update_and_score(state, observed_value=999.0, config=WARM)

    assert result.status == "insufficient_baseline"
    assert result.score is None
    assert result.baseline_mean_at_time is None
    assert result.baseline_scale_at_time is None
    assert result.threshold_used is None
    assert result.is_incident is False
    # Warmup still progresses - this window's own update is applied to
    # new_state even though it wasn't warm enough to be scored itself
    # (DEFENSE.md #45's fix to incidents_schema.sql's own comment).
    assert result.new_state.observation_count == WARM.min_observations


def test_first_ever_observation_has_no_deviation_to_score():
    state = BucketState(observation_count=0, ewma_mean=None, ewma_mad=None)

    result = update_and_score(state, observed_value=42.0, config=WARM)

    assert result.status == "insufficient_baseline"
    assert result.new_state.ewma_mean == 42.0
    assert result.new_state.ewma_mad == 0.0
    assert result.new_state.observation_count == 1
