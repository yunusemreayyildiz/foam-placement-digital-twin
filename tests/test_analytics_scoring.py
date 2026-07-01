import pytest
from sklearn.ensemble import IsolationForest

from analytics.scoring import (
    FEATURE_STATE_ORDER,
    ewma_update,
    feature_vector,
    score_cycle,
    stat_anomaly_flag,
)


def test_ewma_update_seeds_on_first_call():
    mean, var = ewma_update(None, None, 10.0)
    assert mean == 10.0
    assert var == 0.0


def test_ewma_update_moves_toward_new_value():
    mean, var = ewma_update(10.0, 0.0, 20.0, alpha=0.5)
    assert mean == pytest.approx(15.0)
    assert var >= 0.0


def test_stat_anomaly_flag_none_before_min_history():
    assert stat_anomaly_flag(x=100.0, mean=10.0, var=1.0, cycles_seen=1) is None


def test_stat_anomaly_flag_true_for_outlier_with_enough_history():
    # mean=10, stddev=1 -> anything beyond +/-3 is flagged.
    assert stat_anomaly_flag(x=50.0, mean=10.0, var=1.0, cycles_seen=10, k=3.0) is True


def test_stat_anomaly_flag_false_within_control_limits():
    assert stat_anomaly_flag(x=10.5, mean=10.0, var=1.0, cycles_seen=10, k=3.0) is False


def test_feature_vector_shape_matches_state_order():
    features = {
        "cycle_duration_seconds": 5.0,
        "dwell_seconds": {state: float(i) for i, state in enumerate(FEATURE_STATE_ORDER)},
        "red_handling_seconds": 0.0,
        "had_red_handling": False,
    }

    vector = feature_vector(features)

    # duration + one entry per state + red_handling_seconds + had_red_handling flag
    assert len(vector) == 1 + len(FEATURE_STATE_ORDER) + 1 + 1
    assert vector[0] == 5.0
    assert vector[-1] == 0.0


def test_feature_vector_defaults_missing_state_dwell_to_zero():
    features = {
        "cycle_duration_seconds": 5.0,
        "dwell_seconds": {},
        "red_handling_seconds": 0.0,
        "had_red_handling": True,
    }

    vector = feature_vector(features)

    assert vector[-1] == 1.0
    assert all(v == 0.0 for v in vector[1:-2])


def test_score_cycle_flags_obvious_outlier():
    normal_features = [
        {
            "cycle_duration_seconds": 4.0 + 0.1 * i,
            "dwell_seconds": {state: 0.5 for state in FEATURE_STATE_ORDER},
            "red_handling_seconds": 0.0,
            "had_red_handling": False,
        }
        for i in range(20)
    ]
    vectors = [feature_vector(f) for f in normal_features]

    model = IsolationForest(n_estimators=50, contamination="auto", random_state=42)
    model.fit(vectors)

    outlier_features = {
        "cycle_duration_seconds": 500.0,
        "dwell_seconds": {state: 50.0 for state in FEATURE_STATE_ORDER},
        "red_handling_seconds": 100.0,
        "had_red_handling": True,
    }

    score, is_anomaly = score_cycle(model, outlier_features)

    assert isinstance(score, float)
    assert is_anomaly is True
