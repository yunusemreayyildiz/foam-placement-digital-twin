"""
Pure anomaly-scoring functions for the analytics layer (claude.md Phase 5).

Two independent, explainable layers:
  1. EWMA + control limits on cycle_duration_seconds - a classic SPC
     technique, bootstraps in ~5-10 cycles, adapts to gradual baseline
     drift while still catching sudden deviations.
  2. IsolationForest over the full engineered feature vector - trained
     offline by analytics/model_training/train_isolation_forest.py,
     loaded and only *scored* against here (see score_cycle()).
"""

from __future__ import annotations

DEFAULT_EWMA_ALPHA = 0.2
DEFAULT_STAT_ANOMALY_K = 3.0
MIN_CYCLES_FOR_STAT_ANOMALY = 5

# Order matters: this is the exact feature vector shape
# train_isolation_forest.py fits its model on - keep the two in sync.
FEATURE_STATE_ORDER = [
    "VACUUM_ON",
    "ROBOT_RUNNING",
    "CAMERA_CHECK",
    "MARKING",
    "RIGHT_RELEASE",
    "LEFT_RELEASE_WAIT",
    "WAIT_LEFT_IN_KASA",
]


def ewma_update(
    prev_mean: float | None,
    prev_var: float | None,
    x: float,
    alpha: float = DEFAULT_EWMA_ALPHA,
) -> tuple[float, float]:
    """
    One step of an exponentially-weighted moving average/variance
    update (Welford-style EWMA variance). On the first call
    (prev_mean is None) seeds mean=x, var=0. Returns (new_mean, new_var).
    """
    if prev_mean is None:
        return x, 0.0
    diff = x - prev_mean
    incr = alpha * diff
    new_mean = prev_mean + incr
    new_var = (1 - alpha) * (prev_var + diff * incr)
    return new_mean, new_var


def stat_anomaly_flag(
    x: float,
    mean: float,
    var: float,
    cycles_seen: int,
    k: float = DEFAULT_STAT_ANOMALY_K,
) -> bool | None:
    """
    Returns None (insufficient history) before MIN_CYCLES_FOR_STAT_ANOMALY
    cycles have been observed - an honest "don't know yet" rather than
    a guess - else True/False per an EWMA +/- k*stddev control limit.
    """
    if cycles_seen < MIN_CYCLES_FOR_STAT_ANOMALY:
        return None
    stddev = var**0.5
    if stddev == 0:
        return False
    return abs(x - mean) > k * stddev


def feature_vector(features: dict) -> list[float]:
    """
    Build the fixed-order numeric feature vector an IsolationForest
    model is trained/scored on, from a compute_cycle_features()-shaped
    dict: [cycle_duration_seconds, dwell_seconds for each state in
    FEATURE_STATE_ORDER, red_handling_seconds, had_red_handling (0/1)].
    """
    dwell = features["dwell_seconds"]
    vector = [features["cycle_duration_seconds"]]
    vector.extend(dwell.get(state, 0.0) for state in FEATURE_STATE_ORDER)
    vector.append(features["red_handling_seconds"])
    vector.append(1.0 if features["had_red_handling"] else 0.0)
    return vector


def score_cycle(model, features: dict) -> tuple[float, bool]:
    """
    Score one cycle's features with a pre-trained IsolationForest.
    Returns (anomaly_score, is_anomaly) - scikit-learn's
    decision_function() is negative for outliers, so is_anomaly is
    exactly score < 0.
    """
    vector = feature_vector(features)
    score = float(model.decision_function([vector])[0])
    return score, score < 0
