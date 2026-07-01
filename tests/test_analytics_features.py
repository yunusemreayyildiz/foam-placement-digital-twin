import pytest

from analytics.features import compute_cycle_features


def _event(state, ts, **overrides):
    e = {
        "timestamp": ts,
        "state": state,
        "right_count": 0,
        "left_count": 0,
        "error_count": 0,
        "alarm_code": "NONE",
    }
    e.update(overrides)
    return e


def test_nominal_cycle_computes_duration_and_dwell():
    events = [
        _event("IDLE", "2026-07-01T00:00:00+00:00"),
        _event("VACUUM_ON", "2026-07-01T00:00:01+00:00"),
        _event("ROBOT_RUNNING", "2026-07-01T00:00:02+00:00"),
        _event("CYCLE_DONE", "2026-07-01T00:00:03+00:00", right_count=1, left_count=1),
        _event("IDLE", "2026-07-01T00:00:04+00:00", right_count=1, left_count=1),  # boundary
    ]

    features = compute_cycle_features(events)

    assert features["cycle_duration_seconds"] == pytest.approx(4.0)
    assert features["dwell_seconds"]["IDLE"] == pytest.approx(1.0)
    assert features["dwell_seconds"]["VACUUM_ON"] == pytest.approx(1.0)
    assert features["dwell_seconds"]["ROBOT_RUNNING"] == pytest.approx(1.0)
    assert features["dwell_seconds"]["CYCLE_DONE"] == pytest.approx(1.0)
    assert features["had_red_handling"] is False
    assert features["red_handling_seconds"] == 0.0
    assert features["alarm_codes"] == []
    assert features["right_count_delta"] == 1
    assert features["left_count_delta"] == 1
    assert features["error_count_at_cycle_end"] == 0


def test_red_handling_excursion_flagged_with_correct_dwell():
    events = [
        _event("IDLE", "2026-07-01T00:00:00+00:00"),
        _event("ROBOT_RUNNING", "2026-07-01T00:00:01+00:00"),
        _event("RED_HANDLING", "2026-07-01T00:00:02+00:00", alarm_code="ERROR_ACIL_STOP", error_count=1),
        _event("RED_HANDLING", "2026-07-01T00:00:03+00:00", alarm_code="ERROR_ACIL_STOP", error_count=1),
        _event("ROBOT_RUNNING", "2026-07-01T00:00:04.5+00:00", error_count=1),
        _event("CYCLE_DONE", "2026-07-01T00:00:05+00:00", right_count=1, left_count=1, error_count=1),
        _event("IDLE", "2026-07-01T00:00:06+00:00", right_count=1, left_count=1, error_count=1),  # boundary
    ]

    features = compute_cycle_features(events)

    assert features["had_red_handling"] is True
    # dwell runs from the first RED_HANDLING event (t=2) until the
    # state actually changes at t=4.5, i.e. 2.5s, not just the 1s gap
    # between the two RED_HANDLING samples.
    assert features["red_handling_seconds"] == pytest.approx(2.5)
    assert features["alarm_codes"] == ["ERROR_ACIL_STOP"]
    assert features["error_count_at_cycle_end"] == 1


def test_requires_at_least_two_events():
    with pytest.raises(ValueError):
        compute_cycle_features([_event("IDLE", "2026-07-01T00:00:00+00:00")])


def test_missing_state_key_raises():
    with pytest.raises(ValueError):
        compute_cycle_features(
            [
                {"timestamp": "2026-07-01T00:00:00+00:00"},
                _event("IDLE", "2026-07-01T00:00:01+00:00"),
            ]
        )


def test_malformed_timestamp_raises():
    with pytest.raises(ValueError):
        compute_cycle_features(
            [
                _event("IDLE", "not-a-timestamp"),
                _event("IDLE", "2026-07-01T00:00:01+00:00"),
            ]
        )
