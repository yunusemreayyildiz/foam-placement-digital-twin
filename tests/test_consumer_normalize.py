"""
Unit tests for consumer.consumer.normalize_event - the only piece of
the Kafka -> TimescaleDB pipeline that has pure, dependency-free logic
(everything else touches a live broker/DB, so it belongs in an
integration test suite once the stack is actually running via
docker-compose).
"""

from consumer.consumer import normalize_event


def _raw_event(**overrides):
    base = {
        "timestamp": "2026-07-01T08:00:00.000Z",
        "machine_id": "foam_placement_01",
        "state": "MARKING",
        "right_count": 3,
        "left_count": 2,
        "error_count": 1,
        "alarm_code": "NONE",
        "sensor": {
            "vacuum_active": True,
            "camera_ok": True,
            "light_barrier_ok": True,
        },
    }
    base.update(overrides)
    return base


def test_normalize_event_flattens_sensor_block():
    result = normalize_event(_raw_event())

    assert result["state"] == "MARKING"
    assert result["right_count"] == 3
    assert result["vacuum_active"] is True
    assert result["camera_ok"] is True
    assert result["light_barrier_ok"] is True
    # sensor sub-dict should not leak into the flat row.
    assert "sensor" not in result


def test_normalize_event_defaults_missing_sensor_block():
    raw = _raw_event()
    del raw["sensor"]

    result = normalize_event(raw)

    assert result["vacuum_active"] is False
    assert result["camera_ok"] is False
    assert result["light_barrier_ok"] is True


def test_normalize_event_defaults_missing_machine_id():
    raw = _raw_event()
    del raw["machine_id"]

    result = normalize_event(raw)

    assert result["machine_id"] == "foam_placement_01"


def test_normalize_event_requires_state_and_timestamp():
    raw = _raw_event()
    del raw["state"]

    try:
        normalize_event(raw)
        assert False, "expected KeyError for missing 'state'"
    except KeyError:
        pass
