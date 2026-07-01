from analytics.cycle_boundary import CycleAccumulator


def _event(state, i):
    return {"timestamp": f"2026-07-01T00:00:{i:02d}+00:00", "state": state}


def test_no_boundary_before_leaving_idle():
    acc = CycleAccumulator()
    assert acc.push(_event("IDLE", 0)) is None
    # Still IDLE, never left - re-seeing IDLE is not a boundary yet.
    assert acc.push(_event("IDLE", 1)) is None


def test_closes_cycle_on_idle_reentry_after_leaving():
    acc = CycleAccumulator()
    acc.push(_event("IDLE", 0))
    acc.push(_event("VACUUM_ON", 1))
    acc.push(_event("ROBOT_RUNNING", 2))
    acc.push(_event("CYCLE_DONE", 3))
    closed = acc.push(_event("IDLE", 4))

    assert closed is not None
    assert [e["state"] for e in closed] == ["IDLE", "VACUUM_ON", "ROBOT_RUNNING", "CYCLE_DONE", "IDLE"]


def test_red_handling_excursion_does_not_close_cycle():
    acc = CycleAccumulator()
    acc.push(_event("IDLE", 0))
    acc.push(_event("ROBOT_RUNNING", 1))
    acc.push(_event("RED_HANDLING", 2))
    acc.push(_event("RED_HANDLING", 3))
    # Reset recovers back into ROBOT_RUNNING (per core/fsm.py), not IDLE.
    acc.push(_event("ROBOT_RUNNING", 4))
    assert acc.push(_event("CAMERA_CHECK", 5)) is None
    closed = acc.push(_event("IDLE", 6))

    assert closed is not None
    states = [e["state"] for e in closed]
    assert states == [
        "IDLE",
        "ROBOT_RUNNING",
        "RED_HANDLING",
        "RED_HANDLING",
        "ROBOT_RUNNING",
        "CAMERA_CHECK",
        "IDLE",
    ]


def test_new_cycle_starts_immediately_after_close():
    acc = CycleAccumulator()
    acc.push(_event("IDLE", 0))
    acc.push(_event("VACUUM_ON", 1))
    acc.push(_event("IDLE", 2))  # closes cycle 1, opens cycle 2

    acc.push(_event("VACUUM_ON", 3))
    closed_2 = acc.push(_event("IDLE", 4))

    assert [e["state"] for e in closed_2] == ["IDLE", "VACUUM_ON", "IDLE"]
