from core.fsm import FSM, MockSensors
from core.states import State


def make_running_sensors():
    s = MockSensors()
    s.vacuum_1477_left_ok = True
    s.vacuum_1480_left_ok = True
    s.robotsuz_mode = False
    return s


def test_estop_from_robot_running_enters_red_handling():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    s = make_running_sensors()
    s.estop_pressed = True

    result = fsm.step(s)

    assert result["state"] == "RED_HANDLING"
    assert result["alarm_code"] == "ERROR_ACIL_STOP"
    assert result["error_count"] == 1
    assert fsm._state_before_error == State.ROBOT_RUNNING


def test_red_handling_stays_locked_without_reset_edge():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    s = make_running_sensors()
    s.estop_pressed = True
    fsm.step(s)  # -> RED_HANDLING

    # The fault clears physically, but nobody has pressed reset yet.
    s.estop_pressed = False
    result = fsm.step(s)

    assert result["state"] == "RED_HANDLING"
    assert result["error_count"] == 1


def test_red_handling_ignores_reset_already_held_high():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    # Reset button was already held down before this fault occurred, so
    # there is no False->True edge to react to when the fault fires.
    fsm._prev_reset_switch = True
    s = make_running_sensors()
    s.estop_pressed = True
    s.reset_switch = True

    result = fsm.step(s)

    assert result["state"] == "RED_HANDLING"


def test_red_handling_edge_triggered_reset_returns_to_previous_state():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    s = make_running_sensors()
    s.estop_pressed = True
    fsm.step(s)  # -> RED_HANDLING

    s.estop_pressed = False  # hazard cleared
    fsm.step(s)  # still locked, reset_switch is still False

    s.reset_switch = True  # operator presses reset: False -> True edge
    result = fsm.step(s)

    assert result["state"] == "ROBOT_RUNNING"
    assert result["alarm_code"] == "NONE"
    assert result["error_count"] == 1


def test_red_handling_reentry_increments_error_count():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    s = make_running_sensors()
    s.estop_pressed = True
    fsm.step(s)  # -> RED_HANDLING (error_count=1)

    s.estop_pressed = False
    s.reset_switch = True
    fsm.step(s)  # -> back to ROBOT_RUNNING
    s.reset_switch = False  # operator releases the reset button

    s.estop_pressed = True  # faults again
    result = fsm.step(s)

    assert result["state"] == "RED_HANDLING"
    assert result["error_count"] == 2


if __name__ == "__main__":
    test_estop_from_robot_running_enters_red_handling()
    test_red_handling_stays_locked_without_reset_edge()
    test_red_handling_ignores_reset_already_held_high()
    test_red_handling_edge_triggered_reset_returns_to_previous_state()
    test_red_handling_reentry_increments_error_count()
    print("Tüm testler geçti.")
