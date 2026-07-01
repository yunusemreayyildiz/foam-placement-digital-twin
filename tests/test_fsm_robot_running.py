from core.fsm import FSM, MockSensors
from core.states import State


def make_sensors(robotsuz, manual_complete):
    s = MockSensors()
    s.vacuum_1477_left_ok = True
    s.vacuum_1480_left_ok = True
    s.robotsuz_mode = robotsuz
    s.manual_move_complete = manual_complete
    s.right_part_in_kasa = True
    s.left_part_in_kasa = True
    return s

def test_right_release_advances_when_part_confirmed():
    fsm = FSM()
    fsm.state = State.RIGHT_RELEASE
    s = make_sensors(robotsuz=False, manual_complete=False)
    s.right_part_in_kasa = True      
    result = fsm.step(s)               
    assert result["state"] == "LEFT_RELEASE_WAIT"
    assert result["right_count"] == 1

def test_right_release_advances_when_part_unconfirmed():
    fsm = FSM()
    fsm.state = State.RIGHT_RELEASE
    s = make_sensors(robotsuz=False, manual_complete=False)
    s.right_part_in_kasa = False      
    result = fsm.step(s)               
    s.right_part_in_kasa = False
    assert result["state"] == "RIGHT_RELEASE"
    assert result["right_count"] == 0

def test_robotsuz_mode_waits_for_manual_move():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    result = fsm.step(make_sensors(robotsuz=True, manual_complete=False))
    assert result["state"] == "ROBOT_RUNNING"


def test_robotsuz_mode_advances_after_manual_move():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    result = fsm.step(make_sensors(robotsuz=True, manual_complete=True))
    assert result["state"] == "CAMERA_CHECK"


def test_robotlu_mode_advances_automatically():
    fsm = FSM()
    fsm.state = State.ROBOT_RUNNING
    result = fsm.step(make_sensors(robotsuz=False, manual_complete=False))
    assert result["state"] == "CAMERA_CHECK"



if __name__ == "__main__":
    # pytest henüz kurulmadığı için manuel çalıştırma yöntemi.
    # pytest eklendiğinde bu blok hiç gerekmeyecek.
    #test_robotsuz_mode_waits_for_manual_move()
    #test_robotsuz_mode_advances_after_manual_move()
    #test_robotlu_mode_advances_automatically()
    test_right_release_advances_when_part_confirmed()
    test_right_release_advances_when_part_unconfirmed()
    print("Tüm testler geçti.")