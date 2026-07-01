"""
Finite State Machine engine for the Foam Placement Machine Digital Twin.

Implements:
  - The nominal process flow (IDLE -> ... -> CYCLE_DONE) from architecture.md.
  - A parallel safety check that can interrupt from ANY monitored state.
  - An edge-triggered (False -> True) operator reset, so the FSM never
    auto-recovers from a safety fault on its own (see claude.md Phase 2).

This module is intentionally kept agnostic of Kafka / TimescaleDB.
telemetry/kafka_producer.py is responsible for serializing FSM output.
"""

from dataclasses import dataclass, field

from core.states import ErrorCode, SAFETY_MONITORED_STATES, State


@dataclass
class MockSensors:
    """
    Simulated PLC digital inputs, named after the sheet tags in
    config/io_mapping.yaml (IN81..IN112). Only the subset relevant to
    the current FSM logic is modeled; extend as new branches are added.
    """

    # Safety inputs
    light_curtain_ok: bool = True        # IN82 (True = clear, no breach)
    door_vibration_ok: bool = True        # IN105
    estop_pressed: bool = False           # IN106
    box_inner_barrier_right_ok: bool = True   # IN96
    box_inner_barrier_left_ok: bool = True    # IN97

    # Operator inputs
    reset_switch: bool = False            # IN83 - raw, level signal
    start_pushed: bool = False            # IN81

    # Process inputs
    part_present: bool = False            # sponge pick-up area sensor, IN101
    vacuum_1477_left_ok: bool = False     # IN92
    vacuum_1477_right_ok: bool = False    # IN93
    vacuum_1480_left_ok: bool = False     # IN94
    vacuum_1480_right_ok: bool = False    # IN95
    camera_ok: bool = False               # camera exe acknowledge, OUT110 feedback
    right_kasa_full: bool = False
    left_kasa_full: bool = False
    robotsuz_mode: bool = False           # IN100 - manual/robotless override
    manual_move_complete: bool = False    # IN84-87 result feedback
    right_part_in_kasa: bool = False   # sensor that confirms the right part has fallen into the kasa
    left_part_in_kasa: bool = False    # sensor that confirms the left part has fallen into the kasa
    


@dataclass
class FSM:
    """
    Deterministic scan-cycle FSM. Call step(sensors) once per scan tick.
    """

    state: State = State.IDLE
    error_code: ErrorCode = ErrorCode.NONE

    # Production counters (telemetry payload fields per architecture.md 3.4)
    right_count: int = 0
    left_count: int = 0
    error_count: int = 0

    # Internal bookkeeping
    _state_before_error: State = field(default=State.IDLE, repr=False)
    _prev_reset_switch: bool = field(default=False, repr=False)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def step(self, sensors: MockSensors) -> dict:
        """
        Advance the FSM by exactly one scan cycle.
        Returns a telemetry-ready dict (see architecture.md 3.4 schema,
        timestamp/machine_id are added by the caller / kafka_producer).
        """
        reset_edge = self._consume_reset_edge(sensors.reset_switch)

        if self.state == State.RED_HANDLING:
            self._handle_red_state(reset_edge)
        else:
            fault = self._check_safety(sensors)
            if fault is not None:
                self._enter_red_handling(fault)
            else:
                self._advance(sensors)

        return self._build_telemetry(sensors)

    # ------------------------------------------------------------------
    # Safety
    # ------------------------------------------------------------------
    def _check_safety(self, sensors: MockSensors) -> ErrorCode | None:
        """
        Parallel safety check, evaluated every scan cycle while in a
        SAFETY_MONITORED_STATES state. Returns the fault reason, or
        None if all safety inputs are healthy.
        """
        if self.state not in SAFETY_MONITORED_STATES:
            return None

        if sensors.estop_pressed:
            return ErrorCode.ERROR_ACIL_STOP
        if not sensors.light_curtain_ok:
            return ErrorCode.ERROR_ISIK_BARIYERI_YETKISIZ
        if not sensors.door_vibration_ok:
            return ErrorCode.ERROR_KAPI_TITRESIM
        if not (sensors.box_inner_barrier_right_ok and sensors.box_inner_barrier_left_ok):
            return ErrorCode.ERROR_KASA_ICI_HATALI
        if sensors.right_kasa_full or sensors.left_kasa_full:
            return ErrorCode.ERROR_KASA_DOLU

        # Vacuum loss is only fatal mid-handling, not while idling.
        if self.state in (
            State.ROBOT_RUNNING,
            State.CAMERA_CHECK,
            State.MARKING,
            State.RIGHT_RELEASE,
            State.LEFT_RELEASE_WAIT,
        ):
            vacuum_ok = (
                sensors.vacuum_1477_left_ok or sensors.vacuum_1477_right_ok
            ) and (sensors.vacuum_1480_left_ok or sensors.vacuum_1480_right_ok)
            if not vacuum_ok:
                return ErrorCode.ERROR_VACUUM_KAYBI

        return None

    def _enter_red_handling(self, fault: ErrorCode) -> None:
        self._state_before_error = self.state
        self.state = State.RED_HANDLING
        self.error_code = fault
        self.error_count += 1

    def _handle_red_state(self, reset_edge: bool) -> None:
        """
        Stay locked in RED_HANDLING until an explicit False->True
        transition on the reset switch is observed (Phase 2 fix:
        no silent auto-recovery on the next scan cycle).
        """
        if reset_edge:
            self.state = self._state_before_error
            self.error_code = ErrorCode.NONE

    def _consume_reset_edge(self, reset_switch: bool) -> bool:
        edge = reset_switch and not self._prev_reset_switch
        self._prev_reset_switch = reset_switch
        return edge

    # ------------------------------------------------------------------
    # Nominal process flow
    # ------------------------------------------------------------------
    def _advance(self, sensors: MockSensors) -> None:
        if self.state == State.IDLE:
            if sensors.start_pushed and sensors.part_present:
                self.state = State.VACUUM_ON

        elif self.state == State.VACUUM_ON:
            vacuum_ok = (
                sensors.vacuum_1477_left_ok or sensors.vacuum_1477_right_ok
            ) and (sensors.vacuum_1480_left_ok or sensors.vacuum_1480_right_ok)
            if vacuum_ok:
                self.state = State.ROBOT_RUNNING

        elif self.state == State.ROBOT_RUNNING:
            if sensors.robotsuz_mode:
                if sensors.manual_move_complete:
                    self.state = State.CAMERA_CHECK
            else:
                self.state = State.CAMERA_CHECK

        elif self.state == State.CAMERA_CHECK:
            if sensors.camera_ok:
                self.state = State.MARKING

        elif self.state == State.MARKING:
            self.state = State.RIGHT_RELEASE

        elif self.state == State.RIGHT_RELEASE:
            if sensors.right_part_in_kasa:
                self.right_count += 1
                self.state = State.LEFT_RELEASE_WAIT

        elif self.state == State.LEFT_RELEASE_WAIT:
            self.state = State.WAIT_LEFT_IN_KASA

        elif self.state == State.WAIT_LEFT_IN_KASA:
            if sensors.left_part_in_kasa:
                self.left_count += 1
                self.state = State.CYCLE_DONE

        elif self.state == State.CYCLE_DONE:
            self.state = State.IDLE

    # ------------------------------------------------------------------
    # Telemetry
    # ------------------------------------------------------------------
    def _build_telemetry(self, sensors: MockSensors) -> dict:
        return {
            "state": self.state.name,
            "right_count": self.right_count,
            "left_count": self.left_count,
            "error_count": self.error_count,
            "alarm_code": self.error_code.name,
            "sensor": {
                "vacuum_active": (
                    sensors.vacuum_1477_left_ok or sensors.vacuum_1477_right_ok
                )
                and (sensors.vacuum_1480_left_ok or sensors.vacuum_1480_right_ok),
                "camera_ok": sensors.camera_ok,
                "light_barrier_ok": sensors.light_curtain_ok,
            },
        }