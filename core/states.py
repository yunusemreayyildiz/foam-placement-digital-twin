"""
System state definitions for the Foam Placement Machine Digital Twin.

These states mirror the physical SCARA robot cell's process sequence,
as defined in docs/architecture.md (Section 3.3 - FSM State Flow) and
claude.md (Phase 1 backlog).
"""

from enum import Enum, auto


class State(Enum):
    """Primary FSM states. Order reflects the nominal (happy-path) flow."""

    IDLE = auto()
    VACUUM_ON = auto()
    ROBOT_RUNNING = auto()
    CAMERA_CHECK = auto()
    MARKING = auto()
    RIGHT_RELEASE = auto()
    LEFT_RELEASE_WAIT = auto()
    WAIT_LEFT_IN_KASA = auto()
    CYCLE_DONE = auto()


    # Non-nominal / safety state. The FSM enters here from ANY state when
    # a safety interrupt fires, and only leaves via an explicit, edge-
    # triggered operator reset (see Phase 2 in claude.md).
    RED_HANDLING = auto()


class ErrorCode(Enum):
    """
    Specific reasons the FSM may transition into RED_HANDLING.
    Kept separate from State so telemetry can report *why* the machine
    is halted without multiplying the number of FSM states.
    """

    NONE = auto()
    ERROR_ISIK_BARIYERI_YETKISIZ = auto()   # Light curtain breached
    ERROR_KASA_ICI_HATALI = auto()          # Box-inner barrier fault
    ERROR_ACIL_STOP = auto()                # Hardware E-Stop pressed
    ERROR_KAPI_TITRESIM = auto()            # Door / vibration safety fault
    ERROR_VACUUM_KAYBI = auto()             # Vacuum feedback lost mid-cycle
    ERROR_KASA_DOLU = auto()                # Right/left kasa full warning


# States from which a safety interrupt is checked every scan cycle.
# Only RED_HANDLING is excluded: the FSM is already in the safety
# state itself there, and recovery is handled separately via the
# edge-triggered reset (see _handle_red_state in fsm.py).
SAFETY_MONITORED_STATES = frozenset(
    {
        State.IDLE,
        State.VACUUM_ON,
        State.ROBOT_RUNNING,
        State.CAMERA_CHECK,
        State.MARKING,
        State.RIGHT_RELEASE,
        State.LEFT_RELEASE_WAIT,
        State.WAIT_LEFT_IN_KASA,
        State.CYCLE_DONE,
    }
)
