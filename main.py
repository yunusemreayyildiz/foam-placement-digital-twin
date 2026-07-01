"""
Entry point for the Foam Placement Machine Digital Twin edge service.

Wires together (architecture.md, Section 3.1 - Control Axis):
  - core.fsm.FSM              the deterministic scan-cycle controller
  - a sensor driver            currently a scripted MockSensors demo
                                sequence (no physical PLC wired up yet -
                                swap _DemoSensorDriver for a real IO
                                bridge later without touching the FSM)
  - telemetry.kafka_producer   publishes telemetry asynchronously,
                                decoupled from the control loop
                                (architecture.md 4.2)

Run:
    python main.py
"""

from __future__ import annotations

import asyncio
import logging
import os

from core.fsm import FSM, MockSensors
from core.states import State
from telemetry.kafka_producer import TelemetryProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] main: %(message)s",
)
logger = logging.getLogger(__name__)

SCAN_INTERVAL_SECONDS = float(os.getenv("SCAN_INTERVAL_SECONDS", "0.5"))

# Phase 2 live verification (claude.md): when enabled, periodically drives
# the FSM through a real RED_HANDLING safety-interrupt scenario instead of
# only the happy path, so the Kafka -> consumer -> TimescaleDB -> Grafana
# pipeline actually carries an error event end-to-end.
ENABLE_FAULT_SCENARIO = os.getenv("ENABLE_FAULT_SCENARIO", "false").strip().lower() in (
    "1",
    "true",
    "yes",
)
FAULT_EVERY_N_CYCLES = int(os.getenv("FAULT_EVERY_N_CYCLES", "3"))
RED_HANDLING_HOLD_TICKS = int(os.getenv("RED_HANDLING_HOLD_TICKS", "3"))


class _DemoSensorDriver:
    """
    Scripted sensor sequence that walks the FSM through one full
    happy-path cycle (IDLE -> ... -> CYCLE_DONE) and repeats.

    This stands in for the real PLC IO bridge (config/io_mapping.yaml)
    until that hardware/OPC integration exists. It only decides *when*
    each condition becomes true; the FSM itself still owns all
    transition logic (core/fsm.py) - this class never touches FSM
    state directly.
    """

    def __init__(self) -> None:
        self._sensors = MockSensors()
        self._ticks_in_state = 0

    def read(self, fsm: FSM) -> MockSensors:
        s = self._sensors
        self._ticks_in_state += 1

        if fsm.state == State.IDLE:
            s.start_pushed = True
            s.part_present = True

        elif fsm.state == State.VACUUM_ON:
            # Simulate vacuum feedback arriving after one scan cycle.
            s.vacuum_1477_left_ok = True
            s.vacuum_1480_left_ok = True

        elif fsm.state == State.ROBOT_RUNNING:
            s.manual_move_complete = True  # only matters if robotsuz_mode

        elif fsm.state == State.CAMERA_CHECK:
            s.camera_ok = True

        elif fsm.state == State.RIGHT_RELEASE:
            s.right_part_in_kasa = True

        elif fsm.state == State.WAIT_LEFT_IN_KASA:
            s.left_part_in_kasa = True

        elif fsm.state == State.CYCLE_DONE:
            # Reset one-shot flags before the next cycle starts.
            s.start_pushed = False
            s.part_present = False
            s.vacuum_1477_left_ok = False
            s.vacuum_1480_left_ok = False
            s.manual_move_complete = False
            s.camera_ok = False
            s.right_part_in_kasa = False
            s.left_part_in_kasa = False
            self._ticks_in_state = 0

        return s


class _FaultInjectingSensorDriver(_DemoSensorDriver):
    """
    Extends the happy-path demo with a periodic E-Stop safety interrupt,
    so the RED_HANDLING path (claude.md Phase 2) gets exercised through
    the real pipeline instead of only unit tests.

    Every FAULT_EVERY_N_CYCLES completed cycles, the next ROBOT_RUNNING
    entry is met with a simulated E-Stop press. The FSM must latch into
    RED_HANDLING and stay there - even after the E-Stop is physically
    released - until a separate, edge-triggered reset button (False ->
    True on reset_switch) is pressed. This mirrors the real machine's
    two-step recovery (clear the hazard, then explicitly acknowledge
    it) and this driver still never touches FSM state directly.
    """

    def __init__(
        self,
        fault_every_n_cycles: int = FAULT_EVERY_N_CYCLES,
        hold_ticks: int = RED_HANDLING_HOLD_TICKS,
    ) -> None:
        super().__init__()
        self._fault_every_n_cycles = fault_every_n_cycles
        self._hold_ticks = hold_ticks
        self._completed_cycles = 0
        self._fault_stage = None  # None | "holding" | "releasing" | "resetting"
        self._red_ticks = 0

    def read(self, fsm: FSM) -> MockSensors:
        s = super().read(fsm)

        if fsm.state == State.CYCLE_DONE:
            self._completed_cycles += 1

        due_for_fault = (
            self._fault_stage is None
            and self._completed_cycles > 0
            and self._completed_cycles % self._fault_every_n_cycles == 0
        )

        if fsm.state == State.ROBOT_RUNNING and due_for_fault:
            logger.warning("[fault-scenario] injecting E-Stop press in ROBOT_RUNNING")
            s.estop_pressed = True
            self._fault_stage = "holding"
            self._red_ticks = 0
            return s

        if self._fault_stage == "holding":
            self._red_ticks += 1
            if self._red_ticks >= self._hold_ticks:
                logger.warning(
                    "[fault-scenario] releasing physical E-Stop (still locked, awaiting reset)"
                )
                s.estop_pressed = False
                self._fault_stage = "releasing"
            return s

        if self._fault_stage == "releasing":
            logger.warning("[fault-scenario] operator presses reset button")
            s.reset_switch = True
            self._fault_stage = "resetting"
            return s

        if self._fault_stage == "resetting":
            s.reset_switch = False  # operator releases the reset button
            self._fault_stage = None
            self._completed_cycles = 0  # restart the countdown to the next fault
            return s

        return s


async def run() -> None:
    fsm = FSM()
    driver = _FaultInjectingSensorDriver() if ENABLE_FAULT_SCENARIO else _DemoSensorDriver()
    producer = TelemetryProducer()
    loop = asyncio.get_running_loop()

    logger.info(
        "Starting Foam Placement Machine Digital Twin (scan interval=%.2fs, fault_scenario=%s)",
        SCAN_INTERVAL_SECONDS,
        ENABLE_FAULT_SCENARIO,
    )

    last_state = None
    try:
        while True:
            sensors = driver.read(fsm)
            telemetry = fsm.step(sensors)

            # producer.publish() only enqueues onto librdkafka's internal
            # buffer and services already-completed callbacks via
            # poll(0) - it never blocks, so it's safe to call directly
            # from this coroutine without an executor.
            producer.publish(telemetry)

            if telemetry["state"] != last_state:
                logger.info(
                    "state=%s right=%d left=%d errors=%d alarm=%s",
                    telemetry["state"],
                    telemetry["right_count"],
                    telemetry["left_count"],
                    telemetry["error_count"],
                    telemetry["alarm_code"],
                )
                last_state = telemetry["state"]

            await asyncio.sleep(SCAN_INTERVAL_SECONDS)
    finally:
        # flush()/close() genuinely block waiting for broker acks, so
        # they're pushed off the event loop rather than called inline.
        await loop.run_in_executor(None, producer.flush, 5)
        await loop.run_in_executor(None, producer.close)


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down (Ctrl+C received)...")
