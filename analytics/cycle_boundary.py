"""
Pure, in-memory cycle-boundary detection for the analytics layer
(claude.md Phase 5).

Watches an ordered stream of raw telemetry event dicts and yields a
closed cycle's event list each time the FSM re-enters IDLE after
having left it. IDLE re-entry is the correct boundary regardless of
whether the cycle faulted: per core/fsm.py's _handle_red_state, an
operator reset returns the FSM to _state_before_error rather than
restarting the cycle, so a faulted-and-recovered cycle still only
closes at its next IDLE, exactly like the nominal path (core/states.py).
"""

from __future__ import annotations

IDLE_STATE = "IDLE"


class CycleAccumulator:
    """
    Feed telemetry event dicts one at a time via push(). Each closed
    cycle is returned as a list of events ending with the boundary
    event that closed it (see analytics/features.py's docstring for
    why the boundary event is included rather than dropped).

    Any events pushed before the first IDLE->non-IDLE->IDLE round-trip
    is observed belong to a cycle whose true start was never seen -
    the live service (analytics/analytics_consumer.py) is responsible
    for dropping that first closed cycle rather than this class, since
    "is this a genuine restart" is a service-startup policy, not a
    boundary-detection concern.
    """

    def __init__(self) -> None:
        self._buffer: list[dict] = []
        self._left_idle = False

    def push(self, event: dict) -> list[dict] | None:
        """
        Append one event to the in-progress cycle. Returns the closed
        cycle's event list if this event closed a cycle, else None.
        """
        state = event["state"]

        if state == IDLE_STATE and self._left_idle and self._buffer:
            closed = self._buffer + [event]
            self._buffer = [event]
            self._left_idle = False
            return closed

        self._buffer.append(event)
        if state != IDLE_STATE:
            self._left_idle = True
        return None
