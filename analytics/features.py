"""
Pure per-cycle feature engineering for the analytics / predictive-
maintenance layer (claude.md Phase 5).

Kept dependency-free (no Kafka/DB) so it can be unit-tested directly
and shared between the live service (analytics/analytics_consumer.py)
and the offline model trainer (analytics/model_training/train_isolation_forest.py)
- a single source of truth for what a "feature" means.
"""

from __future__ import annotations

from datetime import datetime

NON_FAULT_ALARM_CODE = "NONE"
RED_HANDLING_STATE = "RED_HANDLING"


def _parse_timestamp(value) -> datetime:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str) or not value:
        raise ValueError(f"event timestamp must be a non-empty ISO8601 string, got {value!r}")
    text = value[:-1] + "+00:00" if value.endswith("Z") else value
    try:
        return datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"could not parse event timestamp {value!r}: {exc}") from exc


def compute_cycle_features(events: list[dict]) -> dict:
    """
    Compute engineered features for one completed production cycle.

    `events` must be an ordered list of raw telemetry dicts (the wire
    schema from docs/architecture.md 3.4: timestamp, state, error_count,
    alarm_code, right_count, left_count) for the cycle's own content,
    with the event that closed the cycle (the *next* cycle's opening
    IDLE) appended as the final element. That boundary event's
    timestamp closes out the last state's dwell time and becomes this
    cycle's cycle_end_time; none of its other fields describe this
    cycle's content. Requires at least 2 events (one piece of content
    plus the boundary marker) - see analytics/cycle_boundary.py for how
    this list is assembled from a live Kafka stream.

    Raises ValueError if there are fewer than 2 events, or if any event
    is missing 'timestamp'/'state' or has an unparsable timestamp -
    these are treated as malformed input, never silently skipped.
    """
    if len(events) < 2:
        raise ValueError(
            "compute_cycle_features requires at least 2 events (cycle "
            f"content + boundary marker), got {len(events)}"
        )

    for event in events:
        if "timestamp" not in event or "state" not in event:
            raise ValueError(f"event missing required 'timestamp'/'state' key: {event!r}")

    timestamps = [_parse_timestamp(e["timestamp"]) for e in events]
    content_events = events[:-1]
    cycle_start_time = timestamps[0]
    cycle_end_time = timestamps[-1]

    dwell_seconds: dict[str, float] = {}
    for i in range(len(events) - 1):
        state = events[i]["state"]
        delta = (timestamps[i + 1] - timestamps[i]).total_seconds()
        dwell_seconds[state] = dwell_seconds.get(state, 0.0) + delta

    had_red_handling = any(e["state"] == RED_HANDLING_STATE for e in content_events)
    red_handling_seconds = dwell_seconds.get(RED_HANDLING_STATE, 0.0)
    alarm_codes = sorted(
        {
            e.get("alarm_code", NON_FAULT_ALARM_CODE)
            for e in content_events
            if e.get("alarm_code", NON_FAULT_ALARM_CODE) != NON_FAULT_ALARM_CODE
        }
    )

    first_event, last_event = events[0], events[-1]
    right_count_delta = last_event.get("right_count", 0) - first_event.get("right_count", 0)
    left_count_delta = last_event.get("left_count", 0) - first_event.get("left_count", 0)

    return {
        "cycle_start_time": cycle_start_time,
        "cycle_end_time": cycle_end_time,
        "cycle_duration_seconds": (cycle_end_time - cycle_start_time).total_seconds(),
        "dwell_seconds": dwell_seconds,
        "had_red_handling": had_red_handling,
        "red_handling_seconds": red_handling_seconds,
        "alarm_codes": alarm_codes,
        "right_count_delta": right_count_delta,
        "left_count_delta": left_count_delta,
        "error_count_at_cycle_end": last_event.get("error_count", 0),
    }
