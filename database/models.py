"""
TimescaleDB schema and data-access layer for the Foam Placement Machine
Digital Twin telemetry pipeline.

This module is intentionally kept agnostic of Kafka and the FSM state
logic (see claude.md, Section 2 - Separation of Concerns). It only knows
how to:
  1. Open a connection to TimescaleDB.
  2. Create the `telemetry_events` hypertable if it does not exist yet.
  3. Insert telemetry events in micro-batches (Phase 3, claude.md).

The row schema mirrors the telemetry payload documented in
docs/architecture.md, Section 3.4.
"""

from __future__ import annotations

import os

import psycopg2
import psycopg2.extras

# ----------------------------------------------------------------------
# Connection settings (overridable via environment variables / .env)
# ----------------------------------------------------------------------
TIMESCALE_HOST = os.getenv("TIMESCALE_HOST", "localhost")
TIMESCALE_PORT = int(os.getenv("TIMESCALE_PORT", "5432"))
TIMESCALE_DB = os.getenv("TIMESCALE_DB", "foam_placement")
TIMESCALE_USER = os.getenv("TIMESCALE_USER", "foam_admin")
TIMESCALE_PASSWORD = os.getenv("TIMESCALE_PASSWORD", "foam_admin_pw")

# ----------------------------------------------------------------------
# Schema
# ----------------------------------------------------------------------
# `time` is the hypertable partitioning column. Everything else maps
# 1:1 onto the telemetry JSON schema from architecture.md 3.4, flattened
# for efficient time-series storage/queries (Grafana panels read this
# table directly).
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS telemetry_events (
    time                TIMESTAMPTZ         NOT NULL,
    machine_id          TEXT                NOT NULL DEFAULT 'foam_placement_01',
    state               TEXT                NOT NULL,
    right_count         INTEGER             NOT NULL DEFAULT 0,
    left_count          INTEGER             NOT NULL DEFAULT 0,
    error_count         INTEGER             NOT NULL DEFAULT 0,
    alarm_code          TEXT                NOT NULL DEFAULT 'NONE',
    vacuum_active       BOOLEAN             NOT NULL DEFAULT FALSE,
    camera_ok           BOOLEAN             NOT NULL DEFAULT FALSE,
    light_barrier_ok    BOOLEAN             NOT NULL DEFAULT TRUE
);
"""

# create_hypertable is idempotent when if_not_exists => TRUE, so it is
# safe to call this on every service start (see init_db below).
CREATE_HYPERTABLE_SQL = """
SELECT create_hypertable(
    'telemetry_events', 'time',
    if_not_exists => TRUE
);
"""

CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_telemetry_events_state
    ON telemetry_events (state, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_events_alarm
    ON telemetry_events (alarm_code, time DESC)
    WHERE alarm_code <> 'NONE';
"""

INSERT_EVENT_SQL = """
INSERT INTO telemetry_events (
    time, machine_id, state, right_count, left_count, error_count,
    alarm_code, vacuum_active, camera_ok, light_barrier_ok
) VALUES (
    %(timestamp)s, %(machine_id)s, %(state)s, %(right_count)s,
    %(left_count)s, %(error_count)s, %(alarm_code)s,
    %(vacuum_active)s, %(camera_ok)s, %(light_barrier_ok)s
);
"""


def get_connection():
    """Open a new psycopg2 connection using the module-level settings."""
    return psycopg2.connect(
        host=TIMESCALE_HOST,
        port=TIMESCALE_PORT,
        dbname=TIMESCALE_DB,
        user=TIMESCALE_USER,
        password=TIMESCALE_PASSWORD,
    )


def init_db(conn) -> None:
    """
    Create the telemetry_events hypertable and supporting indexes if
    they do not already exist. Safe to call on every consumer startup.
    """
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLE_SQL)
        cur.execute(CREATE_HYPERTABLE_SQL)
        cur.execute(CREATE_INDEX_SQL)
    conn.commit()


def insert_events_batch(conn, events: list[dict]) -> int:
    """
    Micro-batch insert (claude.md Phase 3): a single round-trip via
    psycopg2.extras.execute_batch instead of one INSERT per event.

    `events` is a list of flat dicts matching INSERT_EVENT_SQL's named
    parameters (see consumer/consumer.py for how Kafka messages are
    normalized into this shape). Returns the number of rows inserted.
    Raises whatever psycopg2 raises on failure; the caller is
    responsible for retry/backoff (see consumer.py's resilience loop).
    """
    if not events:
        return 0

    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, INSERT_EVENT_SQL, events, page_size=100)
    conn.commit()
    return len(events)
