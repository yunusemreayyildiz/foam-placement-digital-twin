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


# ----------------------------------------------------------------------
# Analytics schema (claude.md Phase 5 - anomaly detection / predictive
# maintenance). Additive: telemetry_events and init_db() above are
# untouched. Only analytics/analytics_consumer.py calls init_analytics_db.
# ----------------------------------------------------------------------
# dwell_seconds/alarm_codes are JSONB rather than fixed columns so this
# schema survives future FSM state additions without a migration.
CREATE_CYCLE_METRICS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS cycle_metrics (
    cycle_end_time                  TIMESTAMPTZ         NOT NULL,
    machine_id                      TEXT                NOT NULL DEFAULT 'foam_placement_01',
    cycle_start_time                TIMESTAMPTZ         NOT NULL,
    cycle_duration_seconds           DOUBLE PRECISION    NOT NULL,
    dwell_seconds                   JSONB               NOT NULL,
    had_red_handling                BOOLEAN             NOT NULL DEFAULT FALSE,
    red_handling_seconds             DOUBLE PRECISION    NOT NULL DEFAULT 0,
    alarm_codes                     JSONB               NOT NULL DEFAULT '[]',
    right_count_delta                INTEGER             NOT NULL DEFAULT 0,
    left_count_delta                 INTEGER             NOT NULL DEFAULT 0,
    error_count_at_cycle_end          INTEGER             NOT NULL DEFAULT 0,
    time_since_last_fault_seconds     DOUBLE PRECISION,
    ewma_cycle_duration              DOUBLE PRECISION,
    ewma_stddev_cycle_duration        DOUBLE PRECISION,
    stat_anomaly_flag                BOOLEAN,
    ml_anomaly_flag                  BOOLEAN,
    ml_anomaly_score                 DOUBLE PRECISION,
    model_version                   TEXT
);
"""

CREATE_CYCLE_METRICS_HYPERTABLE_SQL = """
SELECT create_hypertable(
    'cycle_metrics', 'cycle_end_time',
    if_not_exists => TRUE
);
"""

CREATE_CYCLE_METRICS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_cycle_metrics_anomaly
    ON cycle_metrics (ml_anomaly_flag, cycle_end_time DESC)
    WHERE ml_anomaly_flag IS TRUE;
"""

INSERT_CYCLE_METRIC_SQL = """
INSERT INTO cycle_metrics (
    cycle_end_time, machine_id, cycle_start_time, cycle_duration_seconds,
    dwell_seconds, had_red_handling, red_handling_seconds, alarm_codes,
    right_count_delta, left_count_delta, error_count_at_cycle_end,
    time_since_last_fault_seconds, ewma_cycle_duration,
    ewma_stddev_cycle_duration, stat_anomaly_flag, ml_anomaly_flag,
    ml_anomaly_score, model_version
) VALUES (
    %(cycle_end_time)s, %(machine_id)s, %(cycle_start_time)s, %(cycle_duration_seconds)s,
    %(dwell_seconds)s, %(had_red_handling)s, %(red_handling_seconds)s, %(alarm_codes)s,
    %(right_count_delta)s, %(left_count_delta)s, %(error_count_at_cycle_end)s,
    %(time_since_last_fault_seconds)s, %(ewma_cycle_duration)s,
    %(ewma_stddev_cycle_duration)s, %(stat_anomaly_flag)s, %(ml_anomaly_flag)s,
    %(ml_anomaly_score)s, %(model_version)s
);
"""

LATEST_FAULT_CYCLE_END_SQL = """
SELECT MAX(cycle_end_time) FROM cycle_metrics WHERE had_red_handling IS TRUE;
"""

SELECT_RECENT_CYCLE_FEATURES_SQL = """
SELECT cycle_duration_seconds, dwell_seconds, red_handling_seconds, had_red_handling
FROM cycle_metrics
ORDER BY cycle_end_time DESC
LIMIT %(limit)s;
"""


def init_analytics_db(conn) -> None:
    """
    Create the cycle_metrics hypertable and supporting index if they do
    not already exist. Called only by analytics/analytics_consumer.py -
    consumer.py's init_db() and telemetry_events are untouched.
    """
    with conn.cursor() as cur:
        cur.execute(CREATE_CYCLE_METRICS_TABLE_SQL)
        cur.execute(CREATE_CYCLE_METRICS_HYPERTABLE_SQL)
        cur.execute(CREATE_CYCLE_METRICS_INDEX_SQL)
    conn.commit()


def latest_fault_cycle_end(conn):
    """Return the cycle_end_time of the most recent RED_HANDLING excursion, or None."""
    with conn.cursor() as cur:
        cur.execute(LATEST_FAULT_CYCLE_END_SQL)
        (value,) = cur.fetchone()
        return value


def insert_cycle_metric(conn, metric: dict) -> None:
    """
    Single-row insert for one completed production cycle. Cycles
    complete far less often than raw telemetry ticks, so unlike
    insert_events_batch there is no need for micro-batching here.

    `metric` is a flat dict matching INSERT_CYCLE_METRIC_SQL's named
    parameters; dwell_seconds/alarm_codes must be plain dict/list
    (wrapped in psycopg2.extras.Json here, not by the caller).
    """
    row = dict(metric)
    row["dwell_seconds"] = psycopg2.extras.Json(row["dwell_seconds"])
    row["alarm_codes"] = psycopg2.extras.Json(row["alarm_codes"])

    with conn.cursor() as cur:
        cur.execute(INSERT_CYCLE_METRIC_SQL, row)
    conn.commit()


def fetch_recent_cycle_features(conn, limit: int) -> list[dict]:
    """
    Read the most recent `limit` completed cycles' engineered features
    back out for model training (analytics/model_training/train_isolation_forest.py).
    Returns rows oldest-first (reversed from the DESC query) so a model
    trained on them sees cycles in chronological order.
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(SELECT_RECENT_CYCLE_FEATURES_SQL, {"limit": limit})
        rows = cur.fetchall()
    return [dict(row) for row in reversed(rows)]
