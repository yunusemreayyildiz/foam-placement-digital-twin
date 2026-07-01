"""
Kafka -> TimescaleDB consumer service for the Foam Placement Machine
Digital Twin (claude.md, Phase 3: Data Engineering Optimization).

Subscribes to the `scara.telemetry.v1` topic and writes events into
TimescaleDB using an in-memory micro-batch buffer: a batch is flushed
whenever it reaches BATCH_MAX_SIZE (50) records OR BATCH_MAX_SECONDS
(2.0) have elapsed since the last flush, whichever comes first
(architecture.md, Section 4.3).

Uses Confluent's official Python client (confluent-kafka, librdkafka-
backed): poll(timeout) naturally doubles as the micro-batch timer -
it blocks for at most BATCH_MAX_SECONDS and returns None when idle,
so the time-based flush trigger falls out of the loop for free.

Kept agnostic of the FSM state logic (claude.md, Section 2): this
module only knows the wire-level telemetry JSON schema (architecture.md
3.4), not why the machine is in a given state.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import os
import time

from confluent_kafka import Consumer, KafkaException
from psycopg2 import OperationalError

from database.models import get_connection, init_db, insert_events_batch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] consumer: %(message)s",
)
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Config
# ----------------------------------------------------------------------
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "scara.telemetry.v1")
CONSUMER_GROUP_ID = os.getenv("CONSUMER_GROUP_ID", "foam-placement-consumer")

BATCH_MAX_SIZE = int(os.getenv("BATCH_MAX_SIZE", "50"))
BATCH_MAX_SECONDS = float(os.getenv("BATCH_MAX_SECONDS", "2.0"))

DB_RETRY_BACKOFF_SECONDS = 3.0
KAFKA_RESTART_BACKOFF_SECONDS = 3.0


def normalize_event(raw: dict) -> dict:
    """
    Flatten the wire-level telemetry JSON (architecture.md 3.4, with a
    nested `sensor` object) into the flat dict shape expected by
    database.models.INSERT_EVENT_SQL.
    """
    sensor = raw.get("sensor", {})
    return {
        "timestamp": raw["timestamp"],
        "machine_id": raw.get("machine_id", "foam_placement_01"),
        "state": raw["state"],
        "right_count": raw.get("right_count", 0),
        "left_count": raw.get("left_count", 0),
        "error_count": raw.get("error_count", 0),
        "alarm_code": raw.get("alarm_code", "NONE"),
        "vacuum_active": sensor.get("vacuum_active", False),
        "camera_ok": sensor.get("camera_ok", False),
        "light_barrier_ok": sensor.get("light_barrier_ok", True),
    }


def build_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            # Commit is manual (see _flush): auto-commit fires on a
            # wall-clock timer regardless of whether buffered messages
            # have actually been written to TimescaleDB yet, so a crash
            # between an auto-commit and the next flush would lose the
            # buffered batch for good (Kafka would never redeliver it).
            "enable.auto.commit": False,
            # Resilience first (claude.md #3): auto-reconnect on broker
            # drops instead of raising and killing the service.
            "reconnect.backoff.ms": 500,
            "reconnect.backoff.max.ms": 5000,
        }
    )
    consumer.subscribe([TELEMETRY_TOPIC])
    return consumer


async def connect_db_with_retry():
    loop = asyncio.get_running_loop()
    while True:
        try:
            conn = await loop.run_in_executor(None, get_connection)
            await loop.run_in_executor(None, init_db, conn)
            logger.info("Connected to TimescaleDB and ensured hypertable exists.")
            return conn
        except OperationalError as exc:
            logger.warning(
                "TimescaleDB unavailable (%s); retrying in %.1fs...",
                exc,
                DB_RETRY_BACKOFF_SECONDS,
            )
            await asyncio.sleep(DB_RETRY_BACKOFF_SECONDS)


async def run() -> None:
    loop = asyncio.get_running_loop()
    conn = [await connect_db_with_retry()]  # 1-element mutable box; see _flush()

    while True:
        consumer = build_consumer()
        logger.info(
            "Subscribed to '%s' (batch: %d records / %.1fs)",
            TELEMETRY_TOPIC,
            BATCH_MAX_SIZE,
            BATCH_MAX_SECONDS,
        )
        try:
            await _consume_loop(consumer, conn)
        except KafkaException as exc:
            logger.warning(
                "Kafka consumer error (%s); rebuilding consumer in %.1fs...",
                exc,
                KAFKA_RESTART_BACKOFF_SECONDS,
            )
            await asyncio.sleep(KAFKA_RESTART_BACKOFF_SECONDS)
        finally:
            await loop.run_in_executor(None, consumer.close)


async def _consume_loop(consumer: Consumer, conn: list) -> None:
    """
    Micro-batch loop. consumer.poll(timeout) blocks for at most
    BATCH_MAX_SECONDS and returns None when there's nothing new, which
    doubles as the time-based flush trigger - no separate timer needed.
    It's a real blocking C call, so it's pushed onto the default
    executor rather than freezing the event loop for up to 2 seconds
    at a time.
    """
    loop = asyncio.get_running_loop()
    buffer: list[dict] = []
    last_flush = time.monotonic()

    while True:
        msg = await loop.run_in_executor(None, consumer.poll, BATCH_MAX_SECONDS)

        if msg is not None:
            if msg.error():
                raise KafkaException(msg.error())

            try:
                raw = json.loads(msg.value().decode("utf-8"))
                buffer.append(normalize_event(raw))
            except (KeyError, TypeError, ValueError) as exc:
                logger.error("Dropping malformed telemetry event: %s (%s)", msg.value(), exc)

        if len(buffer) >= BATCH_MAX_SIZE:
            buffer = await _flush(consumer, conn, buffer)
            last_flush = time.monotonic()
        elif buffer and (time.monotonic() - last_flush) >= BATCH_MAX_SECONDS:
            buffer = await _flush(consumer, conn, buffer)
            last_flush = time.monotonic()


async def _flush(consumer: Consumer, conn: list, buffer: list[dict]) -> list[dict]:
    """
    Write the buffer to TimescaleDB and only then commit the Kafka
    offsets for it, so a crash before this point replays the buffered
    messages on restart instead of silently losing them (enable.auto.commit
    is off - see build_consumer). Retries the write until it succeeds
    rather than letting an OperationalError bubble up and crash the
    service - `conn` is a 1-element box (see _consume_loop) so a
    reconnect here updates the connection its caller sees too. psycopg2
    and confluent-kafka are both synchronous libraries, so every call
    here runs on the default executor instead of blocking the loop.
    """
    loop = asyncio.get_running_loop()

    while True:
        try:
            n = await loop.run_in_executor(None, insert_events_batch, conn[0], buffer)
            break
        except OperationalError as exc:
            logger.warning("DB write failed (%s); reconnecting and retrying...", exc)
            try:
                await loop.run_in_executor(None, conn[0].close)
            except Exception:
                pass
            conn[0] = await connect_db_with_retry()

    await loop.run_in_executor(None, functools.partial(consumer.commit, asynchronous=False))
    logger.info("Flushed %d events to TimescaleDB.", n)
    return []


if __name__ == "__main__":
    asyncio.run(run())
