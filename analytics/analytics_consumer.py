"""
Analytics consumer for the Foam Placement Machine Digital Twin
(claude.md, Phase 5: Anomaly Detection / Predictive Maintenance).

Subscribes to `scara.telemetry.v1` under its own Kafka consumer group -
kept decoupled from consumer/consumer.py's ingestion path (claude.md
Section 2, Separation of Concerns: consumer.py stays agnostic of FSM
semantics; this is the one service that knows what a "cycle" means).
Reconstructs completed production cycles (analytics/cycle_boundary.py),
engineers features for each (analytics/features.py), scores them with
an EWMA statistical baseline plus an optional pre-trained
IsolationForest (analytics/scoring.py), and writes the result to the
cycle_metrics hypertable (database/models.py).

Mirrors consumer/consumer.py's resilience style: manual offset commit
only after a cycle's row is durably written (or after a non-boundary
event is safely buffered in memory), DB writes retried indefinitely on
failure, and all blocking confluent-kafka/psycopg2/joblib calls pushed
onto the default executor so this coroutine never blocks the event loop.
"""

from __future__ import annotations

import asyncio
import functools
import json
import logging
import os

from confluent_kafka import Consumer, KafkaException
from psycopg2 import OperationalError

from analytics.cycle_boundary import CycleAccumulator
from analytics.features import compute_cycle_features
from analytics.scoring import ewma_update, score_cycle, stat_anomaly_flag
from database.models import (
    get_connection,
    init_analytics_db,
    insert_cycle_metric,
    latest_fault_cycle_end,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] analytics: %(message)s",
)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "scara.telemetry.v1")
ANALYTICS_CONSUMER_GROUP_ID = os.getenv("ANALYTICS_CONSUMER_GROUP_ID", "foam-placement-analytics")
MACHINE_ID = os.getenv("MACHINE_ID", "foam_placement_01")

EWMA_ALPHA = float(os.getenv("EWMA_ALPHA", "0.2"))
STAT_ANOMALY_K = float(os.getenv("STAT_ANOMALY_K", "3.0"))

# A cycle spanning a real dev-session gap (main.py stopped/restarted,
# Kafka backlog replayed from an old offset) can appear to last minutes
# instead of a few seconds - this service has no way to distinguish
# "the machine was genuinely idle that long" from "nothing was running
# at all", so anything implausibly long is treated as a gap artifact,
# not a real cycle: dropped before it can corrupt the EWMA baseline,
# exactly like the first-cycle-after-startup case below.
MAX_PLAUSIBLE_CYCLE_SECONDS = float(os.getenv("MAX_PLAUSIBLE_CYCLE_SECONDS", "30.0"))

DB_RETRY_BACKOFF_SECONDS = 3.0
KAFKA_RESTART_BACKOFF_SECONDS = 3.0
POLL_TIMEOUT_SECONDS = 1.0

MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
MODEL_MANIFEST_PATH = os.path.join(MODEL_DIR, "MODEL_MANIFEST.json")


def build_consumer() -> Consumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
            "group.id": ANALYTICS_CONSUMER_GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "reconnect.backoff.ms": 500,
            "reconnect.backoff.max.ms": 5000,
        }
    )
    consumer.subscribe([TELEMETRY_TOPIC])
    return consumer


def load_model():
    """
    Load the active IsolationForest model per MODEL_MANIFEST.json, if
    one has been trained yet (analytics/model_training/train_isolation_forest.py).
    Returns (model, model_version), both None if no model exists yet.

    v1 loads once at process startup only (restart-to-reload, not
    hot-reload) - simpler, and a manual retrain is already a deliberate,
    infrequent action (see claude.md Phase 5 notes on this trade-off).
    """
    if not os.path.exists(MODEL_MANIFEST_PATH):
        logger.info(
            "No trained model yet (%s not found) - ml_anomaly_score/flag will stay NULL until one is trained.",
            MODEL_MANIFEST_PATH,
        )
        return None, None

    import joblib

    with open(MODEL_MANIFEST_PATH, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    model_filename = manifest["active_model_filename"]
    model = joblib.load(os.path.join(MODEL_DIR, model_filename))
    logger.info("Loaded IsolationForest model %s", model_filename)
    return model, model_filename


async def connect_db_with_retry():
    loop = asyncio.get_running_loop()
    while True:
        try:
            conn = await loop.run_in_executor(None, get_connection)
            await loop.run_in_executor(None, init_analytics_db, conn)
            logger.info("Connected to TimescaleDB and ensured cycle_metrics hypertable exists.")
            return conn
        except OperationalError as exc:
            logger.warning(
                "TimescaleDB unavailable (%s); retrying in %.1fs...", exc, DB_RETRY_BACKOFF_SECONDS
            )
            await asyncio.sleep(DB_RETRY_BACKOFF_SECONDS)


async def run() -> None:
    loop = asyncio.get_running_loop()
    conn = [await connect_db_with_retry()]  # 1-element mutable box; see _insert_with_retry()
    model, model_version = load_model()

    while True:
        consumer = build_consumer()
        logger.info("Subscribed to '%s' as group '%s'", TELEMETRY_TOPIC, ANALYTICS_CONSUMER_GROUP_ID)
        try:
            await _consume_loop(consumer, conn, model, model_version)
        except KafkaException as exc:
            logger.warning(
                "Kafka consumer error (%s); rebuilding consumer in %.1fs...",
                exc,
                KAFKA_RESTART_BACKOFF_SECONDS,
            )
            await asyncio.sleep(KAFKA_RESTART_BACKOFF_SECONDS)
        finally:
            await loop.run_in_executor(None, consumer.close)


async def _consume_loop(consumer: Consumer, conn: list, model, model_version) -> None:
    loop = asyncio.get_running_loop()
    accumulator = CycleAccumulator()

    ewma_mean = None
    ewma_var = None
    cycles_seen = 0
    cycles_closed = 0

    while True:
        msg = await loop.run_in_executor(None, consumer.poll, POLL_TIMEOUT_SECONDS)

        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())

        try:
            event = json.loads(msg.value().decode("utf-8"))
        except (TypeError, ValueError) as exc:
            logger.error("Dropping malformed telemetry event: %s (%s)", msg.value(), exc)
            await _commit(loop, consumer)
            continue

        closed_cycle = accumulator.push(event)

        if closed_cycle is None:
            await _commit(loop, consumer)
            continue

        cycles_closed += 1
        if cycles_closed == 1:
            # Service just (re)started mid-cycle - this buffer's true
            # start was never observed, so the first closed "cycle" is
            # a partial artifact, not real data (documented limitation,
            # see analytics/cycle_boundary.py).
            logger.info("Dropping first closed cycle after startup (partially observed, true start unknown).")
            await _commit(loop, consumer)
            continue

        features = compute_cycle_features(closed_cycle)

        if features["cycle_duration_seconds"] > MAX_PLAUSIBLE_CYCLE_SECONDS:
            logger.warning(
                "Dropping implausibly long cycle (%.1fs > %.1fs) - likely a dev-session "
                "gap (main.py stopped/restarted), not a real production cycle.",
                features["cycle_duration_seconds"],
                MAX_PLAUSIBLE_CYCLE_SECONDS,
            )
            await _commit(loop, consumer)
            continue

        cycles_seen += 1
        ewma_mean, ewma_var = ewma_update(
            ewma_mean, ewma_var, features["cycle_duration_seconds"], EWMA_ALPHA
        )
        stat_flag = stat_anomaly_flag(
            features["cycle_duration_seconds"], ewma_mean, ewma_var, cycles_seen, STAT_ANOMALY_K
        )

        if model is not None:
            ml_score, ml_flag = score_cycle(model, features)
        else:
            ml_score, ml_flag = None, None

        prior_fault_end = await _call_with_db_retry(conn, latest_fault_cycle_end)
        if prior_fault_end is not None:
            time_since_last_fault_seconds = (
                features["cycle_end_time"] - prior_fault_end
            ).total_seconds()
        else:
            time_since_last_fault_seconds = None

        metric = {
            "cycle_end_time": features["cycle_end_time"],
            "machine_id": event.get("machine_id", MACHINE_ID),
            "cycle_start_time": features["cycle_start_time"],
            "cycle_duration_seconds": features["cycle_duration_seconds"],
            "dwell_seconds": features["dwell_seconds"],
            "had_red_handling": features["had_red_handling"],
            "red_handling_seconds": features["red_handling_seconds"],
            "alarm_codes": features["alarm_codes"],
            "right_count_delta": features["right_count_delta"],
            "left_count_delta": features["left_count_delta"],
            "error_count_at_cycle_end": features["error_count_at_cycle_end"],
            "time_since_last_fault_seconds": time_since_last_fault_seconds,
            "ewma_cycle_duration": ewma_mean,
            "ewma_stddev_cycle_duration": ewma_var**0.5,
            "stat_anomaly_flag": stat_flag,
            "ml_anomaly_flag": ml_flag,
            "ml_anomaly_score": ml_score,
            "model_version": model_version,
        }

        await _call_with_db_retry(conn, insert_cycle_metric, metric)
        await _commit(loop, consumer)

        logger.info(
            "cycle closed: duration=%.2fs had_red_handling=%s stat_anomaly=%s ml_anomaly=%s",
            features["cycle_duration_seconds"],
            features["had_red_handling"],
            stat_flag,
            ml_flag,
        )


async def _commit(loop, consumer: Consumer) -> None:
    await loop.run_in_executor(None, functools.partial(consumer.commit, asynchronous=False))


async def _call_with_db_retry(conn: list, func, *args):
    """
    Run a blocking DB call (insert_cycle_metric, latest_fault_cycle_end,
    ...) with retry-on-OperationalError, reconnecting conn[0] as needed
    (see connect_db_with_retry). Every TimescaleDB touchpoint in this
    loop must go through here: a transient outage must never propagate
    past this point, since an unhandled OperationalError would crash
    the process (only KafkaException is caught in run()) and reset all
    in-memory state - the accumulator, EWMA baseline, and cycles_seen -
    on restart.
    """
    loop = asyncio.get_running_loop()
    while True:
        try:
            return await loop.run_in_executor(None, func, conn[0], *args)
        except OperationalError as exc:
            logger.warning("DB call failed (%s); reconnecting and retrying...", exc)
            try:
                await loop.run_in_executor(None, conn[0].close)
            except Exception:
                pass
            conn[0] = await connect_db_with_retry()


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        logger.info("Shutting down (Ctrl+C received)...")
