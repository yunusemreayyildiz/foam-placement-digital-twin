"""
Kafka telemetry producer for the Foam Placement Machine Digital Twin.

Publishes FSM.step() output (see core/fsm.py) onto the
`scara.telemetry.v1` topic, on every scan cycle / state change, using
the JSON schema documented in docs/architecture.md, Section 3.4.

Uses Confluent's official Python client (confluent-kafka, librdkafka-
backed) rather than a pure-Python client - it ships prebuilt wheels
for all current CPython/OS combinations (including Windows + 3.13)
and its reconnect/retry handling is implemented in the same battle-
tested C library used by Kafka clients in production everywhere.

Design notes (claude.md, Section 2 - Global AI Instructions):
  - Kept agnostic of the FSM's internal state logic: it only accepts
    the already-built telemetry dict from FSM.step() and stamps it
    with timestamp/machine_id before publishing.
  - Resilience first: the producer is configured with retries and
    reconnect backoff so a momentary broker blip does not crash the
    edge control loop (main.py never blocks on this - see 3.1/3.2 in
    architecture.md, the Kafka path is decoupled from FSM control).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone

from confluent_kafka import Producer

logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------
# Config (overridable via environment variables / .env)
# ----------------------------------------------------------------------
# NOTE on dual listeners: docker-compose.yml exposes Kafka on two
# listeners - INTERNAL (kafka:29092, used by containers on the compose
# network, e.g. consumer.py running as a service) and EXTERNAL
# (localhost:9092, used by processes running on the host, e.g. main.py
# during local development). Set KAFKA_BOOTSTRAP_SERVERS accordingly;
# this module does not care which one it talks to.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TELEMETRY_TOPIC = os.getenv("TELEMETRY_TOPIC", "scara.telemetry.v1")
MACHINE_ID = os.getenv("MACHINE_ID", "foam_placement_01")


class TelemetryProducer:
    """Thin wrapper around confluent_kafka.Producer for FSM telemetry."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        topic: str = TELEMETRY_TOPIC,
        machine_id: str = MACHINE_ID,
    ) -> None:
        self.topic = topic
        self.machine_id = machine_id
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",  # wait for full ISR ack -> no silent event loss
                "retries": 5,
                "reconnect.backoff.ms": 500,
                "reconnect.backoff.max.ms": 5000,
                "linger.ms": 20,  # small batching window, still near-real-time
            }
        )

    def publish(self, fsm_telemetry: dict) -> None:
        """
        Stamp an FSM.step() telemetry dict with timestamp/machine_id and
        publish it to Kafka. produce() itself is non-blocking (it only
        enqueues onto librdkafka's internal buffer); poll(0) below just
        services already-completed delivery callbacks without waiting,
        so a slow/unreachable broker never stalls the caller's scan
        cycle (architecture.md 4.2).
        """
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "machine_id": self.machine_id,
            **fsm_telemetry,
        }

        try:
            self._producer.produce(
                self.topic,
                key=self.machine_id.encode("utf-8"),
                value=json.dumps(payload).encode("utf-8"),
                callback=self._on_delivery,
            )
        except BufferError:
            # librdkafka's local queue is full (broker unreachable for a
            # while) - drop this cycle's event rather than blocking the
            # FSM scan loop; poll() below gives it a chance to drain.
            logger.warning("Producer queue full; dropping this cycle's telemetry event.")

        self._producer.poll(0)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            logger.warning("Failed to publish telemetry event: %s", err)

    def flush(self, timeout: float = 5.0) -> None:
        self._producer.flush(timeout)

    def close(self) -> None:
        self._producer.flush(5.0)
