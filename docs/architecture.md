# ** Architecture & data pipeline Documentation **

This document details the system architecture, data ingestion pipeline, Finite State Machine (FSM) logic, and data models for the **Foam Placement Machine Digital Twin** project.
---

## 1. System Architecture Overview
This system is inspired by the Purdue Model, but implemented in a modernized form.
The architecture keeps the same OT/IT separation concept while using contemporary data streaming and digital twin technologies.

- simulator / FSM = Level 2-like control layer
- Kafka = data transport / integration backbone
- consumer / TimescaleDB = Level 3/4-like data and analytics layer

### Component Responsibilities
- FSM Simulator: runs the deterministic scan-cycle, performs safety checks, simulates sensors/actuators, and exposes direct start/stop/reset command handling.
- Direct Command Layer: handles low-latency operator and control system requests, routing them straight to the FSM without Kafka broker dependency.
- Kafka: stores and forwards event data as an asynchronous, durable message bus, decoupling edge control from data analytics and persistence.
- Consumer: subscribes to the Kafka topic, enriches and batches telemetry events, and writes them to TimescaleDB with minimal backpressure on the edge.
- TimescaleDB: maintains time-series historical records, supports hypertable storage, and enables analytics/dashboard queries without affecting control logic.

### Portable Architecture Diagram
```text
     +-------------------+         +------------------+
     |   Direct Command  |=======> |  FSM Simulator   |
     |      Layer        |         |  (Edge Control)  |
     +-------------------+         +--+---------------+
                                        |
                                        | telemetry events
                                        v
                                   +----+------+
                                   |   Kafka    |
                                   +----+------+
                                        |
                                        | event stream
                                        v
                              +---------+----------+
                              |      Consumer      |
                              |  (micro-batch DB)  |
                              +---------+----------+
                                        |
                                        v
                                  +-----+------+
                                  | TimescaleDB |
                                  +------------+
```

### Why this is a modern Purdue-style architecture
- Traditional Purdue Model often uses OPC UA and plant network integrations.
- In this project, we use an event bus (Kafka) and a digital twin pattern instead.
- That makes the OT control vs IT analytics separation more flexible and scalable.

## 2. Traditional Purdue Model (ISA-95)
The Purdue Model is an architecture standard developed in the 1990s to protect and operate industrial control systems by dividing the plant into strict layers. These layers are hierarchical, and data typically flows sequentially between them.

- Level 0 (Physical Process): Field motors, valves, and sensors.
- Level 1 (Intelligent Devices): PLCs and PID controllers. They read sensors and drive actuators.
- Level 2 (Control Systems): SCADA and HMI screens. This is where operators monitor the machine and send Start/Stop/Reset commands.
- Level 3 (Manufacturing Operations): MES, production recipes, quality tracking, and historians.
- Level 4 (Enterprise Network): ERP systems, business analytics, and broader IT infrastructure.

### The Traditional Model’s Strict Rule
Communication between layers is hierarchical. Level 1 cannot speak directly to Level 3; data always flows Level 1 -> Level 2 -> Level 3. This requires strict firewalls and separation, but it can also create bottlenecks and latency.

## 3. Our Designed Model
Our architecture preserves the Purdue philosophy of safety and determinism, while removing data bottlenecks by  separating control and data responsibilities — an approach inspired by CQRS's separation-of-concerns principle.

### 3.1 Control Axis (Level 1 & 2-like)
- FSM Simulator (Level 1): Simulates the PLC scan cycle and executes safety checks and sensor logic deterministically within the edge service.
- Direct Command Layer (Level 2): Operator commands (`Start`, `Stop`, `Reset`, `Acknowledge`) go directly to the FSM service via API/RPC without going through a broker. This enables millisecond-level command processing.

### 3.2 Data Axis (Level 3 & 4-like)
- Kafka & Consumer: The FSM emits telemetry, alarms, and metrics on each cycle or state change, sending them asynchronously to Kafka.
- TimescaleDB: The consumer reads the event stream and writes the data into TimescaleDB in micro-batches.

### 3.3 FSM State Flow
The FSM drives the machine from `IDLE` to `CYCLE_DONE` through ordered control states. This flow includes safety checks, vacuum activation, marking, and special error handling.

```text
IDLE
  |
  v
VACUUM_ON
  |
  v
ROBOT_RUNNING
  |
  v
CAMERA_CHECK
  |
  v
MARKING
  |
  v
RIGHT_RELEASE
  |
  v
LEFT_RELEASE_WAIT
  |
  v
WAIT_LEFT_IN_KASA
  |
  v
CYCLE_DONE
```

If a safety error occurs, the FSM may transition into a red handling or error state such as `RED_HANDLING`, and remains there until an explicit reset or acknowledgement is received.

### 3.4 Telemetry Payload Schema
Telemetry events sent from the FSM follow a JSON-like structure. Key counters and state fields are included for analytics and alerting.

```json
{
  "timestamp": "2026-06-28T12:34:56.789Z",
  "machine_id": "foam_placement_01",
  "state": "MARKING",
  "right_count": 12,
  "left_count": 8,
  "error_count": 1,
  "alarm_code": "RED_HANDLING",
  "sensor": {
    "vacuum_active": true,
    "camera_ok": true,
    "light_barrier_ok": false
  }
}
```

- `right_count`, `left_count`, `error_count`: core production counters emitted each cycle.
- `state`: current FSM state name.
- `alarm_code`: used for safety and exception tracking.
- `sensor`: optional diagnostic and safety signal details.

## 4. What We Improved Over the Purdue Model
We modernized the traditional Purdue model with an Edge-to-Cloud vision without breaking its safety intent.

### 4.1 Unified Namespace and Event-Driven Transition
- Traditional flaw: A PLC signal from Level 1 must pass through SCADA and MES to reach Level 4, which introduces data loss and latency.
- Our improvement: We keep control isolated at the edge while turning data into a Kafka event stream accessible across the plant and the cloud. A Level 4 dashboard can consume live data from Kafka without loading the machine.

### 4.2 Command/Telemetry Path Separation (CQRS-Inspired)
This architecture is not a literal implementation of classic CQRS — there are no separate command/query models or separate data stores within the same service. Instead, we borrow CQRS's core principle: decoupling write/control responsibilities from read/analytics responsibilities.

Traditional flaw: In the classic Purdue model, if a historian or MES slows down, the PLC's output buffers can fill up and affect the control loop (scan cycle). When control and data collection share the same resource path, one can block the other.

Our improvement: The FSM Simulator processes commands (`Start`, `Stop`, `Reset`) directly and synchronously, through a path that is completely independent of Kafka. Telemetry data is published asynchronously to Kafka and later persisted by the consumer into TimescaleDB.

As a result:
- If the Kafka broker slows down or goes offline, the FSM control loop remains unaffected.
- If TimescaleDB hits disk I/O limits, only the consumer's write throughput is impacted; the control layer sees no backpressure.

This is best described as "decoupled control and telemetry paths, inspired by CQRS's separation-of-concerns principle," rather than a full CQRS implementation.

### 4.3 Micro-Batching and Time-Series Optimization
- Traditional systems often insert irregular single rows.
- Our pipeline: Kafka -> Consumer micro-batch (N = 50 records or T = 2 seconds) -> TimescaleDB hypertable. This keeps industrial time-series data efficient, high-throughput, and resource-friendly.

## 5. Key Benefits for the Project
- The OT control process remains deterministic and isolated.
- Analytics and persistence scale asynchronously.
- Clear separation exists between operator commands and telemetry events.
- This control/telemetry decoupling accelerates data flow while preserving traditional Purdue safety.


