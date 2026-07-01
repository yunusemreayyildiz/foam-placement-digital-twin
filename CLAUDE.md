# AI-Assisted Development & Refactoring Guide: IIoT Digital Twin
This guide serves as a structured blueprint and context-provider for AI assistants to collaborate on refactoring, optimizing, and scaling the Foam Placement Machine Digital Twin pipeline.

---

## 1. Project Context & Architecture Overview
* **Domain:** Industrial IoT (IIoT), Digital Twin, Industry 4.0.
* **Core Logic:** Simulating a Programmable Logic Controller (PLC) scan-cycle using a Finite State Machine (FSM) in Python.
* **Architecture Pattern:** Decoupled Microservices / Event-Driven Architecture.
* **Data Pipeline Flow:**
  `simulator (Python FSM)` -> `Apache Kafka (KRaft Mode)` -> `consumer (Python Worker)` -> `TimescaleDB (Hypertable)`

---

## 2. Global AI Instructions (Read This Before Coding)
When generating code, refactoring, or writing tests for this repository, adhere to the following architectural constraints:
1. **Separation of Concerns:** Keep the simulator agnostic of the database schema, and keep the consumer agnostic of the FSM state logic.
2. **Non-Blocking Execution:** Ensure time-critical loops mimic real-time hardware. Avoid blocking operations (`time.sleep`) inside state evaluations if asynchronous alternatives exist.
3. **Resilience First:** Network boundaries (Kafka, TimescaleDB) must handle connection drops gracefully using retry mechanisms.

---

## 3. Development Backlog & Prompt Templates

### Phase 1: State Machine Refactoring (V2 Feature Alignment)
**Objective:** Merge the advanced operator process specifications into the simulation logic.

* [ ] **Task 1.1:** Update the `State` enum in `foam_placement_sm_v2.py` to match the exact physical steps: `IDLE`, `VACUUM_ON`, `ROBOT_RUNNING`, `CAMERA_CHECK`, `MARKING`, `RIGHT_RELEASE`, `LEFT_RELEASE_WAIT`, `WAIT_LEFT_IN_KASA`, `CYCLE_DONE`, `RED_HANDLING`.
* [ ] **Task 1.2:** Map the metric outputs inside `step()` to telemetry fields: `right_count`, `left_count`, and `error_count` instead of a singular `parts_produced`.

> 📋 **Prompt Template for Task 1.1 & 1.2:**
> *"Act as an expert Industrial Automation and Python engineer. I will provide you with a V1 skeleton of an FSM and a V2 process definition. I need you to refactor the `step()` function in `foam_placement_sm_v2.py` to transition across the new states (`VACUUM_ON`, `MARKING`, etc.) and seamlessly output `right_count`, `left_count`, and `error_count` metrics into the Kafka payload. Here is the source code: [Insert Code Here]"*

---

### Phase 2: Safety & Interruption Logic Fixes
**Objective:** Resolve the infinite loop vulnerability inside the `_safety_check()` mechanism where the system auto-resets without waiting for a manual operator acknowledgment.

* [ ] **Task 2.1:** Implement a realistic hardware acknowledgment check. Add an explicit `is_reset_pressed` flag or mock input signal inside `MockSensors`.
* [ ] **Task 2.2:** Ensure that when a safety interrupt occurs (`ERROR_ISIK_BARIYERI_YETKISIZ`, `ERROR_KASA_ICI_HATALI`), the FSM remains locked in that error state until the reset button transitions from `False` to `True`.

> 📋 **Prompt Template for Phase 2:**
> *"Review the `_safety_check` method in my FSM code. Currently, it automatically recovers from errors in the subsequent scan-cycle, which is a dangerous anti-pattern in industrial safety. Refactor it so that the state machine halts in the respective error state and only rolls back to `_state_before_error` when a simulated physical reset button is triggered. Here is the file: [Insert Code Here]"*

---

### Phase 3: Data Engineering Optimization (Batching)
**Objective:** Optimize database write efficiency by switching from single-row inserts to micro-batch inserts within `consumer.py`.

* [ ] **Task 3.1:** Introduce an in-memory buffer array inside the consumer consumer loop.
* [ ] **Task 3.2:** Modify the pipeline to write events to TimescaleDB using `cursor.executemany()` either when the buffer length hits 50 records or when a maximum of 2 seconds has elapsed since the last flush.

> 📋 **Prompt Template for Phase 3:**
> *"Act as a Senior Data Engineer specializing in high-throughput streaming pipelines. Modify the provided `consumer.py` script. Instead of invoking a single database insert for every single incoming Kafka message, implement an in-memory micro-batching mechanism using `psycopg2`'s batch execution patterns. Ensure it handles partial buffers gracefully if the stream slows down. Here is the script: [Insert Code Here]"*

---

### Phase 4: Modernization & Asynchronous Transformation (Bonus)
**Objective:** Transition the polling and ingestion layers to native asynchronous Python (`asyncio`) to prepare the codebase for high-concurrency workloads.

* [ ] **Task 4.1:** Migrate the synchronous `time.sleep()` scan delay to `await asyncio.sleep()`.
* [ ] **Task 4.2:** Replace `kafka-python` with `aiokafka` inside both the simulator and consumer modules.

---

## 4. Progress Tracking Dashboard
| Phase | Feature / Fix | Status | Target Date | Notes |
| :--- | :--- | :--- | :--- | :--- |
| **1** | V2 State Schema Integration | ✅ Done | | `core/states.py` / `core/fsm.py`, 9/9 tests passing |
| **2** | Hardware Reset / Interruption Lock | ✅ Done (live-verified) | | Edge-triggered reset in `core/fsm.py` (`_handle_red_state`); unit-covered in `tests/test_fsm_red_handling.py` (5 tests) and exercised end-to-end through Kafka -> consumer -> TimescaleDB -> Grafana via `main.py`'s opt-in `ENABLE_FAULT_SCENARIO` fault-injecting driver |
| **3** | Consumer Micro-batching | ✅ Done | | `consumer/consumer.py`: flush at 50 records / 2s, whichever first (`psycopg2.extras.execute_batch`); manual Kafka offset commit *after* a confirmed TimescaleDB write (`enable.auto.commit=False` + `consumer.commit()` in `_flush`) closes a data-loss gap where auto-commit could ack offsets for still-unflushed buffered messages |
| **4** | Asyncio Migration | ✅ Done (live-verified) | | `main.py`'s scan loop and `consumer/consumer.py`'s poll/DB/commit loop are now `async def` (`asyncio.sleep`, `asyncio.run`); confluent-kafka/psycopg2 calls are kept off the event loop via `loop.run_in_executor`. Kept `confluent-kafka` rather than migrating to `aiokafka` (backlog text predated the `kafka-python-ng` -> `confluent-kafka` switch) - swapping a client that's already proven resilient live wasn't worth re-verifying from scratch. DB-outage recovery re-tested live under the async consumer with identical results to Phase 3 |

### 5. Infrastructure (added this session)
- `docker-compose.yml`: Kafka (KRaft mode, dual INTERNAL/EXTERNAL listener), TimescaleDB, Grafana, `consumer` service, optional `simulator` service (profile-gated).
- `database/models.py`: hypertable DDL + `insert_events_batch()` used by Phase 3 above.
- `telemetry/kafka_producer.py`: non-blocking publish, decoupled from the FSM control loop per architecture.md 4.2.
- `main.py`: wires FSM + a scripted `_DemoSensorDriver` (stand-in for the real PLC IO bridge) + the producer. Optional `_FaultInjectingSensorDriver` (enabled via `ENABLE_FAULT_SCENARIO=true` env var) periodically drives a real E-Stop -> `RED_HANDLING` -> edge-triggered-reset scenario through the live pipeline, so the Grafana "Error Count Over Time" panel gets real data instead of staying empty.
- `config/grafana/`: auto-provisioned datasource + dashboard (5 panels: kasa counter, error count over time, event distribution by state, state timeline, last 20 events table).
- Known gotcha: `kafka-python` 2.0.2 is broken on Python 3.12; swapped to `kafka-python-ng` (drop-in fork, same `import kafka`).

---
*Generated for architectural development iteration 2026.*