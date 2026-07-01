# Foam Placement Machine Digital Twin (SCARA Robot Cell Control & Telemetry System)

This project implements a finite state machine (FSM) driven control and real-time telemetry system for a SCARA robot production cell. It maps PLC inputs/outputs to a Python-based state machine and broadcasts system telemetry via Apache Kafka.

The architecture follows the Purdue Enterprise Reference Architecture (PERA), isolating machine control (Level 1/2) from data ingestion and analytics (Level 3/4).

## System Architecture

The project bridges industrial automation with modern software engineering practices:

- **Level 1 (Basic Control):** PLC IO execution mapping based on the SCARA Robot wiring diagrams.
- **Level 2 (Area Control):** Python Finite State Machine (FSM) acting as the simulator/controller brain.
- **Level 3 (Operations):** Real-time data streaming via Apache Kafka for telemetry and diagnostics.

## Table of Contents

- Features
- Project structure
- IO mapping overview
- Finite State Machine (FSM)
- Kafka telemetry schema
- Predictive maintenance / anomaly detection
- Getting started

## Features

- **Industrial IO Mapping:** Comprehensive digital input/output translation from electrical schemas (`sources/SCARA ROBOT BAĞLANTIN ŞEMASI (1).xlsx`, parsed into `config/io_mapping.yaml` by `scripts/parse_xlsx_xml.py`).
- **State Machine Controller:** Python FSM handling safe state transitions (`IDLE`, `VACUUM_ON`, `ROBOT_RUNNING`, `CAMERA_CHECK`, `MARKING`, `RIGHT_RELEASE`, `LEFT_RELEASE_WAIT`, `WAIT_LEFT_IN_KASA`, `CYCLE_DONE`, `RED_HANDLING`).
- **Vacuum Control Logic:** Feedback synchronization for dual-vacuum grippers (Model 1477 & 1480) with blow-off sequences.
- **Safety Interlocks:** Monitoring of light curtains, door/vibration sensors, and hardware E-Stop switches.
- **Data Streaming Pipeline:** Low-latency telemetry broadcasting to Kafka topics for analytics.
- **Predictive Maintenance:** A decoupled analytics service reconstructs completed production cycles from the telemetry stream, engineers per-cycle features (duration, per-state dwell time, safety-interrupt excursions), and flags anomalies with both an EWMA statistical baseline and a trained IsolationForest model - see [Predictive maintenance / anomaly detection](#predictive-maintenance--anomaly-detection).

## Project structure

```
├── config/
│   └── io_mapping.yaml       # PLC register ve yazılım eşleşmeleri
├── core/
│   ├── __init__.py
│   ├── fsm.py                # Python Finite State Machine motoru
│   └── states.py             # System State Enums (IDLE, VACUUM_ON, ROBOT_RUNNING vb.)
├── telemetry/
│   ├── __init__.py
│   └── kafka_producer.py     # State değiştikçe Kafka'ya veri basan ajan
├── consumer/
│   ├── __init__.py
│   └── consumer.py           # Kafka'yı dinleyip veritabanına (DB) yazan servis
├── analytics/                 # Anomali tespiti / öngörücü bakım servisi
│   ├── features.py           # Saf çevrim başına özellik mühendisliği
│   ├── cycle_boundary.py      # Saf çevrim sınırı tespiti (CycleAccumulator)
│   ├── scoring.py             # EWMA + IsolationForest skorlama
│   ├── analytics_consumer.py  # Canlı asyncio Kafka tüketicisi (kendi consumer group'u)
│   └── model_training/
│       └── train_isolation_forest.py  # Tek seferlik/periyodik model eğitim script'i
├── database/                 # Opsiyonel: DB bağlantı ve şema ayarları
│   ├── __init__.py
│   └── models.py             # Zaman serisi veya ilişkisel DB modelleri
├── .env.example               # Kafka broker adresleri, DB şifreleri vb. (kopyala: .env)
├── docker-compose.yml        # Kafka (KRaft mode), TimescaleDB, Grafana ve Python servisleri
├── main.py                   # FSM ve Telemetry'yi başlatan ana giriş kapısı
├── requirements.txt          # Bağımlılıklar (confluent-kafka, psycopg2, scikit-learn vb.)
├── LICENSE
└── README.md
```

## IO mapping overview

The system processes physical IO mapped into software variables. The full, canonical mapping is stored in [config/io_mapping.yaml](config/io_mapping.yaml). A condensed, human-readable view follows.

### Digital Inputs (DI)

| PLC address | Sheet tag | Description (English) | Notes |
|-------------|-----------|-----------------------|-------|
| I0.0 | IN81  | Start (BAŞLA)                                 | Start push / auto-start |
| I0.1 | IN82  | Light curtain (IŞIK BARİYERİ)                 | Safety curtain input |
| I0.2 | IN83  | Reset switch (RESET ANAHTARI)                 | Operator reset |
| I0.3 | IN84  | Rotary forward (ROTARY İLERİ)                 | Rotary encoder/button |
| I0.4 | IN85  | Rotary backward (ROTARY GERİ)                 | |
| I0.5 | IN86  | Linear gripper forward (LİNEAR TUTUCU İLERİ)  | |
| I0.6 | IN87  | Linear gripper back (LİNEAR TUTUCU GERİ)      | |
| I0.7 | IN88  | Linear pulling cylinder forward               | |
| I1.0 | IN89  | Linear pulling cylinder back                  | |
| I1.1 | IN90  | Linear locking cylinder forward               | |
| I1.2 | IN91  | Linear locking cylinder back                  | |
| I1.3 | IN92  | 1477 left vacuum OK                            | Sensor OK flag |
| I1.4 | IN93  | 1477 right vacuum OK                           | |
| I1.5 | IN94  | 1480 left vacuum OK                            | |
| I1.6 | IN95  | 1480 right vacuum OK                           | |
| I1.7 | IN96  | Right box inner barrier (SAĞ KASA İÇİ BARİYER) | Safety input |
| I2.0 | IN97  | Left box inner barrier                         | Safety input |
| I2.1 | IN98  | Reject part pushbutton (RED PARÇA BUTON)      | Reject request |
| I2.2 | IN99  | Robot mode (ROBOTLU)                          | Mode selector |
| I2.3 | IN100 | Robotless mode (ROBOTSUZ)                     | Mode selector |
| I2.4 | IN101 | Sponge pick-up area sensor                     | |
| I2.5 | IN102 | Tool sponge present                            | |
| I2.6 | IN103 | 1477 rotary table                              | |
| I2.7 | IN104 | 1480 rotary table                              | |
| I3.0 | IN105 | Door & vibration safety                        | Safety interlock |
| I3.1 | IN106 | Emergency stop (ACİL STOP)                     | E-stop pressed |
| I3.2 | IN107 | Table clamp forward                            | |
| I3.3 | IN108 | Table clamp back                               | |
| I3.4 | IN109 | Spare input 1                                  | Unused |
| I3.5 | IN110 | Spare input 2                                  | Unused |
| I3.6 | IN111 | Spare input 3                                  | Unused |
| I3.7 | IN112 | Spare input 4                                  | Unused |

### Digital Outputs (DO)

| PLC tag | Description (English) | Notes |
|---------|-----------------------|-------|
| OUT81  | Linear gripper actuator                            | |
| OUT82  | Linear locking cylinder                            | |
| OUT83  | Linear pulling cylinder                            | |
| OUT84  | Robot tool gripper                                 | Command to robot tool |
| OUT85  | 1477 needle insertion                              | |
| OUT86  | 1480 needle insertion                              | |
| OUT87  | Table clamp                                        | |
| OUT88  | Pilot warning valve (NC)                           | NC valve control |
| OUT89  | Pilot warning valve (duplicate/config)             | |
| OUT90  | Closed-center valve rotary forward                 | |
| OUT91  | Closed-center valve rotary back                    | |
| OUT92  | Closed-center valve forward                        | |
| OUT93  | Closed-center valve back                           | |
| OUT94  | 1477 left vacuum ON                                | Vacuum control |
| OUT95  | 1477 left vacuum blow-off                          | Vacuum blow-off |
| OUT96  | 1477 right vacuum ON                               | |
| OUT97  | 1477 right vacuum blow-off                         | |
| OUT98  | 1480 left vacuum ON                                | |
| OUT99  | 1480 left vacuum blow-off                          | |
| OUT100 | 1480 right vacuum ON                               | |
| OUT101 | 1480 right vacuum blow-off                         | |
| OUT102 | System air (compressor / air)                      | |
| OUT103 | Alarm output                                       | Audible/visual alarm |
| OUT104 | Green lamp (YEŞİL LAMBA)                           | Indicator |
| OUT105 | Right box full indicator                           | |
| OUT106 | Left box full indicator                            | |
| OUT107 | Camera exe command on                              | Camera control |
| OUT108 | Camera exe 1 on                                    | |
| OUT109 | Camera exe 2 on                                    | |
| OUT110 | Camera exe acknowledge                             | |
| OUT111 | Lighting (AYDINLATMA)                              | |
| OUT112 | Spare output (BOŞ2)                                | Unused |

## Finite State Machine (FSM)

The FSM enforces safe transitions to prevent illegal operations (for example, moving while vacuum feedback is lost). The diagram below uses Mermaid syntax.

```mermaid
stateDiagram-v2
    [*] --> IDLE
    IDLE --> VACUUM_ON : Start pushed + part present
    VACUUM_ON --> ROBOT_RUNNING : Vacuum OK (IN92-95)
    ROBOT_RUNNING --> CAMERA_CHECK : Robot move complete
    CAMERA_CHECK --> MARKING : Camera OK
    MARKING --> RIGHT_RELEASE
    RIGHT_RELEASE --> LEFT_RELEASE_WAIT : Right part in kasa
    LEFT_RELEASE_WAIT --> WAIT_LEFT_IN_KASA
    WAIT_LEFT_IN_KASA --> CYCLE_DONE : Left part in kasa
    CYCLE_DONE --> IDLE

    state "RED_HANDLING" as RH
    IDLE --> RH : Safety interrupt
    VACUUM_ON --> RH : Safety interrupt
    ROBOT_RUNNING --> RH : Safety interrupt
    CAMERA_CHECK --> RH : Safety interrupt
    MARKING --> RH : Safety interrupt
    RIGHT_RELEASE --> RH : Safety interrupt
    LEFT_RELEASE_WAIT --> RH : Safety interrupt
    WAIT_LEFT_IN_KASA --> RH : Safety interrupt
    CYCLE_DONE --> RH : Safety interrupt
```

`RED_HANDLING` is reachable from any monitored state (E-Stop, light curtain, door/vibration, box-inner-barrier, or mid-cycle vacuum loss - see `core/fsm.py`'s `_check_safety`). It is **not** shown looping back in the diagram above because the return path is dynamic: an edge-triggered (`False -> True`) operator reset rolls the FSM back to whichever state it faulted from, not to a fixed state - it never auto-recovers on its own (`core/fsm.py`'s `_handle_red_state`).

## Kafka telemetry schema

The pipeline emits one event to the `scara.telemetry.v1` topic per scan cycle (see `core/fsm.py`'s `step()` and `telemetry/kafka_producer.py`). Example message (full schema documented in [docs/architecture.md](docs/architecture.md), Section 3.4):

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

## Predictive maintenance / anomaly detection

`analytics/analytics_consumer.py` subscribes to `scara.telemetry.v1` under its own Kafka consumer group (`foam-placement-analytics`) - decoupled from `consumer/consumer.py` so the ML stack can never stall raw telemetry ingestion. It reconstructs each completed production cycle (`analytics/cycle_boundary.py`), engineers per-cycle features such as cycle duration, per-state dwell time, and safety-interrupt (`RED_HANDLING`) excursions (`analytics/features.py`), and scores every cycle with two independent, explainable layers (`analytics/scoring.py`):

1. **EWMA + control limits** on cycle duration - a classic statistical-process-control technique, usable from the very first few cycles.
2. **IsolationForest** (scikit-learn) over the full feature vector - trained offline, not online, so it needs a manual training step once enough cycles exist.

Results land in the `cycle_metrics` hypertable (`database/models.py`) and are visualized in a second, auto-provisioned Grafana dashboard, "Foam Placement Machine - Predictive Maintenance" (`config/grafana/dashboards/analytics.json`): cycle-time trend with 3-sigma control bands, anomaly score over time, a table of flagged anomalous cycles, and a rolling error rate.

To train the IsolationForest model (needs ~50+ completed cycles first - just let `main.py` and `analytics` run for a while):

```bash
docker compose run --rm analytics python analytics/model_training/train_isolation_forest.py
docker compose restart analytics   # v1 is restart-to-reload, not hot-reload
```

## Getting started

### Prerequisites

- Python 3.10+ (tested on 3.12)
- Docker + Docker Compose (for Kafka, TimescaleDB, Grafana, and the consumer)

### Quick start - full stack (recommended)

Bring up Kafka (KRaft mode), TimescaleDB, Grafana, and the consumer service:

```bash
docker compose up -d
```

This starts:

| Service | Port | Notes |
|---|---|---|
| Kafka | `localhost:9092` (EXTERNAL) | dual listener: `kafka:29092` for containers, `localhost:9092` for host processes |
| TimescaleDB | `localhost:5432` | db=`foam_placement`, user=`foam_admin` (see `docker-compose.yml`) |
| Grafana | `localhost:3000` | login `admin` / `admin`; dashboards "Foam Placement Machine - Digital Twin" and "Foam Placement Machine - Predictive Maintenance" auto-provisioned |
| consumer | - | Kafka -> TimescaleDB micro-batch writer (50 records / 2s, whichever first) |
| analytics | - | Cycle-boundary detection + anomaly scoring -> `cycle_metrics` (see [Predictive maintenance / anomaly detection](#predictive-maintenance--anomaly-detection)) |

The FSM simulator (`main.py`) is **not** started by `docker compose up` by default - run it locally against the host-exposed Kafka port so you can iterate on it quickly:

```bash
python -m venv .venv
source .venv/bin/activate  # .venv\Scripts\Activate.ps1 on Windows
pip install -r requirements.txt
cp .env.example .env       # defaults already point at localhost:9092 / localhost:5432
python main.py
```

To containerize the simulator too instead: `docker compose --profile simulator up -d`.

### Verifying the pipeline

- `docker compose logs -f consumer` - should show `Flushed N events to TimescaleDB.` roughly every 2 seconds once `main.py` is running.
- `docker compose logs -f analytics` - should show `cycle closed: duration=...` roughly once per full IDLE->CYCLE_DONE->IDLE cycle.
- Open Grafana at `http://localhost:3000` - the "Digital Twin" dashboard shows kasa counters, error count over time, event distribution by state, a state timeline, and the last 20 raw events; the "Predictive Maintenance" dashboard shows cycle-time trend/control limits, anomaly scores, flagged anomalous cycles, and rolling error rate.
- `psql -h localhost -U foam_admin -d foam_placement -c "SELECT * FROM telemetry_events ORDER BY time DESC LIMIT 5;"` (password `foam_admin_pw`).
- `psql -h localhost -U foam_admin -d foam_placement -c "SELECT cycle_end_time, cycle_duration_seconds, had_red_handling, stat_anomaly_flag FROM cycle_metrics ORDER BY cycle_end_time DESC LIMIT 5;"`.

### Running tests

```bash
pip install pytest
python -m pytest tests/ -v
```

## Notes & next steps

- Kafka client is Confluent's official `confluent-kafka` (librdkafka-backed, prebuilt wheels for Windows/Linux on Python 3.12/3.13). The original `kafka-python` package is broken on Python 3.12+, and its `kafka-python-ng` fork had its own broken releases too - see the comment in `requirements.txt` for the full history.
- Grafana datasource/dashboard provisioning lives in `config/grafana/`; edit either dashboard JSON file and Grafana will pick up changes within `updateIntervalSeconds` (30s).
- Model artifacts for the predictive-maintenance layer (`analytics/models/*.joblib`) are gitignored - they're a training *output*, regenerate them with `analytics/model_training/train_isolation_forest.py` rather than expecting them in a fresh clone.
- Licensed under the terms in `LICENSE`.
