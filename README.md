# TransitFlow

**Real-time transit delay prediction platform using Helsinki Regional Transport (HSL) data.**

[![Production Pipeline](https://github.com/mdgolammafuz/transitflow/actions/workflows/deploy.yml/badge.svg)](https://github.com/mdgolammafuz/transitflow/actions/workflows/deploy.yml)

[![Live Demo](https://img.shields.io/badge/Demo-Live_UI-green?style=for-the-badge&logo=googlecloud)](http://34.16.120.44/)

---

## Overview

TransitFlow predicts transit vehicle delays in real-time by processing streaming GPS data through a modern lakehouse architecture. This project utilizes the **Helsinki Regional Transport Authority (HSL)** open data API, specifically concentrating on **Line 600** to demonstrate high-frequency telemetry processing.

Designed for extreme efficiency, TransitFlow demonstrates how to run a complete enterprise data stack (Kafka, Flink, Spark, Delta Lake, MLflow) on a single, resource-constrained Cloud VM while maintaining production-grade reliability and observability.

---

## Key Features

| Category | Features |
|----------|----------|
| **Data Engineering** | Medallion Lakehouse (Delta Lake), PySpark Batch Processing |
| **Stream Processing** | Apache Flink for stateful real-time enrichment |
| **ML Operations** | MLflow tracking, automated drift detection (PSI), dynamic retraining |
| **Reliability** | Schema Propagation (empty-batch handling), Feature Circuit Breakers |
| **Resource Ops** | Aggressive automated disk cleanup, Docker log rotation, Cron orchestration |
| **User Interface** | Real-time JS dashboard natively served via FastAPI |

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              TransitFlow Architecture                        │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐    ┌──────────┐    ┌──────────────────────────────────────┐   │
│  │  HSL     │───▶│  Kafka   │───▶│           Medallion Lakehouse         │   │
│  │  MQTT    │    │ (Redpanda)│    │  ┌────────┐ ┌────────┐ ┌────────┐   │   │
│  └──────────┘    └──────────┘    │  │ Bronze │▶│ Silver │▶│  Gold  │   │   │
│                       │          │  │ (raw)  │ │(clean) │ │ (agg)  │   │   │
│                       ▼          │  └────────┘ └────────┘ └────────┘   │   │
│                  ┌──────────┐    │         Delta Lake + Spark           │   │
│                  │  Flink   │    └──────────────────────────────────────┘   │
│                  │ (stream) │              │                                │
│                  └──────────┘              ▼                                │
│                       │          ┌──────────────────┐                       │
│                       ▼          │   Feature Store  │                       │
│                  ┌──────────┐    │ ┌──────┐ ┌─────┐ │                       │
│                  │  Redis   │◀───│ │Postgres│Redis│ │                       │
│                  │ (online) │    │ │offline││online│                        │
│                  └──────────┘    │ └──────┘ └─────┘ │                       │
│                       │          └──────────────────┘                       │
│                       ▼                   │                                 │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         ML Platform                                   │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐      │   │
│  │  │  Training  │  │  MLflow    │  │  Serving   │  │  Shadow    │      │   │
│  │  │  (XGBoost) │─▶│  Registry  │─▶│    API     │◀─│  Deploy    │      │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘      │   │
│  │                        │               │                              │   │
│  │                        ▼               ▼                              │   │
│  │                  ┌────────────┐  ┌────────────┐                       │   │
│  │                  │ Validation │  │  Circuit   │                       │   │
│  │                  │   Gates    │  │  Breaker   │                       │   │
│  │                  └────────────┘  └────────────┘                       │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                         Observability                                 │   │
│  │  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌────────────┐      │   │
│  │  │ Prometheus │─▶│  Grafana   │  │   Drift    │  │   Alerts   │      │   │
│  │  │  Metrics   │  │ Dashboards │  │ Detection  │  │  (PSI)     │      │   │
│  │  └────────────┘  └────────────┘  └────────────┘  └────────────┘      │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```
---
## Quick Start

### Prerequisites
- Docker & Docker Compose
- Linux/MacOS environment (8GB RAM, 30GB Disk minimum recommended)

### 1. Spin up the Infrastructure
Start the core streaming and database containers:

    cd infra/local
    docker compose up -d

*(Note: The Flink job will automatically submit itself to the cluster once running).*

### 2. Run the Batch Pipeline (First Run)
Execute the end-to-end pipeline to generate Bronze/Silver/Gold data, train the initial ML model, and check for drift:

    cd ../../
    chmod +x scripts/run_batch_pipeline.sh
    ./scripts/run_batch_pipeline.sh

### 3. Access the UI
Open your browser and navigate to `http://localhost/` (or your VM's Public IP). 
- The UI runs on Port 80 (mapped to container port 8001).
- View API Docs at `http://localhost/docs`.

---

## Pipeline Automation (Cron)

In production, the batch pipeline manages its own lifecycle and disk space automatically. 

To set up the nightly run (e.g., at 2:00 AM):

    crontab -e

Add the following line to the bottom:

    0 2 * * * cd /path/to/transitflow && ./scripts/run_batch_pipeline.sh >> /path/to/transitflow/pipeline.log 2>&1

---

## MLOps Commands

Because TransitFlow operates in a containerized environment, MLOps tasks are executed securely via Docker.

**Check Data Drift manually:**

    docker exec -it serving-api python scripts/monitor_drift.py

*(Outputs results to `drift.json` in the container).*

**Evaluate Retraining Rules:**

    docker exec -it serving-api python mlops/retraining.py --drift-file drift.json


**View MLflow UI:**
Navigate to `http://localhost:5001` to view experiments, run metrics, and the Model Registry.

---

## Project Structure

    transitflow/
    ├── scripts/                # Cron orchestrator (run_batch_pipeline.sh)
    ├── ingestion/              # Python producer / Bridge scripts
    ├── flink/                  # Java/Maven Stream processing jobs
    ├── spark/                  # PySpark Medallion scripts & Maintenance
    ├── feature_store/          # Online (Redis) + Offline (Postgres) logic
    ├── ml_pipeline/            # XGBoost training pipeline
    ├── mlops/                  # Retraining logic and evaluations
    ├── infra/local/            # Docker Compose configurations
    ├── serving/                # FastAPI application, Circuit Breakers, Shadow mode
    └── ui/                     # Frontend HTML/JS dashboard
    

---

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - Technical design details

- [Operations](docs/OPERATIONS.md) - Operations and troubleshooting

---
## Acknowledgments
- [Helsinki Regional Transport Authority (HSL)](https://www.hsl.fi/) for transit data
- Built using [Apache Flink](https://flink.apache.org/), [Apache Spark](https://spark.apache.org/), and [Delta Lake](https://delta.io/).
- Model Registry powered by [MLflow](https://mlflow.org/).