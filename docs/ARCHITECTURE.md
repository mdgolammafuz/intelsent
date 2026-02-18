# TransitFlow Architecture

Technical architecture documentation for the TransitFlow real-time transit delay prediction platform. 

**Design Philosophy:** This architecture is designed for "Survival Mode"—delivering enterprise-grade Medallion Lakehouse and MLOps capabilities while running entirely on a single, resource-constrained Cloud VM (e.g., 8GB RAM, 30GB Disk).

---

## System Overview

TransitFlow implements a **lambda architecture** with unified stream and batch processing. The system ingests simulated/live GPS data, processes it through real-time and batch layers, and serves predictions via a fault-tolerant ML serving API that natively hosts its own UI.

---

## Data Flow

    Producer (Python) → Redpanda (Kafka) → Flink (Stream) → Redpanda (Enriched)
                                               ↓
                                             Redis (Online Feature Store)
                                               
    Redpanda (Enriched) → Spark (Batch) → MinIO (Bronze → Silver → Gold Delta Lake)
                                               ↓
                                            Postgres (Offline Feature Store / Metadata)
                                               ↓
                                          Serving API (FastAPI) ←→ MLflow (Model Registry)
                                               ↓
                                      End User (Web UI / JSON)

---

## Component Details

### 1. Ingestion Layer
- **Component**: Python Producer (`run_bridge.py`)
- **Transport**: Redpanda (Kafka compatible, lightweight C++ implementation)
- **Role**: Simulates or ingests live vehicle telemetry (speed, location, delay).

### 2. Stream Processing (Flink)
- **Purpose**: Real-time stateful enrichment.
- **Resilience**: Configured with `fixed-delay` restart strategy to survive transient Redpanda disconnects automatically.
- **Output**: Pushes immediate state updates to Redis (Online Feature Store) for ultra-low latency (<5ms) feature retrieval.

### 3. Batch Processing (Spark & Delta Lake)
- **Purpose**: Medallion architecture transformations operating on a nightly Cron schedule.
- **Resilience (Schema Propagation)**: Scripts are designed to handle "empty data" days gracefully. If 0 rows are found, Spark propagates an empty DataFrame to maintain downstream table schemas without crashing the pipeline.

| Layer | Action | Retention Strategy |
|-------|--------|--------------------|
| Bronze | Append-only raw events | Aggressively pruned (>1 day) via MinIO `mc rm` |
| Silver | Deduplication via Window functions | Aggressively pruned (>1 day) via MinIO `mc rm` |
| Gold | Hourly/Daily Aggregations | Retained for ML training. Vacuumed with 0-hour retention to drop ghost files. |

### 4. Feature Store (Dual-Store Pattern)
- **Online (Redis)**: Stores the absolute latest location/delay for active vehicles.
- **Offline (Postgres)**: Stores Gold-layer aggregated historical data (e.g., historical average delay at a specific stop at a specific hour).

### 5. Serving API & UI (FastAPI)
- **Unified Hosting**: FastAPI serves both the ML prediction endpoints (`/predict`) and the static Frontend UI (`/`), eliminating the need for an Nginx reverse proxy to save memory.
- **Circuit Breakers**: Wraps Redis and Postgres calls. If a database goes down, the API falls back to "Heuristic Mode" (Current Delay = Predicted Delay).
- **Shadow Mode**: Evaluates candidate models (Staging) against live Production traffic asynchronously without impacting user latency.

### 6. MLOps & Automation
- **Drift Detection (PSI)**: Uses Population Stability Index to compare today's data distribution against the training baseline. Supports "Survival Mode" 50/50 fallback splitting if historical baseline data is purged to save disk space.
- **Automated Retraining**: Triggers XGBoost retraining automatically if Drift PSI > threshold.
- **Orchestration**: Managed entirely by a lean Bash script (`run_batch_pipeline.sh`) hooked into Linux `cron`.

---

## Resource Management & "Survival Mode"

To operate indefinitely on a 30GB disk, the architecture enforces strict lifecycle rules:
1. **Docker Log Rotation**: Enforced via `json-file` driver (`max-size: 10m`).
2. **Dangling Image Pruning**: `docker system prune -f` runs nightly.
3. **Data Vacuuming**: Delta Lake `VACUUM` runs nightly with `RETAIN 0 HOURS` for Gold tables.
4. **Hard Deletes**: MinIO client natively bypasses Delta Lake to hard-delete raw Bronze/Silver parquet files older than 24 hours.