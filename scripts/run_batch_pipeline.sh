#!/bin/bash
set -e  # Exit immediately if any command fails

# Detect TTY for manual runs vs cron
INTERACTIVE=""
if [ -t 0 ]; then
    INTERACTIVE="-it"
else
    INTERACTIVE="-i"
fi

echo "[$(date)] === Starting TransitFlow Daily Pipeline ==="

# --- PERMANENT INFRASTRUCTURE SETUP ---
# Ensure the marts schema exists before Spark or API tries to use it
docker exec postgres psql -U transit -d transit -c "CREATE SCHEMA IF NOT EXISTS marts;"
echo "[$(date)] 0. Ensuring Flink Stream is Active..."
make flink-deploy

# --- Phase 1: Ingestion & Storage ---
echo "[$(date)] 1. Running Bronze Ingestion (Batch Mode)..."
make spark-bronze

echo "[$(date)] 2. Syncing to Postgres..."
make spark-sync

echo "[$(date)] 3. Running Silver Transformation..."
make spark-silver

echo "[$(date)] 4. Verifying Data Integrity (Reconciliation)..."
make spark-reconcile

# --- Phase 2: Analytics & Metadata ---
echo "[$(date)] 5. Initializing Gold Metadata..."
make metadata-init

echo "[$(date)] 6. Running Gold Aggregation..."
make spark-gold

echo "[$(date)] 6.5. Capturing Slowly Changing Dimensions (Snapshots)..."
make dbt-snapshot

echo "[$(date)] 6.6. Running dbt to populate Marts..."
make dbt-run

# --- Phase 3: Maintenance ---
echo "[$(date)] 7. Running Lakehouse Maintenance..."
# Vacuums Delta files and Cleans Postgres history (>3 days)
make spark-maintenance

echo "[$(date)] 8. Pruning Dangling Docker Images (Health Check)..."
# Cleans up 1-2GB of orphaned images/networks safely
docker system prune -f

# --- Phase 4: MLOps ---
echo "[$(date)] 9. Checking for Data Drift..."
docker exec $INTERACTIVE serving-api python scripts/monitor_drift.py

echo "[$(date)] 10. Evaluating Retraining Rules..."
docker exec $INTERACTIVE serving-api python mlops/retraining.py --drift-file drift.json

# --- Phase 5: CLEANUP (Survival Mode) ---
echo "[$(date)] 11. CLEANUP: Removing processed raw data..."
# Authenticate securely inside the container
docker exec $INTERACTIVE minio sh -c 'mc alias set myminio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD > /dev/null'

# Aggressively delete Bronze and Silver data older than 1 day to save disk space.
docker exec $INTERACTIVE minio mc rm -r --force --older-than 1d myminio/transitflow-lakehouse/bronze/ || true
docker exec $INTERACTIVE minio mc rm -r --force --older-than 1d myminio/transitflow-lakehouse/silver/ || true

echo "[$(date)] === Pipeline Complete ==="