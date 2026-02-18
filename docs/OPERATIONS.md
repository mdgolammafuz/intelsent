# TransitFlow Operations & Deployment Guide

This document combines deployment instructions, health checks, and troubleshooting runbooks for the TransitFlow single-node architecture.

---

## 1. Deployment Guide

TransitFlow is designed to run entirely via Docker Compose on a single Cloud VM (minimum 8GB RAM, 30GB Storage).

### 1.1 Initial Setup

1. **Clone the repository:**
   ```bash
   git clone [https://github.com/YOUR_USERNAME/transitflow.git](https://github.com/YOUR_USERNAME/transitflow.git)
   cd transitflow
   ```

2. **Configure Environment:**
   Create a `.env` file in the project root. **Never commit this file to version control.**
   ```bash
   POSTGRES_USER=transit
   POSTGRES_PASSWORD=change_me_secure_db_password
   POSTGRES_DB=transit
   REDIS_PASSWORD=change_me_secure_redis_password
   MINIO_ROOT_USER=minioadmin
   MINIO_ROOT_PASSWORD=change_me_secure_minio_password
   ```

3. **Start the Infrastructure:**
   ```bash
   cd infra/local
   docker compose up -d
   ```
   *(Wait ~60 seconds for Redpanda, MinIO, and Postgres to initialize).*

### 1.2 Start the Pipeline

The pipeline is orchestrated via a Bash script. Run it manually for the first time to seed the database and train the initial model:
   ```bash
   chmod +x scripts/run_batch_pipeline.sh
   ./scripts/run_batch_pipeline.sh
   ```

### 1.3 Configure Automation (Cron)
To ensure the pipeline runs nightly and cleans the disk:
   ```bash
   crontab -e
   ```
Add this line to the bottom of the file:
   ```text
   0 2 * * * cd /home/YOUR_USER/transitflow && ./scripts/run_batch_pipeline.sh >> /home/YOUR_USER/transitflow/pipeline.log 2>&1
   ```

---

## 2. Health Checks & Verification

Use these commands on the host VM to verify system health.

### 2.1 Container Status
   ```bash
   docker compose -f infra/local/docker-compose.yml ps
   ```

### 2.2 API & UI Health
   ```bash
   curl -s http://localhost:8001/health
   ```

### 2.3 Verify Data Landed in Lakehouse (MinIO)
   ```bash
   docker exec minio sh -c 'mc alias set myminio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD > /dev/null && mc ls -r myminio/transitflow-lakehouse/gold/'
   ```

### 2.4 Verify Data Landed in Database (Postgres)
   ```bash
   docker exec postgres psql -U transit -d transit -c "
   SELECT 'Bronze Enriched' as layer, count(*) FROM bronze.enriched UNION ALL
   SELECT 'Gold Stops', count(*) FROM public.fct_stop_arrivals UNION ALL
   SELECT 'Gold Daily', count(*) FROM public.fct_daily_performance;"
   ```

---

## 3. MLOps Commands

MLOps operations are executed inside the `serving-api` container where the MLflow environment is loaded.

### 3.1 Check for Data Drift
Manually run the Population Stability Index (PSI) calculation:
   ```bash
   docker exec -it serving-api python scripts/monitor_drift.py
   docker exec -it serving-api cat drift.json
   ```

### 3.2 Evaluate Retraining
Trigger the evaluation script. It will read `drift.json` and decide if retraining is necessary based on the PSI threshold.
   ```bash
   docker exec -it serving-api python mlops/retraining.py --drift-file drift.json
   ```

---

## 4. Troubleshooting Runbook

### Issue: UI Shows "DEGRADED" or "OFFLINE"
**Cause:** The FastAPI serving layer is unreachable or the model failed to load.
**Fix:** 1. Check the logs: `docker logs serving-api --tail 50`
2. Restart the API: `docker compose restart serving-api`

### Issue: Flink Stream Stops / UI Data Goes Stale
**Cause:** Apache Flink encountered a transient network error with Kafka (Redpanda) and dropped the job.
**Fix:**
*(Note: Flink is configured with a Fixed Delay Restart strategy to prevent this, but if the cluster fully crashes, manual intervention is needed).*
1. Ensure the JobManager is up: `docker ps | grep flink`
2. Resubmit the job manually:
   ```bash
   make flink-deploy
   ```

### Issue: `run_batch_pipeline.sh` Fails Overnight
**Cause:** Usually a Spark memory issue or MinIO connection error.
**Fix:** 1. Check the cron logs: `tail -n 50 pipeline.log`
2. Rerun the specific failing phase manually (e.g., `make spark-silver`) to view live stdout errors.

### Issue: VM Disk Space is Full (100% Usage)
**Cause:** MinIO cleanup failed, or Docker logs grew too large.
**Fix:**
1. Manually prune dangling Docker images:
   ```bash
   docker system prune -f
   ```
2. Manually trigger MinIO cleanup (Hard Delete):
   ```bash
   docker exec minio sh -c 'mc alias set myminio http://localhost:9000 $MINIO_ROOT_USER $MINIO_ROOT_PASSWORD > /dev/null && mc rm -r --force --older-than 1d myminio/transitflow-lakehouse/bronze/'
   ```