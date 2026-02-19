"""
ML Serving API.

Patterns:
- Circuit Breaker (Resiliency)
- Distributed Tracing (Observability)
- Shadow Deployment (Safe Release)
- Prometheus Metrics (Monitoring)

FastAPI application for delay predictions with:
- Circuit breaker protection for feature store
- OpenTelemetry distributed tracing
- Model caching with periodic refresh
- Shadow mode for validating Staging models against Production traffic
"""

import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, List

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response, FileResponse
from pydantic import BaseModel, Field
from prometheus_client import generate_latest
from fastapi.staticfiles import StaticFiles

# --- Project Imports ---
from feature_store.config import FeatureStoreConfig
from feature_store.feature_service import CombinedFeatures, FeatureService
from ml_pipeline.config import MLConfig
from ml_pipeline.registry import ModelRegistry
from ml_pipeline.training import DelayPredictor
from serving.circuit_breaker import CircuitBreaker, CircuitBreakerConfig, circuit_registry
from serving.tracing import get_tracer, init_tracer
from serving.shadow import ShadowPredictor
import monitoring.metrics as metrics

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Feature Flag for Shadow Mode (Default: False to save resources)
ENABLE_SHADOW = os.getenv("ENABLE_SHADOW", "false").lower() == "true"

@dataclass
class CachedModel:
    """Cached model with metadata."""
    predictor: DelayPredictor
    loaded_at: datetime
    version: str

class PredictionRequest(BaseModel):
    """Prediction request body."""
    vehicle_id: int
    stop_id: Optional[int] = None
    line_id: Optional[str] = None
    hour_of_day: Optional[int] = Field(None, ge=0, le=23)
    day_of_week: Optional[int] = Field(None, ge=0, le=6)

class PredictionResponse(BaseModel):
    """Prediction response."""
    vehicle_id: int
    route_id: Optional[str] = "Unknown"
    predicted_delay_seconds: float
    prediction_timestamp: int
    model_version: str
    features_used: Dict[str, Any]
    online_features_available: bool
    offline_features_available: bool
    latency_ms: float
    trace_id: Optional[str] = None
    shadow_mode: bool

class HealthResponse(BaseModel):
    """Health check response."""
    status: str
    model_loaded: bool
    model_version: Optional[str]
    shadow_enabled: bool
    feature_store: Dict[str, str]
    circuit_breakers: Dict[str, str]

class ActiveVehicle(BaseModel):
    vehicle_id: str
    line_id: Optional[str] = None
    last_updated: Optional[float] = None

# --- Global State ---
feature_config = FeatureStoreConfig.from_env()
ml_config = MLConfig.from_env()

feature_service: Optional[FeatureService] = None
cached_model: Optional[CachedModel] = None
shadow_predictor: Optional[ShadowPredictor] = None

online_circuit: Optional[CircuitBreaker] = None
offline_circuit: Optional[CircuitBreaker] = None

def load_production_model() -> Optional[CachedModel]:
    """Load primary production model. Falls back to Disk/Mock on failure."""
    # 1. Try MLflow Registry (Protected Block)
    try:
        registry = ModelRegistry(ml_config)
        raw_model = registry.load_latest_model()
        
        predictor = DelayPredictor(ml_config)
        predictor._model = raw_model
        
        logger.info("Successfully loaded Production model from MLflow")
        return CachedModel(
            predictor=predictor,
            loaded_at=datetime.now(timezone.utc),
            version="registry_latest",
        )
    except Exception as e:
        # CRITICAL FIX: Log warning but DO NOT CRASH
        logger.warning(f"MLflow load failed (Network/Service issue): {e}")

    # 2. Fallback to Disk
    model_path = os.environ.get("MODEL_PATH", "models/delay_predictor.pkl")
    if os.path.exists(model_path):
        try:
            predictor = DelayPredictor(ml_config)
            predictor.load(model_path)
            logger.info(f"Loaded fallback model from disk: {model_path}")
            return CachedModel(
                predictor=predictor,
                loaded_at=datetime.now(timezone.utc),
                version="local_disk",
            )
        except Exception:
            pass
            
    # 3. Mock (Last Resort)
    logger.warning("No model found. Using Mock Predictor (System Safe Mode).")
    predictor = DelayPredictor(ml_config)
    return CachedModel(
        predictor=predictor,
        loaded_at=datetime.now(timezone.utc),
        version="mock",
    )

def load_shadow_model() -> Optional[Any]:
    """Load candidate model. Returns None if MLflow is unreachable."""
    if not ENABLE_SHADOW:
        return None
        
    try:
        import mlflow
        model_name = ml_config.model_name
        model_uri = f"models:/{model_name}/Staging"
        
        # This will now timeout fast due to ENV vars
        shadow_model = mlflow.sklearn.load_model(model_uri)
        
        predictor = DelayPredictor(ml_config)
        predictor._model = shadow_model
        logger.info("Shadow Model loaded successfully.")
        return predictor
    except Exception as e:
        # Log warning but DO NOT CRASH
        logger.warning(f"Shadow Model unavailable: {e}. Continuing in Single-Model mode.")
        return None

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifecycle."""
    global feature_service, cached_model, shadow_predictor, online_circuit, offline_circuit

    # 1. Tracing
    init_tracer("transitflow-serving", os.environ.get("OTLP_ENDPOINT"))

    # 2. Circuit Breakers
    online_circuit = circuit_registry.get_or_create(
        "online_store", CircuitBreakerConfig(failure_threshold=5, timeout_seconds=30.0)
    )
    offline_circuit = circuit_registry.get_or_create(
        "offline_store", CircuitBreakerConfig(failure_threshold=5, timeout_seconds=30.0)
    )

    # 3. Feature Store Connection
    feature_service = FeatureService(feature_config)
    try:
        feature_service.connect()
        logger.info("Feature Service connected.")
    except Exception as e:
        logger.error(f"Feature Service connection failed: {e}")

    # 4. Model Loading (Production + Shadow)
    cached_model = load_production_model()
    shadow_model_instance = load_shadow_model()

    # 5. Initialize Shadow Predictor
    # This wrapper handles the async comparison logic
    shadow_predictor = ShadowPredictor(
        prod_model=cached_model.predictor if cached_model else None,
        shadow_model=shadow_model_instance,
        agreement_threshold=60.0 # 60 seconds tolerance
    )

    logger.info(f"Serving API Started (Shadow Mode: {shadow_predictor.enabled})")
    yield

    if feature_service:
        feature_service.close()
    logger.info("Serving API Stopped")


app = FastAPI(
    title="TransitFlow Prediction API",
    description="Real-time delay predictions with Shadow Mode",
    version="2.0.0",
    lifespan=lifespan,
)

# --- Middleware ---
@app.middleware("http")
async def add_trace_header(request: Request, call_next):
    response = await call_next(request)
    trace_id = get_tracer().get_current_trace_id()
    if trace_id:
        response.headers["X-Trace-ID"] = trace_id
    return response

# --- Helper Functions ---
def get_features_with_circuit_breaker(
    vehicle_id: int,
    stop_id: Optional[int],
    line_id: Optional[str],
    hour_of_day: Optional[int],
    day_of_week: Optional[int],
) -> CombinedFeatures:
    """Get features with circuit breaker protection and Smart Lookup."""
    tracer = get_tracer()
    combined = CombinedFeatures(
        vehicle_id=vehicle_id,
        request_timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
    )

    # 1. Online Features (Redis) - ALWAYS fetch first
    with tracer.span("get_online_features", {"vehicle_id": vehicle_id}):
        try:
            if online_circuit.call(lambda: feature_service._online_store.is_healthy(), lambda: False):
                online = feature_service._online_store.get_features(vehicle_id)
                if online:
                    combined.online = online
                    combined.online_available = True
        except Exception:
            pass # Circuit breaker handles logging

    # 2. Smart Lookup (The Logic Fix)
    # If the user didn't provide stop/line info, try to get it from the live Redis data
    target_stop_id = stop_id
    target_line_id = line_id

    if combined.online_available:
        # Auto-fill from Redis if User didn't provide them
        if not target_stop_id and combined.online.next_stop_id:
            target_stop_id = combined.online.next_stop_id
        if not target_line_id and combined.online.line_id:
            target_line_id = combined.online.line_id

    # 3. Offline Features (Postgres)
    # Now we query Postgres using either the User's ID or the Auto-filled ID
    if target_stop_id and target_line_id:
        with tracer.span("get_offline_features", {"stop_id": target_stop_id}):
            now = datetime.now(timezone.utc)
            h = hour_of_day if hour_of_day is not None else now.hour
            d = day_of_week if day_of_week is not None else now.weekday()
            
            try:
                if offline_circuit.call(lambda: feature_service._offline_store.is_healthy(), lambda: False):
                    # Ensure IDs are correct types for the query
                    offline = feature_service._offline_store.get_stop_features(
                        int(target_stop_id), 
                        str(target_line_id), 
                        h, 
                        d
                    )
                    if offline:
                        combined.offline = offline
                        combined.offline_available = True
            except Exception as e:
                logger.error(f"CRITICAL: Offline feature fetch crashed: {e}")

    return combined

# --- Endpoints ---

@app.get("/vehicles/active", response_model=List[ActiveVehicle])
async def list_active_vehicles():
    """Scans Redis for active vehicles."""
    results = []
    try:
        if feature_service and feature_service._online_store:
            client = feature_service._online_store._client
            # Scan Redis
            cursor, keys = client.scan(cursor=0, match="features:vehicle:*", count=50)
            
            for key in keys:
                # FIX: 'key' is already a string, do not decode it!
                if isinstance(key, bytes):
                    key_str = key.decode("utf-8")
                else:
                    key_str = str(key)
                
                vid = key_str.split(":")[-1]
                results.append(ActiveVehicle(vehicle_id=vid))
                
    except Exception as e:
        logger.error(f"Failed to scan active vehicles: {e}")
    return results

@app.get("/health", response_model=HealthResponse)
async def health_check():
    """Health check including Shadow Mode status."""
    circuit_states = {k: v.state.value for k, v in circuit_registry.get_all_stats().items()}
    
    fs_health = feature_service.health_check() if feature_service else {}
    
    return HealthResponse(
        status="healthy" if cached_model else "degraded",
        model_loaded=cached_model is not None,
        model_version=cached_model.version if cached_model else None,
        shadow_enabled=shadow_predictor.enabled if shadow_predictor else False,
        feature_store={
            "online": fs_health.get("online_store", "unknown"),
            "offline": fs_health.get("offline_store", "unknown")
        },
        circuit_breakers=circuit_states,
    )

@app.get("/metrics")
async def metrics_endpoint():
    """Expose Prometheus metrics."""
    return Response(content=generate_latest(), media_type="text/plain")

@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """
    Predict delay at next stop.
    Robustness: Falls back to Heuristics (Current Delay) if no model exists.
    """
    tracer = get_tracer()
    start_time = time.perf_counter()
    status = "success"

    try:
        # 1. Check Model Availability
        if not cached_model:
            raise HTTPException(status_code=503, detail="Model not loaded")

        # 2. Fetch Features
        with tracer.span("get_features"):
            combined = get_features_with_circuit_breaker(
                request.vehicle_id, request.stop_id, request.line_id,
                request.hour_of_day, request.day_of_week
            )
            
        # 3. Predict
        features_dict = combined.to_feature_vector()
        
        with tracer.span("model_inference"):
            # --- ROBUSTNESS FIX: Cold Start Handling ---
            if cached_model.version == "mock":
                # If we are on Cloud (Day 0) with no history, use the Live Delay.
                # This ensures the UI shows accurate "Current Status" instead of 0s.
                prediction = float(features_dict.get("current_delay", 0.0))
                logger.info(f"Heuristic Fallback for {request.vehicle_id}: {prediction}s")
            else:
                # We have a trained model (Day 1+), use ML.
                prediction = await shadow_predictor.predict(features_dict, str(request.vehicle_id))

        latency_ms = (time.perf_counter() - start_time) * 1000

        # 4. Record Standard Metrics
        metrics.PREDICTION_LATENCY.labels(
            model_version=cached_model.version, 
            status=status
        ).observe(latency_ms / 1000)
        
        metrics.PREDICTION_COUNT.labels(
            model_version=cached_model.version, 
            status=status
        ).inc()

        return PredictionResponse(
            vehicle_id=request.vehicle_id,
            predicted_delay_seconds=round(prediction, 1),
            route_id=combined.online.line_id if combined.online else None,
            prediction_timestamp=int(datetime.now(timezone.utc).timestamp() * 1000),
            model_version=cached_model.version,
            features_used=features_dict,
            online_features_available=combined.online_available,
            offline_features_available=combined.offline_available,
            latency_ms=round(latency_ms, 2),
            trace_id=tracer.get_current_trace_id(),
            shadow_mode=shadow_predictor.enabled
        )

    except Exception as e:
        status = "error"
        logger.error(f"Prediction failed: {e}")
        metrics.PREDICTION_COUNT.labels(
            model_version=cached_model.version if cached_model else "none", 
            status=status
        ).inc()
        raise HTTPException(status_code=500, detail=str(e))
    
# --- Shadow Management Endpoints ---

@app.post("/shadow/enable")
async def enable_shadow():
    """Runtime toggle: Enable Shadow Mode."""
    if not shadow_predictor.shadow_model:
        # Try to load it dynamically
        shadow_predictor.shadow_model = load_shadow_model()
        
    if shadow_predictor.shadow_model:
        shadow_predictor.enabled = True
        return {"status": "enabled", "msg": "Shadow mode activated"}
    return {"status": "failed", "msg": "Could not load shadow model"}

@app.post("/shadow/disable")
async def disable_shadow():
    """Runtime toggle: Disable Shadow Mode."""
    if shadow_predictor:
        shadow_predictor.enabled = False
    return {"status": "disabled"}


@app.get("/circuits")
async def get_circuits():
    """Expose rich circuit breaker stats for monitoring."""
    stats = {}
    for name, breaker in circuit_registry.get_all_stats().items():
        stats[name] = {
            "state": breaker.state.value,  # Script expects a dict with "state" key
            "failures": breaker.failure_count,
            "last_failure": str(breaker.last_failure_time) if breaker.last_failure_time else None
        }
    return stats

# Check if the UI directory exists
ui_path = os.path.join(os.getcwd(), "ui")
if os.path.exists(ui_path):
    # Mount the 'ui' directory at the root '/' path
    # We mount the directory itself, but handle the root index.html manually
    # to avoid conflicts with other API routes.
    app.mount("/static", StaticFiles(directory=ui_path), name="ui-static")

    @app.get("/")
    async def serve_ui():
        """Serve the main UI page."""
        index_file = os.path.join(ui_path, "index.html")
        if os.path.exists(index_file):
            return FileResponse(index_file)
        return {"error": "index.html not found"}
else:
    logger.warning(f"UI directory not found at {ui_path}. UI will not be served.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)