"""
ML Pipeline configuration.
Aligned with verified dbt schemas.
"""

import os
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class MLConfig:
    """Configuration for ML training and serving."""

    mlflow_tracking_uri: str
    mlflow_experiment_name: str
    model_name: str

    delta_lake_path: str
    training_table: str

    postgres_host: str
    postgres_db: str

    feature_columns: List[str]
    target_column: str

    test_size: float
    random_state: int

    xgb_n_estimators: int
    xgb_max_depth: int
    xgb_learning_rate: float

    model_cache_ttl_seconds: int

    @classmethod
    def from_env(cls) -> "MLConfig":
        """Load configuration from environment variables."""

        default_features = [
            "stop_id",
            "line_id",
            "hour_of_day",
            "day_of_week",
            "historical_arrival_count",
            "historical_avg_delay", 
            "avg_dwell_time_ms",
        ]

        features_env = os.environ.get("FEATURE_COLUMNS")
        feature_list = features_env.split(",") if features_env else default_features

        return cls(
            mlflow_tracking_uri=os.environ.get("MLFLOW_TRACKING_URI", "http://mlflow:5000"),
            mlflow_experiment_name=os.environ.get(
                "MLFLOW_EXPERIMENT_NAME", "transitflow-delay-prediction"
            ),
            model_name=os.environ.get("MODEL_NAME", "delay-predictor"),
            delta_lake_path=os.environ.get("DELTA_LAKE_PATH", "s3a://transitflow-lakehouse"),
            # Matches the Gold table
            # This matches your dbt project structure and Postgres syntax.
            training_table=os.environ.get("TRAINING_TABLE", "marts.fct_stop_arrivals"),
            postgres_host=os.environ.get("POSTGRES_HOST", "postgres"),
            postgres_db=os.environ.get("POSTGRES_DB", "transit"),
            feature_columns=feature_list,
            # We treat the historical average as the target for this specific regression task
            # (predicting the expected delay for this time slot)
            target_column=os.environ.get("TARGET_COLUMN", "historical_avg_delay"),
            test_size=float(os.environ.get("TEST_SIZE", "0.2")),
            random_state=int(os.environ.get("RANDOM_STATE", "42")),
            xgb_n_estimators=int(os.environ.get("XGB_N_ESTIMATORS", "100")),
            xgb_max_depth=int(os.environ.get("XGB_MAX_DEPTH", "6")),
            xgb_learning_rate=float(os.environ.get("XGB_LEARNING_RATE", "0.1")),
            model_cache_ttl_seconds=int(os.environ.get("MODEL_CACHE_TTL_SECONDS", "300")),
        )

    @classmethod
    def default(cls) -> "MLConfig":
        """Default configuration for local development."""
        return cls.from_env()