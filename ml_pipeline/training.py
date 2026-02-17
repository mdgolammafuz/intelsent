"""
ML Training Pipeline.

Pattern: ML Reproducibility
"""

import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import structlog
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split

from ml_pipeline.config import MLConfig
from ml_pipeline.registry import ModelRegistry

logger = structlog.get_logger(__name__)

try:
    import xgboost as xgb

    XGB_AVAILABLE = True
except ImportError:
    XGB_AVAILABLE = False
    logger.warning("xgboost_not_installed", action="using_mock_predictor")


@dataclass
class TrainingResult:
    """Detailed results of a training run."""

    model_version: str
    train_mae: float
    test_mae: float
    train_rmse: float
    test_rmse: float
    r2_score: float
    feature_importance: Dict[str, float]
    training_samples: int
    test_samples: int
    training_duration_seconds: float
    delta_version: Optional[int] = None


class MockPredictor:
    """Fallback predictor for environments without XGBoost."""

    def __init__(self):
        self._mean = 0.0
        self.is_trained = True

    def fit(self, X: np.ndarray, y: np.ndarray):
        self._mean = float(np.mean(y)) if len(y) > 0 else 0.0

    def predict(self, X: np.ndarray) -> np.ndarray:
        return np.full(len(X), self._mean)


class DelayPredictor:
    """Wrapper for XGBoost model with training and inference logic."""

    def __init__(self, config: MLConfig):
        self._config = config
        self._model: Optional[Any] = None
        self._feature_columns: List[str] = config.feature_columns
        self._registry = ModelRegistry(config)
        self._trained_at: Optional[datetime] = None

    @property
    def is_trained(self) -> bool:
        """
        Checks if the model is loaded and ready for inference.
        Supports both raw XGBoost models and MLflow PyFunc wrappers.
        """
        if self._model is None:
            return False
            
        # 1. Handle Raw XGBoost Model (Strict Check)
        # If it has get_booster, it's a raw model, so we verify it's actually fitted.
        if hasattr(self._model, "get_booster"):
            try:
                self._model.get_booster()
                return True
            except Exception:
                return False
                
        # 2. Handle MLflow Wrapper / Mock / Sklearn
        # If it doesn't have get_booster but has .predict, it's a loaded generic model.
        # Since we load these from disk/registry, they are inherently "trained".
        return True

    def _prepare_data(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.Series, List[str]]:
        """Validates columns and filters features based on what is available in the DF."""
        target = self._config.target_column

        if target not in df.columns:
            logger.error("target_missing", target=target, available=df.columns.tolist())
            raise ValueError(f"Target column {target} missing from data")

        available_features = [col for col in self._feature_columns if col in df.columns]

        if not available_features:
            logger.error(
                "no_features_available",
                expected=self._feature_columns,
                available=df.columns.tolist(),
            )
            raise ValueError("None of the configured feature columns were found in the dataset")

        X = df[available_features].copy()
        for col in X.columns:
            # Raise error on invalid numeric data instead of silent 0.0
            try:
                X[col] = pd.to_numeric(X[col], errors="raise")
            except ValueError as e:
                logger.error("data_type_mismatch", column=col, error=str(e))
                raise ValueError(f"Column {col} contains non-numeric data that cannot be processed.") from e
            
            # Only fill genuine NaNs (missing values), not bad types
            X[col] = X[col].fillna(0)

        # Validate Target Variable
        try:
            y = pd.to_numeric(df[target], errors="raise").fillna(0)
        except ValueError as e:
            logger.error("target_type_mismatch", column=target, error=str(e))
            raise ValueError(f"Target {target} contains non-numeric data.") from e

        return X, y, available_features

    def train(self, df: pd.DataFrame) -> TrainingResult:
        """Train the model using a Pandas DataFrame."""
        start_time = time.time()

        X_df, y_series, actual_features = self._prepare_data(df)
        self._feature_columns = actual_features

        X = X_df.values
        y = y_series.values

        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=self._config.test_size,
            random_state=self._config.random_state,
        )

        if XGB_AVAILABLE:
            params = {
                "n_estimators": self._config.xgb_n_estimators,
                "max_depth": self._config.xgb_max_depth,
                "learning_rate": self._config.xgb_learning_rate,
                "random_state": self._config.random_state,
                "n_jobs": -1,
            }
            self._model = xgb.XGBRegressor(**params)
            self._model.fit(X_train, y_train)

            train_pred = self._model.predict(X_train)
            test_pred = self._model.predict(X_test)

            importance = dict(
                zip(
                    actual_features,
                    self._model.feature_importances_.tolist(),
                )
            )
        else:
            self._model = MockPredictor()
            self._model.fit(X_train, y_train)
            train_pred = self._model.predict(X_train)
            test_pred = self._model.predict(X_test)
            params = {"model": "mock"}
            importance = {col: 0.0 for col in actual_features}

        self._trained_at = datetime.utcnow()

        result = TrainingResult(
            model_version=self._trained_at.strftime("%Y%m%d_%H%M%S"),
            train_mae=float(mean_absolute_error(y_train, train_pred)),
            test_mae=float(mean_absolute_error(y_test, test_pred)),
            train_rmse=float(np.sqrt(mean_squared_error(y_train, train_pred))),
            test_rmse=float(np.sqrt(mean_squared_error(y_test, test_pred))),
            r2_score=float(r2_score(y_test, test_pred)),
            feature_importance=importance,
            training_samples=len(X_train),
            test_samples=len(X_test),
            training_duration_seconds=time.time() - start_time,
        )

        metrics = {
            "test_mae": result.test_mae,
            "test_rmse": result.test_rmse,
            "r2_score": result.r2_score,
        }

        self._registry.log_model(
            model=self._model,
            metrics=metrics,
            params=params if XGB_AVAILABLE else {"model_type": "mock"},
            X_sample=X_train[:5],
            y_sample=train_pred[:5],
        )

        return result
    
    def predict(self, features: Any) -> float:
        """
        Universal predict method. 
        Handles:
        1. Dictionary (Single API Request) -> Converts to DF -> Predicts
        2. DataFrame/Array (Shadow Mode/MLflow) -> Predicts directly
        """
        if not self.is_trained:
            logger.warning("predict_called_on_untrained_model")
            return 0.0

        # --- CASE 1: API Request (Dictionary) ---
        if isinstance(features, dict):
            # Convert single dict to DataFrame [1 row]
            data = {}
            for col in self._feature_columns:
                val = features.get(col, 0)
                data[col] = [val]
            
            df = pd.DataFrame(data)
            
            # Ensure numeric types
            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col], errors='raise')
                except ValueError as e:
                    logger.error("data_type_error", column=col, error=str(e))
                    raise e
            
            try:
                if hasattr(self._model, "predict"):
                    pred = self._model.predict(df)
                    if isinstance(pred, (np.ndarray, list)):
                        return float(pred[0])
                    return float(pred)
                elif isinstance(self._model, MockPredictor):
                    pred = self._model.predict(df.values)
                    return float(pred[0])
            except Exception as e:
                logger.error("prediction_execution_failed", error=str(e))
                raise e

        # --- CASE 2: ML Tooling (DataFrame / Numpy Array) ---
        else:
            try:
                pred = self._model.predict(features)
                if isinstance(pred, (np.ndarray, list)):
                    if len(pred) == 0: return 0.0
                    return float(pred[0])
                return float(pred)
            except Exception as e:
                logger.error("batch_prediction_failed", error=str(e))
                raise e
            
        return 0.0

def train_model(
    config: MLConfig, df: pd.DataFrame, delta_version: int = 0
) -> Tuple[DelayPredictor, TrainingResult]:
    predictor = DelayPredictor(config)
    result = predictor.train(df)
    result.delta_version = delta_version
    return predictor, result