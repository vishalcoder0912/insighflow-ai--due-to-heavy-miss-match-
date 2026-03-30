"""ML Engine - Safe machine learning models."""

from __future__ import annotations

import logging
from typing import Any
from enum import Enum

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

try:
    from sklearn.linear_model import (
        LinearRegression,
        Ridge,
        Lasso,
        ElasticNet,
        LogisticRegression,
    )
    from sklearn.ensemble import (
        RandomForestClassifier,
        RandomForestRegressor,
        GradientBoostingClassifier,
        GradientBoostingRegressor,
    )
    from sklearn.cluster import KMeans, DBSCAN
    from sklearn.preprocessing import StandardScaler, LabelEncoder
    from sklearn.model_selection import train_test_split
    from sklearn.metrics import (
        accuracy_score,
        precision_score,
        recall_score,
        f1_score,
        r2_score,
        mean_squared_error,
        mean_absolute_error,
    )
    from sklearn.inspection import permutation_importance
except ImportError:
    logger.warning("scikit-learn not installed")

MAX_ROWS_FOR_ML = 100000
MAX_FEATURES = 100
MAX_CLUSTERS = 10
TRAIN_TIMEOUT = 300


class ModelType(str, Enum):
    """ML model types."""

    LINEAR_REGRESSION = "linear_regression"
    RIDGE = "ridge"
    LASSO = "lasso"
    ELASTICNET = "elasticnet"
    LOGISTIC_REGRESSION = "logistic_regression"
    RANDOM_FOREST = "random_forest"
    GRADIENT_BOOSTING = "gradient_boosting"
    KMEANS = "kmeans"
    DBSCAN = "dbscan"


class MLEngine:
    """Safe machine learning engine."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data

        self.model = None
        self.scaler = None
        self.feature_names = []
        self.is_fitted = False
        self.model_type = None

    def _prepare_data(
        self,
        target_column: str,
        feature_columns: list[str] | None = None,
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Prepare data for training."""
        if feature_columns is None:
            feature_columns = [c for c in self.df.columns if c != target_column]
            feature_columns = feature_columns[:MAX_FEATURES]

        X = self.df[feature_columns].copy()
        y = self.df[target_column].copy()

        X = X.apply(pd.to_numeric, errors="coerce")
        X = X.fillna(X.median())

        if y.dtype == "object":
            le = LabelEncoder()
            y = le.fit_transform(y.astype(str))

        return X, y

    def _sample_if_needed(
        self, X: pd.DataFrame, y: pd.Series
    ) -> tuple[pd.DataFrame, pd.Series]:
        """Sample data if too large."""
        if len(X) > MAX_ROWS_FOR_ML:
            sample_idx = np.random.choice(len(X), MAX_ROWS_FOR_ML, replace=False)
            X = X.iloc[sample_idx]
            y = y.iloc[sample_idx]
            logger.info(f"Sampled {MAX_ROWS_FOR_ML} rows for ML training")
        return X, y

    def _scale_features(self, X: pd.DataFrame) -> np.ndarray:
        """Scale features."""
        self.scaler = StandardScaler()
        return self.scaler.fit_transform(X)

    def train_regression(
        self,
        target_column: str,
        model_type: str = ModelType.LINEAR_REGRESSION.value,
        feature_columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Train a regression model."""
        try:
            X, y = self._prepare_data(target_column, feature_columns)
            X, y = self._sample_if_needed(X, y)

            X_scaled = self._scale_features(X)
            X_train, X_test, y_train, y_test = train_test_split(
                X_scaled, y, test_size=0.2, random_state=42
            )

            if model_type == ModelType.LINEAR_REGRESSION.value:
                self.model = LinearRegression()
            elif model_type == ModelType.RIDGE.value:
                self.model = Ridge(alpha=1.0)
            elif model_type == ModelType.LASSO.value:
                self.model = Lasso(alpha=1.0)
            elif model_type == ModelType.ELASTICNET.value:
                self.model = ElasticNet(alpha=1.0, l1_ratio=0.5)
            else:
                self.model = RandomForestRegressor(
                    n_estimators=50, max_depth=10, random_state=42
                )

            self.model.fit(X_train, y_train)
            self.is_fitted = True
            self.model_type = model_type
            self.feature_names = list(X.columns)

            y_pred = self.model.predict(X_test)

            return {
                "status": "success",
                "model_type": model_type,
                "target_column": target_column,
                "feature_columns": list(X.columns),
                "metrics": {
                    "r2_score": round(r2_score(y_test, y_pred), 4),
                    "rmse": round(np.sqrt(mean_squared_error(y_test, y_pred)), 4),
                    "mae": round(mean_absolute_error(y_test, y_pred), 4),
                },
                "training_samples": len(X_train),
                "test_samples": len(X_test),
            }

        except Exception as e:
            logger.error(f"Regression training failed: {e}")
            return {
                "status": "error",
                "message": str(e),
                "model_type": model_type,
            }

    def train_classification(
        self,
        target_column: str,
        model_type: str = ModelType.LOGISTIC_REGRESSION.value,
        feature_columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Train a classification model."""
        try:
            X, y = self._prepare_data(target_column, feature_columns)
            X, y = self._sample_if_needed(X, y)

            X_scaled = self._scale_features(X)
            X_train, X_test, y_train, y_test = train_test_split(
                X_scaled, y, test_size=0.2, random_state=42
            )

            if model_type == ModelType.LOGISTIC_REGRESSION.value:
                self.model = LogisticRegression(max_iter=1000)
            elif model_type == ModelType.RANDOM_FOREST.value:
                self.model = RandomForestClassifier(
                    n_estimators=50, max_depth=10, random_state=42
                )
            elif model_type == ModelType.GRADIENT_BOOSTING.value:
                self.model = GradientBoostingClassifier(
                    n_estimators=50, max_depth=5, random_state=42
                )
            else:
                self.model = RandomForestClassifier(
                    n_estimators=50, max_depth=10, random_state=42
                )

            self.model.fit(X_train, y_train)
            self.is_fitted = True
            self.model_type = model_type
            self.feature_names = list(X.columns)

            y_pred = self.model.predict(X_test)

            return {
                "status": "success",
                "model_type": model_type,
                "target_column": target_column,
                "feature_columns": list(X.columns),
                "metrics": {
                    "accuracy": round(accuracy_score(y_test, y_pred), 4),
                    "precision": round(
                        precision_score(
                            y_test, y_pred, average="weighted", zero_division=0
                        ),
                        4,
                    ),
                    "recall": round(
                        recall_score(
                            y_test, y_pred, average="weighted", zero_division=0
                        ),
                        4,
                    ),
                    "f1": round(
                        f1_score(y_test, y_pred, average="weighted", zero_division=0), 4
                    ),
                },
                "training_samples": len(X_train),
                "test_samples": len(X_test),
                "classes": list(self.model.classes_),
            }

        except Exception as e:
            logger.error(f"Classification training failed: {e}")
            return {
                "status": "error",
                "message": str(e),
                "model_type": model_type,
            }

    def train_clustering(
        self,
        n_clusters: int = 3,
        feature_columns: list[str] | None = None,
        algorithm: str = ModelType.KMEANS.value,
    ) -> dict[str, Any]:
        """Train a clustering model."""
        try:
            if feature_columns is None:
                feature_columns = list(
                    self.df.select_dtypes(include=[np.number]).columns[:MAX_FEATURES]
                )

            X = self.df[feature_columns].copy()
            X = X.apply(pd.to_numeric, errors="coerce")
            X = X.fillna(X.median())
            X, _ = self._sample_if_needed(X, pd.Series([0] * len(X)))

            n_clusters = min(n_clusters, MAX_CLUSTERS, len(X))

            X_scaled = self._scale_features(X)

            if algorithm == ModelType.KMEANS.value:
                self.model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                labels = self.model.fit_predict(X_scaled)
            elif algorithm == ModelType.DBSCAN.value:
                self.model = DBSCAN(eps=0.5, min_samples=5)
                labels = self.model.fit_predict(X_scaled)
                n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
            else:
                self.model = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
                labels = self.model.fit_predict(X_scaled)

            self.is_fitted = True
            self.model_type = algorithm
            self.feature_names = list(X.columns)

            cluster_sizes = {}
            for label in np.unique(labels):
                cluster_sizes[f"cluster_{label}"] = int((labels == label).sum())

            return {
                "status": "success",
                "algorithm": algorithm,
                "n_clusters": int(n_clusters),
                "feature_columns": list(X.columns),
                "cluster_sizes": cluster_sizes,
                "labels": [int(l) for l in labels[:1000]],
            }

        except Exception as e:
            logger.error(f"Clustering failed: {e}")
            return {
                "status": "error",
                "message": str(e),
            }

    def get_feature_importance(self) -> dict[str, Any]:
        """Get feature importance (for tree-based models)."""
        if not self.is_fitted:
            return {"status": "error", "message": "Model not trained"}

        try:
            if hasattr(self.model, "feature_importances_"):
                importances = self.model.feature_importances_
            elif hasattr(self.model, "coef_"):
                importances = np.abs(self.model.coef_)
            else:
                return {
                    "status": "error",
                    "message": "Model does not support feature importance",
                }

            importance_df = pd.DataFrame(
                {
                    "feature": self.feature_names,
                    "importance": importances,
                }
            ).sort_values("importance", ascending=False)

            return {
                "status": "success",
                "feature_importance": importance_df.to_dict("records"),
            }

        except Exception as e:
            logger.error(f"Feature importance calculation failed: {e}")
            return {"status": "error", "message": str(e)}

    def predict(self, data: list[dict[str, Any]] | pd.DataFrame) -> dict[str, Any]:
        """Make predictions."""
        if not self.is_fitted:
            return {"status": "error", "message": "Model not trained"}

        try:
            if isinstance(data, list):
                X = pd.DataFrame(data)
            else:
                X = data

            X = X[self.feature_names].copy()
            X = X.apply(pd.to_numeric, errors="coerce")
            X = X.fillna(X.median() if X.median().notna().any() else 0)

            if self.scaler:
                X_scaled = self.scaler.transform(X)
            else:
                X_scaled = X

            predictions = self.model.predict(X_scaled)

            return {
                "status": "success",
                "predictions": [float(p) for p in predictions[:1000]],
                "count": len(predictions),
            }

        except Exception as e:
            logger.error(f"Prediction failed: {e}")
            return {"status": "error", "message": str(e)}


def train_regression_model(
    data: list[dict[str, Any]] | pd.DataFrame,
    target_column: str,
    model_type: str = "linear_regression",
    feature_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Convenience function for regression."""
    engine = MLEngine(data)
    return engine.train_regression(target_column, model_type, feature_columns)


def train_classification_model(
    data: list[dict[str, Any]] | pd.DataFrame,
    target_column: str,
    model_type: str = "logistic_regression",
    feature_columns: list[str] | None = None,
) -> dict[str, Any]:
    """Convenience function for classification."""
    engine = MLEngine(data)
    return engine.train_classification(target_column, model_type, feature_columns)


def train_clustering_model(
    data: list[dict[str, Any]] | pd.DataFrame,
    n_clusters: int = 3,
    feature_columns: list[str] | None = None,
    algorithm: str = "kmeans",
) -> dict[str, Any]:
    """Convenience function for clustering."""
    engine = MLEngine(data)
    return engine.train_clustering(n_clusters, feature_columns, algorithm)
