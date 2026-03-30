"""Regression analysis service."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.impute import SimpleImputer
from sklearn.linear_model import ElasticNet, LinearRegression, Ridge
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

from app.services.monitoring import (
    timed_operation,
    log_analysis_start,
    log_analysis_complete,
)
from app.services.validation import (
    PreparedDataset,
    evaluate_regression_residuals,
    prepare_analysis_dataset,
)
from app.core.exceptions import NoVarianceError


def _expand_datetime_features(
    frame: pd.DataFrame, datetime_columns: list[str]
) -> pd.DataFrame:
    working = frame.copy()
    for column in datetime_columns:
        timestamps = pd.to_datetime(working[column], errors="coerce")
        working[f"{column}_year"] = timestamps.dt.year
        working[f"{column}_month"] = timestamps.dt.month
        working[f"{column}_dayofweek"] = timestamps.dt.dayofweek
        working[f"{column}_days_since_start"] = (timestamps - timestamps.min()).dt.days
        working = working.drop(columns=[column])
    return working


@timed_operation("advanced_regression", target_ms=30000)
def run_regression(
    df: pd.DataFrame,
    *,
    dataset_id: str | int | None = None,
    options: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Fit a predictive regression model with diagnostics."""

    options = options or {}
    prepared: PreparedDataset = prepare_analysis_dataset(
        df,
        analysis_type="regression",
        dataset_id=dataset_id,
        options=options,
        correlation_id=correlation_id,
    )
    plan = prepared.plan
    target_column = plan["target_column"]
    feature_columns = plan["feature_columns"]
    frame = prepared.dataframe[[target_column] + feature_columns].copy()
    datetime_columns = [
        column
        for column in feature_columns
        if plan["expected_types"].get(column) == "datetime"
    ]
    frame = _expand_datetime_features(frame, datetime_columns)

    target = pd.to_numeric(frame[target_column], errors="coerce")
    features = frame.drop(columns=[target_column])
    numeric_features = features.select_dtypes(include=[np.number]).columns.tolist()
    categorical_features = [
        column for column in features.columns if column not in numeric_features
    ]

    preprocessor = ColumnTransformer(
        transformers=[
            (
                "numeric",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="median")),
                        ("scaler", StandardScaler()),
                    ]
                ),
                numeric_features,
            ),
            (
                "categorical",
                Pipeline(
                    steps=[
                        ("imputer", SimpleImputer(strategy="most_frequent")),
                        ("encoder", OneHotEncoder(handle_unknown="ignore")),
                    ]
                ),
                categorical_features,
            ),
        ],
    )

    x_train, x_test, y_train, y_test = train_test_split(
        features, target, test_size=0.2, random_state=42
    )
    model_candidates: list[tuple[str, Any]] = [
        (
            "elastic_net",
            ElasticNet(alpha=0.05, l1_ratio=0.3, random_state=42, max_iter=5000),
        ),
        ("ridge", Ridge(alpha=1.0)),
        ("linear_regression", LinearRegression()),
    ]
    warnings = list(prepared.warnings)
    selected_name = "linear_regression"
    selected_pipeline: Pipeline | None = None

    for name, estimator in model_candidates:
        try:
            pipeline = Pipeline(
                steps=[("preprocessor", preprocessor), ("model", estimator)]
            )
            pipeline.fit(x_train, y_train)
            selected_name = name
            selected_pipeline = pipeline
            break
        except Exception as exc:  # pragma: no cover
            warnings.append(f"{name} training failed: {exc}")

    if selected_pipeline is None:  # pragma: no cover
        selected_pipeline = Pipeline(
            steps=[("preprocessor", preprocessor), ("model", LinearRegression())]
        )
        selected_pipeline.fit(x_train, y_train)

    predictions = selected_pipeline.predict(x_test)
    residuals = np.asarray(y_test) - np.asarray(predictions)
    transformed = selected_pipeline.named_steps["preprocessor"].transform(features)
    if hasattr(transformed, "toarray"):
        transformed = transformed.toarray()
    transformed = np.asarray(transformed)

    feature_names = list(
        selected_pipeline.named_steps["preprocessor"].get_feature_names_out()
    )
    coefficients = selected_pipeline.named_steps["model"].coef_
    feature_importance = sorted(
        [
            {
                "feature": feature_names[idx],
                "coefficient": round(float(coefficients[idx]), 4),
                "magnitude": round(abs(float(coefficients[idx])), 4),
            }
            for idx in range(min(len(feature_names), len(coefficients)))
        ],
        key=lambda item: item["magnitude"],
        reverse=True,
    )[:10]

    assumptions = evaluate_regression_residuals(residuals, transformed)
    return {
        "status": "SUCCESS",
        "confidence": "HIGH"
        if r2_score(y_test, predictions) >= 0.6
        else ("MEDIUM" if r2_score(y_test, predictions) >= 0.3 else "LOW"),
        "analysis_type": "regression",
        "processed_rows": int(len(prepared.dataframe)),
        "total_rows": int(len(df)),
        "excluded_rows": prepared.excluded_rows,
        "exclusion_reasons": {"preprocessing": prepared.excluded_rows},
        "quality_score": prepared.validation["quality_metrics"]["overall_score"],
        "validation": prepared.validation,
        "missing_values_analysis": prepared.missing_values_analysis,
        "results": {
            "algorithm": selected_name,
            "target_column": target_column,
            "feature_count": int(features.shape[1]),
            "metrics": {
                "r2": round(float(r2_score(y_test, predictions)), 4),
                "mae": round(float(mean_absolute_error(y_test, predictions)), 4),
                "rmse": round(
                    float(np.sqrt(mean_squared_error(y_test, predictions))), 4
                ),
            },
            "feature_importance": feature_importance,
            "assumptions": assumptions,
        },
        "warnings": warnings,
    }


class RegressionEngine:
    """Regression analysis engine"""

    def __init__(self, df: pd.DataFrame, dataset_id: str):
        self.df = df.copy()
        self.dataset_id = dataset_id
        self.scaler = StandardScaler()
        self.best_model = None
        self.best_model_name = None

    @timed_operation("Regression Analysis")
    def regress(
        self, target_col: str, feature_cols: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """Perform regression analysis"""

        log_analysis_start(
            self.dataset_id, "regression", len(self.df), len(self.df.columns)
        )

        y = self.df[target_col].values

        if np.std(y) == 0:
            raise NoVarianceError(target_col)

        if feature_cols is None:
            feature_cols = [
                col
                for col in self.df.columns
                if col != target_col and pd.api.types.is_numeric_dtype(self.df[col])
            ]

        X = self.df[feature_cols].values

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )

        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        models = {
            "linear": LinearRegression(),
            "ridge": Ridge(alpha=1.0),
            "elastic_net": ElasticNet(alpha=1.0, l1_ratio=0.5),
        }

        results = {}
        for name, model in models.items():
            try:
                model.fit(X_train_scaled, y_train)

                y_pred_train = model.predict(X_train_scaled)
                y_pred_test = model.predict(X_test_scaled)

                train_r2 = r2_score(y_train, y_pred_train)
                test_r2 = r2_score(y_test, y_pred_test)
                rmse = np.sqrt(mean_squared_error(y_test, y_pred_test))

                results[name] = {
                    "model": model,
                    "train_r2": train_r2,
                    "test_r2": test_r2,
                    "rmse": rmse,
                    "y_pred": y_pred_test,
                }
            except Exception as e:
                pass

        if not results:
            from app.services.error_handling import ModelTrainingError

            raise ModelTrainingError(
                message="All regression models failed",
                error_code="MOD_001",
                severity="MEDIUM",
            )

        best_result = max(results.items(), key=lambda x: x[1]["test_r2"])
        self.best_model_name = best_result[0]
        self.best_model = best_result[1]["model"]

        feature_importance = self._extract_feature_importance(
            self.best_model, feature_cols, best_result[1]["test_r2"]
        )

        log_analysis_complete(
            self.dataset_id,
            "regression",
            0,
            self.best_model_name,
            {"r2": best_result[1]["test_r2"], "rmse": best_result[1]["rmse"]},
        )

        return {
            "status": "SUCCESS",
            "target": target_col,
            "model_type": self.best_model_name,
            "r2_score": best_result[1]["test_r2"],
            "rmse": best_result[1]["rmse"],
            "features": feature_importance,
            "overfitting_detected": (
                best_result[1]["train_r2"] - best_result[1]["test_r2"]
            )
            > 0.15,
        }

    def _extract_feature_importance(
        self, model, feature_cols: List[str], r2_score: float
    ) -> List[Dict[str, Any]]:
        """Extract and rank feature importance"""

        coefficients = model.coef_

        abs_coef = np.abs(coefficients)
        coef_sum = np.sum(abs_coef)
        importance_scores = (
            (abs_coef / coef_sum).tolist() if coef_sum > 0 else [0] * len(coefficients)
        )

        features = []
        for col, coef, importance in zip(feature_cols, coefficients, importance_scores):
            features.append(
                {
                    "feature": col,
                    "coefficient": float(coef),
                    "importance": float(importance),
                    "interpretation": f"{col} impact: ${coef:.2f} per unit",
                }
            )

        features = sorted(features, key=lambda x: x["importance"], reverse=True)

        return features
