"""Pipeline Engine - Auto-configuration and execution."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Callable

import pandas as pd

logger = logging.getLogger(__name__)


class PipelineStage(str, Enum):
    """Pipeline stages."""

    VALIDATION = "validation"
    CLEANING = "cleaning"
    TRANSFORMATION = "transformation"
    FEATURE_ENGINEERING = "feature_engineering"
    ANALYSIS_PREPARATION = "analysis_preparation"


class PipelineStatus(str, Enum):
    """Pipeline execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class PipelineStageResult:
    """Result of a pipeline stage."""

    def __init__(
        self,
        stage: PipelineStage,
        status: PipelineStatus,
        output: Any = None,
        error: str | None = None,
        duration_ms: float = 0,
    ):
        self.stage = stage
        self.status = status
        self.output = output
        self.error = error
        self.duration_ms = duration_ms
        self.timestamp = datetime.utcnow()

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage.value,
            "status": self.status.value,
            "output": self.output,
            "error": self.error,
            "duration_ms": self.duration_ms,
            "timestamp": self.timestamp.isoformat(),
        }


class Pipeline:
    """Data processing pipeline."""

    def __init__(
        self,
        data: list[dict[str, Any]] | pd.DataFrame,
        config: dict[str, Any] | None = None,
    ):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data.copy()

        self.config = config or {}
        self.pipeline_id = str(uuid.uuid4())
        self.results: list[PipelineStageResult] = []
        self.status = PipelineStatus.PENDING

    def _log(self, message: str, level: str = "info") -> None:
        """Log pipeline message."""
        log_msg = f"[Pipeline {self.pipeline_id[:8]}] {message}"
        getattr(logger, level)(log_msg)

    def validate(self) -> PipelineStageResult:
        """Validation stage."""
        start = datetime.utcnow()
        try:
            errors = []

            if self.df.empty:
                errors.append("Dataset is empty")

            if self.df.columns.empty:
                errors.append("No columns found")

            null_ratio = self.df.isna().mean().mean()
            if null_ratio > 0.5:
                errors.append(f"High null ratio: {null_ratio:.2%}")

            duplicate_rows = self.df.duplicated().sum()
            if duplicate_rows > 0:
                errors.append(f"Found {duplicate_rows} duplicate rows")

            result = PipelineStageResult(
                stage=PipelineStage.VALIDATION,
                status=PipelineStatus.COMPLETED,
                output={
                    "valid": len(errors) == 0,
                    "errors": errors,
                    "row_count": len(self.df),
                    "column_count": len(self.df.columns),
                    "duplicate_rows": int(duplicate_rows),
                    "null_ratio": round(null_ratio, 4),
                },
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

            self._log(f"Validation: {'passed' if result.output['valid'] else 'failed'}")
            return result

        except Exception as e:
            self._log(f"Validation error: {e}", "error")
            return PipelineStageResult(
                stage=PipelineStage.VALIDATION,
                status=PipelineStatus.FAILED,
                error=str(e),
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

    def clean(self) -> PipelineStageResult:
        """Cleaning stage."""
        start = datetime.utcnow()
        try:
            original_nulls = self.df.isna().sum().sum()

            for col in self.df.columns:
                if self.df[col].dtype == "object":
                    null_patterns = [
                        "",
                        "NULL",
                        "N/A",
                        "NA",
                        "none",
                        "None",
                        "NaN",
                        "null",
                        "nan",
                    ]
                    self.df[col] = self.df[col].replace(null_patterns, None)

                self.df[col] = self.df[col].replace(["", " "], None)

            self.df = self.df.drop_duplicates()

            cleaned_nulls = self.df.isna().sum().sum()
            rows_dropped = original_nulls - cleaned_nulls

            result = PipelineStageResult(
                stage=PipelineStage.CLEANING,
                status=PipelineStatus.COMPLETED,
                output={
                    "nulls_removed": int(original_nulls - cleaned_nulls),
                    "duplicates_removed": int(self.df.duplicated().sum()),
                    "current_row_count": len(self.df),
                },
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

            self._log(f"Cleaning: removed {rows_dropped} nulls")
            return result

        except Exception as e:
            self._log(f"Cleaning error: {e}", "error")
            return PipelineStageResult(
                stage=PipelineStage.CLEANING,
                status=PipelineStatus.FAILED,
                error=str(e),
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

    def transform(self) -> PipelineStageResult:
        """Transformation stage."""
        start = datetime.utcnow()
        try:
            transformations = []

            for col in self.df.columns:
                if self.df[col].dtype == "object":
                    self.df[col] = self.df[col].astype(str).str.strip()
                    transformations.append(f"trimmed: {col}")

                if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                    self.df[col] = pd.to_datetime(self.df[col], errors="coerce")
                    transformations.append(f"datetime: {col}")

                numeric = pd.to_numeric(self.df[col], errors="coerce")
                if numeric.notna().mean() > 0.8:
                    self.df[col] = numeric
                    transformations.append(f"numeric: {col}")

            result = PipelineStageResult(
                stage=PipelineStage.TRANSFORMATION,
                status=PipelineStatus.COMPLETED,
                output={
                    "transformations_applied": transformations,
                    "columns_transformed": len(transformations),
                },
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

            self._log(f"Transformation: {len(transformations)} columns")
            return result

        except Exception as e:
            self._log(f"Transformation error: {e}", "error")
            return PipelineStageResult(
                stage=PipelineStage.TRANSFORMATION,
                status=PipelineStatus.FAILED,
                error=str(e),
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

    def feature_engineering(self) -> PipelineStageResult:
        """Feature engineering stage."""
        start = datetime.utcnow()
        try:
            features_created = []

            for col in self.df.columns:
                if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                    dt = pd.to_datetime(self.df[col], errors="coerce")
                    if dt.notna().any():
                        self.df[f"{col}_year"] = dt.dt.year
                        self.df[f"{col}_month"] = dt.dt.month
                        self.df[f"{col}_day"] = dt.dt.day
                        self.df[f"{col}_dayofweek"] = dt.dt.dayofweek
                        features_created.extend(
                            [
                                f"{col}_year",
                                f"{col}_month",
                                f"{col}_day",
                                f"{col}_dayofweek",
                            ]
                        )

            numeric_cols = self.df.select_dtypes(include=["number"]).columns
            if len(numeric_cols) >= 2:
                self.df["_row_sum"] = self.df[numeric_cols].sum(axis=1)
                self.df["_row_mean"] = self.df[numeric_cols].mean(axis=1)
                features_created.extend(["_row_sum", "_row_mean"])

            result = PipelineStageResult(
                stage=PipelineStage.FEATURE_ENGINEERING,
                status=PipelineStatus.COMPLETED,
                output={
                    "features_created": features_created,
                    "total_features": len(features_created),
                },
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

            self._log(f"Feature engineering: created {len(features_created)} features")
            return result

        except Exception as e:
            self._log(f"Feature engineering error: {e}", "error")
            return PipelineStageResult(
                stage=PipelineStage.FEATURE_ENGINEERING,
                status=PipelineStatus.FAILED,
                error=str(e),
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

    def prepare_analysis(self) -> PipelineStageResult:
        """Analysis preparation stage."""
        start = datetime.utcnow()
        try:
            numeric_cols = list(self.df.select_dtypes(include=["number"]).columns)
            categorical_cols = list(
                self.df.select_dtypes(include=["object", "category"]).columns
            )
            datetime_cols = [
                col
                for col in self.df.columns
                if pd.api.types.is_datetime64_any_dtype(self.df[col])
            ]

            result = PipelineStageResult(
                stage=PipelineStage.ANALYSIS_PREPARATION,
                status=PipelineStatus.COMPLETED,
                output={
                    "numeric_columns": numeric_cols,
                    "categorical_columns": categorical_cols,
                    "datetime_columns": datetime_cols,
                    "ready_for_analysis": len(numeric_cols) > 0
                    or len(datetime_cols) > 0,
                },
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

            self._log("Analysis preparation: complete")
            return result

        except Exception as e:
            self._log(f"Analysis preparation error: {e}", "error")
            return PipelineStageResult(
                stage=PipelineStage.ANALYSIS_PREPARATION,
                status=PipelineStatus.FAILED,
                error=str(e),
                duration_ms=(datetime.utcnow() - start).total_seconds() * 1000,
            )

    def execute(self) -> dict[str, Any]:
        """Execute full pipeline."""
        self.status = PipelineStatus.RUNNING
        self._log("Pipeline execution started")

        stages = [
            self.validate,
            self.clean,
            self.transform,
            self.feature_engineering,
            self.prepare_analysis,
        ]

        for stage in stages:
            result = stage()
            self.results.append(result)

            if result.status == PipelineStatus.FAILED:
                self.status = PipelineStatus.FAILED
                self._log(f"Stage {result.stage} failed: {result.error}", "error")
                break

        if self.status != PipelineStatus.FAILED:
            self.status = PipelineStatus.COMPLETED
            self._log("Pipeline execution completed")

        return self._build_result()

    def _build_result(self) -> dict[str, Any]:
        """Build final result."""
        return {
            "pipeline_id": self.pipeline_id,
            "status": self.status.value,
            "input_rows": len(self.df),
            "input_columns": len(self.df.columns),
            "output_rows": len(self.df),
            "output_columns": len(self.df.columns),
            "stages": [r.to_dict() for r in self.results],
            "total_duration_ms": sum(r.duration_ms for r in self.results),
            "data": self.df.to_dict(orient="records"),
        }


def auto_configure_pipeline(
    data: list[dict[str, Any]] | pd.DataFrame,
) -> dict[str, Any]:
    """Auto-configure and execute pipeline."""
    df = pd.DataFrame(data) if isinstance(data, list) else data

    config = {
        "stages": [
            {"name": "validation", "enabled": True},
            {"name": "cleaning", "enabled": True},
            {"name": "transformation", "enabled": True},
            {"name": "feature_engineering", "enabled": True},
            {"name": "analysis_preparation", "enabled": True},
        ],
    }

    pipeline = Pipeline(df, config)
    return pipeline.execute()
