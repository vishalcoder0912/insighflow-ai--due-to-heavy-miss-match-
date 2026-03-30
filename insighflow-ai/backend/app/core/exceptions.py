"""Custom exception hierarchy for InsightFlow Analytics Engine.
Organized by severity and recovery strategy.
"""

from typing import Optional, List, Dict, Any
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class InsightFlowException(Exception):
    """Base exception for all InsightFlow errors"""

    def __init__(
        self,
        message: str,
        error_code: str,
        severity: str = "HIGH",
        remediation: Optional[List[str]] = None,
        details: Optional[Dict[str, Any]] = None,
    ):
        self.message = message
        self.error_code = error_code
        self.severity = severity
        self.remediation = remediation or []
        self.details = details or {}
        self.timestamp = datetime.utcnow()

        super().__init__(self.message)

    def to_dict(self):
        return {
            "error": True,
            "error_code": self.error_code,
            "severity": self.severity,
            "message": self.message,
            "details": self.details,
            "remediation": self.remediation,
            "timestamp": self.timestamp.isoformat(),
        }


class ValidationError(InsightFlowException):
    """Base validation error - expected user input issues"""

    def __init__(self, message: str, **kwargs):
        if "error_code" not in kwargs:
            kwargs["error_code"] = "VAL_001"
        super().__init__(message, severity="HIGH", **kwargs)


class SchemaMismatchError(ValidationError):
    """Required columns missing or wrong type"""

    def __init__(self, missing_cols: List[str], provided_cols: List[str], **kwargs):
        message = f"Schema mismatch. Missing columns: {missing_cols}"
        details = {"missing_columns": missing_cols, "provided_columns": provided_cols}
        remediation = [
            f"Provide columns: {', '.join(missing_cols)}",
            "Check data format and column names",
        ]
        kwargs.setdefault("error_code", "VAL_002")
        super().__init__(
            message,
            remediation=remediation,
            details=details,
            **kwargs,
        )


class InsufficientDataError(ValidationError):
    """Not enough data points for analysis"""

    def __init__(self, analysis_type: str, required: int, actual: int, **kwargs):
        message = f"{analysis_type}: Need {required} data points, got {actual}"
        details = {
            "analysis_type": analysis_type,
            "required_points": required,
            "actual_points": actual,
        }
        remediation = [
            f"Upload dataset with at least {required} rows",
            "Consider collecting more historical data",
        ]
        kwargs.setdefault("error_code", "VAL_003")
        super().__init__(
            message,
            remediation=remediation,
            details=details,
            **kwargs,
        )


class ColumnMissingError(ValidationError):
    """Required column not found"""

    def __init__(self, column_name: str, available_cols: List[str], **kwargs):
        message = f"Required column '{column_name}' not found"
        details = {"missing_column": column_name, "available_columns": available_cols}
        remediation = [
            f"Rename or add column '{column_name}'",
            f"Available columns: {', '.join(available_cols)}",
        ]
        kwargs.setdefault("error_code", "VAL_004")
        super().__init__(
            message,
            remediation=remediation,
            details=details,
            **kwargs,
        )


class DataQualityError(ValidationError):
    """Data quality too low to analyze"""

    def __init__(self, quality_score: float, threshold: float, **kwargs):
        message = f"Data quality {quality_score:.2f} below threshold {threshold:.2f}"
        details = {"quality_score": quality_score, "threshold": threshold}
        remediation = [
            "Clean data: remove duplicates, handle missing values",
            "Improve data completeness and consistency",
        ]
        super().__init__(
            message,
            error_code="VAL_005",
            remediation=remediation,
            details=details,
            **kwargs,
        )


class EmptyDatasetError(ValidationError):
    """Dataset is empty"""

    def __init__(self, **kwargs):
        message = "Dataset is empty (0 rows)"
        remediation = ["Upload a non-empty dataset"]
        kwargs.setdefault("error_code", "VAL_006")
        super().__init__(message, remediation=remediation, **kwargs)


class NoVarianceError(ValidationError):
    """Column has no variation (all same value)"""

    def __init__(self, column_name: str, **kwargs):
        message = f"Column '{column_name}' has no variation (constant value)"
        details = {"column": column_name}
        remediation = [
            f"Ensure '{column_name}' has varying values",
            "Remove constant columns",
        ]
        kwargs.setdefault("error_code", "VAL_007")
        super().__init__(
            message,
            remediation=remediation,
            details=details,
            **kwargs,
        )


class InsufficientFeaturesError(ValidationError):
    """Not enough features/dimensions"""

    def __init__(self, required: int, actual: int, analysis_type: str, **kwargs):
        message = f"{analysis_type}: Need {required} features, got {actual}"
        details = {
            "required_features": required,
            "actual_features": actual,
            "analysis_type": analysis_type,
        }
        remediation = [
            f"Provide at least {required} feature columns",
            "Ensure columns are numeric or properly encoded",
        ]
        kwargs.setdefault("error_code", "VAL_008")
        super().__init__(
            message,
            remediation=remediation,
            details=details,
            **kwargs,
        )


class ModelTrainingError(InsightFlowException):
    """Model failed to train - try fallback"""

    def __init__(self, model_name: str, error_details: str, **kwargs):
        message = f"Model training failed: {model_name}"
        details = {"model": model_name, "error": error_details}
        super().__init__(
            message,
            error_code="MOD_001",
            severity="MEDIUM",
            remediation=["Trying fallback model"],
            details=details,
            **kwargs,
        )


class ConvergenceError(ModelTrainingError):
    """Model didn't converge"""

    def __init__(self, model_name: str, iterations: int, **kwargs):
        message = f"{model_name} did not converge after {iterations} iterations"
        details = {"model": model_name, "iterations": iterations}
        super().__init__(model_name, message, **kwargs)
        self.details = details


class FeatureEngineeringError(ModelTrainingError):
    """Feature preparation failed"""

    def __init__(self, step: str, error_details: str, **kwargs):
        message = f"Feature engineering failed at: {step}"
        details = {"step": step, "error": error_details}
        super().__init__("feature_engineering", error_details, **kwargs)
        self.details = details


class SystemError(InsightFlowException):
    """Unexpected system failure"""

    def __init__(self, message: str, error_details: str, **kwargs):
        super().__init__(
            message,
            error_code="SYS_001",
            severity="CRITICAL",
            details={"error": error_details},
            **kwargs,
        )


class TimeoutError(SystemError):
    """Processing took too long"""

    def __init__(self, operation: str, timeout_seconds: int, **kwargs):
        message = f"Operation '{operation}' exceeded {timeout_seconds}s timeout"
        details = {"operation": operation, "timeout_seconds": timeout_seconds}
        super().__init__(
            message, error_details=f"Timeout after {timeout_seconds}s", **kwargs
        )
        self.details = details


class MemoryError(SystemError):
    """Out of memory"""

    def __init__(self, available_mb: float, required_mb: float, **kwargs):
        message = (
            f"Insufficient memory: {available_mb}MB available, {required_mb}MB required"
        )
        details = {"available_mb": available_mb, "required_mb": required_mb}
        super().__init__(
            message,
            error_details=f"Need {required_mb}MB, have {available_mb}MB",
            **kwargs,
        )
        self.details = details


class DatabaseError(SystemError):
    """Database operation failed"""

    def __init__(self, operation: str, error_details: str, **kwargs):
        message = f"Database error during {operation}"
        super().__init__(message, error_details=error_details, **kwargs)


class ApiException(Exception):
    """Structured exception used by the API."""

    def __init__(
        self,
        *,
        status_code: int,
        code: str,
        message: str,
        details: Any | None = None,
    ) -> None:
        self.status_code = status_code
        self.code = code
        self.message = message
        self.details = details
        super().__init__(message)
