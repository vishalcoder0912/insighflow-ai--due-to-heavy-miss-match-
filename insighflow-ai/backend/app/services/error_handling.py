"""Advanced analytics exceptions and recovery helpers."""

from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any, TypeVar

from app.core.exceptions import ApiException

T = TypeVar("T")


class InsightFlowException(ApiException):
    """Base exception for analytics workflows."""

    def __init__(
        self,
        *,
        message: str,
        error_code: str,
        severity: str,
        remediation: list[str] | None = None,
        status_code: int = 400,
        details: Any | None = None,
        analysis_type: str | None = None,
        dataset_id: str | int | None = None,
        correlation_id: str | None = None,
        related_error: str | None = None,
    ) -> None:
        super().__init__(status_code=status_code, code=error_code, message=message, details=details)
        self.error_code = error_code
        self.severity = severity
        self.remediation = remediation or []
        self.analysis_type = analysis_type
        self.dataset_id = str(dataset_id) if dataset_id is not None else None
        self.correlation_id = correlation_id
        self.related_error = related_error


class AnalyticsValidationError(InsightFlowException):
    """Raised when dataset validation fails."""

    def __init__(self, *, message: str, error_code: str, severity: str = "HIGH", **kwargs: Any) -> None:
        super().__init__(
            message=message,
            error_code=error_code,
            severity=severity,
            status_code=400,
            **kwargs,
        )


class SchemaMismatchError(AnalyticsValidationError):
    """Raised when required columns are missing or incompatible."""


class ColumnMissingError(SchemaMismatchError):
    """Raised when a required column cannot be found."""


class TypeMismatchError(SchemaMismatchError):
    """Raised when a required column has the wrong semantic type."""


class EmptyDatasetError(AnalyticsValidationError):
    """Raised when an empty dataset reaches analytics."""


class InsufficientDataError(AnalyticsValidationError):
    """Raised when there are not enough usable rows to analyze."""


class DataQualityError(AnalyticsValidationError):
    """Raised when data quality is too low to proceed."""


class ModelTrainingError(InsightFlowException):
    """Raised when a model cannot be fitted."""

    def __init__(self, *, message: str, error_code: str, severity: str = "MEDIUM", **kwargs: Any) -> None:
        super().__init__(
            message=message,
            error_code=error_code,
            severity=severity,
            status_code=200,
            **kwargs,
        )


class ConvergenceError(ModelTrainingError):
    """Raised when a model fails to converge."""


class FeatureEngineeringError(ModelTrainingError):
    """Raised when model-ready features cannot be created."""


class AnalyticsSystemError(InsightFlowException):
    """Raised for unexpected analytics engine failures."""

    def __init__(self, *, message: str, error_code: str, severity: str = "CRITICAL", **kwargs: Any) -> None:
        super().__init__(
            message=message,
            error_code=error_code,
            severity=severity,
            status_code=500,
            **kwargs,
        )


class AnalyticsTimeoutError(AnalyticsSystemError):
    """Raised when analysis execution exceeds the allowed budget."""


class AnalyticsMemoryError(AnalyticsSystemError):
    """Raised when analytics would exceed safe memory usage."""


def serialize_exception(exc: InsightFlowException) -> dict[str, Any]:
    """Project analytics exceptions into a frontend-safe error payload."""

    return {
        "error": True,
        "error_code": exc.error_code,
        "severity": exc.severity,
        "message": exc.message,
        "details": exc.details,
        "analysis_type": exc.analysis_type,
        "dataset_id": exc.dataset_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "remediation": exc.remediation,
        "related_error": exc.related_error,
        "correlation_id": exc.correlation_id,
    }


def retry_with_backoff(
    func: Callable[[], T],
    *,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> T:
    """Retry transient failures with exponential backoff."""

    for attempt in range(max_retries):
        try:
            return func()
        except (AnalyticsTimeoutError, ConnectionError, TimeoutError):
            if attempt == max_retries - 1:
                raise
            time.sleep(base_delay * (2**attempt))
        except (AnalyticsValidationError, ModelTrainingError):
            raise
    raise AnalyticsSystemError(
        message="Retry loop exhausted unexpectedly.",
        error_code="SYS_999",
        remediation=["Retry the request later.", "Inspect application logs for the underlying failure."],
    )
