"""Structured analytics logging and performance tracking."""

from __future__ import annotations

import functools
import logging
import time
import tracemalloc
from collections.abc import Callable
from datetime import datetime
from typing import Any, TypeVar, Dict

try:
    import psutil
except ImportError:  # pragma: no cover
    psutil = None

T = TypeVar("T")

LOGGER_NAME = "insightflow_analytics"


def get_analytics_logger() -> logging.Logger:
    """Return a configured analytics logger."""

    logger = logging.getLogger(LOGGER_NAME)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger

    handler = logging.StreamHandler()
    if jsonlogger is not None:
        formatter = jsonlogger.JsonFormatter(
            "%(asctime)s %(name)s %(levelname)s %(message)s"
        )
    else:
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
        )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


logger = get_analytics_logger()


def ensure_tracing_started() -> None:
    """Enable memory tracing if it is not already active."""

    if not tracemalloc.is_tracing():
        tracemalloc.start()


def current_memory_mb() -> float | None:
    """Return traced current memory usage in MB when available."""

    if not tracemalloc.is_tracing():
        return None
    current, _ = tracemalloc.get_traced_memory()
    return round(current / (1024 * 1024), 2)


def build_context(
    *,
    dataset_id: str | int | None,
    analysis_type: str,
    correlation_id: str | None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Construct a consistent structured log context."""

    payload = {
        "dataset_id": str(dataset_id) if dataset_id is not None else None,
        "analysis_type": analysis_type,
        "correlation_id": correlation_id,
    }
    if extra:
        payload.update(extra)
    return payload


def log_performance_check(metric: str, value_ms: float, target_ms: float) -> None:
    """Log threshold checks with a consistent structure."""

    status = "OK"
    if value_ms > target_ms * 2:
        status = "RED"
    elif value_ms > target_ms:
        status = "WARNING"
    logger.info(
        "performance_check",
        extra={
            "performance_check": {
                "metric": metric,
                "value_ms": round(value_ms, 2),
                "target_ms": round(target_ms, 2),
                "status": status,
                "action": "Investigate if repeated." if status != "OK" else "None",
            }
        },
    )


def timed_operation(
    operation_name: str, target_ms: float | None = None
) -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator for measuring analytics execution time."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            ensure_tracing_started()
            start = time.perf_counter()
            try:
                result = func(*args, **kwargs)
                duration_ms = (time.perf_counter() - start) * 1000
                logger.info(
                    "%s completed",
                    operation_name,
                    extra={
                        "operation": operation_name,
                        "duration_ms": round(duration_ms, 2),
                        "status": "success",
                        "memory_used_mb": current_memory_mb(),
                    },
                )
                if target_ms is not None:
                    log_performance_check(operation_name, duration_ms, target_ms)
                return result
            except Exception as exc:
                duration_ms = (time.perf_counter() - start) * 1000
                logger.error(
                    "%s failed",
                    operation_name,
                    extra={
                        "operation": operation_name,
                        "duration_ms": round(duration_ms, 2),
                        "status": "failed",
                        "error": str(exc),
                        "memory_used_mb": current_memory_mb(),
                    },
                )
                raise

        return wrapper

    return decorator


def log_analysis_start(
    dataset_id: str, analysis_type: str, rows: int, columns: int
) -> None:
    """Log analysis start"""
    logger.info(
        f"{analysis_type} analysis started",
        extra={
            "dataset_id": dataset_id,
            "analysis_type": analysis_type,
            "rows": rows,
            "columns": columns,
            "event": "analysis_start",
        },
    )


def log_analysis_complete(
    dataset_id: str,
    analysis_type: str,
    duration_ms: float,
    model_used: str,
    metrics: Dict[str, float],
) -> None:
    """Log analysis completion"""
    logger.info(
        f"{analysis_type} analysis completed",
        extra={
            "dataset_id": dataset_id,
            "analysis_type": analysis_type,
            "duration_ms": duration_ms,
            "model_used": model_used,
            "metrics": metrics,
            "event": "analysis_complete",
        },
    )


def log_validation_result(
    dataset_id: str, valid: bool, quality_score: float, issues: list
) -> None:
    """Log validation result"""
    logger.info(
        f"Validation {'passed' if valid else 'failed'}",
        extra={
            "dataset_id": dataset_id,
            "valid": valid,
            "quality_score": quality_score,
            "issues": issues,
            "event": "validation",
        },
    )


def log_warning(message: str, dataset_id: str, context: Dict[str, Any]) -> None:
    """Log warning"""
    logger.warning(message, extra={"dataset_id": dataset_id, **context})


def log_error(
    message: str, dataset_id: str, error: str, context: Dict[str, Any]
) -> None:
    """Log error"""
    logger.error(message, extra={"dataset_id": dataset_id, "error": error, **context})


class PerformanceMonitor:
    """Track performance metrics"""

    def __init__(self, dataset_id: str):
        self.dataset_id = dataset_id
        self.metrics: Dict[str, Any] = {}
        self.start_time = time.time()
        self.start_memory = 0.0
        if psutil is not None:
            self.start_memory = psutil.Process().memory_info().rss / 1024 / 1024

    def record_step(self, step_name: str) -> None:
        """Record completion of a step"""
        duration = time.time() - self.start_time
        memory = 0.0
        if psutil is not None:
            memory = psutil.Process().memory_info().rss / 1024 / 1024

        self.metrics[step_name] = {
            "duration_ms": duration * 1000,
            "memory_mb": memory,
            "memory_delta_mb": memory - self.start_memory,
        }

        logger.debug(
            f"Step: {step_name}",
            extra={
                "dataset_id": self.dataset_id,
                "step": step_name,
                **self.metrics[step_name],
            },
        )

    def get_metrics(self) -> Dict[str, Any]:
        """Get all metrics"""
        total_duration = time.time() - self.start_time
        peak_memory = 0.0
        if psutil is not None:
            peak_memory = psutil.Process().memory_info().rss / 1024 / 1024

        return {
            "total_duration_ms": total_duration * 1000,
            "peak_memory_mb": peak_memory,
            "step_metrics": self.metrics,
        }


class CustomJsonFormatter:
    """Custom JSON formatter for structured logging"""

    def __init__(self, fmt: str = "%(timestamp)s %(level)s %(message)s"):
        self.fmt = fmt

    def format(self, record: logging.LogRecord) -> str:
        """Format log record as JSON"""
        log_data = {
            "timestamp": datetime.now().isoformat(),
            "level": record.levelname,
            "module": record.name,
            "message": record.getMessage(),
        }
        return str(log_data)


def setup_logging(log_name: str = "insightflow_analytics") -> logging.Logger:
    """Configure JSON logging"""
    return get_analytics_logger()
