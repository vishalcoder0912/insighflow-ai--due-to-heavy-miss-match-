"""Advanced analytics orchestrator."""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.user import User
from app.services import analysis as analysis_service
from app.services import datasets as dataset_service
from app.services.advanced_insights import generate_advanced_insights
from app.services.clustering import run_clustering
from app.services.cohort_analysis import run_cohort_analysis
from app.services.error_handling import AnalyticsSystemError, InsightFlowException, serialize_exception
from app.services.forecasting import run_forecasting
from app.services.monitoring import build_context, logger
from app.services.regression import run_regression
from app.services.rfm_analysis import run_rfm_analysis

SERVICE_REGISTRY = {
    "forecasting": run_forecasting,
    "clustering": run_clustering,
    "regression": run_regression,
    "cohort": run_cohort_analysis,
    "rfm": run_rfm_analysis,
}


async def _load_dataset_frame(dataset_path: Path) -> pd.DataFrame:
    with dataset_path.open("rb") as handle:
        sniff_bytes = handle.read(50000)
    dataframe, _ = await asyncio.to_thread(analysis_service._load_dataframe, dataset_path, sniff_bytes)
    return dataframe


async def run_advanced_analytics(
    session: AsyncSession,
    *,
    actor: User,
    dataset_id: int,
    analyses: list[str],
    options: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Run one or more advanced analytics modules against a stored dataset."""

    dataset = await dataset_service.get_dataset_or_404(session, dataset_id=dataset_id, actor=actor)
    dataset_path = Path(dataset.stored_path)
    if not dataset_path.exists():
        raise AnalyticsSystemError(
            message="Stored dataset file was not found for advanced analytics.",
            error_code="SYS_101",
            remediation=["Re-upload the dataset.", "Verify the storage volume is mounted correctly."],
            dataset_id=dataset_id,
            correlation_id=correlation_id,
        )

    dataframe = await _load_dataset_frame(dataset_path)
    requested = analyses or list(SERVICE_REGISTRY.keys())
    results: dict[str, Any] = {}
    failures: list[dict[str, Any]] = []

    for analysis_name in requested:
        service = SERVICE_REGISTRY.get(analysis_name)
        if service is None:
            failures.append(
                {
                    "analysis_type": analysis_name,
                    "error": True,
                    "error_code": "VAL_998",
                    "severity": "MEDIUM",
                    "message": f"Unsupported analysis '{analysis_name}'.",
                    "remediation": ["Choose one of: forecasting, clustering, regression, cohort, rfm."],
                }
            )
            continue

        context = build_context(dataset_id=dataset_id, analysis_type=analysis_name, correlation_id=correlation_id)
        logger.info("advanced_analysis_started", extra={"context": context})
        try:
            result = await asyncio.to_thread(
                service,
                dataframe.copy(),
                dataset_id=dataset_id,
                options=options or {},
                correlation_id=correlation_id,
            )
            results[analysis_name] = result
            logger.info("advanced_analysis_completed", extra={"context": context, "status": result.get("status")})
        except InsightFlowException as exc:
            failure = serialize_exception(exc)
            failure["analysis_type"] = analysis_name
            failures.append(failure)
            logger.warning("advanced_analysis_failed", extra={"context": context, "error": failure})
        except Exception as exc:  # pragma: no cover
            failure = {
                "error": True,
                "error_code": "SYS_500",
                "severity": "CRITICAL",
                "message": "Unexpected analytics engine failure.",
                "details": str(exc),
                "analysis_type": analysis_name,
                "dataset_id": str(dataset_id),
                "remediation": ["Retry the request.", "Inspect server logs for the stack trace."],
                "correlation_id": correlation_id,
            }
            failures.append(failure)
            logger.exception("advanced_analysis_crashed", extra={"context": context})

    overall_status = "SUCCESS"
    if results and failures:
        overall_status = "PARTIAL_SUCCESS"
    elif failures and not results:
        overall_status = "FAILED"

    return {
        "dataset_id": dataset.id,
        "filename": dataset.original_filename,
        "detected_domain": dataset.detected_domain,
        "status": overall_status,
        "analyses_requested": requested,
        "successful_analyses": sorted(results.keys()),
        "failed_analyses": [failure["analysis_type"] for failure in failures],
        "results": results,
        "failures": failures,
        "ai_insights": generate_advanced_insights(results, failures),
    }
