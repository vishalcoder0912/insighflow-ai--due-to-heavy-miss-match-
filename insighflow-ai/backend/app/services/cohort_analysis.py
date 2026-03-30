"""Cohort analysis service."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from app.services.monitoring import (
    timed_operation,
    log_analysis_start,
    log_analysis_complete,
)
from app.services.validation import PreparedDataset, prepare_analysis_dataset
from app.services.error_handling import InsufficientDataError


def _month_diff(current: pd.Series, cohort: pd.Series) -> pd.Series:
    return (current.dt.year - cohort.dt.year) * 12 + (
        current.dt.month - cohort.dt.month
    )


@timed_operation("advanced_cohort_analysis", target_ms=30000)
def run_cohort_analysis(
    df: pd.DataFrame,
    *,
    dataset_id: str | int | None = None,
    options: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Build cohort retention and value matrices."""

    options = options or {}
    prepared: PreparedDataset = prepare_analysis_dataset(
        df,
        analysis_type="cohort",
        dataset_id=dataset_id,
        options=options,
        correlation_id=correlation_id,
    )
    plan = prepared.plan
    time_column = plan["time_column"]
    cohort_column = plan["cohort_column"]
    metric_column = plan["metric_column"]

    working = prepared.dataframe[[time_column, cohort_column, metric_column]].copy()
    working[time_column] = pd.to_datetime(working[time_column], errors="coerce")
    working["period"] = working[time_column].dt.to_period("M").dt.to_timestamp()
    working["cohort_period"] = working.groupby(cohort_column)["period"].transform("min")
    working["cohort_index"] = _month_diff(working["period"], working["cohort_period"])

    cohort_sizes = working.groupby("cohort_period")[cohort_column].nunique()
    retention = (
        working.groupby(["cohort_period", "cohort_index"])[cohort_column]
        .nunique()
        .unstack(fill_value=0)
    )
    retention = retention.divide(cohort_sizes, axis=0).round(4)
    revenue = (
        working.groupby(["cohort_period", "cohort_index"])[metric_column]
        .sum()
        .unstack(fill_value=0)
        .round(4)
    )

    retention_rows = [
        {
            "cohort_period": str(index.date()),
            "values": {str(column): float(value) for column, value in row.items()},
        }
        for index, row in retention.iterrows()
    ]
    revenue_rows = [
        {
            "cohort_period": str(index.date()),
            "values": {str(column): float(value) for column, value in row.items()},
        }
        for index, row in revenue.iterrows()
    ]

    avg_retention = (
        float(retention.iloc[:, 1:].mean().mean())
        if retention.shape[1] > 1
        else float(retention.mean().mean())
    )
    return {
        "status": "SUCCESS",
        "confidence": "MEDIUM",
        "analysis_type": "cohort",
        "processed_rows": int(len(prepared.dataframe)),
        "total_rows": int(len(df)),
        "excluded_rows": prepared.excluded_rows,
        "exclusion_reasons": {"preprocessing": prepared.excluded_rows},
        "quality_score": prepared.validation["quality_metrics"]["overall_score"],
        "validation": prepared.validation,
        "missing_values_analysis": prepared.missing_values_analysis,
        "results": {
            "time_column": time_column,
            "cohort_column": cohort_column,
            "metric_column": metric_column,
            "cohort_count": int(len(retention_rows)),
            "period_count": int(retention.shape[1]),
            "average_retention": round(avg_retention, 4),
            "retention_matrix": retention_rows,
            "revenue_matrix": revenue_rows,
        },
        "warnings": list(prepared.warnings),
    }


class CohortAnalyzer:
    """Cohort analysis engine"""

    def __init__(self, df: pd.DataFrame, dataset_id: str):
        self.df = df.copy()
        self.dataset_id = dataset_id
        self.cohort_matrix = None

    @timed_operation("Cohort Analysis")
    def analyze(
        self,
        cohort_type: str = "temporal",
        cohort_dimension: str = "signup_month",
        time_dimension: str = "transaction_month",
        metric: str = "revenue",
        min_cohorts: int = 3,
        min_periods: int = 3,
    ) -> Dict[str, Any]:
        """Perform cohort analysis"""

        log_analysis_start(
            self.dataset_id, "cohort", len(self.df), len(self.df.columns)
        )

        if cohort_dimension not in self.df.columns:
            raise ValueError(f"Cohort dimension '{cohort_dimension}' not found")

        if time_dimension not in self.df.columns:
            raise ValueError(f"Time dimension '{time_dimension}' not found")

        if metric not in self.df.columns:
            raise ValueError(f"Metric '{metric}' not found")

        if cohort_type == "temporal":
            cohort_data = self._build_temporal_cohort(
                cohort_dimension, time_dimension, metric
            )
        else:
            cohort_data = self._build_dimensional_cohort(
                cohort_dimension, time_dimension, metric
            )

        n_cohorts = len(cohort_data)
        if n_cohorts < min_cohorts:
            raise InsufficientDataError(
                message=f"Need at least {min_cohorts} cohorts, got {n_cohorts}",
                error_code="VAL_200",
                severity="HIGH",
                analysis_type="cohort",
            )

        insights = self._generate_cohort_insights(cohort_data, metric)

        log_analysis_complete(
            self.dataset_id, "cohort", 0, "cohort_analysis", {"cohorts": n_cohorts}
        )

        return {
            "status": "SUCCESS",
            "cohort_type": cohort_type,
            "cohort_dimension": cohort_dimension,
            "metric_tracked": metric,
            "cohorts": cohort_data,
            "total_cohorts": n_cohorts,
            "insights": insights,
        }

    def _build_temporal_cohort(
        self, cohort_dim: str, time_dim: str, metric: str
    ) -> List[Dict[str, Any]]:
        """Build time-based cohort matrix"""

        cohort_df = self.df.copy()
        cohort_df[cohort_dim] = pd.to_datetime(cohort_df[cohort_dim])
        cohort_df[time_dim] = pd.to_datetime(cohort_df[time_dim])

        cohorts_list = []

        for cohort_period in sorted(cohort_df[cohort_dim].unique()):
            cohort_data_subset = cohort_df[cohort_df[cohort_dim] == cohort_period]
            cohort_size = len(cohort_data_subset)

            time_data = []
            for time_period in sorted(cohort_data_subset[time_dim].unique()):
                period_data = cohort_data_subset[
                    cohort_data_subset[time_dim] == time_period
                ]

                if metric in period_data.columns:
                    if pd.api.types.is_numeric_dtype(period_data[metric]):
                        value = period_data[metric].sum()
                    else:
                        value = len(period_data)
                else:
                    value = len(period_data)

                time_data.append(
                    {
                        "period": str(time_period),
                        "value": float(value),
                        "count": len(period_data),
                    }
                )

            cohorts_list.append(
                {
                    "cohort_name": str(cohort_period),
                    "cohort_size": int(cohort_size),
                    "time_periods": time_data,
                }
            )

        return cohorts_list

    def _build_dimensional_cohort(
        self, cohort_dim: str, time_dim: str, metric: str
    ) -> List[Dict[str, Any]]:
        """Build dimension-based cohort matrix"""

        cohorts_list = []

        for cohort_value in self.df[cohort_dim].unique():
            cohort_data_subset = self.df[self.df[cohort_dim] == cohort_value]
            cohort_size = len(cohort_data_subset)

            time_data = []
            for time_period in sorted(cohort_data_subset[time_dim].unique()):
                period_data = cohort_data_subset[
                    cohort_data_subset[time_dim] == time_period
                ]

                if metric in period_data.columns:
                    if pd.api.types.is_numeric_dtype(period_data[metric]):
                        value = period_data[metric].sum()
                    else:
                        value = len(period_data)
                else:
                    value = len(period_data)

                time_data.append(
                    {
                        "period": str(time_period),
                        "value": float(value),
                        "count": len(period_data),
                    }
                )

            cohorts_list.append(
                {
                    "cohort_name": str(cohort_value),
                    "cohort_size": int(cohort_size),
                    "time_periods": time_data,
                }
            )

        return cohorts_list

    def _generate_cohort_insights(
        self, cohorts: List[Dict[str, Any]], metric: str
    ) -> Dict[str, Any]:
        """Generate insights from cohort analysis"""

        if not cohorts:
            return {}

        cohort_totals = [
            (c["cohort_name"], sum(tp["value"] for tp in c["time_periods"]))
            for c in cohorts
        ]

        if cohort_totals:
            best_cohort = max(cohort_totals, key=lambda x: x[1])
            worst_cohort = min(cohort_totals, key=lambda x: x[1])
        else:
            best_cohort = worst_cohort = None

        return {
            "best_performing_cohort": best_cohort[0] if best_cohort else None,
            "worst_performing_cohort": worst_cohort[0] if worst_cohort else None,
            "metric_tracked": metric,
            "total_cohorts": len(cohorts),
            "key_finding": f"Cohort '{best_cohort[0]}' outperforms others with ${best_cohort[1]:.0f} total {metric}"
            if best_cohort
            else "No cohorts analyzed",
        }
