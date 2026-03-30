"""Data Quality Engine - Comprehensive validation and scoring."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

MAX_ROWS_FOR_DUPLICATE_CHECK = 100000
SAMPLE_SIZE_FOR_LARGE_DATA = 50000


class IssueSeverity(str, Enum):
    """Issue severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


class DataQualityEngine:
    """Comprehensive data quality validation engine."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data

        self.issues: list[dict[str, Any]] = []
        self.quality_score = 0
        self._run_quality_checks()

    def _add_issue(
        self,
        severity: IssueSeverity,
        category: str,
        message: str,
        column: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        """Add a quality issue."""
        self.issues.append(
            {
                "severity": severity.value,
                "category": category,
                "message": message,
                "column": column,
                "details": details or {},
            }
        )

    def _run_quality_checks(self) -> None:
        """Run all quality checks."""
        self._check_completeness()
        self._check_duplicates()
        self._check_data_types()
        self._check_outliers()
        self._check_consistency()
        self._calculate_quality_score()

    def _check_completeness(self) -> None:
        """Check for missing values."""
        total_cells = self.df.shape[0] * self.df.shape[1]
        null_cells = self.df.isna().sum().sum()
        null_ratio = null_cells / total_cells if total_cells > 0 else 0

        if null_ratio > 0.5:
            self._add_issue(
                IssueSeverity.CRITICAL,
                "completeness",
                f"Critical: {null_ratio:.1%} of data is missing",
                details={
                    "null_count": int(null_cells),
                    "null_ratio": round(null_ratio, 4),
                },
            )
        elif null_ratio > 0.2:
            self._add_issue(
                IssueSeverity.ERROR,
                "completeness",
                f"High missing values: {null_ratio:.1%}",
                details={
                    "null_count": int(null_cells),
                    "null_ratio": round(null_ratio, 4),
                },
            )
        elif null_ratio > 0.05:
            self._add_issue(
                IssueSeverity.WARNING,
                "completeness",
                f"Moderate missing values: {null_ratio:.1%}",
                details={
                    "null_count": int(null_cells),
                    "null_ratio": round(null_ratio, 4),
                },
            )

        for col in self.df.columns:
            col_nulls = self.df[col].isna().sum()
            col_null_ratio = col_nulls / len(self.df) if len(self.df) > 0 else 0

            if col_null_ratio > 0.5:
                self._add_issue(
                    IssueSeverity.ERROR,
                    "completeness",
                    f"Column '{col}' has {col_null_ratio:.1%} missing values",
                    column=col,
                    details={
                        "null_count": int(col_nulls),
                        "null_ratio": round(col_null_ratio, 4),
                    },
                )
            elif col_null_ratio > 0.2:
                self._add_issue(
                    IssueSeverity.WARNING,
                    "completeness",
                    f"Column '{col}' has {col_null_ratio:.1%} missing values",
                    column=col,
                    details={
                        "null_count": int(col_nulls),
                        "null_ratio": round(col_null_ratio, 4),
                    },
                )

    def _check_duplicates(self) -> None:
        """Check for duplicate rows."""
        check_df = self.df.head(MAX_ROWS_FOR_DUPLICATE_CHECK)
        duplicates = check_df.duplicated().sum()
        dup_ratio = duplicates / len(check_df) if len(check_df) > 0 else 0

        if dup_ratio > 0.1:
            self._add_issue(
                IssueSeverity.ERROR,
                "duplicates",
                f"High duplicate rate: {dup_ratio:.1%}",
                details={
                    "duplicate_count": int(duplicates),
                    "duplicate_ratio": round(dup_ratio, 4),
                },
            )
        elif dup_ratio > 0.01:
            self._add_issue(
                IssueSeverity.WARNING,
                "duplicates",
                f"Moderate duplicates: {dup_ratio:.1%}",
                details={
                    "duplicate_count": int(duplicates),
                    "duplicate_ratio": round(dup_ratio, 4),
                },
            )

        for col in self.df.columns:
            if self.df[col].dtype == "object":
                dup_cols = self.df[col].duplicated().sum()
                if dup_cols > 0:
                    self._add_issue(
                        IssueSeverity.INFO,
                        "duplicates",
                        f"Column '{col}' has {dup_cols} duplicate values",
                        column=col,
                        details={"duplicate_count": int(dup_cols)},
                    )

    def _check_data_types(self) -> None:
        """Check for mixed data types."""
        for col in self.df.columns:
            if self.df[col].dtype == "object":
                unique_values = self.df[col].dropna().unique()
                if len(unique_values) > 0:
                    type_patterns = {
                        "numeric": [],
                        "date": [],
                        "text": [],
                    }

                    for val in unique_values[:100]:
                        val_str = str(val)
                        try:
                            float(val_str)
                            type_patterns["numeric"].append(val)
                        except (ValueError, TypeError):
                            pass

                        try:
                            pd.to_datetime(val_str)
                            type_patterns["date"].append(val)
                        except (ValueError, TypeError):
                            pass

                    numeric_ratio = (
                        len(type_patterns["numeric"]) / len(unique_values[:100])
                        if len(unique_values) > 0
                        else 0
                    )

                    if 0.2 < numeric_ratio < 0.8:
                        self._add_issue(
                            IssueSeverity.WARNING,
                            "data_types",
                            f"Column '{col}' has mixed numeric and text values",
                            column=col,
                            details={"numeric_ratio": round(numeric_ratio, 4)},
                        )

    def _check_outliers(self) -> None:
        """Check for outliers using IQR and Z-score."""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            series = pd.to_numeric(self.df[col], errors="coerce").dropna()

            if len(series) < 10:
                continue

            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1

            if iqr == 0:
                continue

            lower = q1 - 1.5 * iqr
            upper = q3 + 1.5 * iqr

            outliers_iqr = series[(series < lower) | (series > upper)]
            outlier_ratio_iqr = (
                len(outliers_iqr) / len(series) if len(series) > 0 else 0
            )

            if outlier_ratio_iqr > 0.1:
                self._add_issue(
                    IssueSeverity.WARNING,
                    "outliers",
                    f"Column '{col}' has {outlier_ratio_iqr:.1%} outliers (IQR method)",
                    column=col,
                    details={
                        "outlier_count": len(outliers_iqr),
                        "outlier_ratio": round(outlier_ratio_iqr, 4),
                    },
                )

            if len(series) >= 30:
                z_scores = np.abs(stats.zscore(series))
                outliers_z = series[z_scores > 3]
                outlier_ratio_z = (
                    len(outliers_z) / len(series) if len(series) > 0 else 0
                )

                if outlier_ratio_z > 0.05:
                    self._add_issue(
                        IssueSeverity.INFO,
                        "outliers",
                        f"Column '{col}' has {outlier_ratio_z:.1%} extreme outliers (Z-score > 3)",
                        column=col,
                        details={
                            "outlier_count": len(outliers_z),
                            "outlier_ratio": round(outlier_ratio_z, 4),
                        },
                    )

    def _check_consistency(self) -> None:
        """Check data consistency."""
        for col in self.df.columns:
            if self.df[col].dtype == "object":
                whitespace_issues = self.df[col].astype(str).str.strip() != self.df[
                    col
                ].astype(str)
                ws_count = whitespace_issues.sum()

                if ws_count > 0:
                    self._add_issue(
                        IssueSeverity.INFO,
                        "consistency",
                        f"Column '{col}' has {ws_count} values with leading/trailing whitespace",
                        column=col,
                        details={"issue_count": int(ws_count)},
                    )

        if self.df.shape[1] > 0:
            empty_cols = [col for col in self.df.columns if self.df[col].isna().all()]
            if empty_cols:
                self._add_issue(
                    IssueSeverity.ERROR,
                    "consistency",
                    f"Columns with all nulls: {empty_cols}",
                    details={"empty_columns": empty_cols},
                )

    def _calculate_quality_score(self) -> None:
        """Calculate overall quality score (0-100)."""
        score = 100

        critical_count = sum(
            1 for i in self.issues if i["severity"] == IssueSeverity.CRITICAL.value
        )
        error_count = sum(
            1 for i in self.issues if i["severity"] == IssueSeverity.ERROR.value
        )
        warning_count = sum(
            1 for i in self.issues if i["severity"] == IssueSeverity.WARNING.value
        )

        score -= critical_count * 20
        score -= error_count * 10
        score -= warning_count * 3

        null_ratio = self.df.isna().mean().mean()
        score -= null_ratio * 30

        dup_ratio = self.df.duplicated().sum() / len(self.df) if len(self.df) > 0 else 0
        score -= dup_ratio * 20

        self.quality_score = max(0, min(100, round(score)))

    def get_report(self) -> dict[str, Any]:
        """Get comprehensive quality report."""
        return {
            "quality_score": self.quality_score,
            "total_issues": len(self.issues),
            "issues_by_severity": {
                "critical": sum(
                    1
                    for i in self.issues
                    if i["severity"] == IssueSeverity.CRITICAL.value
                ),
                "error": sum(
                    1 for i in self.issues if i["severity"] == IssueSeverity.ERROR.value
                ),
                "warning": sum(
                    1
                    for i in self.issues
                    if i["severity"] == IssueSeverity.WARNING.value
                ),
                "info": sum(
                    1 for i in self.issues if i["severity"] == IssueSeverity.INFO.value
                ),
            },
            "issues": self.issues,
            "recommendations": self._generate_recommendations(),
            "dataset_info": {
                "total_rows": len(self.df),
                "total_columns": len(self.df.columns),
                "memory_usage_bytes": int(self.df.memory_usage(deep=True).sum()),
                "completeness_percentage": round(
                    (1 - self.df.isna().mean().mean()) * 100, 2
                ),
            },
        }

    def _generate_recommendations(self) -> list[dict[str, Any]]:
        """Generate actionable recommendations."""
        recommendations = []

        null_ratio = self.df.isna().mean().mean()
        if null_ratio > 0.1:
            recommendations.append(
                {
                    "priority": "high",
                    "action": "Handle missing values",
                    "suggestion": "Consider imputation strategies (mean, median, mode, or forward fill) based on data type",
                }
            )

        dup_ratio = self.df.duplicated().sum() / len(self.df) if len(self.df) > 0 else 0
        if dup_ratio > 0.01:
            recommendations.append(
                {
                    "priority": "medium",
                    "action": "Remove duplicates",
                    "suggestion": "Consider removing duplicate rows to improve analysis accuracy",
                }
            )

        outlier_issues = [i for i in self.issues if i["category"] == "outliers"]
        if outlier_issues:
            recommendations.append(
                {
                    "priority": "medium",
                    "action": "Investigate outliers",
                    "suggestion": "Review outliers - they may be data entry errors or genuine anomalies",
                }
            )

        if len(self.df) > SAMPLE_SIZE_FOR_LARGE_DATA:
            recommendations.append(
                {
                    "priority": "low",
                    "action": "Consider sampling",
                    "suggestion": f"Dataset has {len(self.df)} rows. Consider sampling for faster ML training",
                }
            )

        if self.quality_score < 50:
            recommendations.append(
                {
                    "priority": "high",
                    "action": "Data cleaning required",
                    "suggestion": "Low quality score indicates significant data issues that should be addressed",
                }
            )

        return recommendations


def assess_data_quality(data: list[dict[str, Any]] | pd.DataFrame) -> dict[str, Any]:
    """Convenience function to assess data quality."""
    engine = DataQualityEngine(data)
    return engine.get_report()
