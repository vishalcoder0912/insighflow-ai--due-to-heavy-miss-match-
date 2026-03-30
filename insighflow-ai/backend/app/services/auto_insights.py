"""Auto-Insight Engine - Intelligent analytics insights generator."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

try:
    from sklearn.ensemble import IsolationForest
except ImportError:
    IsolationForest = None


class AutoInsightEngine:
    """Automatic insight generation engine."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data

        self.insights: list[dict[str, Any]] = []
        self.kpis: list[dict[str, Any]] = []
        self._generate_insights()

    def _generate_insights(self) -> None:
        """Generate all insights."""
        self._generate_kpis()
        self._generate_trend_insights()
        self._generate_anomaly_insights()
        self._generate_correlation_insights()
        self._generate_distribution_insights()
        self._generate_comparison_insights()

    def _generate_kpis(self) -> None:
        """Generate KPI summaries."""
        self.kpis.append(
            {
                "id": "total_records",
                "title": "Total Records",
                "value": len(self.df),
                "format": "number",
                "icon": "database",
            }
        )

        numeric_cols = self.df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols[:5]:
            series = pd.to_numeric(self.df[col], errors="coerce").dropna()
            if not series.empty:
                self.kpis.append(
                    {
                        "id": f"sum_{col}",
                        "title": f"Total {col.title()}",
                        "value": round(float(series.sum()), 2),
                        "format": "number",
                        "icon": "calculator",
                    }
                )

                self.kpis.append(
                    {
                        "id": f"avg_{col}",
                        "title": f"Avg {col.title()}",
                        "value": round(float(series.mean()), 2),
                        "format": "decimal",
                        "icon": "trending",
                    }
                )

        self.kpis = self.kpis[:8]

    def _generate_trend_insights(self) -> None:
        """Generate trend-related insights."""
        datetime_cols = []
        for col in self.df.columns:
            if pd.api.types.is_datetime64_any_dtype(self.df[col]):
                datetime_cols.append(col)
            elif "date" in col.lower() or "time" in col.lower():
                try:
                    converted = pd.to_datetime(self.df[col], errors="coerce")
                    if converted.notna().mean() > 0.5:
                        datetime_cols.append(col)
                except Exception:
                    pass

        numeric_cols = self.df.select_dtypes(include=[np.number]).columns

        for dt_col in datetime_cols[:1]:
            for num_col in numeric_cols[:3]:
                try:
                    temp_df = (
                        pd.DataFrame(
                            {
                                dt_col: pd.to_datetime(
                                    self.df[dt_col], errors="coerce"
                                ),
                                num_col: pd.to_numeric(
                                    self.df[num_col], errors="coerce"
                                ),
                            }
                        )
                        .dropna()
                        .sort_values(dt_col)
                    )

                    if len(temp_df) < 10:
                        continue

                    x = np.arange(len(temp_df))
                    y = temp_df[num_col].values

                    slope = np.polyfit(x, y, 1)[0]
                    percent_change = (y[-1] - y[0]) / y[0] * 100 if y[0] != 0 else 0

                    if abs(percent_change) > 20:
                        direction = "increased" if percent_change > 0 else "decreased"
                        self.insights.append(
                            {
                                "type": "trend",
                                "severity": "high"
                                if abs(percent_change) > 50
                                else "medium",
                                "title": f"{num_col.title()} Trend",
                                "description": f"{num_col.title()} has {direction} by {abs(percent_change):.1f}% over the time period.",
                                "metric": num_col,
                                "value": round(percent_change, 1),
                                "unit": "%",
                            }
                        )

                except Exception as e:
                    logger.debug(f"Trend analysis failed for {dt_col}/{num_col}: {e}")

    def _generate_anomaly_insights(self) -> None:
        """Generate anomaly insights."""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols[:3]:
            try:
                series = pd.to_numeric(self.df[col], errors="coerce").dropna()

                if len(series) < 20:
                    continue

                q1, q3 = series.quantile(0.25), series.quantile(0.75)
                iqr = q3 - q1

                if iqr == 0:
                    continue

                lower, upper = q1 - 1.5 * iqr, q3 + 1.5 * iqr
                outliers = series[(series < lower) | (series > upper)]

                if len(outliers) > 0:
                    outlier_pct = len(outliers) / len(series) * 100

                    if outlier_pct > 5:
                        self.insights.append(
                            {
                                "type": "anomaly",
                                "severity": "medium",
                                "title": f"Anomalies in {col.title()}",
                                "description": f"Found {len(outliers)} outliers ({outlier_pct:.1f}%) in {col.title()}. These values deviate significantly from the norm.",
                                "metric": col,
                                "value": len(outliers),
                                "unit": "records",
                            }
                        )

            except Exception as e:
                logger.debug(f"Anomaly detection failed for {col}: {e}")

    def _generate_correlation_insights(self) -> None:
        """Generate correlation insights."""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns

        if len(numeric_cols) < 2:
            return

        try:
            corr_matrix = self.df[list(numeric_cols)].corr()

            strong_correlations = []
            for i, col1 in enumerate(corr_matrix.columns):
                for j, col2 in enumerate(corr_matrix.columns):
                    if i < j:
                        corr_val = corr_matrix.loc[col1, col2]
                        if pd.notna(corr_val) and abs(corr_val) > 0.7:
                            strong_correlations.append((col1, col2, corr_val))

            for col1, col2, corr_val in strong_correlations[:3]:
                direction = "positive" if corr_val > 0 else "negative"
                strength = "strong" if abs(corr_val) > 0.8 else "moderate"

                self.insights.append(
                    {
                        "type": "correlation",
                        "severity": "high" if abs(corr_val) > 0.9 else "medium",
                        "title": f"Correlation: {col1.title()} & {col2.title()}",
                        "description": f"There is a {strength} {direction} correlation ({corr_val:.2f}) between {col1.title()} and {col2.title()}.",
                        "metric": f"{col1} vs {col2}",
                        "value": round(corr_val, 2),
                        "unit": "",
                    }
                )

        except Exception as e:
            logger.debug(f"Correlation analysis failed: {e}")

    def _generate_distribution_insights(self) -> None:
        """Generate distribution insights."""
        categorical_cols = self.df.select_dtypes(include=["object", "category"]).columns

        for col in categorical_cols[:3]:
            try:
                value_counts = self.df[col].value_counts()

                if len(value_counts) <= 10:
                    top_value = value_counts.index[0]
                    top_pct = value_counts.iloc[0] / len(self.df) * 100

                    if top_pct > 50:
                        self.insights.append(
                            {
                                "type": "distribution",
                                "severity": "low",
                                "title": f"Dominant Category in {col.title()}",
                                "description": f"'{top_value}' dominates with {top_pct:.1f}% of all {col.title()} values.",
                                "metric": col,
                                "value": top_value,
                                "unit": f"{top_pct:.1f}%",
                            }
                        )

            except Exception as e:
                logger.debug(f"Distribution analysis failed for {col}: {e}")

    def _generate_comparison_insights(self) -> None:
        """Generate comparison insights."""
        categorical_cols = self.df.select_dtypes(include=["object", "category"]).columns
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns

        for cat_col in categorical_cols[:2]:
            for num_col in numeric_cols[:2]:
                try:
                    grouped = (
                        self.df.groupby(cat_col)[num_col]
                        .mean()
                        .sort_values(ascending=False)
                    )

                    if len(grouped) >= 2:
                        top = grouped.index[0]
                        bottom = grouped.index[-1]
                        difference = (
                            (grouped.iloc[0] - grouped.iloc[-1])
                            / grouped.iloc[-1]
                            * 100
                            if grouped.iloc[-1] != 0
                            else 0
                        )

                        if abs(difference) > 50:
                            self.insights.append(
                                {
                                    "type": "comparison",
                                    "severity": "medium",
                                    "title": f"{cat_col.title()} Comparison",
                                    "description": f"{top} has {abs(difference):.1f}% higher {num_col} than {bottom}.",
                                    "metric": f"{cat_col} -> {num_col}",
                                    "value": round(difference, 1),
                                    "unit": "%",
                                }
                            )

                except Exception as e:
                    logger.debug(f"Comparison analysis failed: {e}")

    def get_insights(self) -> list[dict[str, Any]]:
        """Get all generated insights."""
        return self.insights

    def get_kpis(self) -> list[dict[str, Any]]:
        """Get all generated KPIs."""
        return self.kpis

    def get_suggested_questions(self) -> list[str]:
        """Get suggested questions based on data."""
        suggestions = []

        datetime_cols = [
            c for c in self.df.columns if "date" in c.lower() or "time" in c.lower()
        ]
        numeric_cols = list(self.df.select_dtypes(include=[np.number]).columns)
        categorical_cols = list(
            self.df.select_dtypes(include=["object", "category"]).columns
        )

        if datetime_cols and numeric_cols:
            suggestions.append(f"Show {numeric_cols[0]} trend over time")
            suggestions.append(f"Compare {numeric_cols[0]} by month")

        if len(numeric_cols) >= 2:
            suggestions.append(f"What correlates with {numeric_cols[0]}?")

        if categorical_cols and numeric_cols:
            suggestions.append(f"Show {numeric_cols[0]} by {categorical_cols[0]}")
            suggestions.append(f"Top 10 by {numeric_cols[0]}")

        if numeric_cols:
            suggestions.append(f"Find anomalies in {numeric_cols[0]}")
            suggestions.append(f"Show distribution of {numeric_cols[0]}")

        return suggestions[:6]

    def get_report(self) -> dict[str, Any]:
        """Get complete insight report."""
        return {
            "kpis": self.kpis,
            "insights": self.insights,
            "suggested_questions": self.get_suggested_questions(),
            "summary": {
                "total_insights": len(self.insights),
                "high_severity": sum(
                    1 for i in self.insights if i.get("severity") == "high"
                ),
                "medium_severity": sum(
                    1 for i in self.insights if i.get("severity") == "medium"
                ),
                "low_severity": sum(
                    1 for i in self.insights if i.get("severity") == "low"
                ),
            },
        }


def generate_auto_insights(data: list[dict[str, Any]] | pd.DataFrame) -> dict[str, Any]:
    """Convenience function to generate auto insights."""
    engine = AutoInsightEngine(data)
    return engine.get_report()
