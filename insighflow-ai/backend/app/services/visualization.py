"""Visualization Scoring Engine - Recommend best charts for data patterns."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

CHART_SCORES = {
    "line": {
        "time_series": 10,
        "trend": 10,
        "continuous": 8,
        "numeric_numeric": 6,
    },
    "bar": {
        "categorical_numeric": 10,
        "comparison": 9,
        "ranking": 8,
    },
    "horizontal_bar": {
        "categorical_numeric": 9,
        "long_labels": 10,
        "ranking": 9,
    },
    "area": {
        "time_series": 9,
        "cumulative": 8,
        "continuous": 7,
    },
    "scatter": {
        "numeric_numeric": 10,
        "correlation": 10,
        "distribution": 7,
    },
    "pie": {
        "categorical_part_to_whole": 9,
        "few_categories": 8,
        "distribution": 6,
    },
    "donut": {
        "categorical_part_to_whole": 8,
        "few_categories": 7,
        "modern_look": 8,
    },
    "histogram": {
        "distribution": 10,
        "numeric": 9,
        "frequency": 10,
    },
    "heatmap": {
        "correlation": 10,
        "matrix": 9,
        "density": 8,
    },
}


class VisualizationScorer:
    """Score and rank charts based on data patterns."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data

        self.numeric_cols = list(self.df.select_dtypes(include=["number"]).columns)
        self.categorical_cols = list(
            self.df.select_dtypes(include=["object", "category"]).columns
        )
        self.datetime_cols = [
            c
            for c in self.df.columns
            if pd.api.types.is_datetime64_any_dtype(self.df[c])
        ]

    def _get_column_type(self, col: str) -> str:
        """Get column type."""
        if col in self.datetime_cols or pd.api.types.is_datetime64_any_dtype(
            self.df[col]
        ):
            return "datetime"
        if col in self.numeric_cols:
            return "numeric"
        if col in self.categorical_cols:
            return "categorical"
        return "unknown"

    def _is_time_series(self, x_col: str, y_col: str | None = None) -> bool:
        """Check if data represents time series."""
        if x_col in self.datetime_cols:
            return True
        date_patterns = ["date", "time", "timestamp", "day", "month", "year", "week"]
        if any(p in x_col.lower() for p in date_patterns):
            return True
        return False

    def _is_categorical(self, col: str) -> bool:
        """Check if column is categorical."""
        if col in self.categorical_cols:
            return True
        if col in self.df.columns:
            unique_ratio = (
                self.df[col].nunique() / len(self.df) if len(self.df) > 0 else 0
            )
            if unique_ratio < 0.2:
                return True
        return False

    def _is_low_cardinality(self, col: str) -> bool:
        """Check if column has low cardinality."""
        n_unique = self.df[col].nunique()
        return n_unique <= 10

    def _score_line_chart(self, x_col: str, y_col: str | None = None) -> float:
        """Score line chart."""
        score = 5.0

        if y_col:
            if self._is_time_series(x_col, y_col):
                score += CHART_SCORES["line"]["time_series"]
            elif self._get_column_type(y_col) == "numeric":
                score += CHART_SCORES["line"]["continuous"]

        return score

    def _score_bar_chart(self, x_col: str, y_col: str | None = None) -> float:
        """Score bar chart."""
        score = 5.0

        if self._is_categorical(x_col):
            score += CHART_SCORES["bar"]["categorical_numeric"]
        else:
            score += CHART_SCORES["bar"]["comparison"]

        if y_col and self._get_column_type(y_col) == "numeric":
            score += 2

        return score

    def _score_horizontal_bar(self, x_col: str, y_col: str | None = None) -> float:
        """Score horizontal bar chart."""
        score = 5.0

        if self._is_categorical(x_col):
            if len(str(x_col)) > 15:
                score += CHART_SCORES["horizontal_bar"]["long_labels"]
            else:
                score += CHART_SCORES["horizontal_bar"]["categorical_numeric"]

        if y_col:
            score += 2

        return score

    def _score_area_chart(self, x_col: str, y_col: str | None = None) -> float:
        """Score area chart."""
        score = 4.0

        if self._is_time_series(x_col, y_col):
            score += CHART_SCORES["area"]["time_series"]
        elif y_col and self._get_column_type(y_col) == "numeric":
            score += CHART_SCORES["area"]["continuous"]

        return score

    def _score_scatter_chart(self, x_col: str, y_col: str | None = None) -> float:
        """Score scatter chart."""
        score = 4.0

        if x_col in self.numeric_cols and y_col in self.numeric_cols:
            score += CHART_SCORES["scatter"]["numeric_numeric"]

            corr = self.df[[x_col, y_col]].corr().iloc[0, 1]
            if not pd.isna(corr) and abs(corr) > 0.5:
                score += 3

        return score

    def _score_pie_chart(self, category_col: str) -> float:
        """Score pie chart."""
        score = 3.0

        if self._is_categorical(category_col) and self._is_low_cardinality(
            category_col
        ):
            score += CHART_SCORES["pie"]["few_categories"]

        return score

    def _score_histogram(self, col: str) -> float:
        """Score histogram."""
        score = 5.0

        if col in self.numeric_cols:
            score += CHART_SCORES["histogram"]["numeric"]

        return score

    def _score_heatmap(self, x_col: str, y_col: str | None = None) -> float:
        """Score heatmap."""
        score = 3.0

        if x_col in self.numeric_cols and y_col and y_col in self.numeric_cols:
            score += CHART_SCORES["heatmap"]["correlation"]

        return score

    def score_charts(self, top_n: int = 5) -> list[dict[str, Any]]:
        """Score and rank all possible charts."""
        scored_charts = []

        if self.datetime_cols and self.numeric_cols:
            for num_col in self.numeric_cols[:3]:
                scored_charts.append(
                    {
                        "chart_type": "line",
                        "title": f"{num_col} Over Time",
                        "x_field": self.datetime_cols[0],
                        "y_field": num_col,
                        "score": self._score_line_chart(self.datetime_cols[0], num_col),
                        "rationale": "Best for time series data",
                    }
                )

        if self.categorical_cols and self.numeric_cols:
            for cat_col in self.categorical_cols[:2]:
                for num_col in self.numeric_cols[:2]:
                    scored_charts.append(
                        {
                            "chart_type": "bar",
                            "title": f"{num_col} by {cat_col}",
                            "x_field": cat_col,
                            "y_field": num_col,
                            "score": self._score_bar_chart(cat_col, num_col),
                            "rationale": "Best for categorical comparison",
                        }
                    )

                    scored_charts.append(
                        {
                            "chart_type": "horizontal_bar",
                            "title": f"{num_col} by {cat_col}",
                            "x_field": num_col,
                            "y_field": cat_col,
                            "score": self._score_horizontal_bar(num_col, cat_col),
                            "rationale": "Best for ranking with long labels",
                        }
                    )

        if self.categorical_cols:
            for cat_col in self.categorical_cols[:2]:
                if self._is_low_cardinality(cat_col):
                    scored_charts.append(
                        {
                            "chart_type": "pie",
                            "title": f"Distribution of {cat_col}",
                            "category_field": cat_col,
                            "score": self._score_pie_chart(cat_col),
                            "rationale": "Best for part-to-whole comparison",
                        }
                    )

                    scored_charts.append(
                        {
                            "chart_type": "donut",
                            "title": f"{cat_col} Share",
                            "category_field": cat_col,
                            "score": self._score_pie_chart(cat_col) - 1,
                            "rationale": "Modern part-to-whole visualization",
                        }
                    )

        if len(self.numeric_cols) >= 2:
            for i, col1 in enumerate(self.numeric_cols[:3]):
                for col2 in self.numeric_cols[i + 1 : 4]:
                    scored_charts.append(
                        {
                            "chart_type": "scatter",
                            "title": f"{col1} vs {col2}",
                            "x_field": col1,
                            "y_field": col2,
                            "score": self._score_scatter_chart(col1, col2),
                            "rationale": "Best for correlation visualization",
                        }
                    )

        for col in self.numeric_cols[:3]:
            scored_charts.append(
                {
                    "chart_type": "histogram",
                    "title": f"Distribution of {col}",
                    "x_field": col,
                    "score": self._score_histogram(col),
                    "rationale": "Best for showing data distribution",
                }
            )

        scored_charts.sort(key=lambda x: x["score"], reverse=True)

        return scored_charts[:top_n]


def score_visualizations(
    data: list[dict[str, Any]] | pd.DataFrame, top_n: int = 5
) -> list[dict[str, Any]]:
    """Convenience function to score visualizations."""
    scorer = VisualizationScorer(data)
    return scorer.score_charts(top_n)
