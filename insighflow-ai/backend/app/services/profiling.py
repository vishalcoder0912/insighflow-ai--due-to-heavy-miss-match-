"""Data Profiling Engine - Comprehensive column analysis."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

try:
    from sklearn.cluster import KMeans
except ImportError:
    KMeans = None


class DataProfiler:
    """Comprehensive data profiling engine."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data
        self.profiles: dict[str, dict[str, Any]] = {}
        self._profile_all()

    def _profile_all(self) -> None:
        """Profile all columns."""
        for col in self.df.columns:
            self.profiles[col] = self._profile_column(col)

    def _profile_column(self, col: str) -> dict[str, Any]:
        """Profile a single column."""
        series = self.df[col]
        profile = {
            "name": col,
            "data_type": str(series.dtype),
            "total_count": len(series),
            "null_count": int(series.isna().sum()),
            "null_percentage": round(float(series.isna().mean() * 100), 2),
            "unique_count": int(series.nunique()),
        }

        inferred_type = self._infer_type(series, col)
        profile["inferred_type"] = inferred_type

        if inferred_type == "numeric":
            profile.update(self._profile_numeric(series))
        elif inferred_type == "datetime":
            profile.update(self._profile_datetime(series))
        elif inferred_type == "categorical":
            profile.update(self._profile_categorical(series))
        elif inferred_type == "boolean":
            profile.update(self._profile_boolean(series))

        profile["sample_values"] = series.dropna().head(5).tolist()

        return profile

    def _infer_type(self, series: pd.Series, col_name: str) -> str:
        """Infer column type."""
        col_lower = col_name.lower()

        if pd.api.types.is_datetime64_any_dtype(series):
            return "datetime"

        numeric_ratio = pd.to_numeric(series, errors="coerce").notna().mean()
        if numeric_ratio > 0.8:
            return "numeric"

        bool_patterns = {"true", "false", "yes", "no", "1", "0", "y", "n"}
        if series.dtype == "object":
            lowered = series.astype(str).str.lower().str.strip()
            if lowered.isin(bool_patterns).mean() > 0.9:
                return "boolean"

        if any(
            kw in col_lower
            for kw in ["date", "time", "timestamp", "created", "updated"]
        ):
            try:
                if pd.to_datetime(series, errors="coerce").notna().mean() > 0.5:
                    return "datetime"
            except Exception:
                pass

        return "categorical"

    def _profile_numeric(self, series: pd.Series) -> dict[str, Any]:
        """Profile numeric column."""
        numeric = pd.to_numeric(series, errors="coerce").dropna()

        if numeric.empty:
            return {}

        result = {
            "min": float(numeric.min()),
            "max": float(numeric.max()),
            "mean": round(float(numeric.mean()), 4),
            "median": round(float(numeric.median()), 4),
            "std": round(float(numeric.std()), 4) if len(numeric) > 1 else 0,
            "variance": round(float(numeric.var()), 4) if len(numeric) > 1 else 0,
            "q1": round(float(numeric.quantile(0.25)), 4),
            "q3": round(float(numeric.quantile(0.75)), 4),
            "iqr": round(float(numeric.quantile(0.75) - numeric.quantile(0.25)), 4),
        }

        result["skewness"] = round(float(numeric.skew()), 4) if len(numeric) > 2 else 0
        result["kurtosis"] = (
            round(float(numeric.kurtosis()), 4) if len(numeric) > 3 else 0
        )

        q1, q3 = result["q1"], result["q3"]
        iqr = result["iqr"]
        outliers = numeric[(numeric < q1 - 1.5 * iqr) | (numeric > q3 + 1.5 * iqr)]
        result["outlier_count"] = int(len(outliers))
        result["outlier_percentage"] = (
            round(len(outliers) / len(numeric) * 100, 2) if len(numeric) > 0 else 0
        )

        result["is_integer"] = bool(np.allclose(numeric % 1, 0))
        result["is_positive"] = bool((numeric >= 0).all())
        result["range"] = result["max"] - result["min"]

        try:
            if len(numeric) >= 8:
                _, p_value = stats.normaltest(numeric)
                result["normality_p_value"] = round(float(p_value), 4)
                result["is_normal"] = p_value > 0.05
        except Exception:
            pass

        return result

    def _profile_datetime(self, series: pd.Series) -> dict[str, Any]:
        """Profile datetime column."""
        timestamps = pd.to_datetime(series, errors="coerce").dropna()

        if timestamps.empty:
            return {}

        result = {
            "min": str(timestamps.min()),
            "max": str(timestamps.max()),
            "range_days": int((timestamps.max() - timestamps.min()).days),
        }

        sorted_ts = timestamps.sort_values()
        if len(sorted_ts) > 1:
            diffs = sorted_ts.diff().dropna().dt.total_seconds() / 86400
            result["median_interval_days"] = round(float(diffs.median()), 2)

        result["year_distribution"] = timestamps.dt.year.value_counts().to_dict()
        result["month_distribution"] = timestamps.dt.month.value_counts().to_dict()
        result["day_of_week_distribution"] = (
            timestamps.dt.dayofweek.value_counts().to_dict()
        )

        return result

    def _profile_categorical(self, series: pd.Series) -> dict[str, Any]:
        """Profile categorical column."""
        text_values = series.dropna().astype(str)

        if text_values.empty:
            return {}

        value_counts = text_values.value_counts()
        total = len(text_values)

        result = {
            "unique_count": int(text_values.nunique()),
            "most_common": value_counts.head(5).to_dict(),
            "average_length": round(float(text_values.str.len().mean()), 2),
            "max_length": int(text_values.str.len().max()),
            "min_length": int(text_values.str.len().min()),
        }

        if total > 0:
            probs = value_counts / total
            result["entropy"] = round(float(-(probs * np.log2(probs + 1e-10)).sum()), 4)

        cardinality_ratio = result["unique_count"] / total if total > 0 else 0
        result["cardinality"] = (
            "low"
            if cardinality_ratio < 0.1
            else "medium"
            if cardinality_ratio < 0.5
            else "high"
        )

        return result

    def _profile_boolean(self, series: pd.Series) -> dict[str, Any]:
        """Profile boolean column."""
        bool_map = {
            "true": True,
            "yes": True,
            "1": True,
            "y": True,
            "false": False,
            "no": False,
            "0": False,
            "n": False,
        }

        lowered = series.astype(str).str.lower().str.strip()
        bool_values = lowered.map(bool_map)
        bool_values = bool_values[bool_values.notna()]

        if bool_values.empty:
            return {}

        result = {
            "true_count": int(bool_values.sum()),
            "false_count": int((~bool_values).sum()),
            "true_percentage": round(bool_values.mean() * 100, 2),
            "false_percentage": round((1 - bool_values.mean()) * 100, 2),
        }

        return result

    def get_profile(self, column: str) -> dict[str, Any]:
        """Get profile for a specific column."""
        return self.profiles.get(column, {})

    def get_all_profiles(self) -> list[dict[str, Any]]:
        """Get all column profiles."""
        return list(self.profiles.values())

    def get_summary(self) -> dict[str, Any]:
        """Get overall dataset summary."""
        return {
            "total_rows": len(self.df),
            "total_columns": len(self.df.columns),
            "column_profiles": self.get_all_profiles(),
            "memory_usage_bytes": int(self.df.memory_usage(deep=True).sum()),
            "completeness": round((1 - self.df.isna().mean().mean()) * 100, 2),
        }


def profile_data(data: list[dict[str, Any]] | pd.DataFrame) -> dict[str, Any]:
    """Convenience function for profiling data."""
    profiler = DataProfiler(data)
    return profiler.get_summary()
