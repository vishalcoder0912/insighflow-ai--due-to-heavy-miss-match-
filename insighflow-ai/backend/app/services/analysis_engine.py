"""Analysis Engine - Summary, Trends, Correlation, Anomalies, Forecasting."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

try:
    from sklearn.linear_model import LinearRegression
    from sklearn.preprocessing import StandardScaler
except ImportError:
    LinearRegression = None
    StandardScaler = None

try:
    from sklearn.cluster import KMeans
except ImportError:
    KMeans = None


class AnalysisEngine:
    """Comprehensive data analysis engine."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data

        self.numeric_cols = self.df.select_dtypes(include=["number"]).columns.tolist()
        self.categorical_cols = self.df.select_dtypes(
            include=["object", "category"]
        ).columns.tolist()
        self.datetime_cols = [
            c
            for c in self.df.columns
            if pd.api.types.is_datetime64_any_dtype(self.df[c])
        ]

    def summary_statistics(self) -> dict[str, Any]:
        """Generate summary statistics."""
        try:
            if not self.numeric_cols:
                return {"error": "No numeric columns found"}

            numeric_df = self.df[self.numeric_cols]

            summary = {
                "total_rows": len(self.df),
                "total_columns": len(self.df.columns),
                "numeric_columns": self.numeric_cols,
                "categorical_columns": self.categorical_cols,
                "datetime_columns": self.datetime_cols,
                "descriptive_stats": {},
                "missing_values": {},
            }

            for col in self.numeric_cols:
                series = pd.to_numeric(self.df[col], errors="coerce").dropna()
                if not series.empty:
                    summary["descriptive_stats"][col] = {
                        "count": int(series.count()),
                        "mean": round(float(series.mean()), 4),
                        "std": round(float(series.std()), 4) if len(series) > 1 else 0,
                        "min": float(series.min()),
                        "max": float(series.max()),
                        "median": round(float(series.median()), 4),
                        "q25": round(float(series.quantile(0.25)), 4),
                        "q75": round(float(series.quantile(0.75)), 4),
                    }

            summary["missing_values"] = {
                col: int(self.df[col].isna().sum()) for col in self.df.columns
            }

            return summary

        except Exception as e:
            logger.error(f"Summary statistics error: {e}")
            return {"error": str(e)}

    def trend_analysis(
        self, column: str | None = None, date_column: str | None = None
    ) -> dict[str, Any]:
        """Analyze trends in the data."""
        try:
            if not self.numeric_cols:
                return {"error": "No numeric columns found"}

            target_col = column or self.numeric_cols[0]

            if date_column and date_column in self.df.columns:
                dt = pd.to_datetime(self.df[date_column], errors="coerce").dropna()
                if not dt.empty:
                    temp_df = pd.DataFrame(
                        {
                            date_column: dt,
                            target_col: pd.to_numeric(
                                self.df[target_col], errors="coerce"
                            ),
                        }
                    )
                    temp_df = temp_df.dropna().sort_values(date_column)

                    if len(temp_df) < 2:
                        return {"error": "Insufficient data for trend analysis"}

                    y = temp_df[target_col].values
                    x = np.arange(len(y))

                    if LinearRegression:
                        model = LinearRegression()
                        model.fit(x.reshape(-1, 1), y)
                        slope = float(model.coef_[0])
                        trend = "increasing" if slope > 0 else "decreasing"

                        return {
                            "column": target_col,
                            "date_column": date_column,
                            "trend": trend,
                            "slope": round(slope, 4),
                            "data_points": len(temp_df),
                            "min_value": float(y.min()),
                            "max_value": float(y.max()),
                            "avg_value": round(float(y.mean()), 4),
                        }

            series = pd.to_numeric(self.df[target_col], errors="coerce").dropna()
            if len(series) < 2:
                return {"error": "Insufficient data for trend analysis"}

            x = np.arange(len(series))
            if LinearRegression:
                model = LinearRegression()
                model.fit(x.reshape(-1, 1), series.values)
                slope = float(model.coef_[0])

                return {
                    "column": target_col,
                    "trend": "increasing" if slope > 0 else "decreasing",
                    "slope": round(slope, 4),
                    "data_points": len(series),
                    "min_value": float(series.min()),
                    "max_value": float(series.max()),
                    "avg_value": round(float(series.mean()), 4),
                }

            return {"error": "Linear regression not available"}

        except Exception as e:
            logger.error(f"Trend analysis error: {e}")
            return {"error": str(e)}

    def correlation_matrix(self) -> dict[str, Any]:
        """Generate correlation matrix."""
        try:
            if len(self.numeric_cols) < 2:
                return {"error": "Need at least 2 numeric columns"}

            numeric_df = self.df[self.numeric_cols].apply(
                pd.to_numeric, errors="coerce"
            )
            corr = numeric_df.corr()

            corr_dict = {}
            for col in corr.columns:
                corr_dict[col] = {c: round(v, 4) for c, v in corr[col].items()}

            strong_correlations = []
            for i, col1 in enumerate(corr.columns):
                for j, col2 in enumerate(corr.columns):
                    if i < j:
                        val = corr.loc[col1, col2]
                        if pd.notna(val) and abs(val) > 0.7:
                            strong_correlations.append(
                                {
                                    "column1": col1,
                                    "column2": col2,
                                    "correlation": round(float(val), 4),
                                    "strength": "strong_positive"
                                    if val > 0.7
                                    else "strong_negative",
                                }
                            )

            return {
                "correlation_matrix": corr_dict,
                "strong_correlations": strong_correlations,
                "columns_analyzed": self.numeric_cols,
            }

        except Exception as e:
            logger.error(f"Correlation analysis error: {e}")
            return {"error": str(e)}

    def anomaly_detection(
        self, column: str | None = None, method: str = "iqr"
    ) -> dict[str, Any]:
        """Detect anomalies in the data."""
        try:
            target_col = column or (self.numeric_cols[0] if self.numeric_cols else None)

            if not target_col:
                return {"error": "No numeric column found for anomaly detection"}

            series = pd.to_numeric(self.df[target_col], errors="coerce").dropna()

            if len(series) < 10:
                return {"error": "Insufficient data for anomaly detection"}

            results = {
                "column": target_col,
                "method": method,
                "total_points": len(series),
                "anomalies": [],
            }

            if method == "iqr":
                q1 = series.quantile(0.25)
                q3 = series.quantile(0.75)
                iqr = q3 - q1
                lower = q1 - 1.5 * iqr
                upper = q3 + 1.5 * iqr

                anomalies = series[(series < lower) | (series > upper)]
                results["method_description"] = "Interquartile Range (IQR) method"
                results["threshold_lower"] = float(lower)
                results["threshold_upper"] = float(upper)
                results["anomaly_count"] = len(anomalies)
                results["anomaly_percentage"] = round(
                    len(anomalies) / len(series) * 100, 2
                )
                results["anomaly_indices"] = anomalies.index.tolist()[:100]

            elif method == "zscore":
                z_scores = np.abs(stats.zscore(series))
                threshold = 3
                anomalies = series[z_scores > threshold]
                results["method_description"] = "Z-Score method (threshold=3)"
                results["threshold"] = threshold
                results["anomaly_count"] = len(anomalies)
                results["anomaly_percentage"] = round(
                    len(anomalies) / len(series) * 100, 2
                )
                results["anomaly_indices"] = anomalies.index.tolist()[:100]

            elif method == "isolation_forest" and KMeans is None:
                pass

            return results

        except Exception as e:
            logger.error(f"Anomaly detection error: {e}")
            return {"error": str(e)}

    def segmentation(self, n_clusters: int = 3) -> dict[str, Any]:
        """Perform K-Means clustering."""
        try:
            if not self.numeric_cols:
                return {"error": "No numeric columns found for segmentation"}

            numeric_df = self.df[self.numeric_cols].apply(
                pd.to_numeric, errors="coerce"
            )
            numeric_df = numeric_df.fillna(numeric_df.mean())

            if len(numeric_df) < n_clusters:
                return {"error": f"Need at least {n_clusters} rows for clustering"}

            if KMeans is None:
                return {"error": "KMeans not available"}

            kmeans = KMeans(n_clusters=n_clusters, random_state=42, n_init=10)
            clusters = kmeans.fit_predict(numeric_df)

            cluster_stats = {}
            for i in range(n_clusters):
                cluster_data = numeric_df[clusters == i]
                cluster_stats[f"cluster_{i}"] = {
                    "size": int((clusters == i).sum()),
                    "percentage": round((clusters == i).sum() / len(clusters) * 100, 2),
                    "centroid": {
                        col: round(float(kmeans.cluster_centers_[i][j]), 4)
                        for j, col in enumerate(self.numeric_cols)
                    },
                }

            return {
                "n_clusters": n_clusters,
                "features_used": self.numeric_cols,
                "cluster_stats": cluster_stats,
                "cluster_labels": [int(c) for c in clusters],
            }

        except Exception as e:
            logger.error(f"Segmentation error: {e}")
            return {"error": str(e)}

    def forecasting(
        self,
        column: str | None = None,
        date_column: str | None = None,
        periods: int = 5,
    ) -> dict[str, Any]:
        """Simple linear forecasting."""
        try:
            target_col = column or (self.numeric_cols[0] if self.numeric_cols else None)

            if not target_col:
                return {"error": "No numeric column found for forecasting"}

            if date_column and date_column in self.df.columns:
                dt = pd.to_datetime(self.df[date_column], errors="coerce")
                temp_df = (
                    pd.DataFrame(
                        {
                            date_column: dt,
                            target_col: pd.to_numeric(
                                self.df[target_col], errors="coerce"
                            ),
                        }
                    )
                    .dropna()
                    .sort_values(date_column)
                )

                if len(temp_df) < 3:
                    return {"error": "Insufficient data for forecasting"}

                y = temp_df[target_col].values
                x = np.arange(len(y)).reshape(-1, 1)

                if LinearRegression:
                    model = LinearRegression()
                    model.fit(x, y)

                    future_x = np.arange(len(y), len(y) + periods).reshape(-1, 1)
                    predictions = model.predict(future_x)

                    last_date = temp_df[date_column].iloc[-1]
                    freq = pd.infer_freq(temp_df[date_column]) or "D"
                    future_dates = pd.date_range(
                        start=last_date, periods=periods + 1, freq=freq
                    )[1:]

                    return {
                        "column": target_col,
                        "date_column": date_column,
                        "method": "Linear Regression",
                        "model_coefficient": round(float(model.coef_[0]), 4),
                        "model_intercept": round(float(model.intercept_), 4),
                        "r_squared": round(float(model.score(x, y)), 4),
                        "forecast": [
                            {
                                "date": str(future_dates[i].date()),
                                "predicted": round(float(predictions[i]), 4),
                            }
                            for i in range(len(predictions))
                        ],
                    }

            series = pd.to_numeric(self.df[target_col], errors="coerce").dropna()

            if len(series) < 3:
                return {"error": "Insufficient data for forecasting"}

            if LinearRegression:
                x = np.arange(len(series)).reshape(-1, 1)
                model = LinearRegression()
                model.fit(x, series.values)

                future_x = np.arange(len(series), len(series) + periods).reshape(-1, 1)
                predictions = model.predict(future_x)

                return {
                    "column": target_col,
                    "method": "Linear Regression",
                    "model_coefficient": round(float(model.coef_[0]), 4),
                    "model_intercept": round(float(model.intercept_), 4),
                    "r_squared": round(float(model.score(x, series.values)), 4),
                    "forecast": [
                        {"period": i + 1, "predicted": round(float(predictions[i]), 4)}
                        for i in range(len(predictions))
                    ],
                }

            return {"error": "Linear regression not available"}

        except Exception as e:
            logger.error(f"Forecasting error: {e}")
            return {"error": str(e)}


def analyze_summary(data: list[dict[str, Any]] | pd.DataFrame) -> dict[str, Any]:
    """Convenience function for summary analysis."""
    engine = AnalysisEngine(data)
    return engine.summary_statistics()


def analyze_trends(
    data: list[dict[str, Any]] | pd.DataFrame,
    column: str = None,
    date_column: str = None,
) -> dict[str, Any]:
    """Convenience function for trend analysis."""
    engine = AnalysisEngine(data)
    return engine.trend_analysis(column, date_column)


def analyze_correlations(data: list[dict[str, Any]] | pd.DataFrame) -> dict[str, Any]:
    """Convenience function for correlation analysis."""
    engine = AnalysisEngine(data)
    return engine.correlation_matrix()


def detect_anomalies(
    data: list[dict[str, Any]] | pd.DataFrame, column: str = None, method: str = "iqr"
) -> dict[str, Any]:
    """Convenience function for anomaly detection."""
    engine = AnalysisEngine(data)
    return engine.anomaly_detection(column, method)


def segment_data(
    data: list[dict[str, Any]] | pd.DataFrame, n_clusters: int = 3
) -> dict[str, Any]:
    """Convenience function for data segmentation."""
    engine = AnalysisEngine(data)
    return engine.segmentation(n_clusters)


def forecast_data(
    data: list[dict[str, Any]] | pd.DataFrame,
    column: str = None,
    date_column: str = None,
    periods: int = 5,
) -> dict[str, Any]:
    """Convenience function for forecasting."""
    engine = AnalysisEngine(data)
    return engine.forecasting(column, date_column, periods)
