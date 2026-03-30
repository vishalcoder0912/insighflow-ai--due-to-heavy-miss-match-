"""Analytics Engine - Anomaly Detection, Correlation, Forecasting."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats
from sqlalchemy.ext.asyncio import AsyncSession

from app.services.storage_engine import PostgresStorageEngine

logger = logging.getLogger(__name__)

MAX_ANALYSIS_ROWS = 5000


class AnalyticsEngine:
    """Analytics engine for statistical analysis."""

    def __init__(self, session: AsyncSession, dataset_id: int):
        self.session = session
        self.dataset_id = dataset_id
        self.storage = PostgresStorageEngine(session)

    async def detect_anomalies(
        self,
        column: str | None = None,
        method: str = "iqr",
        threshold: float = 1.5,
    ) -> dict[str, Any]:
        """Detect anomalies using IQR or Z-score method."""
        limit = MAX_ANALYSIS_ROWS

        if column:
            query = f'SELECT "{column}", * FROM dataset_{self.dataset_id} LIMIT {limit}'
        else:
            cols = await self.storage.get_table_columns(self.dataset_id)
            numeric_cols = [
                c["name"]
                for c in cols
                if c.get("dtype")
                in ("integer", "bigint", "double precision", "numeric")
            ]
            column = numeric_cols[0] if numeric_cols else None

            if not column:
                return {"status": "error", "message": "No numeric columns found"}

            query = f'SELECT "{column}", * FROM dataset_{self.dataset_id} LIMIT {limit}'

        result = await self.storage.execute_query(self.dataset_id, query)

        if result["status"] != "success" or not result["rows"]:
            return {"status": "error", "message": "Failed to fetch data"}

        df = pd.DataFrame(result["rows"])
        values = pd.to_numeric(df[column], errors="coerce").dropna()

        if len(values) < 3:
            return {
                "status": "error",
                "message": "Insufficient data for anomaly detection",
            }

        anomalies = {"iqr": [], "zscore": []}

        if method in ("iqr", "both"):
            q1 = values.quantile(0.25)
            q3 = values.quantile(0.75)
            iqr = q3 - q1
            lower_bound = q1 - threshold * iqr
            upper_bound = q3 + threshold * iqr

            iqr_anomalies = df[
                (pd.to_numeric(df[column], errors="coerce") < lower_bound)
                | (pd.to_numeric(df[column], errors="coerce") > upper_bound)
            ]
            anomalies["iqr"] = {
                "count": len(iqr_anomalies),
                "percentage": round(len(iqr_anomalies) / len(df) * 100, 2),
                "lower_bound": float(lower_bound),
                "upper_bound": float(upper_bound),
                "rows": iqr_anomalies.head(20).to_dict("records"),
            }

        if method in ("zscore", "both"):
            z_scores = np.abs(stats.zscore(values))
            zscore_anomalies = df[z_scores > threshold]
            anomalies["zscore"] = {
                "count": len(zscore_anomalies),
                "percentage": round(len(zscore_anomalies) / len(df) * 100, 2),
                "threshold": threshold,
                "rows": zscore_anomalies.head(20).to_dict("records"),
            }

        return {
            "status": "success",
            "column": column,
            "method": method,
            "total_rows": len(df),
            "anomalies": anomalies,
            "summary": {
                "mean": float(values.mean()),
                "std": float(values.std()),
                "median": float(values.median()),
                "min": float(values.min()),
                "max": float(values.max()),
            },
        }

    async def calculate_correlation(
        self,
        columns: list[str] | None = None,
    ) -> dict[str, Any]:
        """Calculate correlation matrix for numeric columns."""
        limit = MAX_ANALYSIS_ROWS

        result = await self.storage.execute_query(
            self.dataset_id, f"SELECT * FROM dataset_{self.dataset_id} LIMIT {limit}"
        )

        if result["status"] != "success" or not result["rows"]:
            return {"status": "error", "message": "Failed to fetch data"}

        df = pd.DataFrame(result["rows"])
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if not numeric_cols:
            return {"status": "error", "message": "No numeric columns found"}

        if columns:
            numeric_cols = [c for c in columns if c in numeric_cols]
            if not numeric_cols:
                return {"status": "error", "message": "No matching numeric columns"}

        corr_matrix = df[numeric_cols].corr()

        correlation_list = []
        for i, col1 in enumerate(numeric_cols):
            for j, col2 in enumerate(numeric_cols):
                if i < j:
                    corr_val = float(corr_matrix.loc[col1, col2])
                    if not np.isnan(corr_val):
                        correlation_list.append(
                            {
                                "column1": col1,
                                "column2": col2,
                                "correlation": corr_val,
                                "strength": self._interpret_correlation(corr_val),
                            }
                        )

        correlation_list.sort(key=lambda x: abs(x["correlation"]), reverse=True)

        return {
            "status": "success",
            "columns": numeric_cols,
            "matrix": corr_matrix.to_dict(),
            "top_correlations": correlation_list[:20],
            "summary": {
                "strong_positive": len(
                    [c for c in correlation_list if c["correlation"] > 0.7]
                ),
                "moderate_positive": len(
                    [c for c in correlation_list if 0.3 < c["correlation"] <= 0.7]
                ),
                "weak": len(
                    [c for c in correlation_list if -0.3 <= c["correlation"] <= 0.3]
                ),
                "moderate_negative": len(
                    [c for c in correlation_list if -0.7 <= c["correlation"] < -0.3]
                ),
                "strong_negative": len(
                    [c for c in correlation_list if c["correlation"] < -0.7]
                ),
            },
        }

    def _interpret_correlation(self, corr: float) -> str:
        """Interpret correlation strength."""
        abs_corr = abs(corr)
        if abs_corr > 0.7:
            return "strong"
        elif abs_corr > 0.3:
            return "moderate"
        else:
            return "weak"

    async def forecast(
        self,
        value_column: str,
        time_column: str | None = None,
        periods: int = 5,
    ) -> dict[str, Any]:
        """Generate simple forecast using moving average."""
        limit = min(MAX_ANALYSIS_ROWS, periods * 10 + 100)

        if time_column:
            query = f'SELECT "{time_column}", "{value_column}" FROM dataset_{self.dataset_id} ORDER BY "{time_column}" LIMIT {limit}'
        else:
            cols = await self.storage.get_table_columns(self.dataset_id)
            date_cols = [
                c["name"] for c in cols if "date" in c.get("dtype", "").lower()
            ]
            time_column = date_cols[0] if date_cols else None

            if time_column:
                query = f'SELECT "{time_column}", "{value_column}" FROM dataset_{self.dataset_id} LIMIT {limit}'
            else:
                query = f'SELECT "{value_column}" FROM dataset_{self.dataset_id} LIMIT {limit}'

        result = await self.storage.execute_query(self.dataset_id, query)

        if result["status"] != "success" or not result["rows"]:
            return {"status": "error", "message": "Failed to fetch data"}

        df = pd.DataFrame(result["rows"])

        if time_column and time_column in df.columns:
            df[time_column] = pd.to_datetime(df[time_column], errors="coerce")
            df = df.sort_values(time_column)

        values = pd.to_numeric(df[value_column], errors="coerce").dropna()

        if len(values) < 10:
            return {"status": "error", "message": "Insufficient data for forecasting"}

        window = min(7, len(values) // 3)
        moving_avg = values.rolling(window=window).mean()

        recent_avg = values.tail(window).mean()
        trend = (values.tail(window).mean() - values.tail(window * 2).mean()) / window

        forecast_values = []
        current_value = recent_avg
        for i in range(periods):
            current_value = current_value + trend
            forecast_values.append(float(current_value))

        return {
            "status": "success",
            "value_column": value_column,
            "time_column": time_column,
            "periods": periods,
            "historical": values.tail(20).tolist(),
            "forecast": forecast_values,
            "trend": float(trend),
            "method": "simple_moving_average",
            "window": window,
            "summary": {
                "last_value": float(values.iloc[-1]),
                "average": float(values.mean()),
                "predicted_change": float(forecast_values[-1] - values.iloc[-1]),
                "confidence": "low" if len(values) < 50 else "medium",
            },
        }

    async def get_insights(self) -> dict[str, Any]:
        """Generate automatic insights from dataset."""
        result = await self.storage.execute_query(
            self.dataset_id, f"SELECT * FROM dataset_{self.dataset_id} LIMIT 1000"
        )

        if result["status"] != "success" or not result["rows"]:
            return {"status": "error", "message": "Failed to fetch data"}

        df = pd.DataFrame(result["rows"])
        insights = []

        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        cat_cols = df.select_dtypes(include=["object"]).columns.tolist()

        if numeric_cols:
            for col in numeric_cols[:5]:
                values = pd.to_numeric(df[col], errors="coerce").dropna()
                if len(values) > 0:
                    q1, q3 = values.quantile(0.25), values.quantile(0.75)
                    iqr = q3 - q1
                    outliers = (
                        (values < q1 - 1.5 * iqr) | (values > q3 + 1.5 * iqr)
                    ).sum()

                    if outliers > 0:
                        insights.append(
                            {
                                "type": "anomaly_warning",
                                "column": col,
                                "message": f"Column '{col}' has {outliers} potential outliers ({outliers / len(values) * 100:.1f}%)",
                                "severity": "high"
                                if outliers / len(values) > 0.05
                                else "low",
                            }
                        )

        if cat_cols:
            for col in cat_cols[:3]:
                value_counts = df[col].value_counts()
                if len(value_counts) == len(df):
                    insights.append(
                        {
                            "type": "unique_identifier",
                            "column": col,
                            "message": f"Column '{col}' appears to be a unique identifier",
                        }
                    )
                elif len(value_counts) == 1:
                    insights.append(
                        {
                            "type": "constant_column",
                            "column": col,
                            "message": f"Column '{col}' has only one value across all records",
                        }
                    )

        total_missing = df.isna().sum().sum()
        missing_percent = total_missing / (len(df) * len(df.columns)) * 100
        if missing_percent > 5:
            insights.append(
                {
                    "type": "data_quality",
                    "message": f"Dataset has {missing_percent:.1f}% missing values",
                    "severity": "high" if missing_percent > 20 else "medium",
                }
            )

        if not insights:
            insights.append(
                {
                    "type": "general",
                    "message": "Data looks healthy with no significant issues detected",
                }
            )

        return {
            "status": "success",
            "insights": insights,
            "dataset_summary": {
                "total_rows": len(df),
                "total_columns": len(df.columns),
                "numeric_columns": len(numeric_cols),
                "categorical_columns": len(cat_cols),
                "memory_usage_mb": round(
                    df.memory_usage(deep=True).sum() / 1024 / 1024, 2
                ),
            },
        }
