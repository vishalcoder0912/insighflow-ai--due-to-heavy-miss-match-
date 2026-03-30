"""Time-series forecasting service."""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
from sklearn.metrics import mean_squared_error, mean_absolute_error

from app.services.error_handling import ConvergenceError, ModelTrainingError
from app.services.monitoring import (
    timed_operation,
    log_analysis_start,
    log_analysis_complete,
)
from app.services.validation import (
    PreparedDataset,
    infer_pandas_frequency,
    prepare_analysis_dataset,
)

FREQ_TO_SEASONAL = {"D": 7, "W": 4, "MS": 12, "QS": 4, "YS": 2}

pmdarima = None
ExponentialSmoothing = None
Prophet = None

try:
    import pmdarima
    from pmdarima import auto_arima
except ImportError:
    auto_arima = None

try:
    from statsmodels.tsa.holtwinters import ExponentialSmoothing
except ImportError:
    pass

try:
    from prophet import Prophet
except ImportError:
    pass


def _build_future_index(series: pd.Series, periods: int) -> pd.DatetimeIndex:
    frequency = infer_pandas_frequency(series)
    start = pd.to_datetime(series).max()
    return pd.date_range(start=start, periods=periods + 1, freq=frequency)[1:]


def _fallback_linear_forecast(values: np.ndarray, periods: int) -> np.ndarray:
    x_axis = np.arange(len(values))
    slope, intercept = np.polyfit(x_axis, values, deg=1)
    future_x = np.arange(len(values), len(values) + periods)
    return (slope * future_x) + intercept


@timed_operation("advanced_forecasting", target_ms=30000)
def run_forecasting(
    df: pd.DataFrame,
    *,
    dataset_id: str | int | None = None,
    options: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Forecast a numeric metric over time with fallbacks."""

    options = options or {}
    prepared: PreparedDataset = prepare_analysis_dataset(
        df,
        analysis_type="forecasting",
        dataset_id=dataset_id,
        options=options,
        correlation_id=correlation_id,
    )
    plan = prepared.plan
    periods = int(options.get("forecast_periods", 30))
    metric_column = plan["metric_column"]
    time_column = plan["datetime_column"]
    working = prepared.dataframe[[time_column, metric_column]].copy()
    working = (
        working.sort_values(time_column)
        .groupby(time_column, as_index=False)[metric_column]
        .sum()
    )

    pandas_freq = infer_pandas_frequency(working[time_column])
    seasonal_periods = FREQ_TO_SEASONAL.get(pandas_freq, 7)
    y = pd.to_numeric(working[metric_column], errors="coerce").to_numpy(dtype=float)
    future_index = _build_future_index(working[time_column], periods)
    warnings = list(prepared.warnings)
    model_name = "linear_trend"

    try:
        if auto_arima is not None and len(y) >= 24:
            seasonal = len(y) >= seasonal_periods * 2 and seasonal_periods > 1
            model = auto_arima(
                y,
                seasonal=seasonal,
                m=seasonal_periods if seasonal else 1,
                error_action="ignore",
                suppress_warnings=True,
            )
            forecast_values, intervals = model.predict(
                n_periods=periods, return_conf_int=True
            )
            lower = intervals[:, 0]
            upper = intervals[:, 1]
            model_name = "auto_arima"
        elif ExponentialSmoothing is not None and len(y) >= 12:
            seasonal = (
                "add"
                if len(y) >= seasonal_periods * 2 and seasonal_periods > 1
                else None
            )
            model = ExponentialSmoothing(
                y,
                trend="add",
                seasonal=seasonal,
                seasonal_periods=seasonal_periods if seasonal else None,
            )
            fit = model.fit(optimized=True)
            forecast_values = fit.forecast(periods)
            residual_std = float(np.std(fit.resid)) if len(fit.resid) else 0.0
            lower = forecast_values - (1.96 * residual_std)
            upper = forecast_values + (1.96 * residual_std)
            model_name = "exponential_smoothing"
        else:
            forecast_values = _fallback_linear_forecast(y, periods)
            residual_std = float(np.std(y - np.mean(y))) if len(y) else 0.0
            lower = forecast_values - (1.96 * residual_std)
            upper = forecast_values + (1.96 * residual_std)
            warnings.append(
                "Used linear trend fallback because advanced forecast libraries were unavailable."
            )
    except Exception as exc:
        warnings.append(
            f"Primary forecast model failed: {exc}. Falling back to linear trend."
        )
        forecast_values = _fallback_linear_forecast(y, periods)
        residual_std = float(np.std(y - np.mean(y))) if len(y) else 0.0
        lower = forecast_values - (1.96 * residual_std)
        upper = forecast_values + (1.96 * residual_std)
        model_name = "linear_trend"

    if len(forecast_values) != periods:
        raise ConvergenceError(
            message="Forecast model did not return the requested number of periods.",
            error_code="MOD_100",
            remediation=[
                "Retry with a shorter forecast horizon.",
                "Inspect source data frequency and completeness.",
            ],
            analysis_type="forecasting",
            dataset_id=dataset_id,
            correlation_id=correlation_id,
        )

    last_actual = float(y[-1])
    first_forecast = float(forecast_values[0])
    change_pct = (
        round(((first_forecast - last_actual) / last_actual) * 100, 4)
        if last_actual
        else None
    )
    trend = (
        "increasing"
        if first_forecast > last_actual
        else ("decreasing" if first_forecast < last_actual else "flat")
    )

    forecast_points = [
        {
            "timestamp": future_index[idx].isoformat(),
            "forecast": round(float(forecast_values[idx]), 4),
            "lower_bound": round(float(lower[idx]), 4),
            "upper_bound": round(float(upper[idx]), 4),
        }
        for idx in range(periods)
    ]

    return {
        "status": "SUCCESS",
        "confidence": "HIGH" if model_name != "linear_trend" else "MEDIUM",
        "analysis_type": "forecasting",
        "processed_rows": int(len(prepared.dataframe)),
        "total_rows": int(len(df)),
        "excluded_rows": prepared.excluded_rows,
        "exclusion_reasons": {"preprocessing": prepared.excluded_rows},
        "quality_score": prepared.validation["quality_metrics"]["overall_score"],
        "validation": prepared.validation,
        "missing_values_analysis": prepared.missing_values_analysis,
        "model": {
            "algorithm": model_name,
            "forecast_periods": periods,
            "frequency": pandas_freq,
            "seasonal_periods": seasonal_periods,
        },
        "results": {
            "metric_column": metric_column,
            "datetime_column": time_column,
            "trend": trend,
            "predicted_change_pct": change_pct,
            "seasonality_detected": seasonal_periods > 1,
            "forecast_points": forecast_points,
        },
        "warnings": warnings,
    }


class TimeSeriesForecaster:
    """Time series forecasting engine"""

    def __init__(
        self, df: pd.DataFrame, datetime_col: str, metric_col: str, dataset_id: str
    ):
        self.df = df[[datetime_col, metric_col]].copy()
        self.df[datetime_col] = pd.to_datetime(self.df[datetime_col])
        self.df = self.df.sort_values(datetime_col)
        self.datetime_col = datetime_col
        self.metric_col = metric_col
        self.dataset_id = dataset_id
        self.best_model = None
        self.best_model_name = None
        self.best_metrics = {}

    @timed_operation("Time Series Forecasting")
    def forecast(
        self,
        periods: int = 30,
        confidence_interval: float = 0.95,
        methods: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        """Generate time series forecast with best model"""

        log_analysis_start(self.dataset_id, "forecasting", len(self.df), 1)

        if methods is None:
            methods = ["prophet", "auto_arima", "exponential_smoothing", "linear_trend"]

        train_size = int(0.8 * len(self.df))
        train_df = self.df[:train_size].copy()
        test_df = self.df[train_size:].copy()

        model_results = {}
        for method in methods:
            try:
                if method == "prophet":
                    result = self._test_prophet(train_df, test_df)
                elif method == "auto_arima":
                    result = self._test_arima(train_df, test_df)
                elif method == "exponential_smoothing":
                    result = self._test_exponential_smoothing(train_df, test_df)
                elif method == "linear_trend":
                    result = self._test_linear_trend(train_df, test_df)
                else:
                    continue

                model_results[method] = result
            except Exception:
                pass

        best_result = None
        for method, result in model_results.items():
            if result and (best_result is None or result["rmse"] < best_result["rmse"]):
                best_result = result
                self.best_model_name = method
                self.best_model = result.get("model")

        if not best_result:
            raise ModelTrainingError(
                message="All forecasting models failed",
                error_code="MOD_001",
                severity="MEDIUM",
            )

        forecast_data = self._generate_forecast(
            periods, confidence_interval, self.best_model_name
        )

        log_analysis_complete(
            self.dataset_id,
            "forecasting",
            0,
            self.best_model_name,
            {"rmse": best_result["rmse"], "mae": best_result["mae"]},
        )

        return {
            "status": "SUCCESS",
            "forecast": forecast_data["forecast"],
            "confidence_intervals": forecast_data["confidence_intervals"],
            "model_used": self.best_model_name,
            "rmse": best_result["rmse"],
            "mae": best_result["mae"],
            "trend": self._detect_trend(forecast_data["forecast"]),
            "seasonality_detected": self._detect_seasonality(),
            "forecast_periods": periods,
        }

    def _test_arima(
        self, train_df: pd.DataFrame, test_df: pd.DataFrame
    ) -> Optional[Dict[str, float]]:
        """Test ARIMA model"""

        if auto_arima is None:
            return None

        try:
            model = auto_arima(
                train_df[self.metric_col],
                seasonal=False,
                stepwise=True,
                max_p=5,
                max_q=5,
                max_d=2,
                suppress_warnings=True,
            )

            y_pred = model.predict(n_periods=len(test_df))
            y_true = test_df[self.metric_col].values

            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            mae = mean_absolute_error(y_true, y_pred)

            return {"model": model, "rmse": rmse, "mae": mae, "type": "auto_arima"}
        except Exception:
            return None

    def _test_prophet(
        self, train_df: pd.DataFrame, test_df: pd.DataFrame
    ) -> Optional[Dict[str, float]]:
        """Test Prophet model"""

        if Prophet is None:
            return None

        try:
            prophet_df = train_df.copy()
            prophet_df.columns = ["ds", "y"]

            model = Prophet(
                yearly_seasonality=self._has_yearly_seasonality(),
                weekly_seasonality=True,
                interval_width=0.95,
            )
            model.fit(prophet_df)

            future = pd.DataFrame({"ds": test_df[self.datetime_col]})
            forecast = model.predict(future)

            y_true = test_df[self.metric_col].values
            y_pred = forecast["yhat"].values
            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            mae = mean_absolute_error(y_true, y_pred)

            return {"model": model, "rmse": rmse, "mae": mae, "type": "prophet"}
        except Exception:
            return None

    def _has_yearly_seasonality(self) -> bool:
        """Check if data has yearly seasonality"""
        if len(self.df) >= 365:
            return True
        return False

    def _test_exponential_smoothing(
        self, train_df: pd.DataFrame, test_df: pd.DataFrame
    ) -> Optional[Dict[str, float]]:
        """Test Exponential Smoothing model"""

        if ExponentialSmoothing is None:
            return None

        try:
            seasonal_periods = self._detect_seasonal_period()

            if seasonal_periods > 1:
                model = ExponentialSmoothing(
                    train_df[self.metric_col],
                    seasonal_periods=seasonal_periods,
                    trend="add",
                    seasonal="add",
                    initialization_method="estimated",
                )
            else:
                model = ExponentialSmoothing(
                    train_df[self.metric_col], trend="add", seasonal=None
                )

            fitted_model = model.fit(optimized=True)
            y_pred = fitted_model.forecast(steps=len(test_df))
            y_true = test_df[self.metric_col].values

            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            mae = mean_absolute_error(y_true, y_pred)

            return {
                "model": fitted_model,
                "rmse": rmse,
                "mae": mae,
                "type": "exponential_smoothing",
            }
        except Exception:
            return None

    def _test_linear_trend(
        self, train_df: pd.DataFrame, test_df: pd.DataFrame
    ) -> Optional[Dict[str, float]]:
        """Test Linear Trend model"""

        try:
            x_axis = np.arange(len(train_df))
            slope, intercept = np.polyfit(
                x_axis, train_df[self.metric_col].values, deg=1
            )

            future_x = np.arange(len(train_df), len(train_df) + len(test_df))
            y_pred = (slope * future_x) + intercept
            y_true = test_df[self.metric_col].values

            rmse = np.sqrt(mean_squared_error(y_true, y_pred))
            mae = mean_absolute_error(y_true, y_pred)

            return {
                "model": {"slope": slope, "intercept": intercept},
                "rmse": rmse,
                "mae": mae,
                "type": "linear_trend",
            }
        except Exception:
            return None

    def _generate_forecast(
        self, periods: int, confidence_interval: float, model_type: str
    ) -> Dict[str, Any]:
        """Generate forecast using best model"""

        if model_type == "prophet":
            return self._forecast_prophet(periods, confidence_interval)
        elif model_type == "auto_arima":
            return self._forecast_arima(periods, confidence_interval)
        elif model_type == "exponential_smoothing":
            return self._forecast_exponential_smoothing(periods, confidence_interval)
        else:
            return self._forecast_linear_trend(periods, confidence_interval)

    def _forecast_prophet(
        self, periods: int, confidence_interval: float
    ) -> Dict[str, Any]:
        """Generate Prophet forecast"""

        last_date = self.df[self.datetime_col].max()
        future_dates = pd.date_range(
            start=last_date + timedelta(days=1), periods=periods, freq="D"
        )
        future = pd.DataFrame({"ds": future_dates})

        if hasattr(self.best_model, "predict"):
            forecast = self.best_model.predict(future)
            return {
                "forecast": forecast["yhat"].values.tolist(),
                "confidence_intervals": {
                    "lower": forecast["yhat_lower"].values.tolist(),
                    "upper": forecast["yhat_upper"].values.tolist(),
                },
            }
        raise ValueError("Best model is not a valid Prophet model")

    def _forecast_arima(
        self, periods: int, confidence_interval: float
    ) -> Dict[str, Any]:
        """Generate ARIMA forecast"""

        if hasattr(self.best_model, "predict"):
            forecast = self.best_model.predict(n_periods=periods)
            forecast_values = (
                forecast.tolist() if hasattr(forecast, "tolist") else list(forecast)
            )
            return {
                "forecast": forecast_values,
                "confidence_intervals": {
                    "lower": [v * 0.9 for v in forecast_values],
                    "upper": [v * 1.1 for v in forecast_values],
                },
            }
        raise ValueError("Best model is not a valid ARIMA model")

    def _forecast_exponential_smoothing(
        self, periods: int, confidence_interval: float
    ) -> Dict[str, Any]:
        """Generate Exponential Smoothing forecast"""

        forecast = self.best_model.forecast(steps=periods)

        residual_std = np.std(
            self.df[self.metric_col].values - self.best_model.fittedvalues
        )
        z_score = 1.96
        margin = z_score * residual_std

        return {
            "forecast": forecast.values.tolist(),
            "confidence_intervals": {
                "lower": (forecast.values - margin).tolist(),
                "upper": (forecast.values + margin).tolist(),
            },
        }

    def _forecast_linear_trend(
        self, periods: int, confidence_interval: float
    ) -> Dict[str, Any]:
        """Generate Linear Trend forecast"""

        model = self.best_model
        x_axis = np.arange(len(self.df), len(self.df) + periods)
        forecast_values = (model["slope"] * x_axis) + model["intercept"]

        residual_std = np.std(
            self.df[self.metric_col].values
            - np.polyval([model["slope"], model["intercept"]], np.arange(len(self.df)))
        )
        z_score = 1.96
        margin = z_score * residual_std

        return {
            "forecast": forecast_values.tolist(),
            "confidence_intervals": {
                "lower": (forecast_values - margin).tolist(),
                "upper": (forecast_values + margin).tolist(),
            },
        }

    def _detect_trend(self, forecast: List[float]) -> str:
        """Detect trend direction"""

        if len(forecast) < 2:
            return "flat"

        change = forecast[-1] - forecast[0]
        pct_change = (change / abs(forecast[0])) * 100 if forecast[0] != 0 else 0

        if pct_change > 5:
            return "increasing"
        elif pct_change < -5:
            return "decreasing"
        else:
            return "flat"

    def _detect_seasonality(self) -> bool:
        """Detect if data has seasonality"""

        try:
            seasonal_period = self._detect_seasonal_period()
            return seasonal_period > 1
        except Exception:
            return False

    def _detect_seasonal_period(self) -> int:
        """Auto-detect seasonal period"""

        freq = pd.infer_freq(self.df[self.datetime_col])

        if freq and "D" in freq:
            return 7
        elif freq and "M" in freq:
            return 12
        else:
            return 1
