"""Unit tests for forecasting service."""

from __future__ import annotations

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

from app.services.forecasting import (
    run_forecasting,
    TimeSeriesForecaster,
    _fallback_linear_forecast,
    _build_future_index,
)


@pytest.fixture
def sample_timeseries_data():
    """Generate sample time series data."""
    dates = pd.date_range(start='2024-01-01', periods=100, freq='D')
    values = np.sin(np.linspace(0, 4 * np.pi, 100)) * 100 + 500 + np.random.normal(0, 10, 100)
    df = pd.DataFrame({
        'date': dates,
        'sales': values
    })
    return df


@pytest.fixture
def sample_dataframe(sample_timeseries_data):
    """Create a properly formatted dataset."""
    return sample_timeseries_data


class TestForecastingHelpers:
    """Test helper functions for forecasting."""

    def test_fallback_linear_forecast(self):
        """Test linear fallback forecast."""
        values = np.array([100, 110, 120, 130, 140])
        forecast = _fallback_linear_forecast(values, periods=5)
        
        assert len(forecast) == 5
        assert forecast[0] > values[-1]  # Should trend upward
        assert all(np.isfinite(forecast))  # No NaN or inf

    def test_fallback_linear_forecast_with_noise(self):
        """Test linear forecast with noisy data."""
        np.random.seed(42)
        values = np.linspace(100, 200, 50) + np.random.normal(0, 5, 50)
        forecast = _fallback_linear_forecast(values, periods=10)
        
        assert len(forecast) == 10
        assert all(np.isfinite(forecast))

    def test_build_future_index_daily(self):
        """Test building future date index for daily data."""
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        series = pd.Series(range(30), index=dates)
        future_index = _build_future_index(series, periods=7)
        
        assert len(future_index) == 7
        assert future_index[0] == dates[-1] + timedelta(days=1)
        assert future_index[-1] == dates[-1] + timedelta(days=7)

    def test_build_future_index_weekly(self):
        """Test building future date index for weekly data."""
        dates = pd.date_range('2024-01-01', periods=52, freq='W')
        series = pd.Series(range(52), index=dates)
        future_index = _build_future_index(series, periods=4)
        
        assert len(future_index) == 4


class TestTimeSeriesForecaster:
    """Test TimeSeriesForecaster class."""

    def test_forecaster_initialization(self, sample_dataframe):
        """Test forecaster initialization."""
        forecaster = TimeSeriesForecaster(
            sample_dataframe,
            datetime_col='date',
            metric_col='sales',
            dataset_id='test_123'
        )
        
        assert forecaster.dataset_id == 'test_123'
        assert forecaster.datetime_col == 'date'
        assert forecaster.metric_col == 'sales'
        assert len(forecaster.df) == len(sample_dataframe)

    def test_forecaster_linear_trend_detection(self, sample_dataframe):
        """Test linear trend detection."""
        forecaster = TimeSeriesForecaster(
            sample_dataframe,
            datetime_col='date',
            metric_col='sales',
            dataset_id='test_123'
        )
        
        forecast = [100, 110, 120, 130, 140]
        trend = forecaster._detect_trend(forecast)
        assert trend == "increasing"
        
        forecast_decreasing = [140, 130, 120, 110, 100]
        trend = forecaster._detect_trend(forecast_decreasing)
        assert trend == "decreasing"

    def test_forecaster_flat_trend(self, sample_dataframe):
        """Test flat trend detection."""
        forecaster = TimeSeriesForecaster(
            sample_dataframe,
            datetime_col='date',
            metric_col='sales',
            dataset_id='test_123'
        )
        
        forecast = [100, 101, 100, 99, 101]
        trend = forecaster._detect_trend(forecast)
        assert trend == "flat"

    def test_seasonal_period_detection(self, sample_dataframe):
        """Test seasonal period detection."""
        forecaster = TimeSeriesForecaster(
            sample_dataframe,
            datetime_col='date',
            metric_col='sales',
            dataset_id='test_123'
        )
        
        seasonal_period = forecaster._detect_seasonal_period()
        assert seasonal_period in [1, 7, 12]  # Daily data should return 7

    def test_seasonality_detection(self, sample_dataframe):
        """Test seasonality detection."""
        forecaster = TimeSeriesForecaster(
            sample_dataframe,
            datetime_col='date',
            metric_col='sales',
            dataset_id='test_123'
        )
        
        has_seasonality = forecaster._detect_seasonality()
        assert isinstance(has_seasonality, bool)


@pytest.mark.asyncio
async def test_run_forecasting_basic(sample_dataframe):
    """Test basic forecasting run."""
    result = await run_forecasting(
        sample_dataframe,
        dataset_id='test_123',
        options={'forecast_periods': 30},
        correlation_id='corr_123'
    )
    
    assert result['status'] == 'SUCCESS'
    assert 'model' in result
    assert 'results' in result
    assert len(result['results']['forecast_points']) == 30
    assert 'warnings' in result


@pytest.mark.asyncio
async def test_run_forecasting_with_missing_values():
    """Test forecasting with missing values."""
    dates = pd.date_range('2024-01-01', periods=50, freq='D')
    values = np.linspace(100, 200, 50)
    values[10:15] = np.nan  # Add missing values
    
    df = pd.DataFrame({
        'date': dates,
        'metrics': values
    })
    
    result = await run_forecasting(
        df,
        dataset_id='test_missing',
        options={'forecast_periods': 10},
    )
    
    assert result['status'] == 'SUCCESS'
    assert 'forecast_points' in result['results']


@pytest.mark.asyncio
async def test_run_forecasting_short_period():
    """Test forecasting with short period."""
    dates = pd.date_range('2024-01-01', periods=10, freq='D')
    values = np.linspace(100, 150, 10)
    
    df = pd.DataFrame({
        'date': dates,
        'amount': values
    })
    
    result = await run_forecasting(
        df,
        dataset_id='test_short',
        options={'forecast_periods': 5},
    )
    
    assert result['status'] == 'SUCCESS'
    assert len(result['results']['forecast_points']) == 5


@pytest.mark.asyncio
async def test_run_forecasting_custom_periods():
    """Test forecasting with custom periods."""
    for periods in [5, 10, 30, 60, 90]:
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        values = 100 + np.sin(np.linspace(0, 4 * np.pi, 100)) * 50
        
        df = pd.DataFrame({
            'date': dates,
            'revenue': values
        })
        
        result = await run_forecasting(
            df,
            dataset_id=f'test_{periods}',
            options={'forecast_periods': periods},
        )
        
        assert result['status'] == 'SUCCESS'
        assert len(result['results']['forecast_points']) == periods


class TestForecastingEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.asyncio
    async def test_forecasting_constant_values(self):
        """Test forecasting with constant values."""
        dates = pd.date_range('2024-01-01', periods=30, freq='D')
        df = pd.DataFrame({
            'date': dates,
            'value': [100] * 30
        })
        
        result = await run_forecasting(
            df,
            dataset_id='test_constant',
            options={'forecast_periods': 10},
        )
        
        assert result['status'] == 'SUCCESS'

    @pytest.mark.asyncio
    async def test_forecasting_highly_volatile_data(self):
        """Test forecasting with highly volatile data."""
        np.random.seed(42)
        dates = pd.date_range('2024-01-01', periods=50, freq='D')
        values = np.random.normal(100, 50, 50)  # High volatility
        
        df = pd.DataFrame({
            'date': dates,
            'volatile': values
        })
        
        result = await run_forecasting(
            df,
            dataset_id='test_volatile',
            options={'forecast_periods': 15},
        )
        
        assert result['status'] == 'SUCCESS'

    @pytest.mark.asyncio
    async def test_forecasting_trend_only_data(self):
        """Test forecasting with strong uptrend."""
        dates = pd.date_range('2024-01-01', periods=60, freq='D')
        values = np.linspace(100, 300, 60)  # Strong uptrend
        
        df = pd.DataFrame({
            'date': dates,
            'trending': values
        })
        
        result = await run_forecasting(
            df,
            dataset_id='test_trend',
            options={'forecast_periods': 20},
        )
        
        assert result['status'] == 'SUCCESS'
        # First forecast should be > last actual
        first_forecast = result['results']['forecast_points'][0]['forecast']
        last_actual = result['results']['forecast_points'][-1]['forecast']
        assert first_forecast > 100  # Should continue uptrend


class TestForecastingOutputValidation:
    """Test output validation and structure."""

    @pytest.mark.asyncio
    async def test_forecast_output_structure(self):
        """Test that forecast output has required fields."""
        dates = pd.date_range('2024-01-01', periods=40, freq='D')
        values = np.sin(np.linspace(0, 4 * np.pi, 40)) * 100 + 500
        
        df = pd.DataFrame({
            'date': dates,
            'sales': values
        })
        
        result = await run_forecasting(
            df,
            dataset_id='test_output',
            options={'forecast_periods': 10},
        )
        
        assert 'status' in result
        assert 'model' in result
        assert 'results' in result
        assert 'confidence' in result
        
        # Check forecast points structure
        for point in result['results']['forecast_points']:
            assert 'timestamp' in point
            assert 'forecast' in point
            assert 'lower_bound' in point
            assert 'upper_bound' in point
            assert point['lower_bound'] <= point['forecast'] <= point['upper_bound']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
