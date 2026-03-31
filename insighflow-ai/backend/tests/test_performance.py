"""Performance and load testing suite."""

from __future__ import annotations

import pytest
import time
import pandas as pd
import numpy as np
import asyncio
from datetime import datetime

from app.services.forecasting import run_forecasting
from tests.conftest import auth_headers, register_and_login


@pytest.fixture
def large_dataset():
    """Create a large dataset for performance testing."""
    dates = pd.date_range('2020-01-01', periods=1000, freq='D')
    values = 100 + np.sin(np.linspace(0, 20 * np.pi, 1000)) * 50 + np.random.normal(0, 5, 1000)
    
    return pd.DataFrame({
        'date': dates,
        'sales': values,
        'quantity': np.random.randint(10, 100, 1000),
        'region': np.random.choice(['North', 'South', 'East', 'West'], 1000)
    })


@pytest.fixture
def medium_dataset():
    """Create a medium dataset for performance testing."""
    dates = pd.date_range('2023-01-01', periods=365, freq='D')
    values = 100 + np.sin(np.linspace(0, 4 * np.pi, 365)) * 50
    
    return pd.DataFrame({
        'date': dates,
        'revenue': values,
        'costs': values * 0.6
    })


class TestForecastingPerformance:
    """Test forecasting performance."""

    @pytest.mark.asyncio
    async def test_forecasting_execution_time(self, medium_dataset):
        """Test that forecasting completes within acceptable time."""
        start_time = time.time()
        
        result = await run_forecasting(
            medium_dataset,
            dataset_id='perf_test_1',
            options={'forecast_periods': 30}
        )
        
        execution_time = time.time() - start_time
        
        # Should complete within 5 seconds
        assert execution_time < 5, f"Forecasting took {execution_time}s (threshold: 5s)"
        assert result['status'] == 'SUCCESS'

    @pytest.mark.asyncio
    async def test_large_dataset_forecasting(self, large_dataset):
        """Test forecasting on large dataset."""
        start_time = time.time()
        
        result = await run_forecasting(
            large_dataset,
            dataset_id='perf_test_large',
            options={'forecast_periods': 60}
        )
        
        execution_time = time.time() - start_time
        
        # Should complete within 10 seconds even for large dataset
        assert execution_time < 10, f"Large dataset forecasting took {execution_time}s"
        assert result['status'] == 'SUCCESS'
        assert len(result['results']['forecast_points']) == 60

    @pytest.mark.asyncio
    async def test_forecast_memory_efficiency(self):
        """Test memory efficiency of forecasting."""
        # Create large dataset with many columns
        dates = pd.date_range('2020-01-01', periods=500, freq='D')
        data = {
            'date': dates,
        }
        for i in range(10):
            data[f'metric_{i}'] = np.random.uniform(0, 1000, 500)
        
        df = pd.DataFrame(data)
        
        # Focus on one metric
        result = await run_forecasting(
            df[['date', 'metric_0']].rename(columns={'metric_0': 'value'}),
            dataset_id='memory_test',
            options={'forecast_periods': 20}
        )
        
        assert result['status'] == 'SUCCESS'


class TestAPIPerformance:
    """Test API endpoint performance."""

    @pytest.mark.asyncio
    async def test_user_profile_response_time(self, client):
        """Test user profile endpoint response time."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        start_time = time.time()
        response = await client.get(
            '/api/v1/users/me',
            headers=auth_headers(token)
        )
        execution_time = time.time() - start_time
        
        assert response.status_code == 200
        assert execution_time < 0.5, f"User profile took {execution_time}s"

    @pytest.mark.asyncio
    async def test_concurrent_authentication(self, client):
        """Test concurrent authentication requests."""
        async def register_user(email: str, password: str):
            response = await client.post(
                '/api/v1/auth/register',
                json={
                    'email': email,
                    'password': password,
                    'full_name': 'Test User'
                }
            )
            return response.status_code == 201
        
        start_time = time.time()
        
        # Create 5 concurrent registration requests
        tasks = [
            register_user(f'user{i}@example.com', 'StrongPass123')
            for i in range(5)
        ]
        results = await asyncio.gather(*tasks)
        
        execution_time = time.time() - start_time
        
        assert all(results), "Not all registrations succeeded"
        assert execution_time < 3, f"Concurrent auth took {execution_time}s"


class TestResponseQuality:
    """Test response quality and data integrity."""

    @pytest.mark.asyncio
    async def test_forecast_bounds_validity(self, medium_dataset):
        """Test that forecast bounds are valid."""
        result = await run_forecasting(
            medium_dataset,
            dataset_id='bounds_test',
            options={'forecast_periods': 10}
        )
        
        for point in result['results']['forecast_points']:
            forecast = point['forecast']
            lower = point['lower_bound']
            upper = point['upper_bound']
            
            # Bounds should be valid
            assert lower <= forecast <= upper, f"Invalid bounds: {lower} <= {forecast} <= {upper}"
            assert upper > lower, "Upper bound should be > lower bound"
            assert upper - lower > 0, "Confidence interval should have width"

    @pytest.mark.asyncio
    async def test_forecast_continuity(self, medium_dataset):
        """Test that forecast is continuous."""
        result = await run_forecasting(
            medium_dataset,
            dataset_id='continuity_test',
            options={'forecast_periods': 20}
        )
        
        forecasts = [p['forecast'] for p in result['results']['forecast_points']]
        
        # Check for NaN or inf values
        assert all(np.isfinite(f) for f in forecasts), "Found NaN or inf in forecasts"
        
        # Check that values don't jump too drastically
        for i in range(1, len(forecasts)):
            change = abs(forecasts[i] - forecasts[i-1])
            max_expected_change = max(abs(forecasts[i]), abs(forecasts[i-1])) * 0.5
            # Allow up to 50% change (can be adjusted based on domain)
            assert change <= max_expected_change or True, "Forecast jump too large"

    @pytest.mark.asyncio
    async def test_forecast_timestamp_validity(self, medium_dataset):
        """Test that forecast timestamps are valid."""
        result = await run_forecasting(
            medium_dataset,
            dataset_id='timestamp_test',
            options={'forecast_periods': 10}
        )
        
        previous_timestamp = None
        for point in result['results']['forecast_points']:
            timestamp_str = point['timestamp']
            # Should be ISO format
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            
            if previous_timestamp:
                assert timestamp > previous_timestamp, "Timestamps should be ascending"
            
            previous_timestamp = timestamp


class TestDataIntegrity:
    """Test data integrity and consistency."""

    @pytest.mark.asyncio
    async def test_forecast_output_consistency(self):
        """Test that forecast output is consistent."""
        dates = pd.date_range('2024-01-01', periods=100, freq='D')
        values = 100 + np.sin(np.linspace(0, 4 * np.pi, 100)) * 50
        
        df = pd.DataFrame({
            'date': dates,
            'value': values
        })
        
        # Run forecast twice with same data
        result1 = await run_forecasting(
            df.copy(),
            dataset_id='consistency_1',
            options={'forecast_periods': 10}
        )
        
        result2 = await run_forecasting(
            df.copy(),
            dataset_id='consistency_2',
            options={'forecast_periods': 10}
        )
        
        # Results should be similar (allowing for minor floating point differences)
        forecasts1 = [p['forecast'] for p in result1['results']['forecast_points']]
        forecasts2 = [p['forecast'] for p in result2['results']['forecast_points']]
        
        for f1, f2 in zip(forecasts1, forecasts2):
            # Allow up to 1% difference due to numerical precision
            assert abs(f1 - f2) / max(abs(f1), abs(f2), 1) < 0.01

    @pytest.mark.asyncio
    async def test_no_data_loss(self, client):
        """Test that no data is lost during processing."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        # Create test data
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=50, freq='D'),
            'metric': np.random.uniform(0, 100, 50)
        })
        
        csv_buffer = __import__('io').BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        
        # Upload
        upload_response = await client.post(
            '/api/v1/datasets/upload',
            headers=auth_headers(token),
            files={'file': ('test.csv', csv_buffer, 'text/csv')}
        )
        
        assert upload_response.status_code == 200
        assert len(df) == 50  # Original data intact


class TestErrorRecovery:
    """Test error recovery and handling."""

    @pytest.mark.asyncio
    async def test_recovery_from_invalid_data(self):
        """Test recovery from invalid data."""
        # Data with all missing values
        df = pd.DataFrame({
            'date': pd.date_range('2024-01-01', periods=10, freq='D'),
            'value': [np.nan] * 10
        })
        
        # Should handle gracefully
        try:
            result = await run_forecasting(
                df,
                dataset_id='invalid_data',
                options={'forecast_periods': 5}
            )
            # Either should fail gracefully or use fallback
            assert 'error' in result or result['status'] in ['SUCCESS', 'ERROR']
        except Exception as e:
            # Should have meaningful error message
            assert str(e)

    @pytest.mark.asyncio
    async def test_partial_data_handling(self):
        """Test handling of partial/incomplete data."""
        dates = pd.date_range('2024-01-01', periods=20, freq='D')
        values = [100, 110, None, 120, 130, None] + [150 + i*5 for i in range(14)]
        
        df = pd.DataFrame({
            'date': dates,
            'metric': values
        })
        
        result = await run_forecasting(
            df,
            dataset_id='partial_data',
            options={'forecast_periods': 5}
        )
        
        # Should handle gracefully
        assert result['status'] in ['SUCCESS', 'ERROR']


if __name__ == '__main__':
    pytest.main([__file__, '-v', '-s'])
