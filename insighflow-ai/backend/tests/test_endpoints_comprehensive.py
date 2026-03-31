"""End-to-end API endpoint tests."""

from __future__ import annotations

import pytest
import io
import pandas as pd
import numpy as np

from tests.conftest import auth_headers, register_and_login


@pytest.fixture
def sample_csv_file():
    """Create a sample CSV file for upload."""
    df = pd.DataFrame({
        'date': pd.date_range('2024-01-01', periods=50, freq='D'),
        'product': ['A', 'B'] * 25,
        'sales': np.random.uniform(100, 1000, 50),
        'quantity': np.random.randint(1, 100, 50),
        'region': (['North', 'South', 'East', 'West'] * 13)[:50]
    })
    
    csv_buffer = io.BytesIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    return csv_buffer


class TestFileUploadEndpoints:
    """Test file upload functionality."""

    @pytest.mark.asyncio
    async def test_upload_csv_file(self, client, sample_csv_file):
        """Test uploading a CSV file."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        response = await client.post(
            '/api/v1/datasets/upload',
            headers=auth_headers(token),
            files={'file': ('test.csv', sample_csv_file, 'text/csv')}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert 'dataset_id' in data
        assert 'filename' in data
        assert data['filename'] == 'test.csv'

    @pytest.mark.asyncio
    async def test_upload_file_requires_auth(self, client, sample_csv_file):
        """Test that file upload requires authentication."""
        response = await client.post(
            '/api/v1/datasets/upload',
            files={'file': ('test.csv', sample_csv_file, 'text/csv')}
        )
        
        assert response.status_code == 401


class TestDatasetAnalysisEndpoints:
    """Test dataset analysis endpoints."""

    @pytest.mark.asyncio
    async def test_analyze_dataset(self, client, sample_csv_file):
        """Test dataset analysis."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        # Upload file
        upload_response = await client.post(
            '/api/v1/datasets/upload',
            headers=auth_headers(token),
            files={'file': ('test.csv', sample_csv_file, 'text/csv')}
        )
        assert upload_response.status_code == 200
        dataset_id = upload_response.json()['dataset_id']
        
        # Analyze dataset
        analysis_response = await client.post(
            f'/api/v1/datasets/{dataset_id}/analyze',
            headers=auth_headers(token),
            json={'analysis_type': 'statistics'}
        )
        
        assert analysis_response.status_code in [200, 202]
        data = analysis_response.json()
        assert 'status' in data


class TestForecastingEndpoints:
    """Test forecasting endpoints."""

    @pytest.mark.asyncio
    async def test_forecast_endpoint(self, client, sample_csv_file):
        """Test forecasting endpoint."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        # Upload dataset
        upload_response = await client.post(
            '/api/v1/datasets/upload',
            headers=auth_headers(token),
            files={'file': ('forecast_data.csv', sample_csv_file, 'text/csv')}
        )
        dataset_id = upload_response.json()['dataset_id']
        
        # Create forecast
        forecast_response = await client.post(
            f'/api/v1/datasets/{dataset_id}/forecast',
            headers=auth_headers(token),
            json={
                'datetime_column': 'date',
                'metric_column': 'sales',
                'forecast_periods': 10
            }
        )
        
        assert forecast_response.status_code in [200, 202]


class TestChatEndpoints:
    """Test chat and NL-to-SQL endpoints."""

    @pytest.mark.asyncio
    async def test_chat_endpoint(self, client):
        """Test chat endpoint."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        response = await client.post(
            '/api/v1/chat',
            headers=auth_headers(token),
            json={'message': 'What is the average sales?'}
        )
        
        assert response.status_code in [200, 202]
        data = response.json()
        assert 'response' in data or 'message' in data

    @pytest.mark.asyncio
    async def test_nl_to_sql_endpoint(self, client, sample_csv_file):
        """Test NL-to-SQL conversion."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        # Upload dataset first
        upload_response = await client.post(
            '/api/v1/datasets/upload',
            headers=auth_headers(token),
            files={'file': ('data.csv', sample_csv_file, 'text/csv')}
        )
        dataset_id = upload_response.json()['dataset_id']
        
        # Request NL-to-SQL conversion
        nl_response = await client.post(
            '/api/v1/sql-generation/generate',
            headers=auth_headers(token),
            json={
                'natural_language_query': 'Show me total sales by product',
                'dataset_id': dataset_id
            }
        )
        
        assert nl_response.status_code in [200, 201]


class TestDashboardEndpoints:
    """Test dashboard endpoints."""

    @pytest.mark.asyncio
    async def test_create_dashboard(self, client):
        """Test creating a dashboard."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        response = await client.post(
            '/api/v1/dashboards',
            headers=auth_headers(token),
            json={
                'name': 'Test Dashboard',
                'description': 'Testing dashboard creation'
            }
        )
        
        assert response.status_code in [200, 201]
        data = response.json()
        assert 'dashboard_id' in data or 'id' in data

    @pytest.mark.asyncio
    async def test_get_dashboards(self, client):
        """Test retrieving dashboards."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        response = await client.get(
            '/api/v1/dashboards',
            headers=auth_headers(token)
        )
        
        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)


class TestUserEndpoints:
    """Test user endpoints."""

    @pytest.mark.asyncio
    async def test_get_current_user(self, client):
        """Test getting current user."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        response = await client.get(
            '/api/v1/users/me',
            headers=auth_headers(token)
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['email'] == 'owner@example.com'

    @pytest.mark.asyncio
    async def test_update_user_profile(self, client):
        """Test updating user profile."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        response = await client.put(
            '/api/v1/users/me',
            headers=auth_headers(token),
            json={'full_name': 'Updated Name'}
        )
        
        assert response.status_code == 200
        data = response.json()
        assert data['full_name'] == 'Updated Name'


class TestAuthenticationErrors:
    """Test authentication error handling."""

    @pytest.mark.asyncio
    async def test_invalid_token(self, client):
        """Test invalid token handling."""
        response = await client.get(
            '/api/v1/users/me',
            headers=auth_headers('invalid_token_12345')
        )
        
        assert response.status_code == 401

    @pytest.mark.asyncio
    async def test_expired_token(self, client):
        """Test expired token handling."""
        auth_payload = await register_and_login(client)
        token = auth_payload['tokens']['access_token']
        
        # Token should be valid immediately after registration
        response = await client.get(
            '/api/v1/users/me',
            headers=auth_headers(token)
        )
        
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_missing_token(self, client):
        """Test missing token handling."""
        response = await client.get('/api/v1/users/me')
        
        assert response.status_code == 401


class TestInputValidation:
    """Test input validation and error handling."""

    @pytest.mark.asyncio
    async def test_invalid_email_registration(self, client):
        """Test registration with invalid email."""
        response = await client.post(
            '/api/v1/auth/register',
            json={
                'email': 'not_an_email',
                'password': 'StrongPass123',
                'full_name': 'Test User'
            }
        )
        
        assert response.status_code >= 400

    @pytest.mark.asyncio
    async def test_weak_password_registration(self, client):
        """Test registration with weak password."""
        response = await client.post(
            '/api/v1/auth/register',
            json={
                'email': 'test@example.com',
                'password': '123',  # Too weak
                'full_name': 'Test User'
            }
        )
        
        assert response.status_code >= 400

    @pytest.mark.asyncio
    async def test_duplicate_email_registration(self, client):
        """Test registration with duplicate email."""
        await register_and_login(client, 'duplicate@example.com')
        
        response = await client.post(
            '/api/v1/auth/register',
            json={
                'email': 'duplicate@example.com',
                'password': 'StrongPass123',
                'full_name': 'Another User'
            }
        )
        
        assert response.status_code >= 400


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
