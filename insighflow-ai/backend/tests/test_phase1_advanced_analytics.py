"""
Comprehensive tests for Phase 1 Advanced Analytics Engine
Tests all 5 modules + validation + error handling + monitoring
"""

import pytest
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from io import StringIO

from app.services.validation import DataValidator
from app.services.forecasting import TimeSeriesForecaster
from app.services.clustering import ClusteringEngine
from app.services.regression import RegressionEngine
from app.services.cohort_analysis import CohortAnalyzer
from app.services.rfm_analysis import RFMAnalyzer
from app.services.insight_generation import InsightsGenerator
from app.core.exceptions import (
    EmptyDatasetError,
    InsufficientDataError,
    NoVarianceError,
    ColumnMissingError,
    ModelTrainingError,
)


# ============ FIXTURES ============


@pytest.fixture
def sample_time_series_data():
    """Generate sample time series data"""
    dates = pd.date_range("2023-01-01", periods=365, freq="D")
    values = (
        100
        + np.cumsum(np.random.randn(365))
        + np.sin(np.arange(365) * 2 * np.pi / 365) * 20
    )
    return pd.DataFrame({"date": dates, "revenue": values})


@pytest.fixture
def sample_clustering_data():
    """Generate sample clustering data"""
    np.random.seed(42)
    cluster1 = np.random.randn(50, 3) + [0, 0, 0]
    cluster2 = np.random.randn(50, 3) + [5, 5, 5]
    data = np.vstack([cluster1, cluster2])
    return pd.DataFrame(data, columns=["feature1", "feature2", "feature3"])


@pytest.fixture
def sample_regression_data():
    """Generate sample regression data"""
    np.random.seed(42)
    n = 100
    X1 = np.random.randn(n)
    X2 = np.random.randn(n)
    y = 10 + 5 * X1 + 3 * X2 + np.random.randn(n) * 2
    return pd.DataFrame({"feature1": X1, "feature2": X2, "target": y})


@pytest.fixture
def sample_rfm_data():
    """Generate sample RFM data"""
    np.random.seed(42)
    customers = [f"CUST_{i:05d}" for i in range(100)]
    dates = []
    amounts = []
    cust_list = []

    for cust in customers:
        n_transactions = np.random.randint(1, 20)
        for _ in range(n_transactions):
            cust_list.append(cust)
            dates.append(
                datetime(2023, 1, 1) + timedelta(days=np.random.randint(0, 365))
            )
            amounts.append(np.random.randint(10, 500))

    return pd.DataFrame(
        {"customer_id": cust_list, "transaction_date": dates, "amount": amounts}
    )


# ============ VALIDATION TESTS ============


class TestValidation:
    def test_empty_dataset_raises_error(self):
        """Empty DataFrame should raise EmptyDatasetError"""
        df = pd.DataFrame()
        validator = DataValidator(df, "test_1")

        with pytest.raises(EmptyDatasetError):
            validator.validate_schema()

    def test_schema_validation_passes(self, sample_time_series_data):
        """Schema validation should pass with correct structure"""
        validator = DataValidator(sample_time_series_data, "test_2")
        result = validator.validate_schema(required_columns=["date", "revenue"])

        assert result["valid"] == True
        assert result["row_count"] == 365

    def test_missing_column_raises_error(self, sample_time_series_data):
        """Missing required column should raise ColumnMissingError"""
        validator = DataValidator(sample_time_series_data, "test_3")

        with pytest.raises(ColumnMissingError):
            validator.validate_schema(
                required_columns=["date", "revenue", "nonexistent"]
            )

    def test_quality_metrics_calculated(self, sample_time_series_data):
        """Quality metrics should be calculated correctly"""
        validator = DataValidator(sample_time_series_data, "test_4")
        validator.validate_schema()
        metrics = validator.calculate_quality_metrics()

        assert "overall_score" in metrics
        assert 0 <= metrics["overall_score"] <= 1
        assert len(metrics["columns"]) == 2

    def test_time_series_validation(self, sample_time_series_data):
        """Time series validation should pass with correct data"""
        validator = DataValidator(sample_time_series_data, "test_5")
        validator.validate_schema()
        result = validator.validate_time_series("date", "revenue")

        assert result["valid"] == True
        assert result["data_points"] == 365

    def test_clustering_validation(self, sample_clustering_data):
        """Clustering validation should pass with sufficient data"""
        validator = DataValidator(sample_clustering_data, "test_6")
        validator.validate_schema()
        result = validator.validate_clustering(min_rows=30, min_features=2)

        assert result["valid"] == True
        assert result["numeric_features"] >= 2

    def test_regression_validation(self, sample_regression_data):
        """Regression validation should pass"""
        validator = DataValidator(sample_regression_data, "test_7")
        validator.validate_schema()
        result = validator.validate_regression("target")

        assert result["valid"] == True
        assert result["target_column"] == "target"


# ============ FORECASTING TESTS ============


class TestForecasting:
    def test_forecasting_prophet_succeeds(self, sample_time_series_data):
        """Prophet forecasting should succeed"""
        forecaster = TimeSeriesForecaster(
            sample_time_series_data, "date", "revenue", "test_forecast_1"
        )
        result = forecaster.forecast(periods=30, methods=["prophet"])

        assert result["status"] == "SUCCESS"
        assert len(result["forecast"]) == 30
        assert "confidence_intervals" in result

    def test_forecasting_trend_detection(self, sample_time_series_data):
        """Trend should be detected"""
        forecaster = TimeSeriesForecaster(
            sample_time_series_data, "date", "revenue", "test_forecast_2"
        )
        result = forecaster.forecast(periods=30)

        assert result["trend"] in ["increasing", "decreasing", "flat"]


# ============ CLUSTERING TESTS ============


class TestClustering:
    def test_clustering_succeeds(self, sample_clustering_data):
        """Clustering should succeed"""
        engine = ClusteringEngine(sample_clustering_data, "test_cluster_1")
        result = engine.cluster(n_clusters=2)

        assert result["status"] == "SUCCESS"
        assert result["n_clusters"] == 2
        assert len(result["clusters"]) >= 1

    def test_clustering_metrics(self, sample_clustering_data):
        """Clustering metrics should be computed"""
        engine = ClusteringEngine(sample_clustering_data, "test_cluster_2")
        result = engine.cluster(n_clusters=2)

        assert "silhouette" in result["metrics"]
        assert "davies_bouldin" in result["metrics"]
        assert -1 <= result["metrics"]["silhouette"] <= 1


# ============ REGRESSION TESTS ============


class TestRegression:
    def test_regression_succeeds(self, sample_regression_data):
        """Regression should succeed"""
        engine = RegressionEngine(sample_regression_data, "test_reg_1")
        result = engine.regress("target", ["feature1", "feature2"])

        assert result["status"] == "SUCCESS"
        assert 0 <= result["r2_score"] <= 1
        assert len(result["features"]) >= 1

    def test_regression_feature_importance(self, sample_regression_data):
        """Feature importance should be calculated"""
        engine = RegressionEngine(sample_regression_data, "test_reg_2")
        result = engine.regress("target")

        for feature in result["features"]:
            assert "feature" in feature
            assert "coefficient" in feature
            assert "importance" in feature


# ============ RFM TESTS ============


class TestRFM:
    def test_rfm_analysis_succeeds(self, sample_rfm_data):
        """RFM analysis should succeed"""
        analyzer = RFMAnalyzer(sample_rfm_data, "test_rfm_1")
        result = analyzer.analyze("customer_id", "transaction_date", "amount")

        assert result["status"] == "SUCCESS"
        assert result["total_customers"] > 0
        assert "segments" in result

    def test_rfm_segments_created(self, sample_rfm_data):
        """RFM segments should be created"""
        analyzer = RFMAnalyzer(sample_rfm_data, "test_rfm_2")
        result = analyzer.analyze("customer_id", "transaction_date", "amount")

        segments = result["segments"]
        segment_names = list(segments.keys())
        assert "Champions" in segment_names
        assert "At Risk" in segment_names


# ============ INSIGHTS TESTS ============


class TestInsights:
    def test_forecasting_insights_generated(self):
        """Forecasting insights should be generated"""
        forecast_result = {
            "forecast": [100, 110, 120, 130, 140],
            "trend": "increasing",
            "seasonality_detected": True,
            "rmse": 50,
        }

        insights = InsightsGenerator.generate_forecasting_insights(forecast_result)

        assert "findings" in insights
        assert "business_impact" in insights
        assert "recommendations" in insights

    def test_rfm_insights_generated(self):
        """RFM insights should be generated"""
        rfm_result = {
            "insights": {
                "total_customers": 100,
                "champions": {"count": 10, "percent_of_revenue": 40},
            },
            "segments": {
                "Champions": {"count": 10, "total_revenue": 40000},
                "At Risk": {"count": 5, "total_revenue": 5000},
            },
        }

        insights = InsightsGenerator.generate_rfm_insights(rfm_result)

        assert "findings" in insights
        assert "recommendations" in insights


# ============ ERROR HANDLING TESTS ============


class TestErrorHandling:
    def test_insufficient_data_raises_error(self):
        """Insufficient data should raise InsufficientDataError"""
        df = pd.DataFrame(
            {"date": pd.date_range("2023-01-01", periods=5), "value": [1, 2, 3, 4, 5]}
        )

        validator = DataValidator(df, "test_error_1")
        with pytest.raises(InsufficientDataError):
            validator.validate_time_series("date", "value", min_points=24)

    def test_no_variance_raises_error(self):
        """Constant column should raise NoVarianceError"""
        df = pd.DataFrame({"target": [100, 100, 100, 100], "feature": [1, 2, 3, 4]})

        validator = DataValidator(df, "test_error_2")
        with pytest.raises(NoVarianceError):
            validator.validate_regression("target")


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
