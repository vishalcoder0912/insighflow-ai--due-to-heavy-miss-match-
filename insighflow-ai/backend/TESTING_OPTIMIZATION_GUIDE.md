# Testing Optimization Guide for InsightFlow AI

**Document Purpose**: Guide developers through optimizing and executing the InsightFlow AI test suite.

---

## Quick Start

### Fast Test Suite (Recommended for rapid feedback)
```bash
cd backend

# Run only auth tests (~6 seconds)
python -m pytest tests/test_auth.py -v

# Run auth tests with coverage
python -m pytest tests/test_auth.py -v --cov=app.services.auth --cov-report=html
```

### Full Test Suite (for comprehensive validation)
```bash
# Run all tests with optimized settings (requires ~5-10 minutes)
python -m pytest tests/ -v --tb=short -n auto --durations=10
```

---

## Understanding Test Execution Times

### By Test Module

| Module | Category | Est. Time | Status |
|--------|----------|-----------|--------|
| `test_auth.py` | **Fast** | ~6 seconds | ✅ Recommended |
| `test_forecasting_service.py` | **Slow** | 120+ seconds | ⏳ See optimization |
| `test_endpoints_comprehensive.py` | **Medium** | ~20-30 seconds | ⏳ Pending |
| `test_performance.py` | **Slow** | 60+ seconds | ⏳ Pending |

### Why Forecasting Tests Are Slow

The forecasting module uses real machine learning models:
- **Prophet**: Facebook's time series forecasting model (~5-10s per test)
- **ARIMA**: Auto-regressive integrated moving average (~3-7s per test)
- **Exponential Smoothing**: Statsmodels implementation (~2-5s per test)

These are expensive operations that cannot be quickly bypassed without losing test validity.

---

## Optimization Strategies

### Strategy 1: Use Pre-Computed Mock Forecasts (Fastest)

**Use Case**: Unit testing the forecast API endpoints without validating algorithm quality.

```python
import pytest
from unittest.mock import Mock, patch

@pytest.mark.fast
def test_forecast_endpoint_with_mock(client, mock_forecaster):
    """Test forecast endpoint with mocked forecasting service."""
    
    mock_result = {
        'forecast': [100, 101, 102],
        'lower_bound': [95, 96, 97],
        'upper_bound': [105, 106, 107],
        'method': 'mock'
    }
    
    with patch('app.services.forecasting.run_forecasting') as mock_run:
        mock_run.return_value = mock_result
        # Test endpoint logic without running actual forecasting
```

**Benefits**:
- Execution time: <100ms per test
- Validates API contract, not algorithm
- Safe for continuous integration

**When to use**: Integration tests, API endpoint tests, error handling tests

---

### Strategy 2: Reduce Model Complexity (Fast)

**Use Case**: Testing forecasting logic with smaller datasets that fit faster.

```python
@pytest.mark.slow
def test_forecasting_with_small_dataset():
    """Test forecasting with minimal data for speed."""
    
    # Use just 20 data points instead of 100+
    dates = pd.date_range(start='2024-01-01', periods=20, freq='D')
    values = np.linspace(100, 120, 20)
    df = pd.DataFrame({'date': dates, 'value': values})
    
    # Pass smaller dataset to reduce model fitting time
    forecaster = TimeSeriesForecaster(df, freq='D')
    result = forecaster.detect_trend()
    
    assert result is not None
```

**Benefits**:
- Reduces computation from 5s to 1s
- Still validates algorithm behavior
- More realistic for unit tests

---

### Strategy 3: Parallel Test Execution (Medium)

**Use Case**: Running multiple slow tests simultaneously.

```bash
# Install pytest-xdist plugin
pip install pytest-xdist

# Run tests in parallel with auto-detection of CPU cores
python -m pytest tests/test_forecasting_service.py -v -n auto

# Run with specific number of workers
python -m pytest tests/test_forecasting_service.py -v -n 4
```

**Expected Speedup**:
- 17 sequential tests × 5 seconds = 85 seconds → ~25 seconds with 4 workers
- Effectiveness depends on I/O vs. CPU usage

**Installation**:
```bash
pip install pytest-xdist
```

---

### Strategy 4: Skip Slow Tests During Development (Fastest)

**Use Case**: Rapid feedback during development iteration.

```bash
# Run only fast tests (recommended for development)
python -m pytest tests/ -v -m "not slow"

# Run only slow tests when you have time
python -m pytest tests/ -v -m "slow"
```

**Mark tests appropriately in conftest.py** (already configured):
```python
@pytest.mark.slow
def test_expensive_forecasting_operation():
    """This test will be skipped with: pytest -m 'not slow'"""
    pass
```

---

## Recommended Testing Workflow

### For Local Development (Fast Iteration)
```bash
# 1. Fast auth tests (~6s)
python -m pytest tests/test_auth.py -v

# 2. Fast unit tests (~regular tests with -m "not slow")
python -m pytest -v -m "not slow"

# 3. Slow tests when committing (run overnight if needed)
python -m pytest tests/test_forecasting_service.py -v
```

### For CI/CD Pipeline
```bash
# Run in parallel during build
python -m pytest tests/ -v -n auto --tb=short --junit-xml=test-results.xml

# Or run fast tests immediately, slow tests as separate job
job_fast:
  script: pytest tests/ -v -m "not slow"
  timeout: 5 minutes

job_slow:
  script: pytest tests/test_forecasting_service.py -v
  timeout: 5 minutes
  allow_failure: true  # Don't fail build if slow tests timeout
```

---

## Current Test Status

### ✅ Passing Tests (Ready to run)
- `tests/test_auth.py` - Complete authentication flow
- Helper functions in `test_forecasting_service.py`
- Trend and seasonality detection in `test_forecasting_service.py`

### ⏳ Needs Optimization (Timeout issues)
- `test_run_forecasting_*` functions (slow model fitting)
- `test_forecasting_edge_cases` (intensive calculations)
- Some performan tests (concurrent request load)

### 📋 Pending (Not yet executed)
- Full `test_endpoints_comprehensive.py` suite
- All performance benchmarks in `test_performance.py`

---

## How to Run Each Test Type

### Authentication Tests (Fastest - 6 seconds)
```bash
python -m pytest tests/test_auth.py -v --tb=short

# With coverage
python -m pytest tests/test_auth.py -v --cov=app.services.auth --cov-report=html
```

### Forecasting Algorithm Tests (Medium - 30 seconds with optimization)
```bash
# Using mock forecaster
python -m pytest tests/test_forecasting_service.py::TestForecastingHelpers -v

# With real model fitting (slow)
python -m pytest tests/test_forecasting_service.py::test_run_forecasting_basic -v -s
```

### API Endpoint Tests (Medium - 20 seconds)
```bash
python -m pytest tests/test_endpoints_comprehensive.py -v --tb=short

# Test specific endpoint
python -m pytest tests/test_endpoints_comprehensive.py::TestFileUploadEndpoints -v
```

### Performance Benchmarks (Slow - 60 seconds)
```bash
python -m pytest tests/test_performance.py -v --tb=short --durations=10

# Show slowest 10 tests
python -m pytest tests/test_performance.py -v --durations=10
```

---

## Common Issues and Solutions

### Issue 1: Tests Timeout (120+ seconds)
**Cause**: Complex model fitting in forecasting tests  
**Solution**: Use mock fixtures or skip with `-m "not slow"`

```bash
# Skip slow tests
python -m pytest tests/ -v -m "not slow"

# Or use mock forecaster
pytest tests/test_forecasting_service.py -v -k "not run_forecasting"
```

### Issue 2: "ModuleNotFoundError: No module named 'prophet'"
**Cause**: Optional dependencies not installed  
**Solution**: Install or skip tests

```bash
# Option 1: Install missing packages
pip install prophet pmdarima statsmodels

# Option 2: Gracefully handle missing imports
# (Already configured in forecasting.py with try/except)

# Option 3: Skip tests requiring specific module
pytest tests/ -k "not prophet"
```

### Issue 3: Database Lock (SQLite)
**Cause**: Concurrent test execution with SQLite  
**Solution**: Use separate test database or reduce parallelism

```bash
# Use -n 1 to disable parallelism
pytest tests/ -n 1

# Or use PostgreSQL for testing (recommended for production)
export DATABASE_URL=postgresql+asyncpg://user:pass@localhost/test_db
pytest tests/
```

### Issue 4: Memory Issues During Test Run
**Cause**: Loading large datasets or models  
**Solution**: Reduce dataset size or run tests serially

```bash
# Reduce parallelism
pytest tests/ -n 2  # Use 2 workers instead of all cores

# Run serially
pytest tests/ -n 1
```

---

## Setting Up Continuous Integration

### GitHub Actions Example
```yaml
name: Tests
on: [push, pull_request]

jobs:
  fast-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.14'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-asyncio pytest-cov
      - name: Run fast tests
        run: pytest tests/ -v -m "not slow"
        timeout-minutes: 5

  slow-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.14'
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install pytest pytest-asyncio pytest-cov
      - name: Run slow tests
        run: pytest tests/ -v -m "slow"
        timeout-minutes: 10
```

---

## Coverage Reports

### Generate HTML Coverage Report
```bash
pytest tests/test_auth.py \
  --cov=app \
  --cov-report=html \
  --cov-report=term-missing

# Open htmlcov/index.html in browser to view coverage
```

### View Coverage by Module
```bash
pytest tests/ \
  --cov=app \
  --cov-report=term:skip-covered \
  --cov-report=term-missing
```

### Set Coverage Thresholds
```bash
# Fail if coverage drops below 80%
pytest tests/ --cov=app --cov-fail-under=80
```

---

## Performance Profiling

### Find Slowest Tests
```bash
pytest tests/ --durations=10  # Show 10 slowest tests
```

### Profile Test Execution
```bash
pytest tests/test_forecasting_service.py \
  -v \
  --profile=default \
  --profile-svg  # Generate flame graph
```

### Memory Profiling
```bash
pip install pytest-memray
pytest tests/ --memray  # Generate memory profile
```

---

## Summary

**For Daily Development**:
```bash
# Fast feedback loop
pytest tests/test_auth.py -v

# Before committing
pytest tests/ -v -m "not slow"
```

**For Pre-Release**:
```bash
# Complete validation
pytest tests/ -v --cov=app --cov-fail-under=80 -n auto
```

**For Continuous Integration**:
```bash
# Parallel fast tests + separate slow tests
pytest tests/ -v -m "not slow" -n auto
pytest tests/test_forecasting_service.py -v
```

---

## Reference

- **Pytest Documentation**: https://docs.pytest.org/
- **Pytest-asyncio**: For async test support
- **Pytest-xdist**: For parallel execution
- **Pytest-cov**: For coverage reports

**Last Updated**: 2025-03-31  
**Status**: Complete ✅
