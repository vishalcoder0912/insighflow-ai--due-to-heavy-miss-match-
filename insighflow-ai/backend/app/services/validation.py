"""
Comprehensive data validation framework with 4 levels:
1. Schema validation
2. Quality metrics
3. Analysis-specific checks
4. Statistical assumption tests
"""

import pandas as pd
import numpy as np
from typing import Any, Dict, List, Optional, Tuple, Union
import logging
from datetime import datetime
from scipy.stats import shapiro, jarque_bera

try:
    from statsmodels.tsa.stattools import adfuller
    from statsmodels.stats.outliers_influence import variance_inflation_factor

    STATSMODELS_AVAILABLE = True
except ImportError:
    STATSMODELS_AVAILABLE = False
    adfuller = None
    variance_inflation_factor = None

from dataclasses import dataclass
from pandas.api.types import is_bool_dtype, is_datetime64_any_dtype, is_numeric_dtype
from sklearn.impute import KNNImputer

from app.core.exceptions import (
    SchemaMismatchError,
    EmptyDatasetError,
    NoVarianceError,
    InsufficientDataError,
    ColumnMissingError,
    InsufficientFeaturesError,
    DataQualityError,
)

try:
    from pyod.models.iforest import IForest
except (ImportError, ModuleNotFoundError):
    IForest = None

MAX_ANALYTICS_MEMORY_MB = 1024.0
ANALYSIS_TYPES = {"forecasting", "clustering", "regression", "cohort", "rfm"}
DATE_HINTS = ("date", "time", "timestamp", "period", "week", "month", "year")
VALUE_HINTS = (
    "revenue",
    "sales",
    "amount",
    "value",
    "income",
    "expense",
    "cost",
    "salary",
    "profit",
)
ENTITY_HINTS = ("customer", "client", "user", "account", "employee", "member")

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PreparedDataset:
    """Cleaned dataset bundle passed into model services."""

    dataframe: pd.DataFrame
    validation: dict[str, Any]
    plan: dict[str, Any]
    missing_values_analysis: dict[str, Any]
    warnings: list[str]
    excluded_rows: int
    excluded_columns: list[str]


class DataValidator:
    """Multi-level data validation framework"""

    def __init__(self, df: pd.DataFrame, dataset_id: str):
        self.df = df
        self.dataset_id = dataset_id
        self.validation_report = {
            "dataset_id": dataset_id,
            "timestamp": datetime.utcnow().isoformat(),
            "validation_status": "PASS",
            "severity": "INFO",
            "schema_validation": {},
            "quality_metrics": {},
            "analysis_specific_checks": {},
            "statistical_assumptions": {},
            "recommendations": [],
            "proceed_with_analysis": True,
        }

    # ============ LEVEL 1: SCHEMA VALIDATION ============

    def validate_schema(
        self,
        required_columns: Optional[List[str]] = None,
        column_types: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """Validate DataFrame schema structure"""

        logger.info(f"Starting schema validation for {self.dataset_id}")

        # Check if empty
        if self.df.empty or len(self.df) == 0:
            logger.error("Dataset is empty")
            raise EmptyDatasetError()

        schema_result = {
            "valid": True,
            "errors": [],
            "warnings": [],
            "data_shape": self.df.shape,
            "memory_mb": self.df.memory_usage(deep=True).sum() / 1024 / 1024,
            "provided_columns": list(self.df.columns),
            "column_count": len(self.df.columns),
            "row_count": len(self.df),
        }

        # Check required columns
        if required_columns:
            missing = set(required_columns) - set(self.df.columns)
            if missing:
                logger.error(f"Missing columns: {missing}")
                schema_result["valid"] = False
                schema_result["errors"].append(f"Missing columns: {missing}")
                first_missing = list(missing)[0]
                raise ColumnMissingError(first_missing, list(self.df.columns))

        # Check column types if specified
        if column_types:
            for col, expected_type in column_types.items():
                if col in self.df.columns:
                    actual_type = str(self.df[col].dtype)
                    if expected_type not in actual_type:
                        schema_result["warnings"].append(
                            f"Column '{col}': expected {expected_type}, got {actual_type}"
                        )

        self.validation_report["schema_validation"] = schema_result
        logger.info(f"Schema validation passed: {self.df.shape}")
        return schema_result

    # ============ LEVEL 2: QUALITY METRICS ============

    def calculate_quality_metrics(self) -> Dict[str, Any]:
        """Calculate data quality scores per column"""

        logger.info("Calculating quality metrics")

        quality_metrics = {
            "overall_score": 0.0,
            "columns": [],
            "missing_values_summary": {"total_missing": 0, "percent_missing": 0.0},
        }

        column_scores = []

        for col in self.df.columns:
            col_metric = self._score_column(col)
            column_scores.append(col_metric)
            quality_metrics["columns"].append(col_metric)

        # Overall score
        if column_scores:
            quality_metrics["overall_score"] = np.mean(
                [c["quality_score"] for c in column_scores]
            )

        # Missing values summary
        total_cells = self.df.shape[0] * self.df.shape[1]
        missing_cells = self.df.isnull().sum().sum()
        quality_metrics["missing_values_summary"]["total_missing"] = int(missing_cells)
        quality_metrics["missing_values_summary"]["percent_missing"] = (
            missing_cells / total_cells * 100 if total_cells > 0 else 0
        )

        self.validation_report["quality_metrics"] = quality_metrics
        logger.info(f"Overall quality score: {quality_metrics['overall_score']:.2f}")

        return quality_metrics

    def _score_column(self, col: str) -> Dict[str, Any]:
        """Score individual column quality"""

        series = self.df[col]
        total = len(series)

        # Completeness
        non_null = series.notna().sum()
        completeness = non_null / total if total > 0 else 0

        # Cardinality
        unique_count = series.nunique()
        cardinality = unique_count / total if total > 0 else 0

        # Type
        dtype = str(series.dtype)
        if "float" in dtype or "int" in dtype:
            col_type = "numeric"
        elif dtype == "object":
            col_type = "categorical"
        elif "datetime" in dtype:
            col_type = "datetime"
        else:
            col_type = "mixed"

        # Variability
        if col_type == "numeric":
            variability = series.std() / series.mean() if series.mean() != 0 else 0
        else:
            variability = unique_count

        # Combined score
        quality_score = (
            completeness * 0.4
            + (1 - min(cardinality, 1)) * 0.3
            + (min(variability / 10, 1) if col_type == "numeric" else 0.5) * 0.3
        )

        return {
            "name": col,
            "type": col_type,
            "completeness": float(completeness),
            "cardinality": float(cardinality),
            "unique_count": int(unique_count),
            "variability": float(variability),
            "quality_score": float(quality_score),
            "min": float(series.min()) if col_type == "numeric" else None,
            "max": float(series.max()) if col_type == "numeric" else None,
            "mean": float(series.mean()) if col_type == "numeric" else None,
        }

    # ============ LEVEL 3: ANALYSIS-SPECIFIC CHECKS ============

    def validate_time_series(
        self, datetime_col: str, metric_col: str, min_points: int = 24
    ) -> Dict[str, Any]:
        """Validate data for time series analysis"""

        logger.info(f"Validating time series: {datetime_col}, {metric_col}")

        if datetime_col not in self.df.columns:
            raise ColumnMissingError(datetime_col, list(self.df.columns))

        if metric_col not in self.df.columns:
            raise ColumnMissingError(metric_col, list(self.df.columns))

        # Check data points
        non_null_metric = self.df[metric_col].notna().sum()
        if non_null_metric < min_points:
            raise InsufficientDataError("Time Series", min_points, int(non_null_metric))

        # Parse datetime
        try:
            dates = pd.to_datetime(self.df[datetime_col])
        except Exception as e:
            logger.error(f"Failed to parse datetime: {e}")
            raise ValueError(f"Invalid datetime column: {e}")

        # Check for duplicates
        duplicates = dates.duplicated().sum()
        gaps = (dates.diff() > pd.Timedelta(days=1)).sum()

        # Detect frequency
        try:
            freq = pd.infer_freq(dates.sort_values())
        except:
            freq = "irregular"

        result = {
            "valid": True,
            "datetime_column": datetime_col,
            "metric_column": metric_col,
            "data_points": int(non_null_metric),
            "time_range": f"{dates.min()} to {dates.max()}",
            "frequency": freq,
            "duplicates": int(duplicates),
            "gaps": int(gaps),
            "applicable": True,
        }

        if duplicates > 0:
            logger.warning(f"Found {duplicates} duplicate timestamps")

        self.validation_report["analysis_specific_checks"]["time_series"] = result
        return result

    def validate_clustering(
        self, min_rows: int = 30, min_features: int = 2
    ) -> Dict[str, Any]:
        """Validate data for clustering"""

        logger.info("Validating clustering requirements")

        if len(self.df) < min_rows:
            raise InsufficientDataError("Clustering", min_rows, len(self.df))

        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) < min_features:
            raise InsufficientFeaturesError(
                min_features, len(numeric_cols), "Clustering"
            )

        result = {
            "valid": True,
            "rows": len(self.df),
            "numeric_features": int(len(numeric_cols)),
            "applicable": True,
        }

        self.validation_report["analysis_specific_checks"]["clustering"] = result
        return result

    def validate_regression(
        self, target_col: Optional[str] = None, min_features: int = 2
    ) -> Dict[str, Any]:
        """Validate data for regression"""

        logger.info("Validating regression requirements")

        # Auto-detect target if not specified
        if not target_col:
            target_col = self._detect_target_column()

        if not target_col or target_col not in self.df.columns:
            raise ColumnMissingError(target_col or "target", list(self.df.columns))

        # Check target variance
        target = self.df[target_col]
        if target.std() == 0:
            raise NoVarianceError(target_col)

        # Count predictors
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
        if target_col in numeric_cols:
            numeric_cols.remove(target_col)

        if len(numeric_cols) < min_features:
            raise InsufficientFeaturesError(
                min_features, len(numeric_cols), "Regression"
            )

        result = {
            "valid": True,
            "target_column": target_col,
            "predictors": len(numeric_cols),
            "target_variance": float(target.var()),
            "applicable": True,
        }

        self.validation_report["analysis_specific_checks"]["regression"] = result
        return result

    def validate_rfm(
        self,
        customer_col: str,
        date_col: str,
        amount_col: str,
        min_customers: int = 20,
        min_timespan_days: int = 30,
    ) -> Dict[str, Any]:
        """Validate data for RFM analysis"""

        logger.info("Validating RFM requirements")

        # Check columns exist
        for col in [customer_col, date_col, amount_col]:
            if col not in self.df.columns:
                raise ColumnMissingError(col, list(self.df.columns))

        # Check unique customers
        unique_customers = self.df[customer_col].nunique()
        if unique_customers < min_customers:
            raise InsufficientDataError("RFM", min_customers, unique_customers)

        # Check timespan
        dates = pd.to_datetime(self.df[date_col])
        timespan = (dates.max() - dates.min()).days
        if timespan < min_timespan_days:
            raise InsufficientDataError("RFM Timespan", min_timespan_days, timespan)

        result = {
            "valid": True,
            "unique_customers": int(unique_customers),
            "timespan_days": int(timespan),
            "applicable": True,
        }

        self.validation_report["analysis_specific_checks"]["rfm"] = result
        return result

    # ============ LEVEL 4: STATISTICAL ASSUMPTIONS ============

    def test_stationarity(self, series: pd.Series) -> Dict[str, Any]:
        """ADF test for stationarity (time series)"""

        logger.info("Testing stationarity with ADF test")

        try:
            if not STATSMODELS_AVAILABLE or adfuller is None:
                raise ImportError("statsmodels not available")
            result = adfuller(series.dropna(), autolag="AIC")
            p_value = result[1]
            stationary = p_value < 0.05

            test_result = {
                "test_name": "ADF",
                "p_value": float(p_value),
                "statistic": float(result[0]),
                "lags": int(result[2]),
                "stationary": bool(stationary),
                "action": "proceed" if stationary else "differentiate",
            }
        except Exception as e:
            logger.warning(f"ADF test failed: {e}")
            test_result = {
                "test_name": "ADF",
                "error": str(e),
                "stationary": None,
                "available": STATSMODELS_AVAILABLE,
            }

        self.validation_report["statistical_assumptions"]["stationarity"] = test_result
        return test_result

    def test_multicollinearity(self, X: pd.DataFrame) -> Dict[str, Any]:
        """Calculate VIF for multicollinearity"""

        logger.info("Calculating VIF scores")

        if not STATSMODELS_AVAILABLE or variance_inflation_factor is None:
            logger.warning("statsmodels not available, skipping VIF calculation")
            return {
                "vif_scores": {},
                "high_vif_threshold": 5,
                "columns_to_remove": [],
                "available": False,
            }

        vif_scores = {}
        try:
            for i, col in enumerate(X.columns):
                try:
                    vif = variance_inflation_factor(X.values, i)
                    vif_scores[col] = float(vif)
                except:
                    vif_scores[col] = None
        except Exception as e:
            logger.warning(f"VIF calculation failed: {e}")

        return {
            "vif_scores": vif_scores,
            "high_vif_threshold": 5,
            "columns_to_remove": [
                col for col, vif in vif_scores.items() if vif and vif > 5
            ],
        }

    def _detect_target_column(self) -> Optional[str]:
        """Auto-detect target column"""

        keywords = ["revenue", "sales", "profit", "income", "target", "y", "outcome"]

        for col in self.df.columns:
            if any(kw in col.lower() for kw in keywords):
                return col

        # Default to first numeric column
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        return numeric_cols[0] if len(numeric_cols) > 0 else None

    def get_validation_report(self) -> Dict[str, Any]:
        """Get complete validation report"""
        return self.validation_report


def infer_pandas_frequency(series: pd.Series) -> str:
    """Infer pandas frequency from datetime series"""
    try:
        dates = pd.to_datetime(series).dropna()
        if len(dates) < 3:
            return "unknown"
        freq = pd.infer_freq(dates.sort_values())
        return freq if freq else "unknown"
    except:
        return "unknown"


def prepare_analysis_dataset(
    df: pd.DataFrame,
    analysis_type: str,
    dataset_id: Optional[Union[str, int]] = None,
    options: Optional[Dict[str, Any]] = None,
    correlation_id: Optional[str] = None,
) -> PreparedDataset:
    """Prepare and validate dataset for analysis"""

    options = options or {}
    dataset_id = dataset_id or "unknown"

    logger.info(f"Preparing dataset for {analysis_type}")

    working_df = df.copy()
    original_len = len(working_df)
    warnings: List[str] = []
    excluded_columns: List[str] = []

    missing_analysis = {}
    for col in working_df.columns:
        missing_count = working_df[col].isnull().sum()
        missing_pct = (
            (missing_count / len(working_df)) * 100 if len(working_df) > 0 else 0
        )
        missing_analysis[col] = {
            "missing_count": int(missing_count),
            "missing_percent": float(missing_pct),
        }

    working_df = working_df.dropna()
    excluded_rows = original_len - len(working_df)

    if excluded_rows > 0:
        warnings.append(f"Excluded {excluded_rows} rows with missing values")

    validator = DataValidator(working_df, str(dataset_id))

    try:
        schema = validator.validate_schema()
    except EmptyDatasetError:
        warnings.append("Dataset became empty after cleaning")
        working_df = df.copy()
        excluded_rows = 0
        validator = DataValidator(working_df, str(dataset_id))
        schema = validator.validate_schema()

    quality = validator.calculate_quality_metrics()

    validation_report = validator.get_validation_report()

    plan = {}

    if analysis_type == "forecasting":
        datetime_col = options.get("datetime_column")
        metric_col = options.get("metric_column")

        if not datetime_col:
            for col in working_df.columns:
                if "date" in col.lower() or "time" in col.lower():
                    datetime_col = col
                    break

        if not metric_col:
            numeric_cols = working_df.select_dtypes(include=[np.number]).columns
            for col in numeric_cols:
                if working_df[col].std() > 0:
                    metric_col = col
                    break

        if not datetime_col:
            datetime_col = working_df.columns[0]
        if not metric_col:
            numeric_cols = working_df.select_dtypes(include=[np.number]).columns
            metric_col = (
                numeric_cols[0] if len(numeric_cols) > 0 else working_df.columns[0]
            )

        plan = {
            "datetime_column": datetime_col,
            "metric_column": metric_col,
        }

    elif analysis_type == "clustering":
        numeric_cols = list(working_df.select_dtypes(include=[np.number]).columns)
        plan = {
            "feature_columns": numeric_cols[:10]
            if len(numeric_cols) > 10
            else numeric_cols,
        }

    elif analysis_type == "regression":
        target_col = options.get("target_column")
        if not target_col:
            for col in working_df.columns:
                if any(
                    kw in col.lower()
                    for kw in ["revenue", "sales", "profit", "target", "y"]
                ):
                    target_col = col
                    break
        if not target_col:
            numeric_cols = working_df.select_dtypes(include=[np.number]).columns
            target_col = numeric_cols[0] if len(numeric_cols) > 0 else None

        predictor_cols = [
            c
            for c in working_df.select_dtypes(include=[np.number]).columns
            if c != target_col
        ]

        expected_types = {}
        if target_col:
            expected_types[target_col] = "numeric"
        for col in predictor_cols:
            if col in working_df.columns:
                if "date" in col.lower():
                    expected_types[col] = "datetime"
                elif (
                    working_df[col].dtype == "object" or working_df[col].nunique() <= 20
                ):
                    expected_types[col] = "categorical"
                else:
                    expected_types[col] = "numeric"

        plan = {
            "target_column": target_col,
            "feature_columns": predictor_cols,
            "expected_types": expected_types,
        }

    elif analysis_type == "cohort":
        time_col = options.get("datetime_column")
        cohort_col = options.get("cohort_column")
        metric_col = options.get("metric_column")

        if not time_col:
            for col in working_df.columns:
                if "date" in col.lower() or "time" in col.lower():
                    time_col = col
                    break
        if not time_col:
            time_col = working_df.columns[0]

        if not cohort_col:
            for col in working_df.columns:
                if any(
                    kw in col.lower()
                    for kw in ["customer", "user", "segment", "category"]
                ):
                    cohort_col = col
                    break
        if not cohort_col:
            cohort_col = working_df.columns[0]

        if not metric_col:
            numeric_cols = working_df.select_dtypes(include=[np.number]).columns
            metric_col = (
                numeric_cols[0] if len(numeric_cols) > 0 else working_df.columns[0]
            )

        plan = {
            "time_column": time_col,
            "cohort_column": cohort_col,
            "metric_column": metric_col,
        }

    elif analysis_type == "rfm":
        customer_col = options.get("customer_column")
        date_col = options.get("date_column")
        amount_col = options.get("amount_column")

        if not customer_col:
            for col in working_df.columns:
                if (
                    "customer" in col.lower()
                    or "user" in col.lower()
                    or "id" in col.lower()
                ):
                    customer_col = col
                    break
        if not customer_col:
            customer_col = working_df.columns[0]

        if not date_col:
            for col in working_df.columns:
                if "date" in col.lower() or "time" in col.lower():
                    date_col = col
                    break
        if not date_col:
            date_col = working_df.columns[0]

        if not amount_col:
            for col in working_df.columns:
                if (
                    "amount" in col.lower()
                    or "revenue" in col.lower()
                    or "value" in col.lower()
                ):
                    amount_col = col
                    break
        if not amount_col:
            amount_col = (
                working_df.select_dtypes(include=[np.number]).columns[0]
                if len(working_df.select_dtypes(include=[np.number]).columns) > 0
                else working_df.columns[0]
            )

        plan = {
            "customer_column": customer_col,
            "date_column": date_col,
            "amount_column": amount_col,
        }

    return PreparedDataset(
        dataframe=working_df,
        plan=plan,
        validation=validation_report,
        excluded_rows=excluded_rows,
        missing_values_analysis=missing_analysis,
        warnings=warnings,
        excluded_columns=excluded_columns,
    )


def calculate_quality_metrics(
    df: pd.DataFrame, dataset_id: str = "unknown"
) -> Dict[str, Any]:
    """Calculate quality metrics for a dataframe"""
    validator = DataValidator(df, dataset_id)
    return validator.calculate_quality_metrics()


def validate_schema(
    df: pd.DataFrame,
    required_columns: Optional[List[str]] = None,
    column_types: Optional[Dict[str, str]] = None,
    dataset_id: str = "unknown",
) -> Dict[str, Any]:
    """Validate dataframe schema"""
    validator = DataValidator(df, dataset_id)
    return validator.validate_schema(required_columns, column_types)


def evaluate_regression_residuals(
    residuals: np.ndarray, design_matrix: np.ndarray
) -> dict[str, Any]:
    """Evaluate residual assumptions after training a regression model."""
    from scipy import stats as scipy_stats
    from statsmodels.stats.diagnostic import het_breuschpagan

    output: dict[str, Any] = {
        "residual_normality": {"test_name": "shapiro", "p_value": None, "normal": None},
        "heteroscedasticity": {
            "test_name": "breusch_pagan",
            "p_value": None,
            "heteroscedastic": None,
        },
    }
    if residuals.size == 0:
        return output

    if scipy_stats is not None:
        sample = residuals[: min(len(residuals), 5000)]
        try:
            statistic, p_value = scipy_stats.shapiro(sample)
            output["residual_normality"] = {
                "test_name": "shapiro",
                "test_statistic": round(float(statistic), 4),
                "p_value": round(float(p_value), 4),
                "normal": bool(p_value >= 0.05),
            }
        except Exception:
            pass

    if (
        het_breuschpagan is not None
        and design_matrix.shape[0] == residuals.shape[0]
        and design_matrix.shape[1] > 1
    ):
        try:
            lm_stat, lm_p_value, _, _ = het_breuschpagan(residuals, design_matrix)
            output["heteroscedasticity"] = {
                "test_name": "breusch_pagan",
                "test_statistic": round(float(lm_stat), 4),
                "p_value": round(float(lm_p_value), 4),
                "heteroscedastic": bool(lm_p_value < 0.05),
            }
        except Exception:
            pass
    return output
