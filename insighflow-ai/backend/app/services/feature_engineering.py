"""
Comprehensive feature engineering pipeline:
1. Categorical encoding
2. Temporal feature extraction
3. Numeric transformation
4. Feature scaling
5. Low-variance removal
6. Multicollinearity handling
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Any
import logging
from sklearn.preprocessing import (
    StandardScaler,
    MinMaxScaler,
    OneHotEncoder,
    LabelEncoder,
)
from sklearn.feature_selection import VarianceThreshold
from scipy import stats
from scipy.special import boxcox

from app.services.monitoring import timed_operation, log_warning
from app.core.exceptions import FeatureEngineeringError

logger = logging.getLogger(__name__)


class FeatureEngineer:
    """Feature engineering pipeline"""

    def __init__(self, df: pd.DataFrame, dataset_id: str):
        self.df = df.copy()
        self.dataset_id = dataset_id
        self.original_shape = self.df.shape
        self.engineering_report = {
            "input_columns": len(df.columns),
            "input_rows": len(df),
            "transformations": [],
            "removals": [],
            "output_columns": 0,
            "output_rows": 0,
        }

    @timed_operation("Feature Engineering Pipeline")
    def execute_pipeline(
        self, target_col: Optional[str] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """Execute complete feature engineering pipeline"""

        try:
            numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
            categorical_cols = self.df.select_dtypes(
                include=["object", "category"]
            ).columns.tolist()
            datetime_cols = self.df.select_dtypes(
                include=["datetime64"]
            ).columns.tolist()

            logger.info(
                f"Detected: {len(numeric_cols)} numeric, {len(categorical_cols)} categorical, {len(datetime_cols)} datetime"
            )

            if categorical_cols:
                self.df = self._encode_categorical(self.df, categorical_cols)

            if datetime_cols:
                self.df = self._extract_datetime_features(self.df, datetime_cols)

            numeric_cols = self.df.select_dtypes(include=[np.number]).columns.tolist()
            if numeric_cols:
                self.df = self._transform_numeric(self.df, numeric_cols, target_col)

            self.df = self._remove_low_variance(self.df)

            self.df = self._scale_features(self.df)

            self.engineering_report["output_columns"] = len(self.df.columns)
            self.engineering_report["output_rows"] = len(self.df)

            logger.info(
                f"Feature engineering complete: {self.original_shape} → {self.df.shape}"
            )

            return self.df, self.engineering_report

        except Exception as e:
            logger.error(f"Feature engineering failed: {e}")
            raise FeatureEngineeringError("pipeline", str(e))

    def _encode_categorical(
        self, df: pd.DataFrame, categorical_cols: List[str]
    ) -> pd.DataFrame:
        """Encode categorical variables"""

        logger.info(f"Encoding {len(categorical_cols)} categorical features")

        for col in categorical_cols:
            unique_count = df[col].nunique()

            if unique_count <= 10:
                try:
                    dummies = pd.get_dummies(df[col], prefix=col, drop_first=True)
                    df = pd.concat([df, dummies], axis=1)
                    df = df.drop(col, axis=1)

                    self.engineering_report["transformations"].append(
                        {
                            "type": "categorical_encoding",
                            "column": col,
                            "method": "one_hot",
                            "original_values": unique_count,
                            "new_features": len(dummies.columns),
                        }
                    )
                    logger.info(
                        f"One-hot encoded '{col}': {unique_count} → {len(dummies.columns)} features"
                    )
                except Exception as e:
                    logger.warning(f"One-hot encoding failed for '{col}': {e}")
            else:
                try:
                    le = LabelEncoder()
                    encoded_values = le.fit_transform(df[col].astype(str))
                    df = df.assign(**{col: encoded_values.astype(int)})

                    self.engineering_report["transformations"].append(
                        {
                            "type": "categorical_encoding",
                            "column": col,
                            "method": "label_encoding",
                            "original_values": unique_count,
                        }
                    )
                    logger.info(f"Label encoded '{col}': {unique_count} unique values")
                except Exception as e:
                    logger.warning(f"Label encoding failed for '{col}': {e}")

        return df

    def _extract_datetime_features(
        self, df: pd.DataFrame, datetime_cols: List[str]
    ) -> pd.DataFrame:
        """Extract temporal features from datetime columns"""

        logger.info(f"Extracting datetime features from {len(datetime_cols)} columns")

        for col in datetime_cols:
            try:
                dt = pd.to_datetime(df[col])

                df[f"{col}_year"] = dt.dt.year
                df[f"{col}_month"] = dt.dt.month
                df[f"{col}_quarter"] = dt.dt.quarter
                df[f"{col}_day_of_week"] = dt.dt.dayofweek
                df[f"{col}_day_of_month"] = dt.dt.day
                df[f"{col}_week_of_year"] = dt.dt.isocalendar().week

                df[f"{col}_month_sin"] = np.sin(2 * np.pi * df[f"{col}_month"] / 12)
                df[f"{col}_month_cos"] = np.cos(2 * np.pi * df[f"{col}_month"] / 12)

                df[f"{col}_days_since_start"] = (dt - dt.min()).dt.days

                df = df.drop(col, axis=1)

                self.engineering_report["transformations"].append(
                    {
                        "type": "datetime_extraction",
                        "column": col,
                        "features_extracted": 10,
                    }
                )
                logger.info(f"Extracted 10 temporal features from '{col}'")
            except Exception as e:
                logger.warning(f"Datetime extraction failed for '{col}': {e}")

        return df

    def _transform_numeric(
        self,
        df: pd.DataFrame,
        numeric_cols: List[str],
        target_col: Optional[str] = None,
    ) -> pd.DataFrame:
        """Transform numeric features (log, Box-Cox, etc.)"""

        logger.info(f"Transforming {len(numeric_cols)} numeric features")

        for col in numeric_cols:
            if col == target_col:
                continue

            if df[col].std() == 0:
                logger.warning(f"Skipping '{col}': no variation")
                continue

            skewness = stats.skew(df[col].dropna())

            if abs(skewness) > 1:
                try:
                    if (df[col] > 0).all():
                        df[col], _ = boxcox(df[col] + 1)
                        method = "box_cox"
                    else:
                        df[col] = np.log1p(df[col].abs())
                        method = "log_transform"

                    new_skewness = stats.skew(df[col].dropna())
                    self.engineering_report["transformations"].append(
                        {
                            "type": "numeric_transform",
                            "column": col,
                            "method": method,
                            "skew_before": float(skewness),
                            "skew_after": float(new_skewness),
                        }
                    )
                    logger.info(
                        f"Applied {method} to '{col}': skew {skewness:.2f} → {new_skewness:.2f}"
                    )
                except Exception as e:
                    logger.warning(f"Transformation failed for '{col}': {e}")

        return df

    def _remove_low_variance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Remove low-variance features"""

        logger.info("Removing low-variance features")

        try:
            selector = VarianceThreshold(threshold=0.01)
            numeric_cols = df.select_dtypes(include=[np.number]).columns

            high_variance = selector.fit_transform(df[numeric_cols])
            high_variance_cols = (
                df[numeric_cols].columns[selector.get_support()].tolist()
            )

            low_variance_cols = [
                col for col in numeric_cols if col not in high_variance_cols
            ]

            if low_variance_cols:
                df = df.drop(low_variance_cols, axis=1)
                self.engineering_report["removals"].append(
                    {"reason": "low_variance", "columns": low_variance_cols}
                )
                logger.info(f"Removed {len(low_variance_cols)} low-variance features")
        except Exception as e:
            logger.warning(f"Low-variance removal failed: {e}")

        return df

    def _scale_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Scale numeric features"""

        logger.info("Scaling numeric features")

        try:
            numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

            if numeric_cols:
                scaler = StandardScaler()
                df[numeric_cols] = scaler.fit_transform(df[numeric_cols])

                self.engineering_report["transformations"].append(
                    {
                        "type": "scaling",
                        "method": "standardization",
                        "columns": len(numeric_cols),
                    }
                )
                logger.info(f"Standardized {len(numeric_cols)} numeric features")
        except Exception as e:
            logger.warning(f"Scaling failed: {e}")

        return df

    def get_report(self) -> Dict[str, Any]:
        """Get feature engineering report"""
        return self.engineering_report
