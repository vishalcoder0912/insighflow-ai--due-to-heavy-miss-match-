"""Universal data normalization pipeline."""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

PREVIEW_ROWS = 100
NULL_VALUES = {
    "",
    "NULL",
    "N/A",
    "NA",
    "none",
    "None",
    "NaN",
    "null",
    "nan",
    "n/a",
    "nil",
    "undefined",
}
BOOLEAN_TRUE = {"true", "yes", "1", "y", "on", "t", "enabled"}
BOOLEAN_FALSE = {"false", "no", "0", "n", "off", "f", "disabled"}
CURRENCY_SYMBOLS = {"$", "€", "£", "¥", "₹", "₽", "₩", "₽", "฿", "₫"}


class DataNormalizer:
    """Universal data normalization pipeline."""

    def __init__(self, data: list[dict[str, Any]]):
        self.original_data = data
        self.df = pd.DataFrame(data)
        self.column_profiles: dict[str, dict[str, Any]] = {}

    def normalize(self) -> dict[str, Any]:
        """Run full normalization pipeline."""
        try:
            self._sanitize_column_names()
            self._normalize_nulls()
            self._detect_and_convert_types()
            self._normalize_strings()
            self._normalize_numbers()
            self._normalize_booleans()
            self._normalize_datetimes()

            self._profile_columns()

            return self._build_result()

        except Exception as e:
            logger.error(f"Normalization error: {e}")
            raise

    def _sanitize_column_names(self) -> None:
        """Sanitize column names to be valid identifiers."""
        new_columns = {}
        for col in self.df.columns:
            original = col
            col = str(col).strip()

            col = re.sub(r"[^\w\s\-\.]", "", col)
            col = re.sub(r"[\s\-]+", "_", col)
            col = re.sub(r"[^a-zA-Z0-9_]", "", col)
            col = re.sub(r"^(\d)", r"_\1", col)
            col = col.lower()

            if not col or col == "_":
                col = f"column_{len(new_columns)}"

            new_columns[original] = col

        self.df = self.df.rename(columns=new_columns)

    def _normalize_nulls(self) -> None:
        """Normalize null values."""
        for col in self.df.columns:
            if self.df[col].dtype == "object":
                self.df[col] = self.df[col].replace(NULL_VALUES, None)
                self.df[col] = self.df[col].replace("", None)

            self.df[col] = self.df[col].replace([np.nan, np.inf, -np.inf], None)

    def _detect_and_convert_types(self) -> None:
        """Detect and convert column types."""
        for col in self.df.columns:
            if self.df[col].dtype == "object":
                self._try_convert_datetime(col)
                self._try_convert_boolean(col)
                self._try_convert_numeric(col)

    def _try_convert_datetime(self, col: str) -> None:
        """Try to convert column to datetime."""
        try:
            converted = pd.to_datetime(self.df[col], errors="coerce")
            non_null_ratio = converted.notna().mean() if len(converted) > 0 else 0

            if non_null_ratio >= 0.8:
                self.df[col] = converted.dt.isoformat().where(
                    converted.notna(), self.df[col]
                )
                self.column_profiles[col]["inferred_type"] = "datetime"
        except Exception:
            pass

    def _try_convert_boolean(self, col: str) -> None:
        """Try to convert column to boolean."""
        try:
            lowered = self.df[col].astype(str).str.lower().str.strip()
            all_bool = lowered.isin(BOOLEAN_TRUE | BOOLEAN_FALSE).all()

            if all_bool:
                self.df[col] = lowered.isin(BOOLEAN_TRUE).map(
                    {True: True, False: False}
                )
                self.column_profiles[col]["inferred_type"] = "boolean"
        except Exception:
            pass

    def _try_convert_numeric(self, col: str) -> None:
        """Try to convert column to numeric."""
        try:
            cleaned = self._clean_numeric_string(self.df[col])
            converted = pd.to_numeric(cleaned, errors="coerce")
            non_null_ratio = converted.notna().mean() if len(converted) > 0 else 0

            if non_null_ratio >= 0.9:
                self.df[col] = converted
                self.column_profiles[col]["inferred_type"] = "numeric"
                self.column_profiles[col]["subtype"] = (
                    "integer"
                    if converted.apply(
                        lambda x: x == int(x) if pd.notna(x) else True
                    ).all()
                    else "float"
                )
        except Exception:
            pass

    def _normalize_strings(self) -> None:
        """Normalize string columns."""
        for col in self.df.columns:
            if (
                self.df[col].dtype == "object"
                and not self._is_datetime(col)
                and not self._is_boolean(col)
            ):
                self.df[col] = self.df[col].astype(str).str.strip()
                self.df[col] = self.df[col].replace("None", None)
                self.df[col] = self.df[col].replace("nan", None)

    def _normalize_numbers(self) -> None:
        """Normalize numeric columns."""
        for col in self.df.columns:
            if self.column_profiles.get(col, {}).get("inferred_type") == "numeric":
                try:
                    self.df[col] = self._clean_numeric_string(self.df[col])
                    self.df[col] = pd.to_numeric(self.df[col], errors="coerce")
                except Exception:
                    pass

    def _normalize_booleans(self) -> None:
        """Normalize boolean columns."""
        for col in self.df.columns:
            if self._is_boolean(col):
                try:
                    lowered = self.df[col].astype(str).str.lower().str.strip()
                    self.df[col] = lowered.isin(BOOLEAN_TRUE).map(
                        {True: True, False: False}
                    )
                except Exception:
                    pass

    def _normalize_datetimes(self) -> None:
        """Normalize datetime columns to ISO format."""
        for col in self.df.columns:
            if self._is_datetime(col):
                try:
                    dates = pd.to_datetime(self.df[col], errors="coerce")
                    self.df[col] = dates.dt.isoformat().where(
                        dates.notna(), self.df[col]
                    )
                except Exception:
                    pass

    def _clean_numeric_string(self, series: pd.Series) -> pd.Series:
        """Clean numeric strings by removing currency symbols and commas."""
        if series.dtype == "object":
            cleaned = series.astype(str).str.strip()
            cleaned = cleaned.str.replace(r"[,\$€£¥₹₽₩฿₫]", "", regex=True)
            cleaned = cleaned.str.strip()
            return cleaned
        return series

    def _is_datetime(self, col: str) -> bool:
        """Check if column is datetime type."""
        return self.column_profiles.get(col, {}).get("inferred_type") == "datetime"

    def _is_boolean(self, col: str) -> bool:
        """Check if column is boolean type."""
        return self.column_profiles.get(col, {}).get("inferred_type") == "boolean"

    def _profile_columns(self) -> None:
        """Generate column profiles with statistics."""
        row_count = len(self.df)

        for col in self.df.columns:
            profile = {
                "name": col,
                "inferred_type": "string",
                "subtype": "text",
                "null_count": int(self.df[col].isna().sum()),
                "null_percentage": round(float(self.df[col].isna().mean() * 100), 2),
                "unique_count": int(self.df[col].nunique()),
            }

            if self.df[col].dtype in ["int64", "float64"]:
                profile["inferred_type"] = "numeric"
                profile["subtype"] = (
                    "integer" if self.df[col].dtype == "int64" else "float"
                )
                profile["min"] = (
                    float(self.df[col].min()) if not self.df[col].isna().all() else None
                )
                profile["max"] = (
                    float(self.df[col].max()) if not self.df[col].isna().all() else None
                )
                profile["mean"] = (
                    round(float(self.df[col].mean()), 4)
                    if not self.df[col].isna().all()
                    else None
                )

                non_null = self.df[col].dropna()
                if len(non_null) > 0:
                    profile["median"] = round(float(non_null.median()), 4)

            elif profile["null_percentage"] < 100:
                non_null = self.df[col].dropna().astype(str)
                if len(non_null) > 0:
                    profile["sample_values"] = non_null.head(5).tolist()

            self.column_profiles[col] = profile

    def _build_result(self) -> dict[str, Any]:
        """Build final result dictionary."""
        normalized_data = self.df.to_dict(orient="records")
        preview_data = normalized_data[:PREVIEW_ROWS]

        return {
            "data": normalized_data,
            "data_preview": preview_data,
            "columns": list(self.column_profiles.values()),
            "row_count": len(normalized_data),
            "column_count": len(self.df.columns),
            "statistics": self._compute_statistics(),
        }

    def _compute_statistics(self) -> dict[str, Any]:
        """Compute overall dataset statistics."""
        total_cells = self.df.shape[0] * self.df.shape[1]
        null_cells = self.df.isna().sum().sum()

        return {
            "total_rows": len(self.df),
            "total_columns": len(self.df.columns),
            "total_cells": total_cells,
            "null_cells": int(null_cells),
            "completeness_percentage": round((1 - null_cells / total_cells) * 100, 2)
            if total_cells > 0
            else 0,
            "memory_usage_bytes": int(self.df.memory_usage(deep=True).sum()),
        }


def normalize_data(data: list[dict[str, Any]]) -> dict[str, Any]:
    """Convenience function to normalize data."""
    normalizer = DataNormalizer(data)
    return normalizer.normalize()
