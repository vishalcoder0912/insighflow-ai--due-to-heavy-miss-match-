"""Statistical Testing Engine."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

logger = logging.getLogger(__name__)

MIN_SAMPLE_SIZE = 10


class StatisticalEngine:
    """Statistical testing engine."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data

    def ttest_independent(
        self,
        group1_column: str,
        group2_column: str | None = None,
        group1_values: list | None = None,
        group2_values: list | None = None,
    ) -> dict[str, Any]:
        """Perform independent t-test."""
        try:
            if group1_values is None:
                if group1_column not in self.df.columns:
                    return {
                        "status": "error",
                        "message": f"Column '{group1_column}' not found",
                    }
                group1 = pd.to_numeric(self.df[group1_column], errors="coerce").dropna()
            else:
                group1 = pd.Series(group1_values)

            if group2_values is None:
                if group2_column and group2_column in self.df.columns:
                    group2 = pd.to_numeric(
                        self.df[group2_column], errors="coerce"
                    ).dropna()
                else:
                    return {
                        "status": "error",
                        "message": "Provide group2_column or group2_values",
                    }
            else:
                group2 = pd.Series(group2_values)

            if len(group1) < MIN_SAMPLE_SIZE or len(group2) < MIN_SAMPLE_SIZE:
                return {
                    "status": "error",
                    "message": f"Need at least {MIN_SAMPLE_SIZE} samples per group",
                }

            statistic, p_value = stats.ttest_ind(group1, group2)

            return {
                "status": "success",
                "test": "t-test_independent",
                "group1_n": len(group1),
                "group2_n": len(group2),
                "group1_mean": round(float(group1.mean()), 4),
                "group2_mean": round(float(group2.mean()), 4),
                "t_statistic": round(float(statistic), 4),
                "p_value": round(float(p_value), 6),
                "significant": p_value < 0.05,
                "interpretation": self._interpret_ttest(
                    p_value, group1.mean(), group2.mean()
                ),
            }

        except Exception as e:
            logger.error(f"T-test failed: {e}")
            return {"status": "error", "message": str(e)}

    def ttest_paired(
        self,
        before_column: str,
        after_column: str,
    ) -> dict[str, Any]:
        """Perform paired t-test."""
        try:
            if (
                before_column not in self.df.columns
                or after_column not in self.df.columns
            ):
                return {"status": "error", "message": "Columns not found"}

            before = pd.to_numeric(self.df[before_column], errors="coerce").dropna()
            after = pd.to_numeric(self.df[after_column], errors="coerce").dropna()

            common_idx = before.index.intersection(after.index)
            before = before.loc[common_idx]
            after = after.loc[common_idx]

            if len(before) < MIN_SAMPLE_SIZE:
                return {
                    "status": "error",
                    "message": f"Need at least {MIN_SAMPLE_SIZE} paired samples",
                }

            statistic, p_value = stats.ttest_rel(before, after)

            return {
                "status": "success",
                "test": "t-test_paired",
                "n_pairs": len(before),
                "before_mean": round(float(before.mean()), 4),
                "after_mean": round(float(after.mean()), 4),
                "mean_difference": round(float((after - before).mean()), 4),
                "t_statistic": round(float(statistic), 4),
                "p_value": round(float(p_value), 6),
                "significant": p_value < 0.05,
            }

        except Exception as e:
            logger.error(f"Paired t-test failed: {e}")
            return {"status": "error", "message": str(e)}

    def anova_oneway(
        self,
        group_columns: list[str],
    ) -> dict[str, Any]:
        """Perform one-way ANOVA."""
        try:
            groups = []
            for col in group_columns:
                if col not in self.df.columns:
                    return {"status": "error", "message": f"Column '{col}' not found"}
                group = pd.to_numeric(self.df[col], errors="coerce").dropna()
                if len(group) >= MIN_SAMPLE_SIZE:
                    groups.append(group)

            if len(groups) < 2:
                return {
                    "status": "error",
                    "message": "Need at least 2 groups with sufficient samples",
                }

            statistic, p_value = stats.f_oneway(*groups)

            return {
                "status": "success",
                "test": "ANOVA_oneway",
                "n_groups": len(groups),
                "group_sizes": [len(g) for g in groups],
                "group_means": [round(float(g.mean()), 4) for g in groups],
                "f_statistic": round(float(statistic), 4),
                "p_value": round(float(p_value), 6),
                "significant": p_value < 0.05,
                "interpretation": "Significant difference between groups"
                if p_value < 0.05
                else "No significant difference",
            }

        except Exception as e:
            logger.error(f"ANOVA failed: {e}")
            return {"status": "error", "message": str(e)}

    def chi_square(
        self,
        observed_column: str,
        expected_column: str | None = None,
    ) -> dict[str, Any]:
        """Perform chi-square test."""
        try:
            if observed_column not in self.df.columns:
                return {
                    "status": "error",
                    "message": f"Column '{observed_column}' not found",
                }

            observed = self.df[observed_column].value_counts()

            if len(observed) < 2:
                return {"status": "error", "message": "Need at least 2 categories"}

            if expected_column and expected_column in self.df.columns:
                expected = self.df[expected_column].value_counts()
                if len(observed) != len(expected):
                    return {"status": "error", "message": "Categories mismatch"}
                chi2, p_value = stats.chisquare(observed.values, expected.values)
            else:
                n = len(observed)
                expected_freq = np.full(n, observed.sum() / n)
                chi2, p_value = stats.chisquare(observed.values, expected_freq)

            return {
                "status": "success",
                "test": "chi_square",
                "n_categories": len(observed),
                "chi_square_statistic": round(float(chi2), 4),
                "p_value": round(float(p_value), 6),
                "significant": p_value < 0.05,
                "interpretation": "Significant association between variables"
                if p_value < 0.05
                else "No significant association",
            }

        except Exception as e:
            logger.error(f"Chi-square test failed: {e}")
            return {"status": "error", "message": str(e)}

    def correlation_pearson(
        self,
        column1: str,
        column2: str,
    ) -> dict[str, Any]:
        """Perform Pearson correlation test."""
        try:
            if column1 not in self.df.columns or column2 not in self.df.columns:
                return {"status": "error", "message": "Columns not found"}

            x = pd.to_numeric(self.df[column1], errors="coerce")
            y = pd.to_numeric(self.df[column2], errors="coerce")

            valid_idx = x.notna() & y.notna()
            x = x[valid_idx]
            y = y[valid_idx]

            if len(x) < MIN_SAMPLE_SIZE:
                return {
                    "status": "error",
                    "message": f"Need at least {MIN_SAMPLE_SIZE} samples",
                }

            r, p_value = stats.pearsonr(x, y)

            return {
                "status": "success",
                "test": "pearson_correlation",
                "column1": column1,
                "column2": column2,
                "n_samples": len(x),
                "correlation_coefficient": round(float(r), 4),
                "p_value": round(float(p_value), 6),
                "significant": p_value < 0.05,
                "interpretation": self._interpret_correlation(r),
            }

        except Exception as e:
            logger.error(f"Pearson correlation failed: {e}")
            return {"status": "error", "message": str(e)}

    def correlation_spearman(
        self,
        column1: str,
        column2: str,
    ) -> dict[str, Any]:
        """Perform Spearman correlation test."""
        try:
            if column1 not in self.df.columns or column2 not in self.df.columns:
                return {"status": "error", "message": "Columns not found"}

            x = pd.to_numeric(self.df[column1], errors="coerce")
            y = pd.to_numeric(self.df[column2], errors="coerce")

            valid_idx = x.notna() & y.notna()
            x = x[valid_idx]
            y = y[valid_idx]

            if len(x) < MIN_SAMPLE_SIZE:
                return {
                    "status": "error",
                    "message": f"Need at least {MIN_SAMPLE_SIZE} samples",
                }

            r, p_value = stats.spearmanr(x, y)

            return {
                "status": "success",
                "test": "spearman_correlation",
                "column1": column1,
                "column2": column2,
                "n_samples": len(x),
                "correlation_coefficient": round(float(r), 4),
                "p_value": round(float(p_value), 6),
                "significant": p_value < 0.05,
                "interpretation": self._interpret_correlation(r),
            }

        except Exception as e:
            logger.error(f"Spearman correlation failed: {e}")
            return {"status": "error", "message": str(e)}

    def normality_test(
        self,
        column: str,
    ) -> dict[str, Any]:
        """Test normality using Shapiro-Wilk."""
        try:
            if column not in self.df.columns:
                return {"status": "error", "message": f"Column '{column}' not found"}

            data = pd.to_numeric(self.df[column], errors="coerce").dropna()

            if len(data) < MIN_SAMPLE_SIZE:
                return {
                    "status": "error",
                    "message": f"Need at least {MIN_SAMPLE_SIZE} samples",
                }

            if len(data) > 5000:
                data = data.sample(5000, random_state=42)

            statistic, p_value = stats.shapiro(data)

            return {
                "status": "success",
                "test": "shapiro_wilk",
                "column": column,
                "n_samples": len(data),
                "statistic": round(float(statistic), 4),
                "p_value": round(float(p_value), 6),
                "is_normal": p_value > 0.05,
                "interpretation": "Data appears normally distributed"
                if p_value > 0.05
                else "Data does not appear normally distributed",
            }

        except Exception as e:
            logger.error(f"Normality test failed: {e}")
            return {"status": "error", "message": str(e)}

    def _interpret_ttest(self, p_value: float, mean1: float, mean2: float) -> str:
        """Interpret t-test results."""
        if p_value >= 0.05:
            return "No significant difference between groups"
        if mean1 > mean2:
            return f"Group 1 is significantly higher than Group 2 (diff: {mean1 - mean2:.2f})"
        return (
            f"Group 2 is significantly higher than Group 1 (diff: {mean2 - mean1:.2f})"
        )

    def _interpret_correlation(self, r: float) -> str:
        """Interpret correlation coefficient."""
        abs_r = abs(r)
        if abs_r < 0.1:
            return "No correlation"
        elif abs_r < 0.3:
            strength = "weak"
        elif abs_r < 0.5:
            strength = "moderate"
        elif abs_r < 0.7:
            strength = "strong"
        else:
            strength = "very strong"
        direction = "positive" if r > 0 else "negative"
        return f"{strength} {direction} correlation"


def run_ttest(data: list[dict], group1: str, group2: str = None) -> dict[str, Any]:
    """Convenience function for t-test."""
    engine = StatisticalEngine(data)
    return engine.ttest_independent(group1, group2)


def run_anova(data: list[dict], groups: list[str]) -> dict[str, Any]:
    """Convenience function for ANOVA."""
    engine = StatisticalEngine(data)
    return engine.anova_oneway(groups)


def run_correlation(
    data: list[dict], col1: str, col2: str, method: str = "pearson"
) -> dict[str, Any]:
    """Convenience function for correlation."""
    engine = StatisticalEngine(data)
    if method == "spearman":
        return engine.correlation_spearman(col1, col2)
    return engine.correlation_pearson(col1, col2)
