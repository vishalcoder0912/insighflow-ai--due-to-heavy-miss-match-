"""Statistical testing service."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
from scipy import stats

logger = logging.getLogger(__name__)


class StatisticalTester:
    """Perform statistical tests on data."""

    @staticmethod
    def t_test(group1: list | np.ndarray, group2: list | np.ndarray) -> dict[str, Any]:
        """Perform independent t-test."""
        try:
            group1 = np.array(group1)
            group2 = np.array(group2)

            t_statistic, p_value = stats.ttest_ind(group1, group2)

            return {
                "test": "t-test",
                "test_statistic": float(t_statistic),
                "p_value": float(p_value),
                "significant_at_0_05": p_value < 0.05,
                "significant_at_0_01": p_value < 0.01,
                "interpretation": "Groups are significantly different"
                if p_value < 0.05
                else "No significant difference",
                "group1_mean": float(np.mean(group1)),
                "group2_mean": float(np.mean(group2)),
                "group1_std": float(np.std(group1)),
                "group2_std": float(np.std(group2)),
            }
        except Exception as e:
            logger.error(f"❌ T-test error: {e}")
            raise

    @staticmethod
    def anova(*groups) -> dict[str, Any]:
        """Perform one-way ANOVA."""
        try:
            groups = [np.array(g) for g in groups]
            f_statistic, p_value = stats.f_oneway(*groups)

            return {
                "test": "ANOVA",
                "f_statistic": float(f_statistic),
                "p_value": float(p_value),
                "significant_at_0_05": p_value < 0.05,
                "significant_at_0_01": p_value < 0.01,
                "num_groups": len(groups),
                "interpretation": "Groups are significantly different"
                if p_value < 0.05
                else "No significant difference",
                "group_means": [float(np.mean(g)) for g in groups],
            }
        except Exception as e:
            logger.error(f"❌ ANOVA error: {e}")
            raise

    @staticmethod
    def chi_square(contingency_table: np.ndarray) -> dict[str, Any]:
        """Perform chi-square test."""
        try:
            chi2, p_value, dof, expected = stats.chi2_contingency(contingency_table)

            return {
                "test": "Chi-Square",
                "chi2_statistic": float(chi2),
                "p_value": float(p_value),
                "degrees_of_freedom": int(dof),
                "significant_at_0_05": p_value < 0.05,
                "significant_at_0_01": p_value < 0.01,
                "interpretation": "Variables are independent"
                if p_value < 0.05
                else "No significant association",
            }
        except Exception as e:
            logger.error(f"❌ Chi-square test error: {e}")
            raise

    @staticmethod
    def correlation_significance(
        correlation_matrix: np.ndarray, n_samples: int
    ) -> dict[str, Any]:
        """Calculate p-values for correlations."""
        try:
            correlations = []
            p_values = []

            n_vars = correlation_matrix.shape[0]

            for i in range(n_vars):
                for j in range(i + 1, n_vars):
                    corr = correlation_matrix[i, j]
                    t_stat = (
                        corr * np.sqrt(n_samples - 2) / np.sqrt(1 - corr**2 + 1e-10)
                    )
                    p_value = 2 * (1 - stats.t.cdf(np.abs(t_stat), n_samples - 2))

                    correlations.append(
                        {
                            "var1_idx": int(i),
                            "var2_idx": int(j),
                            "correlation": float(corr),
                            "p_value": float(p_value),
                            "significant_at_0_05": p_value < 0.05,
                        }
                    )

            return {
                "test": "Correlation Significance",
                "correlations": correlations,
                "significant_pairs": [
                    c for c in correlations if c["significant_at_0_05"]
                ],
            }
        except Exception as e:
            logger.error(f"❌ Correlation significance error: {e}")
            raise

    @staticmethod
    def normality_test(data: list | np.ndarray) -> dict[str, Any]:
        """Perform Shapiro-Wilk normality test."""
        try:
            data = np.array(data)
            statistic, p_value = stats.shapiro(data)

            return {
                "test": "Shapiro-Wilk",
                "statistic": float(statistic),
                "p_value": float(p_value),
                "is_normal": p_value > 0.05,
                "interpretation": "Data appears normal"
                if p_value > 0.05
                else "Data is not normally distributed",
            }
        except Exception as e:
            logger.error(f"❌ Normality test error: {e}")
            raise

    @staticmethod
    def confidence_interval(
        data: list | np.ndarray, confidence: float = 0.95
    ) -> dict[str, Any]:
        """Calculate confidence interval for mean."""
        try:
            data = np.array(data)
            mean = np.mean(data)
            sem = stats.sem(data)
            interval = stats.t.interval(confidence, len(data) - 1, loc=mean, scale=sem)

            return {
                "mean": float(mean),
                "std": float(np.std(data)),
                "confidence_level": confidence,
                "lower_bound": float(interval[0]),
                "upper_bound": float(interval[1]),
                "margin_of_error": float(interval[1] - mean),
            }
        except Exception as e:
            logger.error(f"❌ Confidence interval error: {e}")
            raise
