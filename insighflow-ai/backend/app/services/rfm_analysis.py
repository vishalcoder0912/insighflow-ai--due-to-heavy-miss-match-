"""
RFM (Recency, Frequency, Monetary) customer segmentation.
Scores customers on value and engagement for targeted actions.
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime, timedelta

from app.services.monitoring import (
    timed_operation,
    log_analysis_start,
    log_analysis_complete,
)
from app.services.error_handling import InsufficientDataError
from app.services.validation import PreparedDataset, prepare_analysis_dataset

logger = logging.getLogger(__name__)


def _score_series(series: pd.Series, *, reverse: bool = False) -> pd.Series:
    ranked = series.rank(method="first")
    labels = [1, 2, 3, 4]
    if reverse:
        labels = list(reversed(labels))
    return pd.qcut(ranked, q=4, labels=labels).astype(int)


@timed_operation("advanced_rfm", target_ms=30000)
def run_rfm_analysis(
    df: pd.DataFrame,
    *,
    dataset_id: str | int | None = None,
    options: dict[str, Any] | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    """Perform recency-frequency-monetary segmentation."""

    options = options or {}
    prepared: PreparedDataset = prepare_analysis_dataset(
        df,
        analysis_type="rfm",
        dataset_id=dataset_id,
        options=options,
        correlation_id=correlation_id,
    )
    plan = prepared.plan
    customer_column = plan["customer_column"]
    date_column = plan["date_column"]
    amount_column = plan["amount_column"]

    working = prepared.dataframe[[customer_column, date_column, amount_column]].copy()
    working[date_column] = pd.to_datetime(working[date_column], errors="coerce")
    snapshot_date = working[date_column].max() + pd.Timedelta(days=1)

    rfm = (
        working.groupby(customer_column)
        .agg(
            recency=(
                date_column,
                lambda values: int((snapshot_date - values.max()).days),
            ),
            frequency=(date_column, "count"),
            monetary=(amount_column, "sum"),
        )
        .reset_index()
    )
    rfm["recency_score"] = _score_series(rfm["recency"], reverse=True)
    rfm["frequency_score"] = _score_series(rfm["frequency"])
    rfm["monetary_score"] = _score_series(rfm["monetary"])
    rfm["segment"] = [
        _segment_label(
            int(row.recency_score), int(row.frequency_score), int(row.monetary_score)
        )
        for row in rfm.itertuples(index=False)
    ]

    segment_summary = (
        rfm.groupby("segment")
        .agg(
            customers=(customer_column, "count"),
            avg_monetary=("monetary", "mean"),
            avg_frequency=("frequency", "mean"),
        )
        .reset_index()
        .sort_values("customers", ascending=False)
    )

    return {
        "status": "SUCCESS",
        "confidence": "HIGH",
        "analysis_type": "rfm",
        "processed_rows": int(len(prepared.dataframe)),
        "total_rows": int(len(df)),
        "excluded_rows": prepared.excluded_rows,
        "exclusion_reasons": {"preprocessing": prepared.excluded_rows},
        "quality_score": prepared.validation["quality_metrics"]["overall_score"],
        "validation": prepared.validation,
        "missing_values_analysis": prepared.missing_values_analysis,
        "results": {
            "customer_column": customer_column,
            "date_column": date_column,
            "amount_column": amount_column,
            "customer_count": int(rfm[customer_column].nunique()),
            "segments": segment_summary.round(4).to_dict(orient="records"),
            "rfm_sample": rfm.head(100).to_dict(orient="records"),
        },
        "warnings": list(prepared.warnings),
    }


def _segment_label(r: int, f: int, m: int) -> str:
    if r >= 4 and f >= 4 and m >= 4:
        return "champions"
    if r >= 3 and f >= 3:
        return "loyal"
    if r == 4 and f <= 2:
        return "new_customers"
    if r <= 2 and f >= 3:
        return "at_risk"
    return "potential_loyalists"


class RFMAnalyzer:
    """RFM analysis engine"""

    def __init__(self, df: pd.DataFrame, dataset_id: str):
        self.df = df.copy()
        self.dataset_id = dataset_id
        self.rfm_scores = None

    @timed_operation("RFM Analysis")
    def analyze(
        self,
        customer_col: str,
        date_col: str,
        amount_col: str,
        reference_date: Optional[datetime] = None,
        quartiles: int = 4,
    ) -> Dict[str, Any]:
        """Perform RFM analysis"""

        log_analysis_start(self.dataset_id, "rfm", len(self.df), len(self.df.columns))

        for col in [customer_col, date_col, amount_col]:
            if col not in self.df.columns:
                raise ValueError(f"Column '{col}' not found")

        if reference_date is None:
            reference_date = pd.to_datetime(self.df[date_col]).max()
        else:
            reference_date = pd.to_datetime(reference_date)

        logger.info(f"RFM reference date: {reference_date}")

        rfm_df = self._calculate_rfm_metrics(
            customer_col, date_col, amount_col, reference_date
        )

        rfm_scores = self._score_customers(rfm_df, quartiles)

        segments = self._segment_customers(rfm_scores)

        insights = self._generate_rfm_insights(rfm_scores, segments)

        log_analysis_complete(
            self.dataset_id,
            "rfm",
            0,
            "rfm_analysis",
            {"total_customers": len(rfm_scores)},
        )

        return {
            "status": "SUCCESS",
            "total_customers": len(rfm_scores),
            "customers": rfm_scores,
            "segments": segments,
            "insights": insights,
        }

    def _calculate_rfm_metrics(
        self,
        customer_col: str,
        date_col: str,
        amount_col: str,
        reference_date: datetime,
    ) -> pd.DataFrame:
        """Calculate RFM metrics per customer"""

        logger.info("Calculating RFM metrics")

        df = self.df.copy()
        df[date_col] = pd.to_datetime(df[date_col])

        rfm = (
            df.groupby(customer_col)
            .agg(
                {
                    date_col: lambda x: (reference_date - x.max()).days,
                    amount_col: "sum",
                }
            )
            .reset_index()
        )

        frequency = df.groupby(customer_col).size().reset_index(name="frequency")
        rfm = rfm.merge(frequency, on=customer_col)

        rfm.columns = [customer_col, "recency_days", "monetary", "frequency"]

        logger.info(f"Calculated RFM for {len(rfm)} customers")

        return rfm

    def _score_customers(
        self, rfm_df: pd.DataFrame, quartiles: int = 4
    ) -> List[Dict[str, Any]]:
        """Score customers on R, F, M using quartiles"""

        logger.info(f"Scoring customers with {quartiles} quartiles")

        rfm_df["recency_score"] = (
            pd.qcut(
                rfm_df["recency_days"], q=quartiles, labels=False, duplicates="drop"
            )
            + 1
        )
        rfm_df["recency_score"] = quartiles - rfm_df["recency_score"] + 1

        rfm_df["frequency_score"] = (
            pd.qcut(rfm_df["frequency"], q=quartiles, labels=False, duplicates="drop")
            + 1
        )

        rfm_df["monetary_score"] = (
            pd.qcut(rfm_df["monetary"], q=quartiles, labels=False, duplicates="drop")
            + 1
        )

        rfm_df["rfm_score"] = (
            rfm_df["recency_score"].astype(str)
            + rfm_df["frequency_score"].astype(str)
            + rfm_df["monetary_score"].astype(str)
        )

        scores_list = []
        for _, row in rfm_df.iterrows():
            scores_list.append(
                {
                    "customer_id": str(row[rfm_df.columns[0]]),
                    "recency_days": int(row["recency_days"]),
                    "recency_score": int(row["recency_score"]),
                    "frequency": int(row["frequency"]),
                    "frequency_score": int(row["frequency_score"]),
                    "monetary_value": float(row["monetary"]),
                    "monetary_score": int(row["monetary_score"]),
                    "rfm_score": str(row["rfm_score"]),
                }
            )

        self.rfm_scores = scores_list
        return scores_list

    def _segment_customers(
        self, rfm_scores: List[Dict[str, Any]]
    ) -> Dict[str, Dict[str, Any]]:
        """Segment customers based on RFM scores"""

        logger.info("Segmenting customers")

        segments = {
            "Champions": {
                "customers": [],
                "description": "Best customers: Recent, frequent, high-value",
            },
            "Loyal": {
                "customers": [],
                "description": "Consistent buyers with solid history",
            },
            "At Risk": {
                "customers": [],
                "description": "Previously active, now declining",
            },
            "Need Attention": {
                "customers": [],
                "description": "Moderate engagement, slipping",
            },
            "Lost": {"customers": [], "description": "Completely inactive"},
        }

        for customer in rfm_scores:
            r = customer["recency_score"]
            f = customer["frequency_score"]
            m = customer["monetary_score"]

            if r >= 3 and f >= 3 and m >= 3:
                segment = "Champions"
            elif f >= 3 and m >= 2:
                segment = "Loyal"
            elif r < 2 and f >= 2 and m >= 2:
                segment = "At Risk"
            elif r >= 2 and f <= 2 and m <= 2:
                segment = "Need Attention"
            else:
                segment = "Lost"

            segments[segment]["customers"].append(customer)

        for segment_name, segment_data in segments.items():
            customers = segment_data["customers"]
            if customers:
                segment_data["count"] = len(customers)
                segment_data["avg_monetary"] = np.mean(
                    [c["monetary_value"] for c in customers]
                )
                segment_data["total_revenue"] = np.sum(
                    [c["monetary_value"] for c in customers]
                )
                segment_data["percent_of_total"] = (
                    len(customers) / len(rfm_scores)
                ) * 100
            else:
                segment_data["count"] = 0
                segment_data["avg_monetary"] = 0
                segment_data["total_revenue"] = 0
                segment_data["percent_of_total"] = 0

        return segments

    def _generate_rfm_insights(
        self, rfm_scores: List[Dict[str, Any]], segments: Dict[str, Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Generate actionable RFM insights"""

        total_customers = len(rfm_scores)
        total_revenue = sum(c["monetary_value"] for c in rfm_scores)

        champions = segments.get("Champions", {})
        champions_count = champions.get("count", 0)
        champions_revenue = champions.get("total_revenue", 0)

        at_risk = segments.get("At Risk", {})
        at_risk_count = at_risk.get("count", 0)
        at_risk_revenue = at_risk.get("total_revenue", 0)

        champions_pct = (
            (champions_revenue / total_revenue * 100) if total_revenue > 0 else 0
        )

        return {
            "total_customers": total_customers,
            "total_revenue": float(total_revenue),
            "champions": {
                "count": champions_count,
                "revenue": float(champions_revenue),
                "percent_of_customers": (champions_count / total_customers * 100)
                if total_customers > 0
                else 0,
                "percent_of_revenue": champions_pct,
            },
            "at_risk": {
                "count": at_risk_count,
                "revenue": float(at_risk_revenue),
                "action": f"Launch retention campaign for {at_risk_count} customers",
            },
            "key_insights": [
                f"Champions ({champions_count} customers) generate {champions_pct:.1f}% of revenue",
                f"At-risk segment: {at_risk_count} customers, ${at_risk_revenue:.0f} at stake",
                f"Focus retention on champions to protect {champions_pct:.0f}% of revenue",
            ],
        }
