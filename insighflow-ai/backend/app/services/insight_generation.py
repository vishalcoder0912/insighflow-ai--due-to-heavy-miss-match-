"""
AI-powered insights generation engine.
Creates 3-layer insights: What/Why/Do with recommendations.
"""

import logging
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)


class InsightsGenerator:
    """Generate business insights from analysis results"""

    @staticmethod
    def generate_forecasting_insights(
        forecast_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generate insights from forecasting"""

        logger.info("Generating forecasting insights")

        forecast = forecast_result.get("forecast", [])
        trend = forecast_result.get("trend", "flat")
        seasonality = forecast_result.get("seasonality_detected", False)
        rmse = forecast_result.get("rmse", 0)

        # Layer 1: What did we find?
        if trend == "increasing":
            what = (
                f"Strong upward trend detected. Forecast shows growth over next period."
            )
        elif trend == "decreasing":
            what = f"Concerning downward trend detected. Prepare for potential decline."
        else:
            what = "Data shows stable trend with no significant growth or decline."

        if seasonality:
            what += " Seasonal patterns detected - peaks and troughs are predictable."

        # Layer 2: Why is this important?
        if trend == "increasing":
            why = [
                "✓ Growth exceeds targets - opportunity to capitalize",
                "✓ Positive momentum - advantage over competitors",
            ]
        elif trend == "decreasing":
            why = [
                "⚠ Declining trend - investigate causes immediately",
                "⚠ Risk of further decline if trend continues",
            ]
        else:
            why = ["Stable performance provides predictability for planning"]

        # Layer 3: What should I do?
        do = []

        if trend == "increasing":
            do.append(
                {
                    "priority": "HIGH",
                    "action": "Increase inventory/resources by 20-30%",
                    "expected_impact": f"Capture growth, avoid stockouts/bottlenecks",
                }
            )
        elif trend == "decreasing":
            do.append(
                {
                    "priority": "HIGH",
                    "action": "Investigate root causes of decline",
                    "expected_impact": "Identify and address issues before worse performance",
                }
            )

        if seasonality:
            do.append(
                {
                    "priority": "MEDIUM",
                    "action": "Prepare for seasonal peaks and troughs",
                    "expected_impact": "Optimize resources for predictable pattern",
                }
            )

        return {
            "findings": what,
            "business_impact": why,
            "recommendations": do,
            "model_quality": "HIGH"
            if rmse < 100
            else "MEDIUM"
            if rmse < 200
            else "LOW",
        }

    @staticmethod
    def generate_clustering_insights(
        clustering_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generate insights from clustering"""

        logger.info("Generating clustering insights")

        clusters = clustering_result.get("clusters", [])

        # Layer 1: What
        what = f"Customer base divides into {len(clusters)} distinct segments."
        for cluster in clusters[:3]:
            what += f"\n- {cluster['cluster_name']}: {cluster['percentage_of_total']:.1f}% of customers"

        # Layer 2: Why
        why = [
            f"✓ Segment {clusters[0]['cluster_name']} is largest - prioritize retention",
            "✓ Diverse segments indicate need for targeted strategies",
        ]

        # Layer 3: Do
        do = []
        for i, cluster in enumerate(clusters[:2]):
            do.append(
                {
                    "priority": "HIGH" if i == 0 else "MEDIUM",
                    "segment": cluster["cluster_name"],
                    "action": f"Create targeted campaign for {cluster['cluster_name']} segment",
                    "expected_impact": f"Improve engagement in {cluster['percentage_of_total']:.0f}% of customer base",
                }
            )

        return {
            "findings": what,
            "business_impact": why,
            "recommendations": do,
            "segments_identified": len(clusters),
        }

    @staticmethod
    def generate_regression_insights(
        regression_result: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generate insights from regression"""

        logger.info("Generating regression insights")

        features = regression_result.get("features", [])
        r2 = regression_result.get("r2_score", 0)

        # Top drivers
        top_feature = features[0] if features else None

        # Layer 1: What
        what = f"Analysis explains {r2 * 100:.0f}% of variance in target."
        if top_feature:
            what += f"\nTop driver: {top_feature['feature']} (coefficient: {top_feature['coefficient']:.2f})"

        # Layer 2: Why
        why = []
        if r2 > 0.7:
            why.append("✓ STRONG model - findings are reliable")
        elif r2 > 0.5:
            why.append("✓ MODERATE model - useful but other factors exist")
        else:
            why.append("⚠ WEAK model - external factors play major role")

        # Layer 3: Do
        do = []
        if top_feature:
            do.append(
                {
                    "priority": "HIGH",
                    "action": f"Focus on optimizing {top_feature['feature']}",
                    "expected_impact": f"Highest ROI lever for improving target metric",
                    "coefficient": top_feature["coefficient"],
                }
            )

        unexplained = (1 - r2) * 100
        if unexplained > 30:
            do.append(
                {
                    "priority": "MEDIUM",
                    "action": "Investigate unexplained factors",
                    "expected_impact": f"Uncover {unexplained:.0f}% of hidden drivers",
                }
            )

        return {
            "findings": what,
            "business_impact": why,
            "recommendations": do,
            "model_strength": "STRONG"
            if r2 > 0.7
            else "MODERATE"
            if r2 > 0.5
            else "WEAK",
        }

    @staticmethod
    def generate_rfm_insights(rfm_result: Dict[str, Any]) -> Dict[str, Any]:
        """Generate insights from RFM"""

        logger.info("Generating RFM insights")

        insights = rfm_result.get("insights", {})
        segments = rfm_result.get("segments", {})

        champions = segments.get("Champions", {})
        at_risk = segments.get("At Risk", {})

        # Layer 1: What
        what = f"Total {insights.get('total_customers', 0)} customers analyzed."
        what += f"\n{champions.get('count', 0)} Champions generate {insights.get('champions', {}).get('percent_of_revenue', 0):.0f}% of revenue"
        what += f"\n{at_risk.get('count', 0)} At-Risk customers at churn risk"

        # Layer 2: Why
        why = [
            f"✓ Champions ({insights.get('champions', {}).get('percent_of_customers', 0):.0f}% of base) drive {insights.get('champions', {}).get('percent_of_revenue', 0):.0f}% of revenue",
            "⚠ Concentration risk - losing champions would impact revenue significantly",
            f"⚠ {at_risk.get('count', 0)} at-risk customers represent ${at_risk.get('total_revenue', 0):.0f} in jeopardy",
        ]

        # Layer 3: Do
        do = [
            {
                "priority": "CRITICAL",
                "action": "VIP Program for Champions",
                "expected_impact": f"Retain {insights.get('champions', {}).get('percent_of_customers', 0):.0f}% of revenue base",
                "customers_affected": champions.get("count", 0),
            },
            {
                "priority": "HIGH",
                "action": "Launch Win-Back Campaign for At-Risk",
                "expected_impact": f"Protect ${at_risk.get('total_revenue', 0):.0f} revenue",
                "customers_affected": at_risk.get("count", 0),
            },
        ]

        return {
            "findings": what,
            "business_impact": why,
            "recommendations": do,
            "key_alerts": insights.get("key_insights", []),
        }
