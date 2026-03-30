"""Cross-analysis insight generation for advanced analytics."""

from __future__ import annotations

from typing import Any


def generate_advanced_insights(results: dict[str, dict[str, Any]], failures: list[dict[str, Any]]) -> dict[str, Any]:
    """Generate a compact insight layer from advanced analytics outputs."""

    key_findings: list[str] = []
    anomalies: list[str] = []
    trends: list[str] = []
    opportunities: list[str] = []
    risks: list[str] = []
    recommendations: list[str] = []

    forecasting = results.get("forecasting", {}).get("results", {})
    if forecasting:
        trend = forecasting.get("trend")
        change_pct = forecasting.get("predicted_change_pct")
        if trend:
            trends.append(f"Forecast indicates a {trend} trend over the next horizon.")
        if change_pct is not None:
            key_findings.append(f"Projected first-period change is {change_pct}% versus the latest observed value.")
        if trend == "decreasing":
            risks.append("The forecast points to a near-term decline that should be investigated.")

    clustering = results.get("clustering", {}).get("results", {})
    if clustering:
        cluster_count = clustering.get("cluster_count")
        key_findings.append(f"Clustering identified {cluster_count} behavior segments.")
        opportunities.append("Use segment-specific dashboard filters and activation strategies.")

    regression = results.get("regression", {}).get("results", {})
    if regression:
        metrics = regression.get("metrics", {})
        r2 = metrics.get("r2")
        if r2 is not None:
            key_findings.append(f"Regression achieved an R² of {r2}, indicating the share of variance explained.")
            if r2 < 0.3:
                risks.append("Predictive power is limited; consider adding external or more granular features.")
        importance = regression.get("feature_importance", [])
        if importance:
            top_feature = importance[0]["feature"]
            trends.append(f"'{top_feature}' is currently the strongest modeled driver of the target metric.")

    cohort = results.get("cohort", {}).get("results", {})
    if cohort:
        avg_retention = cohort.get("average_retention")
        if avg_retention is not None:
            key_findings.append(f"Average cohort retention is {avg_retention}.")
            if avg_retention < 0.4:
                risks.append("Retention is weak across cohorts and may require lifecycle intervention.")

    rfm = results.get("rfm", {}).get("results", {})
    if rfm:
        segments = rfm.get("segments", [])
        if segments:
            top_segment = segments[0]["segment"]
            key_findings.append(f"The largest RFM segment is '{top_segment}'.")
            opportunities.append("Prioritize lifecycle actions for high-value RFM segments.")

    if failures:
        anomalies.append(f"{len(failures)} advanced analytics modules returned degraded or failed outputs.")
        recommendations.append("Review module-specific remediation guidance before relying on failed analyses.")

    if not recommendations:
        recommendations.append("Expose these advanced results in the dashboard as drill-down analytics cards.")
    if not opportunities:
        opportunities.append("Use the advanced outputs to personalize dashboards by segment, trend, and model driver.")

    return {
        "key_findings": key_findings[:5],
        "anomalies": anomalies[:5],
        "trends": trends[:5],
        "opportunities": opportunities[:5],
        "risks": risks[:5],
        "recommendations": recommendations[:5],
    }
