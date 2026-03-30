"""Dashboard Generation Engine - Auto-generate dashboards from data."""

from __future__ import annotations

import logging
import uuid
from datetime import datetime
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)

CHART_TYPES = [
    "line",
    "bar",
    "horizontal_bar",
    "area",
    "scatter",
    "pie",
    "donut",
    "histogram",
    "heatmap",
]

DOMAIN_THEMES = {
    "sales": {"primary": "#0f766e", "accent": "#f59e0b", "surface": "#f0fdfa"},
    "hr": {"primary": "#1d4ed8", "accent": "#9333ea", "surface": "#eff6ff"},
    "financial": {"primary": "#166534", "accent": "#dc2626", "surface": "#f0fdf4"},
    "operational": {"primary": "#334155", "accent": "#0ea5e9", "surface": "#f8fafc"},
    "customer": {"primary": "#7c2d12", "accent": "#0ea5e9", "surface": "#fff7ed"},
    "inventory": {"primary": "#4338ca", "accent": "#ea580c", "surface": "#eef2ff"},
    "default": {"primary": "#3b82f6", "accent": "#8b5cf6", "surface": "#f8fafc"},
}


class DashboardGenerator:
    """Automatic dashboard generation engine."""

    def __init__(self, data: list[dict[str, Any]] | pd.DataFrame):
        if isinstance(data, list):
            self.df = pd.DataFrame(data)
        else:
            self.df = data

        self.dashboard_id = str(uuid.uuid4())
        self.numeric_cols = self.df.select_dtypes(include=["number"]).columns.tolist()
        self.categorical_cols = self.df.select_dtypes(
            include=["object", "category"]
        ).columns.tolist()
        self.datetime_cols = [
            c
            for c in self.df.columns
            if pd.api.types.is_datetime64_any_dtype(self.df[c])
        ]

    def _detect_domain(self) -> str:
        """Detect data domain from column names."""
        col_text = " ".join(self.df.columns.str.lower())

        domains = {
            "sales": [
                "sales",
                "revenue",
                "order",
                "customer",
                "product",
                "profit",
                "transaction",
            ],
            "hr": [
                "employee",
                "salary",
                "department",
                "payroll",
                "tenure",
                "attrition",
            ],
            "financial": [
                "income",
                "expense",
                "budget",
                "cash",
                "asset",
                "liability",
                "profit",
                "roi",
            ],
            "customer": ["customer", "churn", "nps", "segment", "retention"],
            "inventory": ["inventory", "stock", "sku", "warehouse", "supplier"],
        }

        for domain, keywords in domains.items():
            if any(kw in col_text for kw in keywords):
                return domain

        return "default"

    def _recommend_charts(self) -> list[dict[str, Any]]:
        """Recommend charts based on data structure."""
        recommendations = []
        domain = self._detect_domain()

        if self.datetime_cols and self.numeric_cols:
            date_col = self.datetime_cols[0]
            for num_col in self.numeric_cols[:3]:
                recommendations.append(
                    {
                        "id": f"trend_{num_col}",
                        "title": f"{num_col.replace('_', ' ').title()} Over Time",
                        "chart_type": "line",
                        "x_field": date_col,
                        "y_field": num_col,
                        "aggregation": "sum",
                        "rationale": f"Trend analysis of {num_col} over time",
                    }
                )

        if self.categorical_cols and self.numeric_cols:
            cat_col = self.categorical_cols[0]
            for num_col in self.numeric_cols[:2]:
                recommendations.append(
                    {
                        "id": f"bar_{num_col}",
                        "title": f"{num_col.replace('_', ' ').title()} by {cat_col.replace('_', ' ').title()}",
                        "chart_type": "bar",
                        "x_field": cat_col,
                        "y_field": num_col,
                        "aggregation": "sum",
                        "rationale": f"Compare {num_col} across {cat_col}",
                    }
                )

        if self.categorical_cols:
            cat_col = self.categorical_cols[0]
            value_counts = self.df[cat_col].value_counts()
            if len(value_counts) <= 10:
                recommendations.append(
                    {
                        "id": f"pie_{cat_col}",
                        "title": f"Distribution of {cat_col.replace('_', ' ').title()}",
                        "chart_type": "donut",
                        "category_field": cat_col,
                        "rationale": "Show distribution of categories",
                    }
                )

        if len(self.numeric_cols) >= 2:
            recommendations.append(
                {
                    "id": "scatter_numeric",
                    "title": f"{self.numeric_cols[0].title()} vs {self.numeric_cols[1].title()}",
                    "chart_type": "scatter",
                    "x_field": self.numeric_cols[0],
                    "y_field": self.numeric_cols[1],
                    "rationale": "Show relationship between two numeric variables",
                }
            )

        for num_col in self.numeric_cols[:2]:
            recommendations.append(
                {
                    "id": f"hist_{num_col}",
                    "title": f"Distribution of {num_col.replace('_', ' ').title()}",
                    "chart_type": "histogram",
                    "x_field": num_col,
                    "rationale": f"Show distribution of {num_col}",
                }
            )

        return recommendations[:6]

    def _generate_kpis(self) -> list[dict[str, Any]]:
        """Generate KPI cards."""
        kpis = []

        kpis.append(
            {
                "id": "total_records",
                "title": "Total Records",
                "value": len(self.df),
                "format": "number",
                "icon": "database",
            }
        )

        if self.numeric_cols:
            for col in self.numeric_cols[:3]:
                series = pd.to_numeric(self.df[col], errors="coerce").dropna()
                if not series.empty:
                    kpis.append(
                        {
                            "id": f"total_{col}",
                            "title": f"Total {col.replace('_', ' ').title()}",
                            "value": round(float(series.sum()), 2),
                            "format": "number",
                            "icon": "calculator",
                        }
                    )

                    kpis.append(
                        {
                            "id": f"avg_{col}",
                            "title": f"Avg {col.replace('_', ' ').title()}",
                            "value": round(float(series.mean()), 2),
                            "format": "decimal",
                            "icon": "trending",
                        }
                    )

        return kpis[:6]

    def _generate_layout(
        self, charts: list[dict], kpis: list[dict]
    ) -> list[dict[str, Any]]:
        """Generate dashboard layout."""
        layout = []
        y_offset = 0

        for i, kpi in enumerate(kpis):
            layout.append(
                {
                    "id": kpi["id"],
                    "kind": "kpi_card",
                    "title": kpi["title"],
                    "size": "small",
                    "layout": {
                        "grid_columns": 12,
                        "x": (i % 4) * 3,
                        "y": y_offset,
                        "w": 3,
                        "h": 2,
                    },
                }
            )
            if (i + 1) % 4 == 0:
                y_offset += 2

        y_offset += 2

        for i, chart in enumerate(charts):
            layout.append(
                {
                    "id": chart["id"],
                    "kind": chart["chart_type"],
                    "title": chart["title"],
                    "size": "medium",
                    "layout": {
                        "grid_columns": 12,
                        "x": 0 if i % 2 == 0 else 6,
                        "y": y_offset + (i // 2) * 4,
                        "w": 6,
                        "h": 4,
                    },
                    "config": chart,
                }
            )

        return layout

    def _generate_filters(self) -> list[dict[str, Any]]:
        """Generate available filters."""
        filters = []

        for col in self.categorical_cols:
            if self.df[col].nunique() <= 20:
                filters.append(
                    {
                        "field": col,
                        "type": "select",
                        "label": col.replace("_", " ").title(),
                        "options": self.df[col].dropna().unique().tolist()[:20],
                    }
                )

        if self.datetime_cols:
            filters.append(
                {
                    "field": self.datetime_cols[0],
                    "type": "date_range",
                    "label": "Date Range",
                }
            )

        return filters[:5]

    def generate(self) -> dict[str, Any]:
        """Generate complete dashboard."""
        try:
            domain = self._detect_domain()
            theme = DOMAIN_THEMES.get(domain, DOMAIN_THEMES["default"])

            charts = self._recommend_charts()
            kpis = self._generate_kpis()
            layout = self._generate_layout(charts, kpis)
            filters = self._generate_filters()

            dashboard = {
                "dashboard_id": self.dashboard_id,
                "created_at": datetime.utcnow().isoformat(),
                "domain": domain,
                "theme": theme,
                "metadata": {
                    "total_rows": len(self.df),
                    "total_columns": len(self.df.columns),
                    "numeric_columns": self.numeric_cols,
                    "categorical_columns": self.categorical_cols,
                    "datetime_columns": self.datetime_cols,
                },
                "kpis": kpis,
                "charts": charts,
                "filters": filters,
                "layout": layout,
                "layout_summary": {
                    "total_components": len(layout),
                    "kpi_count": len(kpis),
                    "chart_count": len(charts),
                    "filter_count": len(filters),
                },
            }

            logger.info(f"Generated dashboard {self.dashboard_id[:8]}")
            return dashboard

        except Exception as e:
            logger.error(f"Dashboard generation error: {e}")
            return {"error": str(e)}


def generate_dashboard(data: list[dict[str, Any]] | pd.DataFrame) -> dict[str, Any]:
    """Convenience function to generate dashboard."""
    generator = DashboardGenerator(data)
    return generator.generate()
