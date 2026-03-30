"""Auto Chart Generation Service."""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


class ChartGenerator:
    """Automatic chart configuration generator for frontend visualization."""

    @staticmethod
    def generate_chart_config(
        data: list[dict[str, Any]],
        columns: list[str] | None = None,
        chart_type: str | None = None,
    ) -> dict[str, Any]:
        """Generate chart configuration based on data structure."""
        if not data:
            return ChartGenerator._empty_config()

        df = pd.DataFrame(data)

        if columns:
            df = df[[c for c in columns if c in df.columns]]
            if df.empty:
                return ChartGenerator._empty_config()

        if chart_type:
            return ChartGenerator._generate_typed_chart(df, chart_type)

        suggested_type = ChartGenerator._suggest_chart_type(df)
        return ChartGenerator._generate_typed_chart(df, suggested_type)

    @staticmethod
    def _suggest_chart_type(df: pd.DataFrame) -> str:
        """Suggest best chart type based on data structure."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        all_cols = df.columns.tolist()

        if len(all_cols) == 1:
            return "kpi"

        if len(numeric_cols) == 0:
            return "table"

        if len(all_cols) == 2:
            if len(df) <= 10:
                return "bar"
            return "line"

        if len(numeric_cols) >= 2 and len(all_cols) >= 3:
            return "scatter"

        if len(numeric_cols) == 1:
            date_cols = [
                c for c in all_cols if "date" in c.lower() or "time" in c.lower()
            ]
            if date_cols:
                return "line"
            if len(df) <= 10:
                return "bar"
            return "line"

        return "table"

    @staticmethod
    def _generate_typed_chart(df: pd.DataFrame, chart_type: str) -> dict[str, Any]:
        """Generate chart configuration for specific type."""
        generators = {
            "bar": ChartGenerator._generate_bar_config,
            "line": ChartGenerator._generate_line_config,
            "pie": ChartGenerator._generate_pie_config,
            "scatter": ChartGenerator._generate_scatter_config,
            "kpi": ChartGenerator._generate_kpi_config,
            "table": ChartGenerator._generate_table_config,
            "area": ChartGenerator._generate_area_config,
            "heatmap": ChartGenerator._generate_heatmap_config,
        }

        generator = generators.get(chart_type, ChartGenerator._generate_table_config)
        return generator(df)

    @staticmethod
    def _generate_bar_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate bar chart configuration."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        all_cols = df.columns.tolist()

        if numeric_cols:
            value_col = numeric_cols[0]
        else:
            return ChartGenerator._generate_table_config(df)

        category_col = None
        for col in all_cols:
            if col != value_col:
                category_col = col
                break

        if not category_col:
            return ChartGenerator._generate_kpi_config(df)

        chart_data = []
        for _, row in df.head(20).iterrows():
            chart_data.append(
                {
                    "name": str(row[category_col])[:30],
                    "value": float(row[value_col]) if pd.notna(row[value_col]) else 0,
                }
            )

        colors = [
            "#3b82f6",
            "#10b981",
            "#f59e0b",
            "#ef4444",
            "#8b5cf6",
            "#ec4899",
            "#06b6d4",
            "#84cc16",
            "#f97316",
            "#6366f1",
        ]

        return {
            "type": "bar",
            "data": chart_data,
            "options": {
                "xAxisKey": "name",
                "yAxisKey": "value",
                "colors": colors[: len(chart_data)],
                "xAxisLabel": category_col,
                "yAxisLabel": value_col,
            },
            "title": f"{value_col} by {category_col}",
        }

    @staticmethod
    def _generate_line_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate line chart configuration."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        all_cols = df.columns.tolist()

        chart_data = []
        for i, row in df.head(50).iterrows():
            point = {"index": i}
            for col in numeric_cols[:5]:
                if pd.notna(row[col]):
                    point[col] = float(row[col])
            chart_data.append(point)

        series = []
        colors = ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6"]
        for i, col in enumerate(numeric_cols[:5]):
            series.append(
                {
                    "dataKey": col,
                    "name": col,
                    "color": colors[i % len(colors)],
                }
            )

        return {
            "type": "line",
            "data": chart_data,
            "options": {
                "xAxisKey": "index",
                "series": series,
            },
            "title": f"Trend Analysis"
            if len(numeric_cols) > 1
            else f"{numeric_cols[0]} Trend",
        }

    @staticmethod
    def _generate_pie_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate pie chart configuration."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        all_cols = df.columns.tolist()

        if not numeric_cols:
            return ChartGenerator._generate_table_config(df)

        value_col = numeric_cols[0]
        category_col = None
        for col in all_cols:
            if col != value_col:
                category_col = col
                break

        if not category_col:
            grouped = df[value_col].sum()
            return {
                "type": "pie",
                "data": [{"name": "Total", "value": float(grouped)}],
                "options": {},
                "title": f"Total {value_col}",
            }

        aggregated = df.groupby(category_col)[value_col].sum().reset_index()

        chart_data = []
        colors = [
            "#3b82f6",
            "#10b981",
            "#f59e0b",
            "#ef4444",
            "#8b5cf6",
            "#ec4899",
            "#06b6d4",
            "#84cc16",
            "#f97316",
            "#6366f1",
        ]
        for i, row in aggregated.head(10).iterrows():
            chart_data.append(
                {
                    "name": str(row[category_col])[:20],
                    "value": float(row[value_col]) if pd.notna(row[value_col]) else 0,
                    "color": colors[i % len(colors)],
                }
            )

        return {
            "type": "pie",
            "data": chart_data,
            "options": {
                "colors": colors[: len(chart_data)],
            },
            "title": f"{value_col} Distribution",
        }

    @staticmethod
    def _generate_scatter_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate scatter chart configuration."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if len(numeric_cols) < 2:
            return ChartGenerator._generate_bar_config(df)

        x_col = numeric_cols[0]
        y_col = numeric_cols[1]

        chart_data = []
        for _, row in df.head(200).iterrows():
            if pd.notna(row[x_col]) and pd.notna(row[y_col]):
                chart_data.append(
                    {
                        "x": float(row[x_col]),
                        "y": float(row[y_col]),
                    }
                )

        return {
            "type": "scatter",
            "data": chart_data,
            "options": {
                "xAxisKey": "x",
                "yAxisKey": "y",
                "xAxisLabel": x_col,
                "yAxisLabel": y_col,
            },
            "title": f"{x_col} vs {y_col}",
        }

    @staticmethod
    def _generate_kpi_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate KPI card configuration."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        kpis = []

        if numeric_cols:
            col = numeric_cols[0]
            values = pd.to_numeric(df[col], errors="coerce").dropna()

            if len(values) > 0:
                kpis.append(
                    {
                        "id": col,
                        "label": col,
                        "value": float(values.sum()),
                        "format": "number",
                    }
                )

                if len(values) > 1:
                    kpis.append(
                        {
                            "id": f"{col}_avg",
                            "label": f"Avg {col}",
                            "value": float(values.mean()),
                            "format": "number",
                        }
                    )

                    kpis.append(
                        {
                            "id": f"{col}_max",
                            "label": f"Max {col}",
                            "value": float(values.max()),
                            "format": "number",
                        }
                    )

        kpis.append(
            {
                "id": "row_count",
                "label": "Total Rows",
                "value": len(df),
                "format": "number",
            }
        )

        return {
            "type": "kpi",
            "data": kpis[:6],
            "options": {},
            "title": "Key Metrics",
        }

    @staticmethod
    def _generate_table_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate table configuration."""
        table_data = df.head(50).to_dict("records")

        headers = [{"key": col, "label": col} for col in df.columns]

        return {
            "type": "table",
            "data": table_data,
            "options": {
                "headers": headers,
            },
            "title": "Data Table",
        }

    @staticmethod
    def _generate_area_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate area chart configuration."""
        return ChartGenerator._generate_line_config(df)

    @staticmethod
    def _generate_heatmap_config(df: pd.DataFrame) -> dict[str, Any]:
        """Generate heatmap configuration (simplified)."""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()

        if len(numeric_cols) < 2:
            return ChartGenerator._generate_bar_config(df)

        corr_matrix = df[numeric_cols].corr()

        chart_data = []
        for i, col1 in enumerate(numeric_cols):
            for j, col2 in enumerate(numeric_cols):
                val = corr_matrix.loc[col1, col2]
                if not np.isnan(val):
                    chart_data.append(
                        {
                            "x": col1,
                            "y": col2,
                            "value": float(val),
                        }
                    )

        return {
            "type": "heatmap",
            "data": chart_data,
            "options": {
                "xAxisKey": "x",
                "yAxisKey": "y",
                "valueKey": "value",
            },
            "title": "Correlation Heatmap",
        }

    @staticmethod
    def _empty_config() -> dict[str, Any]:
        """Return empty chart configuration."""
        return {
            "type": "table",
            "data": [],
            "options": {},
            "title": "No Data",
        }

    @staticmethod
    def get_available_chart_types() -> list[dict[str, str]]:
        """Get list of available chart types."""
        return [
            {
                "id": "bar",
                "name": "Bar Chart",
                "description": "Compare values across categories",
            },
            {
                "id": "line",
                "name": "Line Chart",
                "description": "Show trends over time or sequence",
            },
            {
                "id": "area",
                "name": "Area Chart",
                "description": "Show cumulative trends",
            },
            {
                "id": "pie",
                "name": "Pie Chart",
                "description": "Show proportional distribution",
            },
            {
                "id": "scatter",
                "name": "Scatter Plot",
                "description": "Show relationship between two variables",
            },
            {"id": "kpi", "name": "KPI Cards", "description": "Display key metrics"},
            {"id": "table", "name": "Data Table", "description": "Raw data display"},
        ]
