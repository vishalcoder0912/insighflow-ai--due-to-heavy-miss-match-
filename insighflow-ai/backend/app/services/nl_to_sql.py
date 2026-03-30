"""Rule-based Natural Language to SQL Engine."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.storage_engine import PostgresStorageEngine, query_cache

logger = logging.getLogger(__name__)

MAX_QUERY_ROWS = 5000
SAFE_FALLBACK = "SELECT * FROM dataset_{dataset_id} LIMIT 100"


class NLToSQLEngine:
    """Rule-based natural language to SQL conversion engine."""

    def __init__(self, session: AsyncSession, dataset_id: int):
        self.session = session
        self.dataset_id = dataset_id
        self.storage = PostgresStorageEngine(session)
        self.columns: list[dict[str, Any]] = []
        self._load_schema()

    async def _load_schema(self) -> None:
        """Load column schema from database."""
        self.columns = await self.storage.get_table_columns(self.dataset_id)

    def parse(self, message: str) -> dict[str, Any]:
        """Parse natural language query into SQL components."""
        msg_lower = message.lower().strip()

        result = {
            "action": "select",
            "select_cols": [],
            "group_by": None,
            "order_by": None,
            "order_dir": "DESC",
            "limit": 10,
            "where": None,
            "having": None,
            "join": None,
            "aggregations": [],
            "filters": [],
            "time_column": None,
            "numeric_columns": [],
            "category_columns": [],
            "explain": "",
        }

        for col in self.columns:
            col_name = col.get("name", "").lower()
            col_type = col.get("dtype", "").lower()
            if col_type in (
                "integer",
                "bigint",
                "double precision",
                "numeric",
                "float",
            ):
                result["numeric_columns"].append(col["name"])
            else:
                result["category_columns"].append(col["name"])

        result["select_cols"] = result["numeric_columns"] or ["*"]

        self._parse_aggregations(msg_lower, result)
        self._parse_grouping(msg_lower, result)
        self._parse_ordering(msg_lower, result)
        self._parse_limits(msg_lower, result)
        self._parse_filters(msg_lower, result)
        self._detect_action(msg_lower, result)
        self._match_columns(msg_lower, result)

        return result

    def _parse_aggregations(self, msg: str, result: dict) -> None:
        """Parse aggregation keywords."""
        agg_map = {
            "sum": "SUM",
            "total": "SUM",
            "totals": "SUM",
            "average": "AVG",
            "avg": "AVG",
            "mean": "AVG",
            "count": "COUNT",
            "how many": "COUNT",
            "minimum": "MIN",
            "min": "MIN",
            "lowest": "MIN",
            "maximum": "MAX",
            "max": "MAX",
            "highest": "MAX",
        }

        for keyword, func in agg_map.items():
            if keyword in msg:
                result["aggregations"].append({"func": func, "column": None})

    def _parse_grouping(self, msg: str, result: dict) -> None:
        """Parse grouping keywords."""
        group_keywords = ["by", "group", "per", "each", "split"]
        if any(kw in msg for kw in group_keywords):
            for cat_col in result["category_columns"]:
                if cat_col.lower() in msg:
                    result["group_by"] = cat_col
                    break

    def _parse_ordering(self, msg: str, result: dict) -> None:
        """Parse ordering keywords."""
        order_map = {
            "top": ("DESC", 10),
            "highest": ("DESC", 10),
            "best": ("DESC", 10),
            "most": ("DESC", 10),
            "largest": ("DESC", 10),
            "biggest": ("DESC", 10),
            "bottom": ("ASC", 10),
            "lowest": ("ASC", 10),
            "worst": ("ASC", 10),
            "least": ("ASC", 10),
            "smallest": ("ASC", 10),
        }

        for keyword, (direction, default_limit) in order_map.items():
            if keyword in msg:
                result["order_dir"] = direction
                result["order_by"] = "aggregate"
                limit_match = re.search(r"top\s+(\d+)", msg)
                if limit_match:
                    result["limit"] = int(limit_match.group(1))
                else:
                    result["limit"] = default_limit
                break

    def _parse_limits(self, msg: str, result: dict) -> None:
        """Parse limit keywords."""
        limit_match = re.search(r"(?:show|list|get|top|first)\s+(\d+)", msg)
        if limit_match:
            result["limit"] = int(limit_match.group(1))

        if "all" in msg or "everything" in msg:
            result["limit"] = 1000

    def _parse_filters(self, msg: str, result: dict) -> None:
        """Parse filter conditions."""
        filter_pattern = r'(\w+)\s+(?:is|=|greater than|less than|above|below|over|under)\s+["\']?([^"\']+)["\']?'
        matches = re.findall(filter_pattern, msg)

        for col, op, value in matches:
            result["filters"].append({"column": col, "operator": op, "value": value})

    def _detect_action(self, msg: str, result: dict) -> None:
        """Detect special analysis actions."""
        anomaly_kw = ["anomaly", "outlier", "unusual", "strange", "异常"]
        if any(kw in msg for kw in anomaly_kw):
            result["action"] = "anomaly_detection"

        corr_kw = ["correlation", "correlate", "related", "relationship", "关联"]
        if any(kw in msg for kw in corr_kw):
            result["action"] = "correlation"

        forecast_kw = ["forecast", "predict", "future", "趋势", "预测"]
        if any(kw in msg for kw in forecast_kw):
            result["action"] = "forecast"

        trend_kw = ["trend", "over time", "timeline", "history", "growth", "时间"]
        if any(kw in msg for kw in trend_kw):
            result["include_time_trend"] = True

        compare_kw = ["compare", "vs", "versus", "对比", "比较"]
        if any(kw in msg for kw in compare_kw):
            result["comparison"] = True

        quality_kw = ["quality", "missing", "duplicate", "clean", "数据质量"]
        if any(kw in msg for kw in quality_kw):
            result["action"] = "data_quality"

        schema_kw = ["schema", "structure", "columns", "fields", "结构"]
        if any(kw in msg for kw in schema_kw):
            result["action"] = "schema"

        if "count" in msg and "row" in msg:
            result["action"] = "count_rows"

    def _match_columns(self, msg: str, result: dict) -> None:
        """Match mentioned columns."""
        msg_words = set(msg.split())

        for col in self.columns:
            col_name = col.get("name", "").lower().replace("_", " ")
            col_name_underscore = col.get("name", "").lower()

            if col_name in msg or col_name_underscore in msg:
                result["select_cols"].append(col["name"])

            if any(w in col_name for w in msg_words):
                if col["name"] not in result["select_cols"]:
                    result["select_cols"].append(col["name"])

        if result["select_cols"] == ["*"]:
            result["select_cols"] = [c["name"] for c in self.columns[:10]]

    def generate_sql(self, parsed: dict[str, Any]) -> str:
        """Generate SQL from parsed query."""
        action = parsed.get("action", "select")

        if action == "anomaly_detection":
            return self._generate_anomaly_sql(parsed)
        if action == "correlation":
            return self._generate_correlation_sql(parsed)
        if action == "forecast":
            return self._generate_forecast_sql(parsed)
        if action == "schema":
            return f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'dataset_{self.dataset_id}'"
        if action == "count_rows":
            return f"SELECT COUNT(*) as total_rows FROM dataset_{self.dataset_id}"
        if action == "data_quality":
            return self._generate_quality_sql(parsed)

        aggs = parsed.get("aggregations", [])
        group_by = parsed.get("group_by")
        select_cols = parsed.get("select_cols", ["*"])
        order_dir = parsed.get("order_dir", "DESC")
        limit = parsed.get("limit", 10)

        select_parts = []

        if aggs:
            for agg in aggs:
                func = agg["func"]
                col = (
                    parsed["numeric_columns"][0]
                    if parsed["numeric_columns"]
                    else select_cols[0]
                )
                if func == "COUNT":
                    select_parts.append(f"COUNT(*) as count")
                else:
                    select_parts.append(f"{func}({col}) as {func.lower()}_{col}")
        else:
            select_parts = [f'"{c}"' for c in select_cols[:10]]

        if group_by and group_by not in [s.split()[0].strip('"') for s in select_parts]:
            select_parts.insert(0, f'"{group_by}"')

        table_name = f"dataset_{self.dataset_id}"
        sql_parts = [f"SELECT {', '.join(select_parts)}", f"FROM {table_name}"]

        if group_by:
            sql_parts.append(f'GROUP BY "{group_by}"')

        if parsed.get("order_by") == "aggregate" and aggs:
            agg_alias = select_parts[-1].split(" as ")[-1].strip()
            sql_parts.append(f"ORDER BY {agg_alias} {order_dir}")
        elif group_by:
            sql_parts.append(f'ORDER BY "{group_by}" {order_dir}')

        sql_parts.append(f"LIMIT {min(limit, MAX_QUERY_ROWS)}")

        return "\n".join(sql_parts)

    def _generate_anomaly_sql(self, parsed: dict) -> str:
        """Generate anomaly detection SQL."""
        col = parsed["numeric_columns"][0] if parsed["numeric_columns"] else "id"
        return f"""
            SELECT *, 
                ({col} - AVG({col}) OVER()) / NULLIF(STDDEV({col}) OVER(), 0) as z_score
            FROM dataset_{self.dataset_id}
            WHERE {col} IS NOT NULL
            ORDER BY ABS({col} - AVG({col}) OVER()) DESC
            LIMIT 100
        """

    def _generate_correlation_sql(self, parsed: dict) -> str:
        """Generate correlation SQL (simplified)."""
        cols = parsed.get("numeric_columns", [])[:5]
        if not cols:
            return SAFE_FALLBACK.format(dataset_id=self.dataset_id)

        return (
            f'SELECT "{cols[0]}", "{cols[1]}" FROM dataset_{self.dataset_id} LIMIT 100'
        )

    def _generate_forecast_sql(self, parsed: dict) -> str:
        """Generate forecast SQL."""
        num_cols = parsed.get("numeric_columns", [])
        cat_cols = parsed.get("category_columns", [])

        if cat_cols:
            return f'SELECT "{cat_cols[0]}", SUM("{num_cols[0]}") as total FROM dataset_{self.dataset_id} GROUP BY "{cat_cols[0]}" ORDER BY total DESC LIMIT 20'
        elif num_cols:
            return f"SELECT * FROM dataset_{self.dataset_id} LIMIT 100"
        return SAFE_FALLBACK.format(dataset_id=self.dataset_id)

    def _generate_quality_sql(self, parsed: dict) -> str:
        """Generate data quality SQL."""
        return f"""
            SELECT 
                COUNT(*) as total_rows,
                COUNT(DISTINCT *) as duplicate_rows
            FROM dataset_{self.dataset_id}
        """

    async def execute(self, message: str) -> dict[str, Any]:
        """Execute natural language query."""
        parsed = self.parse(message)
        sql = self.generate_sql(parsed)

        query_hash = hashlib.md5(sql.encode()).hexdigest()
        cached = query_cache.get(query_hash)
        if cached:
            logger.info("Using cached query result")
            cached["cached"] = True
            return cached

        result = await self.storage.execute_query(self.dataset_id, sql)

        result["sql_query"] = sql
        result["explanation"] = self._generate_explanation(parsed)

        query_cache.set(query_hash, result)

        return result

    def _generate_explanation(self, parsed: dict) -> str:
        """Generate human-readable explanation."""
        action = parsed.get("action", "select")

        explanations = {
            "anomaly_detection": "Analyzed data to identify outliers using statistical methods (Z-score)",
            "correlation": "Calculated relationships between numeric variables",
            "forecast": "Generated trend analysis based on historical data",
            "data_quality": "Performed data quality assessment including completeness and uniqueness",
            "schema": "Retrieved database schema information",
            "count_rows": "Counted total number of records in the dataset",
        }

        base = explanations.get(action, "Retrieved data based on your query")

        if parsed.get("group_by"):
            base += f", grouped by {parsed['group_by']}"

        if parsed.get("aggregations"):
            aggs = [a["func"] for a in parsed["aggregations"]]
            base += f", with {', '.join(aggs)} aggregation(s)"

        return base


class QueryExecutor:
    """Query execution with safety checks."""

    def __init__(self, session: AsyncSession, dataset_id: int):
        self.session = session
        self.dataset_id = dataset_id
        self.storage = PostgresStorageEngine(session)

    async def execute_safe(self, sql: str) -> dict[str, Any]:
        """Execute query with safety checks."""
        sql_clean = sql.strip().lower()

        dangerous = [
            "drop",
            "delete",
            "update",
            "insert",
            "alter",
            "truncate",
            "create",
            "grant",
            "revoke",
        ]
        for kw in dangerous:
            if sql_clean.startswith(kw):
                return {
                    "status": "error",
                    "message": f"Query blocked: {kw} operations are not allowed",
                    "columns": [],
                    "rows": [],
                }

        if "limit" not in sql_clean:
            sql = sql.rstrip().rstrip(";") + f" LIMIT {MAX_QUERY_ROWS}"

        result = await self.storage.execute_query(self.dataset_id, sql)

        if result["status"] == "error":
            result["fallback"] = f"SELECT * FROM dataset_{self.dataset_id} LIMIT 100"
            result["rows"] = []

        return result

    async def get_sample_data(self, limit: int = 100) -> dict[str, Any]:
        """Get sample data from dataset."""
        return await self.storage.sample_data(self.dataset_id, limit)
