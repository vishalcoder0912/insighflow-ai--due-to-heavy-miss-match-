"""NL-to-SQL Engine with Ollama local LLM + rule-based fallback."""

from __future__ import annotations

import hashlib
import json
import logging
import re
import time
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.services.storage_engine import PostgresStorageEngine, query_cache

logger = logging.getLogger(__name__)

MAX_QUERY_ROWS = 1000
OLLAMA_TIMEOUT = 30

SAFE_FALLBACK_SQL = "SELECT * FROM dataset_{dataset_id} LIMIT 100"


def _get_ollama_config() -> tuple[str, str]:
    """Get Ollama configuration from settings."""
    settings = get_settings()
    return (
        settings.ollama_base_url or "http://localhost:11434",
        settings.ollama_model or "mistral",
    )


class NLToSQLOllama:
    """Natural Language to SQL using Ollama local LLM with rule-based fallback."""

    def __init__(self, session: AsyncSession, dataset_id: int):
        self.session = session
        self.dataset_id = dataset_id
        self.storage = PostgresStorageEngine(session)
        self.columns: list[dict[str, Any]] = []
        self.sample_rows: list[dict[str, Any]] = []
        self._load_schema()

    async def _load_schema(self) -> None:
        """Load column schema from database."""
        try:
            self.columns = await self.storage.get_table_columns(self.dataset_id)
            sample = await self.storage.sample_data(self.dataset_id, 5)
            self.sample_rows = sample.get("rows", [])[:5]
        except Exception as e:
            logger.error(f"Failed to load schema: {e}")
            self.columns = []

    async def generate_sql(
        self,
        message: str,
        use_llm: bool = True,
    ) -> dict[str, Any]:
        """
        Generate SQL from natural language.
        Primary: Ollama LLM
        Fallback: Rule-based SQL generation
        """
        start_time = time.time()

        message_hash = hashlib.sha256(
            f"{self.dataset_id}:{message}".encode()
        ).hexdigest()[:16]

        cached = query_cache.get(message_hash)
        if cached:
            logger.info("Using cached query result")
            cached["is_cached"] = True
            cached["execution_time_ms"] = int((time.time() - start_time) * 1000)
            return cached

        if use_llm:
            try:
                sql_result = await self._generate_with_ollama(message)
                if sql_result.get("sql"):
                    query_cache.set(message_hash, sql_result)
                    sql_result["execution_time_ms"] = int(
                        (time.time() - start_time) * 1000
                    )
                    return sql_result
            except Exception as e:
                logger.warning(f"Ollama failed, using fallback: {e}")

        fallback_result = self._generate_with_rules(message)
        fallback_result["execution_time_ms"] = int((time.time() - start_time) * 1000)
        fallback_result["using_fallback"] = True
        query_cache.set(message_hash, fallback_result)

        return fallback_result

    async def _generate_with_ollama(self, message: str) -> dict[str, Any]:
        """Generate SQL using Ollama local LLM."""
        ollama_url, ollama_model = _get_ollama_config()

        system_prompt = self._build_system_prompt()
        user_prompt = self._build_user_prompt(message)

        try:
            async with httpx.AsyncClient(timeout=OLLAMA_TIMEOUT) as client:
                response = await client.post(
                    f"{ollama_url}/api/generate",
                    json={
                        "model": ollama_model,
                        "prompt": user_prompt,
                        "system": system_prompt,
                        "stream": False,
                        "options": {
                            "temperature": 0.1,
                            "num_predict": 256,
                        },
                    },
                )

                if response.status_code != 200:
                    raise Exception(f"Ollama error: {response.status_code}")

                result = response.json()
                llm_response = result.get("response", "").strip()

                sql = self._extract_sql(llm_response)

                if not sql:
                    raise Exception("No SQL extracted from LLM response")

                validated_sql = self._validate_sql(sql)

                return {
                    "sql": validated_sql,
                    "raw_response": llm_response,
                    "tokens_used": result.get("eval_count", 0),
                    "using_fallback": False,
                    "is_cached": False,
                }
        except httpx.ConnectError:
            logger.warning("Ollama not available, using rule-based fallback")
            raise Exception("Ollama not available")

    def _build_system_prompt(self) -> str:
        """Build system prompt for Ollama."""
        return """You are a PostgreSQL expert AI. Your task is to generate SQL queries from natural language.

RULES:
1. ONLY generate SELECT queries - NEVER generate INSERT, UPDATE, DELETE, DROP, ALTER
2. Use exact column names from the provided schema
3. Always include LIMIT clause (default 10)
4. Use proper SQL syntax
5. When aggregation is mentioned (sum, avg, count, etc.), use GROUP BY
6. When sorting is mentioned (top, highest, lowest), use ORDER BY
7. Wrap column names with special characters in double quotes

OUTPUT FORMAT:
Return ONLY the SQL query, nothing else. No markdown, no explanations."""

    def _build_user_prompt(self, message: str) -> str:
        """Build user prompt with schema context."""
        table_name = f"dataset_{self.dataset_id}"

        columns_info = []
        for col in self.columns:
            col_name = col.get("name", "unknown")
            col_type = col.get("dtype", "text")
            columns_info.append(f"  - {col_name} ({col_type})")

        schema_str = (
            "\n".join(columns_info) if columns_info else "  (no schema available)"
        )

        sample_str = ""
        if self.sample_rows:
            sample_str = f"\nSample data (first 3 rows):\n{json.dumps(self.sample_rows[:3], indent=2)}"

        return f"""Generate a SQL query for this request: "{message}"

Table: {table_name}
Columns:
{schema_str}
{sample_str}

SQL Query:"""

    def _extract_sql(self, llm_response: str) -> str | None:
        """Extract SQL query from LLM response."""
        sql = llm_response.strip()

        sql = re.sub(r"^```sql\n", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"^```\n", "", sql)
        sql = re.sub(r"\n```$", "", sql)

        sql = sql.strip()

        if sql.upper().startswith("SELECT"):
            return sql

        select_match = re.search(r"(SELECT\s+.+)", sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            return select_match.group(1).strip()

        return None

    def _generate_with_rules(self, message: str) -> dict[str, Any]:
        """Rule-based SQL generation as fallback."""
        msg_lower = message.lower().strip()

        result = {
            "select_cols": [],
            "group_by": None,
            "order_by": None,
            "order_dir": "DESC",
            "limit": 10,
            "filters": [],
            "aggregations": [],
            "where": None,
        }

        numeric_cols = [
            c["name"]
            for c in self.columns
            if c.get("dtype")
            in ("integer", "bigint", "double precision", "numeric", "float", "decimal")
        ]
        category_cols = [
            c["name"]
            for c in self.columns
            if c.get("dtype") in ("text", "varchar", "character varying", "string")
        ]

        result["select_cols"] = numeric_cols[:1] if numeric_cols else ["*"]

        agg_keywords = {
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
        for kw, func in agg_keywords.items():
            if kw in msg_lower:
                result["aggregations"].append(
                    {"func": func, "column": numeric_cols[0] if numeric_cols else None}
                )

        order_keywords = {
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
        for kw, (direction, default_limit) in order_keywords.items():
            if kw in msg_lower:
                result["order_dir"] = direction
                result["order_by"] = "aggregate"
                limit_match = re.search(r"top\s+(\d+)", msg_lower)
                if limit_match:
                    result["limit"] = int(limit_match.group(1))
                else:
                    result["limit"] = default_limit
                break

        if " by " in msg_lower:
            for cat_col in category_cols:
                if cat_col.lower() in msg_lower:
                    result["group_by"] = cat_col
                    if cat_col not in result["select_cols"]:
                        result["select_cols"].insert(0, cat_col)
                    break

        limit_match = re.search(r"(?:show|list|get|top|first)\s+(\d+)", msg_lower)
        if limit_match:
            result["limit"] = int(limit_match.group(1))

        if "all" in msg_lower or "everything" in msg_lower:
            result["limit"] = 100

        action = "select"
        anomaly_kw = ["anomaly", "outlier", "unusual", "strange"]
        if any(kw in msg_lower for kw in anomaly_kw):
            action = "anomaly"
        corr_kw = ["correlation", "correlate", "related"]
        if any(kw in msg_lower for kw in corr_kw):
            action = "correlation"

        sql = self._build_sql_from_parsed(result, action)

        return {
            "sql": sql,
            "using_fallback": True,
            "is_cached": False,
            "rule_based": True,
        }

    def _build_sql_from_parsed(self, parsed: dict, action: str = "select") -> str:
        """Build SQL query from parsed components."""
        table_name = f"dataset_{self.dataset_id}"

        if action == "anomaly" and parsed["select_cols"]:
            col = parsed["select_cols"][0]
            return f"""
                SELECT *, 
                    ({col} - AVG({col}) OVER()) / NULLIF(STDDEV({col}) OVER(), 0) as z_score
                FROM {table_name}
                WHERE {col} IS NOT NULL
                ORDER BY ABS({col} - AVG({col}) OVER()) DESC
                LIMIT {parsed["limit"]}
            """.strip()

        select_parts = []

        if parsed["aggregations"]:
            for agg in parsed["aggregations"]:
                func = agg["func"]
                col = (
                    agg["column"] or parsed["select_cols"][0]
                    if parsed["select_cols"]
                    else "*"
                )
                if func == "COUNT":
                    select_parts.append("COUNT(*) as count")
                else:
                    select_parts.append(f"{func}({col}) as {func.lower()}_{col}")
        else:
            select_parts = [f'"{c}"' for c in parsed["select_cols"][:10]] or ["*"]

        if parsed["group_by"] and parsed["group_by"] not in [
            s.split()[0].strip('"') for s in select_parts
        ]:
            select_parts.insert(0, f'"{parsed["group_by"]}"')

        sql_parts = [f"SELECT {', '.join(select_parts)}", f"FROM {table_name}"]

        if parsed["group_by"]:
            sql_parts.append(f'GROUP BY "{parsed["group_by"]}"')

        if parsed["order_by"] == "aggregate" and parsed["aggregations"]:
            agg_alias = select_parts[-1].split(" as ")[-1].strip()
            sql_parts.append(f"ORDER BY {agg_alias} {parsed['order_dir']}")
        elif parsed["group_by"]:
            sql_parts.append(f'ORDER BY "{parsed["group_by"]}" {parsed["order_dir"]}')

        sql_parts.append(f"LIMIT {min(parsed['limit'], MAX_QUERY_ROWS)}")

        return "\n".join(sql_parts)

    def _validate_sql(self, sql: str) -> str:
        """Validate and sanitize SQL query."""
        sql_clean = sql.strip()
        sql_lower = sql_clean.lower()

        dangerous_keywords = [
            "drop",
            "delete",
            "update",
            "insert",
            "alter",
            "truncate",
            "create",
            "grant",
            "revoke",
            "execute",
            "call",
        ]
        for keyword in dangerous_keywords:
            if sql_lower.startswith(keyword):
                logger.warning(f"Blocked dangerous SQL: {sql_clean[:50]}")
                return SAFE_FALLBACK_SQL.format(dataset_id=self.dataset_id)

        if not sql_lower.startswith("select"):
            logger.warning(f"Non-SELECT SQL detected: {sql_clean[:50]}")
            return SAFE_FALLBACK_SQL.format(dataset_id=self.dataset_id)

        if "limit" not in sql_lower:
            sql_clean = sql_clean.rstrip().rstrip(";") + f" LIMIT {MAX_QUERY_ROWS}"

        if f"dataset_{self.dataset_id}" not in sql_clean:
            sql_clean = sql_clean.replace("dataset_", f"dataset_{self.dataset_id}", 1)

        return sql_clean

    async def execute_query(self, sql: str) -> dict[str, Any]:
        """Execute validated SQL query."""
        validated_sql = self._validate_sql(sql)

        try:
            result = await self.storage.execute_query(self.dataset_id, validated_sql)

            if result.get("status") == "error":
                logger.error(f"Query execution error: {result.get('message')}")
                return {
                    "status": "error",
                    "message": result.get("message", "Query execution failed"),
                    "sql": validated_sql,
                    "rows": [],
                    "columns": [],
                    "row_count": 0,
                }

            return {
                "status": "success",
                "sql": validated_sql,
                "rows": result.get("rows", []),
                "columns": result.get("columns", []),
                "row_count": result.get("row_count", 0),
            }

        except Exception as e:
            logger.error(f"Query execution exception: {e}")
            return {
                "status": "error",
                "message": str(e),
                "sql": validated_sql,
                "rows": [],
                "columns": [],
                "row_count": 0,
                "fallback": True,
            }

    def generate_explanation(self, sql: str, message: str) -> str:
        """Generate human-readable explanation of the query."""
        msg_lower = message.lower()

        explanations = []

        if "top" in msg_lower or "highest" in msg_lower or "best" in msg_lower:
            explanations.append("This query retrieves the highest values")
        elif "bottom" in msg_lower or "lowest" in msg_lower or "worst" in msg_lower:
            explanations.append("This query retrieves the lowest values")

        if "sum" in msg_lower or "total" in msg_lower:
            explanations.append("calculating the total sum")
        elif "average" in msg_lower or "avg" in msg_lower:
            explanations.append("calculating the average")
        elif "count" in msg_lower:
            explanations.append("counting the records")

        if " by " in msg_lower:
            group_match = re.search(r"by (\w+)", msg_lower)
            if group_match:
                explanations.append(f"grouped by {group_match.group(1)}")

        if not explanations:
            explanations.append("This query retrieves the requested data")

        return ". ".join(explanations) + "."


class QueryCache:
    """Enhanced query cache with TTL and size limits."""

    def __init__(self, max_size: int = 200, ttl_seconds: int = 3600):
        self._cache: dict[str, dict[str, Any]] = {}
        self._timestamps: dict[str, float] = {}
        self._max_size = max_size
        self._ttl = ttl_seconds

    def get(self, query_hash: str) -> dict[str, Any] | None:
        """Get cached result if not expired."""
        if query_hash not in self._cache:
            return None

        timestamp = self._timestamps.get(query_hash, 0)
        if time.time() - timestamp > self._ttl:
            del self._cache[query_hash]
            del self._timestamps[query_hash]
            return None

        return self._cache[query_hash]

    def set(self, query_hash: str, result: dict[str, Any]) -> None:
        """Set cached result with timestamp."""
        if len(self._cache) >= self._max_size:
            oldest_hash = min(self._timestamps, key=self._timestamps.get)
            del self._cache[oldest_hash]
            del self._timestamps[oldest_hash]

        self._cache[query_hash] = result
        self._timestamps[query_hash] = time.time()

    def clear(self) -> None:
        """Clear all cached results."""
        self._cache.clear()
        self._timestamps.clear()

    def invalidate(self, dataset_id: int) -> None:
        """Invalidate cache for specific dataset."""
        prefix = f"{dataset_id}:"
        to_remove = [k for k in self._cache if k.startswith(prefix)]
        for k in to_remove:
            del self._cache[k]
            if k in self._timestamps:
                del self._timestamps[k]


ollama_query_cache = QueryCache()
