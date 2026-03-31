"""Natural language to SQL conversion service."""

from __future__ import annotations

import logging
import re
from typing import Optional

import requests

logger = logging.getLogger(__name__)


class OllamaNotAvailableError(Exception):
    """Raised when Ollama service is not available."""

    pass


class NLToSQLService:
    """Convert natural language queries to SQL using Ollama Mistral model."""

    def __init__(
        self, ollama_url: str = "http://localhost:11434", model: str = "mistral"
    ):
        """
        Initialize the NL-to-SQL service.

        Args:
            ollama_url: URL where Ollama is running
            model: Model name to use (must be pulled with ollama pull {model})
        """
        self.ollama_url = ollama_url
        self.model = model
        self.timeout = 60
        self._check_ollama_available()

    def _check_ollama_available(self) -> bool:
        """Check if Ollama service is available."""
        try:
            response = requests.get(f"{self.ollama_url}/api/tags", timeout=5)
            if response.status_code == 200:
                logger.info(f"✅ Ollama service available at {self.ollama_url}")
                return True
        except Exception as e:
            logger.warning(f"⚠️ Ollama not available: {e}")
            raise OllamaNotAvailableError(
                f"Ollama service not available at {self.ollama_url}. "
                f"Please start Ollama with: ollama serve"
            )

    async def convert_nl_to_sql(
        self,
        user_message: str,
        schema: dict[str, str],
        table_name: str,
        chat_history: list[dict] | None = None,
    ) -> str:
        """
        Convert natural language question to SQL query.

        Args:
            user_message: User's question in natural language
            schema: Dictionary mapping column names to types
            table_name: Name of the table in PostgreSQL
            chat_history: Previous messages for context (optional)

        Returns:
            SQL query string

        Example:
            >>> schema = {"date": "datetime", "revenue": "float", "product": "string"}
            >>> user_message = "Show top 5 products by revenue"
            >>> sql = await service.convert_nl_to_sql(user_message, schema, "sales")
            >>> # Returns: "SELECT product, SUM(revenue) as total FROM sales GROUP BY product..."
        """

        # Build schema description
        schema_desc = self._format_schema(schema)

        # Build context from history
        context = self._build_context_from_history(chat_history)

        # Create optimized prompt for Mistral
        system_prompt = self._build_system_prompt(table_name, schema_desc, context)

        try:
            # Call Ollama API
            logger.info(f"🔄 Converting to SQL: {user_message[:50]}...")
            sql_query = await self._query_ollama(system_prompt)

            # Validate and clean SQL
            sql_query = self._clean_sql(sql_query)

            if self._is_valid_sql(sql_query):
                logger.info(f"✅ Generated SQL: {sql_query[:100]}...")
                return sql_query
            else:
                logger.warning(f"⚠️ Invalid SQL generated, using fallback")
                return self._generate_fallback_sql(user_message, table_name, schema)

        except Exception as e:
            logger.error(f"❌ NL-to-SQL error: {e}")
            return self._generate_fallback_sql(user_message, table_name, schema)

    def _format_schema(self, schema: dict[str, str]) -> str:
        """Format schema as readable description."""
        lines = []
        for col_name, col_type in schema.items():
            lines.append(f"  - {col_name}: {col_type}")
        return "\n".join(lines)

    def _build_context_from_history(self, chat_history: list[dict] | None) -> str:
        """Build context string from chat history."""
        if not chat_history or len(chat_history) == 0:
            return ""

        context_lines = ["Previous questions (for context):"]
        for msg in chat_history[-3:]:  # Last 3 messages
            if msg.get("role") == "user":
                context_lines.append(f"  Q: {msg['content']}")
            elif msg.get("role") == "assistant" and msg.get("sql_query"):
                context_lines.append(f"  SQL: {msg['sql_query'][:80]}...")

        return "\n".join(context_lines)

    def _build_system_prompt(
        self, table_name: str, schema_desc: str, context: str
    ) -> str:
        """Build optimized system prompt for Mistral."""

        prompt = f"""You are an expert PostgreSQL database analyst.

Your task: Convert natural language to accurate PostgreSQL SQL queries.

TABLE: {table_name}
COLUMNS:
{schema_desc}

RULES (CRITICAL):
1. Return ONLY valid SQL query - NO explanations
2. Use exact column names from the schema
3. Use double quotes for identifiers: "column_name"
4. For TEXT columns, use single quotes for values: 'value'
5. For TOP N results: ORDER BY column DESC LIMIT N
6. For aggregations: Use SUM(), COUNT(), AVG(), MAX(), MIN()
7. For grouping: Always use GROUP BY with aggregation functions
8. For time-based queries: Use appropriate date functions
9. Handle NULL values with COALESCE() or IS NULL
10. Never use * unless asked specifically
11. Add aliases for clarity: AS alias_name
12. Never assume columns that don't exist

{context}

EXAMPLES:
- "Top 5 products by revenue" → SELECT product, SUM(revenue) as total FROM {table_name} GROUP BY product ORDER BY total DESC LIMIT 5
- "Revenue by region" → SELECT region, SUM(revenue) as total FROM {table_name} GROUP BY region ORDER BY total DESC
- "How many records?" → SELECT COUNT(*) as total FROM {table_name}
- "Data from 2024" → SELECT * FROM {table_name} WHERE YEAR(date) = 2024

Now generate the SQL query for the user question.
Only return the SQL query, nothing else."""

        return prompt

    async def _query_ollama(self, prompt: str) -> str:
        """Call Ollama API to generate SQL."""

        try:
            response = requests.post(
                f"{self.ollama_url}/api/generate",
                json={
                    "model": self.model,
                    "prompt": prompt,
                    "stream": False,
                    "temperature": 0.1,  # Low randomness for SQL
                    "top_p": 0.9,
                },
                timeout=self.timeout,
            )

            if response.status_code == 200:
                result = response.json()
                return result.get("response", "").strip()
            else:
                raise Exception(f"Ollama returned status {response.status_code}")

        except requests.exceptions.Timeout:
            raise Exception("Ollama request timed out - check if service is running")
        except requests.exceptions.ConnectionError:
            raise OllamaNotAvailableError("Cannot connect to Ollama service")

    def _clean_sql(self, sql: str) -> str:
        """Clean SQL response (remove markdown, extra text)."""

        # Remove markdown code blocks if present
        sql = re.sub(r"```sql\n?", "", sql)
        sql = re.sub(r"```\n?", "", sql)

        # Extract first valid SELECT statement
        match = re.search(r"SELECT\s+.*?(?:;|$)", sql, re.IGNORECASE | re.DOTALL)
        if match:
            sql = match.group(0)

        # Clean whitespace
        sql = " ".join(sql.split())
        sql = sql.rstrip(";").strip()

        return sql

    def _is_valid_sql(self, sql: str) -> bool:
        """Check if SQL query looks valid."""

        sql_upper = sql.upper().strip()

        # Must start with SELECT or WITH
        if not (sql_upper.startswith("SELECT") or sql_upper.startswith("WITH")):
            return False

        # Must have FROM clause
        if "FROM" not in sql_upper:
            return False

        # Check for obvious syntax errors
        if sql.count("'") % 2 != 0:  # Unmatched quotes
            return False

        if sql.count("(") != sql.count(")"):  # Unmatched parentheses
            return False

        return True

    def _generate_fallback_sql(
        self,
        user_message: str,
        table_name: str,
        schema: dict[str, str],
    ) -> str:
        """Generate SQL using rule-based patterns when LLM fails."""

        message = user_message.lower()

        # Extract numeric and categorical columns
        numeric_cols = [
            col
            for col, dtype in schema.items()
            if any(x in dtype.lower() for x in ["int", "float", "decimal", "numeric"])
        ]
        cat_cols = [
            col
            for col, dtype in schema.items()
            if "string" in dtype.lower()
            or "varchar" in dtype.lower()
            or "text" in dtype.lower()
        ]
        date_cols = [
            col
            for col, dtype in schema.items()
            if "date" in dtype.lower() or "time" in dtype.lower()
        ]

        # Pattern: "Top N [column]"
        if any(word in message for word in ["top", "highest", "largest", "maximum"]):
            if numeric_cols:
                col = numeric_cols[0]
                return f'SELECT * FROM "{table_name}" ORDER BY "{col}" DESC LIMIT 10'

        # Pattern: "Count"
        if "count" in message or "how many" in message:
            return f'SELECT COUNT(*) as total FROM "{table_name}"'

        # Pattern: "[column] by [column]"
        if " by " in message:
            if cat_cols and numeric_cols:
                group_col = cat_cols[0]
                agg_col = numeric_cols[0]
                return f'SELECT "{group_col}", SUM("{agg_col}") as total FROM "{table_name}" GROUP BY "{group_col}" ORDER BY total DESC'

        # Pattern: Time range
        if date_cols and any(
            year in message for year in ["2024", "2023", "2022", "2021"]
        ):
            date_col = date_cols[0]
            year = next(
                (y for y in ["2024", "2023", "2022", "2021"] if y in message), "2024"
            )
            return f'SELECT * FROM "{table_name}" WHERE YEAR("{date_col}") = {year}'

        # Default: return all
        logger.warning(f"Using fallback SQL for: {user_message}")
        return f'SELECT * FROM "{table_name}" LIMIT 100'
