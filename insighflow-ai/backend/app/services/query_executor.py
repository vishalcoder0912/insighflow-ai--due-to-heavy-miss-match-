"""SQL query executor service."""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text

from app.services.storage_engine import StorageEngine

logger = logging.getLogger(__name__)


async def execute_sql_query(
    table_name: str,
    sql_query: str,
    limit: int = 1000,
) -> dict[str, Any]:
    """Execute SQL query and return results.

    Args:
        table_name: The table name (for reference)
        sql_query: SQL query to execute
        limit: Maximum rows to return

    Returns:
        Dictionary with rows and metadata
    """
    from app.api.deps import get_db_session

    async for db_session in get_db_session():
        try:
            result = await db_session.execute(
                text(sql_query),
            )

            columns = result.keys()
            rows = result.fetchmany(limit)

            return {
                "columns": list(columns),
                "rows": [dict(zip(columns, row)) for row in rows],
                "row_count": len(rows),
            }

        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise


async def get_table_data(
    table_name: str,
    limit: int = 100,
    offset: int = 0,
) -> dict[str, Any]:
    """Get data from a table.

    Args:
        table_name: Table to query
        limit: Max rows
        offset: Offset for pagination

    Returns:
        Dictionary with rows
    """
    sql = f'SELECT * FROM "{table_name}" LIMIT {limit} OFFSET {offset}'
    return await execute_sql_query(table_name, sql, limit)
