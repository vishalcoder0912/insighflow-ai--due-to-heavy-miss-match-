"""PostgreSQL storage engine for dynamic dataset tables."""

from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd
import sqlalchemy
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

logger = logging.getLogger(__name__)

SAFE_COLUMN_PATTERN = re.compile(r'^[a-zA-Z_][a-zA-Z0-9_]*$')
MAX_ROWS_PER_INSERT = 5000
MAX_ROWS_STORAGE = 1000000


class PostgresStorageEngine:
    """PostgreSQL storage engine for dynamic dataset tables."""

    def __init__(self, session: AsyncSession):
        self.session = session

    @staticmethod
    def sanitize_column_name(name: str) -> str:
        """Sanitize column name for safe SQL usage."""
        clean = re.sub(r'[^a-zA-Z0-9_]', '_', str(name).strip())
        if not clean[0].isalpha() and clean[0] != '_':
            clean = f'col_{clean}'
        return clean[:64]

    @staticmethod
    def detect_postgres_dtype(dtype: str) -> str:
        """Map pandas dtype to PostgreSQL type."""
        dtype_lower = dtype.lower()
        if 'int' in dtype_lower:
            return 'BIGINT'
        elif 'float' in dtype_lower:
            return 'DOUBLE PRECISION'
        elif 'bool' in dtype_lower:
            return 'BOOLEAN'
        elif 'datetime' in dtype_lower or 'date' in dtype_lower:
            return 'TIMESTAMP'
        else:
            return 'TEXT'

    async def create_dataset_table(
        self,
        dataset_id: int,
        columns: list[dict[str, Any]],
    ) -> bool:
        """Create dynamic table for dataset."""
        table_name = f"dataset_{dataset_id}"
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

        column_defs = []
        for col in columns:
            col_name = self.sanitize_column_name(col.get('name', 'column'))
            dtype = self.detect_postgres_dtype(col.get('dtype', 'text'))
            column_defs.append(f'"{col_name}" {dtype}')

        if not column_defs:
            column_defs = ['id SERIAL PRIMARY KEY', 'data JSONB']

        create_sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({", ".join(column_defs)})'

        try:
            await self.session.execute(text(create_sql))
            await self.session.commit()
            logger.info(f"Created table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to create table {table_name}: {e}")
            await self.session.rollback()
            return False

    async def insert_data(
        self,
        dataset_id: int,
        data: list[dict[str, Any]],
        columns: list[dict[str, Any]],
    ) -> dict[str, Any]:
        """Insert data into dataset table with batch processing."""
        if not data:
            return {"status": "success", "rows_inserted": 0}

        table_name = f"dataset_{dataset_id}"
        table_name = re.sub(r'[^a-zA-Z0-9_]', '_', table_name)

        col_mapping = {self.sanitize_column_name(col.get('name', '')): col.get('name', '') for col in columns}
        db_columns = list(col_mapping.keys())

        rows_inserted = 0
        errors = 0

        for i in range(0, len(data), MAX_ROWS_PER_INSERT):
            batch = data[i:i + MAX_ROWS_PER_INSERT]
            
            values_list = []
            for row in batch:
                values = []
                for db_col in db_columns:
                    original_col = col_mapping[db_col]
                    value = row.get(original_col)
                    if value is None:
                        values.append('NULL')
                    elif isinstance(value, (int, float, bool)):
                        values.append(str(value))
                    elif isinstance(value, datetime):
                        values.append(f"'{value.isoformat()}'")
                    elif isinstance(value, (dict, list)):
                        values.append(f"'{json.dumps(value).replace(\"'\", \"''\")}'")
                    else:
                        escaped = str(value).replace("'", "''")
                        values.append(f"'{escaped}'")
                values_list.append(f"({', '.join(values)})")

            if not values_list:
                continue

            insert_sql = f"""
                INSERT INTO {table_name} ({', '.join(f'"{c}' for c in db_columns)})
                VALUES {', '.join(values_list)}
            """

            try:
                await self.session.execute(text(insert_sql))
                rows_inserted += len(batch)
            except Exception as e:
                logger.error(f"Insert error: {e}")
                errors += len(batch)
                continue

        await self.session.commit()

        return {
            "status": "success",
            "rows_inserted": rows_inserted,
            "errors": errors,
        }

    async def execute_query(
        self,
        dataset_id: int,
        sql: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Execute SQL query on dataset table."""
        table_name = f"dataset_{dataset_id}"
        
        if not self._is_safe_query(sql):
            return {
                "status": "error",
                "message": "Query contains potentially unsafe operations",
                "columns": [],
                "rows": [],
            }

        sql = sql.replace(f"dataset_{dataset_id}", table_name)

        try:
            result = await self.session.execute(text(sql), params or {})
            rows = [dict(row._mapping) for row in result]
            columns = list(result.keys()) if rows else []
            
            return {
                "status": "success",
                "columns": columns,
                "rows": rows[:5000],
                "row_count": len(rows),
            }
        except Exception as e:
            logger.error(f"Query error: {e}")
            return {
                "status": "error",
                "message": str(e),
                "columns": [],
                "rows": [],
            }

    async def get_table_columns(self, dataset_id: int) -> list[dict[str, str]]:
        """Get column information for dataset table."""
        table_name = f"dataset_{dataset_id}"
        
        try:
            result = await self.session.execute(text(f"""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = '{table_name}'
                ORDER BY ordinal_position
            """))
            columns = [{"name": row[0], "dtype": row[1]} for row in result]
            return columns
        except Exception as e:
            logger.error(f"Failed to get columns: {e}")
            return []

    async def get_table_row_count(self, dataset_id: int) -> int:
        """Get row count for dataset table."""
        table_name = f"dataset_{dataset_id}"
        
        try:
            result = await self.session.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
            return result.scalar() or 0
        except Exception:
            return 0

    async def drop_table(self, dataset_id: int) -> bool:
        """Drop dataset table."""
        table_name = f"dataset_{dataset_id}"
        
        try:
            await self.session.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            await self.session.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to drop table: {e}")
            await self.session.rollback()
            return False

    async def sample_data(
        self,
        dataset_id: int,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Get sample data from dataset."""
        result = await self.execute_query(
            dataset_id,
            f"SELECT * FROM dataset_{dataset_id} LIMIT {limit}"
        )
        return result.get("rows", [])

    async def get_column_statistics(
        self,
        dataset_id: int,
        column: str,
    ) -> dict[str, Any]:
        """Get statistics for a specific column."""
        safe_col = self.sanitize_column_name(column)
        
        sql = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(DISTINCT {safe_col}) as unique_count,
                COUNT({safe_col}) as non_null_count,
                MIN({safe_col}) as min_value,
                MAX({safe_col}) as max_value,
                AVG({safe_col}) as avg_value
            FROM dataset_{dataset_id}
            WHERE {safe_col} IS NOT NULL
        """
        
        try:
            result = await self.session.execute(text(sql))
            row = result.fetchone()
            if row:
                return {
                    "total_count": row[0],
                    "unique_count": row[1],
                    "non_null_count": row[2],
                    "min_value": row[3],
                    "max_value": row[4],
                    "avg_value": row[5],
                }
        except Exception as e:
            logger.error(f"Statistics error: {e}")
        
        return {}

    def _is_safe_query(self, sql: str) -> bool:
        """Validate query safety."""
        sql_lower = sql.lower().strip()
        
        dangerous = ['drop', 'delete', 'update', 'insert', 'alter', 'truncate', 'create']
        for keyword in dangerous:
            if sql_lower.startswith(keyword):
                return False
        
        if 'grant' in sql_lower or 'revoke' in sql_lower:
            return False
            
        return True


class QueryCache:
    """Simple in-memory query cache."""

    def __init__(self):
        self._cache: dict[str, dict[str, Any]] = {}
        self._max_size = 100

    def get(self, query_hash: str) -> dict[str, Any] | None:
        """Get cached result."""
        return self._cache.get(query_hash)

    def set(self, query_hash: str, result: dict[str, Any]) -> None:
        """Set cached result."""
        if len(self._cache) >= self._max_size:
            oldest_key = next(iter(self._cache))
            del self._cache[oldest_key]
        self._cache[query_hash] = result

    def clear(self) -> None:
        """Clear all cached results."""
        self._cache.clear()


query_cache = QueryCache()
