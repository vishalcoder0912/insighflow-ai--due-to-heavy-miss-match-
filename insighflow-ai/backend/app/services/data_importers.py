"""Data importer services for various data sources."""

from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
from typing import Any, Optional

import aiohttp
import pandas as pd

logger = logging.getLogger(__name__)


class BaseImporter:
    """Base class for data importers."""

    async def fetch_data(
        self, config: dict[str, Any], limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch data from source."""
        raise NotImplementedError


class PostgreSQLImporter(BaseImporter):
    """PostgreSQL data importer."""

    async def fetch_data(
        self, config: dict[str, Any], limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch data from PostgreSQL."""
        try:
            import asyncpg

            conn = await asyncpg.connect(
                host=config.get("host", "localhost"),
                port=config.get("port", 5432),
                user=config.get("user"),
                password=config.get("password"),
                database=config.get("database"),
            )

            query = config.get("query", "SELECT * FROM public.table_name")
            if limit:
                query += f" LIMIT {limit}"

            rows = await conn.fetch(query)
            await conn.close()

            df = pd.DataFrame(rows)
            logger.info(f"✅ Fetched {len(df)} rows from PostgreSQL")
            return df

        except Exception as e:
            logger.error(f"❌ PostgreSQL import error: {e}")
            raise


class MySQLImporter(BaseImporter):
    """MySQL data importer."""

    async def fetch_data(
        self, config: dict[str, Any], limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch data from MySQL."""
        try:
            import aiomysql

            conn = await aiomysql.connect(
                host=config.get("host", "localhost"),
                port=config.get("port", 3306),
                user=config.get("user"),
                password=config.get("password"),
                db=config.get("database"),
            )

            async with conn.cursor() as cursor:
                query = config.get("query", "SELECT * FROM table_name")
                if limit:
                    query += f" LIMIT {limit}"

                await cursor.execute(query)
                rows = await cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]

            conn.close()

            df = pd.DataFrame(rows, columns=columns)
            logger.info(f"✅ Fetched {len(df)} rows from MySQL")
            return df

        except Exception as e:
            logger.error(f"❌ MySQL import error: {e}")
            raise


class CSVImporter(BaseImporter):
    """CSV file importer."""

    async def fetch_data(
        self, config: dict[str, Any], limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch data from CSV file."""
        try:
            file_path = config.get("file_path")
            if not file_path:
                raise ValueError("file_path required")

            df = pd.read_csv(file_path, nrows=limit)
            logger.info(f"✅ Loaded {len(df)} rows from CSV")
            return df

        except Exception as e:
            logger.error(f"❌ CSV import error: {e}")
            raise


class APIImporter(BaseImporter):
    """REST API importer."""

    async def fetch_data(
        self, config: dict[str, Any], limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch data from REST API."""
        try:
            url = config.get("url")
            if not url:
                raise ValueError("url required")

            headers = config.get("headers", {})
            params = config.get("params", {})
            data_path = config.get("data_path")

            all_data = []
            page = 1
            max_pages = limit if limit else 100

            async with aiohttp.ClientSession() as session:
                while page <= max_pages:
                    try:
                        params["page"] = page
                        async with session.get(
                            url, headers=headers, params=params, timeout=30
                        ) as resp:
                            if resp.status != 200:
                                break

                            data = await resp.json()

                            if data_path:
                                for key in data_path.split("."):
                                    data = data.get(key, [])

                            if not data:
                                break

                            all_data.extend(data if isinstance(data, list) else [data])
                            page += 1

                    except Exception as e:
                        logger.warning(f"Error fetching page {page}: {e}")
                        break

            df = pd.DataFrame(all_data)
            logger.info(f"✅ Fetched {len(df)} rows from API")
            return df

        except Exception as e:
            logger.error(f"❌ API import error: {e}")
            raise


class S3Importer(BaseImporter):
    """AWS S3 importer."""

    async def fetch_data(
        self, config: dict[str, Any], limit: Optional[int] = None
    ) -> pd.DataFrame:
        """Fetch data from S3."""
        try:
            import boto3

            bucket = config.get("bucket")
            key = config.get("key")

            if not bucket or not key:
                raise ValueError("bucket and key required")

            s3 = boto3.client(
                "s3",
                aws_access_key_id=config.get("access_key"),
                aws_secret_access_key=config.get("secret_key"),
                region_name=config.get("region", "us-east-1"),
            )

            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"].read()

            if key.endswith(".csv"):
                df = pd.read_csv(io.BytesIO(body), nrows=limit)
            elif key.endswith(".json"):
                df = pd.read_json(io.BytesIO(body))
            elif key.endswith(".xlsx"):
                df = pd.read_excel(io.BytesIO(body), nrows=limit)
            else:
                raise ValueError(f"Unsupported format: {key}")

            logger.info(f"✅ Fetched {len(df)} rows from S3")
            return df

        except Exception as e:
            logger.error(f"❌ S3 import error: {e}")
            raise


class DataImporterFactory:
    """Factory for creating data importers."""

    IMPORTERS = {
        "postgresql": PostgreSQLImporter,
        "mysql": MySQLImporter,
        "csv_file": CSVImporter,
        "json_file": CSVImporter,
        "api": APIImporter,
        "s3": S3Importer,
    }

    @classmethod
    def create(cls, source_type: str) -> BaseImporter:
        """Create importer for source type."""
        importer_class = cls.IMPORTERS.get(source_type)
        if not importer_class:
            raise ValueError(f"Unknown source type: {source_type}")
        return importer_class()

    @classmethod
    async def import_data(
        cls,
        source_type: str,
        config: dict[str, Any],
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """Import data from source."""
        importer = cls.create(source_type)
        return await importer.fetch_data(config, limit)
