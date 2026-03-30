"""Data Import Automation - Connectors for various data sources."""

from __future__ import annotations

import logging
from enum import Enum
from typing import Any, Callable
from urllib.parse import urlparse

import pandas as pd
import requests

logger = logging.getLogger(__name__)

try:
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import NullPool
except ImportError:
    create_engine = None

MAX_ROWS_IMPORT = 100000
REQUEST_TIMEOUT = 60
MAX_RETRIES = 3


class SourceType(str, Enum):
    """Data source types."""

    CSV = "csv"
    EXCEL = "excel"
    JSON = "json"
    POSTGRESQL = "postgresql"
    REST_API = "rest_api"
    URL = "url"


class ImportResult:
    """Result of data import."""

    def __init__(
        self,
        success: bool,
        data: list[dict[str, Any]] | None = None,
        error: str | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        self.success = success
        self.data = data or []
        self.error = error
        self.metadata = metadata or {}


class DataImporter:
    """Universal data importer."""

    def __init__(self):
        self.engines = {}

    def import_from_url(
        self,
        url: str,
        source_type: str | None = None,
        **kwargs,
    ) -> ImportResult:
        """Import data from a URL."""
        try:
            parsed = urlparse(url)
            ext = parsed.path.split(".")[-1].lower() if "." in parsed.path else ""

            if source_type is None:
                if ext in ["csv", "tsv"]:
                    source_type = SourceType.CSV.value
                elif ext in ["xlsx", "xls"]:
                    source_type = SourceType.EXCEL.value
                elif ext == "json":
                    source_type = SourceType.JSON.value
                else:
                    source_type = SourceType.CSV.value

            response = requests.get(url, timeout=REQUEST_TIMEOUT, stream=True)
            response.raise_for_status()

            content = response.content

            if source_type == SourceType.CSV.value:
                from io import BytesIO

                df = pd.read_csv(BytesIO(content), **kwargs)
            elif source_type == SourceType.EXCEL.value:
                from io import BytesIO

                df = pd.read_excel(BytesIO(content), **kwargs)
            elif source_type == SourceType.JSON.value:
                from io import StringIO

                df = pd.read_json(StringIO(content.decode("utf-8")), **kwargs)
            else:
                return ImportResult(
                    success=False,
                    error=f"Unsupported source type: {source_type}",
                )

            if len(df) > MAX_ROWS_IMPORT:
                df = df.head(MAX_ROWS_IMPORT)
                logger.warning(f"Limited import to {MAX_ROWS_IMPORT} rows")

            return ImportResult(
                success=True,
                data=df.to_dict(orient="records"),
                metadata={
                    "source": url,
                    "source_type": source_type,
                    "rows": len(df),
                    "columns": list(df.columns),
                },
            )

        except Exception as e:
            logger.error(f"URL import failed: {e}")
            return ImportResult(success=False, error=str(e))

    def import_from_csv(
        self,
        file_path: str | None = None,
        content: bytes | None = None,
        **kwargs,
    ) -> ImportResult:
        """Import data from CSV file."""
        try:
            if file_path:
                df = pd.read_csv(file_path, **kwargs)
            elif content:
                from io import BytesIO

                df = pd.read_csv(BytesIO(content), **kwargs)
            else:
                return ImportResult(
                    success=False, error="No file_path or content provided"
                )

            if len(df) > MAX_ROWS_IMPORT:
                df = df.head(MAX_ROWS_IMPORT)

            return ImportResult(
                success=True,
                data=df.to_dict(orient="records"),
                metadata={
                    "source": file_path or "upload",
                    "source_type": SourceType.CSV.value,
                    "rows": len(df),
                    "columns": list(df.columns),
                },
            )

        except Exception as e:
            logger.error(f"CSV import failed: {e}")
            return ImportResult(success=False, error=str(e))

    def import_from_excel(
        self,
        file_path: str | None = None,
        content: bytes | None = None,
        sheet_name: str | int | None = None,
        **kwargs,
    ) -> ImportResult:
        """Import data from Excel file."""
        try:
            if file_path:
                df = pd.read_excel(file_path, sheet_name=sheet_name, **kwargs)
            elif content:
                from io import BytesIO

                df = pd.read_excel(BytesIO(content), sheet_name=sheet_name, **kwargs)
            else:
                return ImportResult(
                    success=False, error="No file_path or content provided"
                )

            if isinstance(df, dict):
                combined = pd.concat(df.values(), ignore_index=True)
                df = combined

            if len(df) > MAX_ROWS_IMPORT:
                df = df.head(MAX_ROWS_IMPORT)

            return ImportResult(
                success=True,
                data=df.to_dict(orient="records"),
                metadata={
                    "source": file_path or "upload",
                    "source_type": SourceType.EXCEL.value,
                    "rows": len(df),
                    "columns": list(df.columns),
                },
            )

        except Exception as e:
            logger.error(f"Excel import failed: {e}")
            return ImportResult(success=False, error=str(e))

    def import_from_json(
        self,
        file_path: str | None = None,
        content: str | bytes | None = None,
        **kwargs,
    ) -> ImportResult:
        """Import data from JSON file."""
        try:
            if file_path:
                df = pd.read_json(file_path, **kwargs)
            elif isinstance(content, str):
                from io import StringIO

                df = pd.read_json(StringIO(content), **kwargs)
            elif content:
                from io import BytesIO

                df = pd.read_json(BytesIO(content), **kwargs)
            else:
                return ImportResult(
                    success=False, error="No file_path or content provided"
                )

            if len(df) > MAX_ROWS_IMPORT:
                df = df.head(MAX_ROWS_IMPORT)

            return ImportResult(
                success=True,
                data=df.to_dict(orient="records"),
                metadata={
                    "source": file_path or "upload",
                    "source_type": SourceType.JSON.value,
                    "rows": len(df),
                    "columns": list(df.columns),
                },
            )

        except Exception as e:
            logger.error(f"JSON import failed: {e}")
            return ImportResult(success=False, error=str(e))

    def import_from_postgresql(
        self,
        connection_string: str,
        query: str,
        **kwargs,
    ) -> ImportResult:
        """Import data from PostgreSQL."""
        try:
            if create_engine is None:
                return ImportResult(success=False, error="SQLAlchemy not installed")

            engine = create_engine(connection_string, poolclass=NullPool)

            df = pd.read_sql(query, engine)

            engine.dispose()

            if len(df) > MAX_ROWS_IMPORT:
                df = df.head(MAX_ROWS_IMPORT)

            return ImportResult(
                success=True,
                data=df.to_dict(orient="records"),
                metadata={
                    "source": "postgresql",
                    "source_type": SourceType.POSTGRESQL.value,
                    "rows": len(df),
                    "columns": list(df.columns),
                },
            )

        except Exception as e:
            logger.error(f"PostgreSQL import failed: {e}")
            return ImportResult(success=False, error=str(e))

    def import_from_rest_api(
        self,
        url: str,
        method: str = "GET",
        headers: dict | None = None,
        params: dict | None = None,
        json_path: str | None = None,
        **kwargs,
    ) -> ImportResult:
        """Import data from REST API."""
        try:
            response = requests.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                timeout=REQUEST_TIMEOUT,
            )
            response.raise_for_status()

            data = response.json()

            if json_path:
                for key in json_path.split("."):
                    data = data.get(key, [])

            if isinstance(data, list):
                df = pd.DataFrame(data)
            else:
                df = pd.DataFrame([data])

            if len(df) > MAX_ROWS_IMPORT:
                df = df.head(MAX_ROWS_IMPORT)

            return ImportResult(
                success=True,
                data=df.to_dict(orient="records"),
                metadata={
                    "source": url,
                    "source_type": SourceType.REST_API.value,
                    "rows": len(df),
                    "columns": list(df.columns),
                },
            )

        except Exception as e:
            logger.error(f"REST API import failed: {e}")
            return ImportResult(success=False, error=str(e))

    def import_auto(
        self,
        source: str,
        source_type: str | None = None,
        **kwargs,
    ) -> ImportResult:
        """Auto-detect source type and import."""
        if source.startswith("http://") or source.startswith("https://"):
            return self.import_from_url(source, source_type, **kwargs)

        if source_type == SourceType.CSV.value:
            return self.import_from_csv(file_path=source, **kwargs)
        elif source_type == SourceType.EXCEL.value:
            return self.import_from_excel(file_path=source, **kwargs)
        elif source_type == SourceType.JSON.value:
            return self.import_from_json(file_path=source, **kwargs)
        else:
            return self.import_from_csv(file_path=source, **kwargs)


def import_data(
    source: str,
    source_type: str | None = None,
    **kwargs,
) -> dict[str, Any]:
    """Convenience function for data import."""
    importer = DataImporter()
    result = importer.import_auto(source, source_type, **kwargs)

    return {
        "status": "success" if result.success else "error",
        "data": result.data,
        "error": result.error,
        "metadata": result.metadata,
    }
