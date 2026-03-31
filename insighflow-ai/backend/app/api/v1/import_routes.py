"""Data Import API endpoints."""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db
from app.models.user import User
from app.services.import_data import DataImporter

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/import", tags=["import"])


class URLImportRequest(BaseModel):
    """Import from URL request."""

    url: str = Field(..., description="URL to import data from")
    source_type: str | None = Field(None, description="Source type: csv, excel, json")


class RESTImportRequest(BaseModel):
    """Import from REST API request."""

    url: str = Field(..., description="API endpoint URL")
    method: str = Field("GET", description="HTTP method: GET or POST")
    headers: dict[str, str] | None = Field(None, description="HTTP headers")
    json_path: str | None = Field(None, description="JSON path to extract data")


class PostgreSQLImportRequest(BaseModel):
    """Import from PostgreSQL request."""

    connection_string: str = Field(..., description="PostgreSQL connection string")
    query: str = Field(..., description="SQL query to execute")


class ImportResponse(BaseModel):
    """Import response."""

    status: str
    rows: int = 0
    columns: list[str] = Field(default_factory=list)
    data: list[dict[str, Any]] | None = None
    error: str | None = None
    metadata: dict[str, Any] | None = None


@router.post("/url", response_model=ImportResponse)
async def import_from_url(
    request: URLImportRequest,
    current_user: User = Depends(get_current_user),
) -> ImportResponse:
    """Import data from a URL (CSV, Excel, JSON)."""
    try:
        importer = DataImporter()
        result = importer.import_from_url(
            url=request.url,
            source_type=request.source_type,
        )

        if result.success:
            return ImportResponse(
                status="success",
                rows=len(result.data),
                columns=result.metadata.get("columns", []),
                data=result.data[:100],
                metadata=result.metadata,
            )
        else:
            return ImportResponse(
                status="error",
                error=result.error,
            )

    except Exception as e:
        logger.error(f"URL import failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/rest", response_model=ImportResponse)
async def import_from_rest(
    request: RESTImportRequest,
    current_user: User = Depends(get_current_user),
) -> ImportResponse:
    """Import data from a REST API."""
    try:
        importer = DataImporter()
        result = importer.import_from_rest_api(
            url=request.url,
            method=request.method,
            headers=request.headers,
            json_path=request.json_path,
        )

        if result.success:
            return ImportResponse(
                status="success",
                rows=len(result.data),
                columns=result.metadata.get("columns", []),
                data=result.data[:100],
                metadata=result.metadata,
            )
        else:
            return ImportResponse(
                status="error",
                error=result.error,
            )

    except Exception as e:
        logger.error(f"REST API import failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/postgresql", response_model=ImportResponse)
async def import_from_postgresql(
    request: PostgreSQLImportRequest,
    current_user: User = Depends(get_current_user),
) -> ImportResponse:
    """Import data from PostgreSQL database."""
    try:
        importer = DataImporter()
        result = importer.import_from_postgresql(
            connection_string=request.connection_string,
            query=request.query,
        )

        if result.success:
            return ImportResponse(
                status="success",
                rows=len(result.data),
                columns=result.metadata.get("columns", []),
                data=result.data[:100],
                metadata=result.metadata,
            )
        else:
            return ImportResponse(
                status="error",
                error=result.error,
            )

    except Exception as e:
        logger.error(f"PostgreSQL import failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/sources")
async def get_import_sources() -> dict[str, Any]:
    """Get list of supported import sources."""
    return {
        "sources": [
            {
                "id": "csv",
                "name": "CSV File",
                "description": "Upload CSV files",
                "extensions": [".csv", ".tsv"],
            },
            {
                "id": "excel",
                "name": "Excel File",
                "description": "Upload Excel files",
                "extensions": [".xlsx", ".xls"],
            },
            {
                "id": "json",
                "name": "JSON File",
                "description": "Upload JSON files",
                "extensions": [".json"],
            },
            {
                "id": "url",
                "name": "URL Import",
                "description": "Import from web URL",
                "supports": ["csv", "excel", "json"],
            },
            {
                "id": "rest_api",
                "name": "REST API",
                "description": "Connect to REST APIs",
                "methods": ["GET", "POST"],
            },
            {
                "id": "postgresql",
                "name": "PostgreSQL",
                "description": "Query PostgreSQL database",
                "requires_connection": True,
            },
        ],
        "max_rows": 100000,
        "max_file_size_mb": 100,
    }
