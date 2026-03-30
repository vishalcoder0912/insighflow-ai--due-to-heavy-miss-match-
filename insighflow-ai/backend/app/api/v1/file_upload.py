"""File ingestion API endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, File, Form, Request, UploadFile, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Any

from app.api.deps import get_current_user, get_db, get_request_ip
from app.models.user import User
from app.services.file_errors import FileIngestionException
from app.services.file_ingestion import FileIngestionService

router = APIRouter(prefix="/files", tags=["file-ingestion"])


class FileUploadResponse(BaseModel):
    """File upload response schema."""

    status: str = Field(..., description="Status of the upload")
    file_id: str | None = Field(..., description="Unique file identifier")
    file_name: str = Field(..., description="Original filename")
    file_type: str = Field(..., description="File type/extension")
    file_size: int = Field(..., description="File size in bytes")
    row_count: int = Field(..., description="Number of rows in the file")
    column_count: int = Field(..., description="Number of columns")
    columns: list[dict[str, Any]] = Field(..., description="Column definitions")
    data_preview: list[dict[str, Any]] = Field(..., description="First 100 rows")
    statistics: dict[str, Any] = Field(..., description="Dataset statistics")
    encoding: str | None = Field(None, description="Detected file encoding")
    skipped_rows: int = Field(0, description="Number of rows skipped due to errors")


class FileUploadErrorResponse(BaseModel):
    """File upload error response schema."""

    status: str = Field(..., description="Status: error")
    message: str = Field(..., description="Error message")
    code: str = Field(..., description="Error code")
    details: dict[str, Any] = Field(..., description="Additional error details")
    file_id: str | None = Field(None, description="File ID if available")


@router.post(
    "/upload",
    response_model=FileUploadResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Upload and ingest file",
    description="""
    Upload a file (CSV, Excel, JSON, or Parquet) for processing.
    
    - **Max file size**: 100MB
    - **Supported formats**: CSV, XLSX, XLS, JSON, Parquet
    - **Features**:
      - Auto-detect encoding (UTF-8, Latin-1, CP1252)
      - Auto-detect CSV delimiter
      - Flatten nested JSON
      - Handle merged Excel cells
      - Preserve Parquet schema
    
    Returns structured data with preview, column profiles, and statistics.
    """,
)
async def upload_file(
    request: Request,
    file: UploadFile = File(..., description="File to upload"),
    session: AsyncSession = Depends(get_db),
    current_user: User = Depends(get_current_user),
) -> FileUploadResponse:
    """Upload and ingest a file."""
    service = FileIngestionService(session)

    result = await service.ingest(
        file=file,
        user_id=current_user.id,
    )

    return FileUploadResponse(**result)


@router.get(
    "/health",
    status_code=status.HTTP_200_OK,
    summary="Check file ingestion service health",
)
async def health_check() -> dict[str, str]:
    """Check if the file ingestion service is available."""
    return {
        "status": "healthy",
        "service": "file-ingestion",
    }
