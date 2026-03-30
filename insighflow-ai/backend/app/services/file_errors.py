"""Error handling utilities for file ingestion."""

from __future__ import annotations

import logging
import traceback
from enum import Enum
from typing import Any

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)


class ErrorCode(str, Enum):
    """Standard error codes for file ingestion."""

    FILE_TOO_LARGE = "file_too_large"
    UNSUPPORTED_FILE_TYPE = "unsupported_file_type"
    FILE_CORRUPTED = "file_corrupted"
    ENCODING_ERROR = "encoding_error"
    EMPTY_FILE = "empty_file"
    INVALID_FORMAT = "invalid_format"
    PROCESSING_ERROR = "processing_error"
    DATABASE_ERROR = "database_error"
    VALIDATION_ERROR = "validation_error"
    MEMORY_ERROR = "memory_error"


class FileIngestionException(Exception):
    """Base exception for file ingestion errors."""

    def __init__(
        self,
        message: str,
        code: ErrorCode = ErrorCode.PROCESSING_ERROR,
        details: dict[str, Any] | None = None,
        status_code: int = status.HTTP_500_INTERNAL_SERVER_ERROR,
    ):
        self.message = message
        self.code = code
        self.details = details or {}
        self.status_code = status_code
        super().__init__(self.message)


class FileTooLargeError(FileIngestionException):
    """File exceeds maximum size limit."""

    def __init__(self, size: int, max_size: int):
        super().__init__(
            message=f"File size {size} bytes exceeds maximum allowed size {max_size} bytes",
            code=ErrorCode.FILE_TOO_LARGE,
            details={"file_size": size, "max_size": max_size},
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        )


class UnsupportedFileTypeError(FileIngestionException):
    """File type is not supported."""

    def __init__(self, file_type: str, supported_types: list[str]):
        super().__init__(
            message=f"Unsupported file type: {file_type}",
            code=ErrorCode.UNSUPPORTED_FILE_TYPE,
            details={"file_type": file_type, "supported_types": supported_types},
            status_code=status.HTTP_400_BAD_REQUEST,
        )


class FileCorruptedError(FileIngestionException):
    """File appears to be corrupted."""

    def __init__(self, filename: str, details: str | None = None):
        super().__init__(
            message=f"File '{filename}' appears to be corrupted",
            code=ErrorCode.FILE_CORRUPTED,
            details={"filename": filename, "details": details},
            status_code=status.HTTP_400_BAD_REQUEST,
        )


class EncodingError(FileIngestionException):
    """File encoding could not be determined."""

    def __init__(self, filename: str, attempted_encodings: list[str]):
        super().__init__(
            message=f"Could not determine encoding for file '{filename}'",
            code=ErrorCode.ENCODING_ERROR,
            details={"filename": filename, "attempted_encodings": attempted_encodings},
            status_code=status.HTTP_400_BAD_REQUEST,
        )


class EmptyFileError(FileIngestionException):
    """File contains no data."""

    def __init__(self, filename: str):
        super().__init__(
            message=f"File '{filename}' is empty or contains no valid data",
            code=ErrorCode.EMPTY_FILE,
            details={"filename": filename},
            status_code=status.HTTP_400_BAD_REQUEST,
        )


class ValidationError(FileIngestionException):
    """Data validation failed."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(
            message=message,
            code=ErrorCode.VALIDATION_ERROR,
            details=details,
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
        )


class MemoryLimitError(FileIngestionException):
    """File would exceed memory limits."""

    def __init__(self, filename: str, estimated_size: int):
        super().__init__(
            message=f"File '{filename}' would exceed available memory",
            code=ErrorCode.MEMORY_ERROR,
            details={"filename": filename, "estimated_size": estimated_size},
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
        )


async def file_ingestion_exception_handler(
    request: Request,
    exc: FileIngestionException,
) -> JSONResponse:
    """Handle file ingestion exceptions."""

    log_data = {
        "url": str(request.url),
        "method": request.method,
        "error_code": exc.code,
        "error_message": exc.message,
        "details": exc.details,
    }

    if exc.code in [ErrorCode.PROCESSING_ERROR, ErrorCode.DATABASE_ERROR]:
        logger.error(
            f"File ingestion error: {exc.message}",
            extra=log_data,
            exc_info=True,
        )
    else:
        logger.warning(
            f"File ingestion client error: {exc.message}",
            extra=log_data,
        )

    return JSONResponse(
        status_code=exc.status_code,
        content={
            "status": "error",
            "message": exc.message,
            "code": exc.code.value,
            "details": exc.details,
            "file_id": None,
        },
    )


async def generic_exception_handler(
    request: Request,
    exc: Exception,
) -> JSONResponse:
    """Handle unexpected exceptions."""

    error_id = id(exc)

    log_data = {
        "url": str(request.url),
        "method": request.method,
        "error_id": error_id,
        "error_type": type(exc).__name__,
    }

    logger.error(
        f"Unexpected error during file ingestion: {type(exc).__name__}",
        extra=log_data,
        exc_info=True,
    )

    return JSONResponse(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
        content={
            "status": "error",
            "message": "An unexpected error occurred while processing your file",
            "code": ErrorCode.PROCESSING_ERROR.value,
            "details": {
                "error_type": type(exc).__name__,
                "error_id": str(error_id),
            },
            "file_id": None,
        },
    )


def safe_execute_parsing_operation(func):
    """Decorator for safe parsing operations."""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileIngestionException:
            raise
        except Exception as e:
            logger.error(f"Parsing operation failed: {e}", exc_info=True)
            raise FileIngestionException(
                message=f"Failed to parse file: {str(e)}",
                details={"operation": func.__name__, "error_type": type(e).__name__},
            )

    return wrapper
