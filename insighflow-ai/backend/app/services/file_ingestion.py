"""File ingestion service - main orchestration."""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import UploadFile
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.uploaded_file import FileStatus, UploadedFile
from app.services.file_errors import (
    EmptyFileError,
    EncodingError,
    FileCorruptedError,
    FileIngestionException,
    FileTooLargeError,
    UnsupportedFileTypeError,
)
from app.services.file_normalizer import normalize_data
from app.services.file_parsers import (
    FileParseError,
    FileParserFactory,
    MAX_FILE_SIZE,
)

logger = logging.getLogger(__name__)


class FileIngestionService:
    """Main file ingestion orchestration service."""

    def __init__(self, session: AsyncSession):
        self.session = session
        self.file_record: UploadedFile | None = None

    async def ingest(
        self,
        file: UploadFile,
        user_id: int | None = None,
    ) -> dict[str, Any]:
        """Main ingestion pipeline."""
        file_id = uuid.uuid4()

        try:
            await self._validate_file(file)

            self.file_record = await self._create_file_record(file, file_id, user_id)
            await self.session.commit()

            parsed_result = await self._parse_file(file)

            normalized_result = await self._normalize_data(parsed_result["data"])

            await self._update_file_record_success(normalized_result, parsed_result)

            await self._store_data_chunks(file_id, normalized_result["data"])

            await self.session.commit()

            return self._build_response(normalized_result, file, parsed_result)

        except FileIngestionException as e:
            await self._handle_error(file_id, file.filename, e)
            raise

        except Exception as e:
            logger.error(f"Unexpected ingestion error: {e}", exc_info=True)
            await self._handle_error(
                file_id,
                file.filename,
                FileIngestionException(
                    message=f"Unexpected error: {str(e)}",
                    details={"error_type": type(e).__name__},
                ),
            )
            raise

    async def _validate_file(self, file: UploadFile) -> None:
        """Validate file before processing."""
        if not file.filename:
            raise FileIngestionException(
                message="Filename is required",
                status_code=400,
            )

        if not FileParserFactory.is_supported(file.filename):
            supported = list(FileParserFactory.PARSERS.keys())
            ext = Path(file.filename).suffix.lower().lstrip(".")
            raise UnsupportedFileTypeError(ext, supported)

        content = await file.read(1)
        await file.seek(0)

        if not content:
            raise EmptyFileError(file.filename)

        await file.seek(0, 2)
        file_size = await file.tell()
        await file.seek(0)

        if file_size > MAX_FILE_SIZE:
            raise FileTooLargeError(file_size, MAX_FILE_SIZE)

        if file_size == 0:
            raise EmptyFileError(file.filename)

    async def _create_file_record(
        self,
        file: UploadFile,
        file_id: uuid.UUID,
        user_id: int | None,
    ) -> UploadedFile:
        """Create initial file record in database."""
        ext = Path(file.filename).suffix.lower().lstrip(".")

        record = UploadedFile(
            id=file_id,
            user_id=user_id,
            file_name=file.filename,
            file_type=ext,
            file_size=0,
            status=FileStatus.UPLOADING.value,
            upload_date=datetime.utcnow(),
        )

        self.session.add(record)
        await self.session.flush()

        return record

    async def _parse_file(self, file: UploadFile) -> dict[str, Any]:
        """Parse file based on type."""
        parser = FileParserFactory.get_parser(file)

        try:
            result = await parser.parse()
            return result

        except FileParseError as e:
            if "No valid data" in e.message or "empty" in e.message.lower():
                raise EmptyFileError(file.filename)
            if "encoding" in e.message.lower():
                raise EncodingError(file.filename, ["utf-8", "latin-1", "cp1252"])
            raise FileCorruptedError(file.filename, e.details.get("message"))

        except Exception as e:
            logger.error(f"Parse error: {e}", exc_info=True)
            raise FileCorruptedError(file.filename, str(e))

    async def _normalize_data(self, data: list[dict[str, Any]]) -> dict[str, Any]:
        """Normalize parsed data."""
        if not data:
            raise EmptyFileError("No data to normalize")

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, normalize_data, data)

        return result

    async def _update_file_record_success(
        self,
        normalized: dict[str, Any],
        parsed: dict[str, Any],
    ) -> None:
        """Update file record with success data."""
        if not self.file_record:
            return

        self.file_record.status = FileStatus.COMPLETED.value
        self.file_record.file_size = sum(
            len(str(v)) for row in normalized["data"] for v in row.values()
        )
        self.file_record.row_count = normalized["row_count"]
        self.file_record.column_count = normalized["column_count"]
        self.file_record.columns = normalized["columns"]
        self.file_record.data_preview = normalized["data_preview"]
        self.file_record.encoding = parsed.get("encoding")
        self.file_record.skipped_rows = parsed.get("skipped_rows", 0)
        self.file_record.statistics = normalized["statistics"]
        self.file_record.metadata = {
            "delimiter": parsed.get("delimiter"),
            "sheets": parsed.get("sheets"),
            "sheet_count": parsed.get("sheet_count"),
        }

    async def _handle_error(
        self,
        file_id: uuid.UUID,
        filename: str | None,
        error: FileIngestionException,
    ) -> None:
        """Handle and log errors."""
        logger.error(f"File ingestion failed: {error.message}")

        try:
            if self.file_record:
                self.file_record.status = FileStatus.FAILED.value
                self.file_record.error_message = error.message
                await self.session.commit()
        except Exception as e:
            logger.error(f"Failed to update error status: {e}")

    async def _store_data_chunks(
        self,
        file_id: uuid.UUID,
        data: list[dict[str, Any]],
    ) -> None:
        """Store data in chunks (placeholder for dynamic tables)."""
        chunk_size = 1000
        for i in range(0, len(data), chunk_size):
            chunk = data[i : i + chunk_size]
            logger.debug(f"Storing chunk {i // chunk_size + 1}: {len(chunk)} rows")

    def _build_response(
        self,
        normalized: dict[str, Any],
        file: UploadFile,
        parsed: dict[str, Any],
    ) -> dict[str, Any]:
        """Build final response."""
        return {
            "status": "success",
            "file_id": str(self.file_record.id) if self.file_record else None,
            "file_name": file.filename,
            "file_type": Path(file.filename).suffix.lower().lstrip("."),
            "file_size": self.file_record.file_size if self.file_record else 0,
            "row_count": normalized["row_count"],
            "column_count": normalized["column_count"],
            "columns": normalized["columns"],
            "data_preview": normalized["data_preview"],
            "statistics": normalized["statistics"],
            "encoding": parsed.get("encoding"),
            "skipped_rows": parsed.get("skipped_rows", 0),
        }
