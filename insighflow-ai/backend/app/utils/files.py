"""File handling utilities."""

from __future__ import annotations

import secrets
from pathlib import Path

from fastapi import UploadFile

from app.core.config import get_settings
from app.core.exceptions import ApiException

SUPPORTED_FILE_EXTENSIONS = {".csv", ".xlsx", ".xls", ".json", ".parquet"}


def ensure_upload_directories() -> None:
    """Create upload directories when missing."""

    settings = get_settings()
    settings.uploads_path.mkdir(parents=True, exist_ok=True)
    (settings.uploads_path / "datasets").mkdir(parents=True, exist_ok=True)
    (settings.uploads_path / "documents").mkdir(parents=True, exist_ok=True)


def validate_filename(filename: str | None) -> str:
    """Validate and normalize an uploaded filename."""

    if not filename:
        raise ApiException(status_code=400, code="invalid_file", message="Uploaded file must have a filename.")
    suffix = Path(filename).suffix.lower()
    if suffix not in SUPPORTED_FILE_EXTENSIONS:
        raise ApiException(
            status_code=400,
            code="unsupported_file_type",
            message=f"Unsupported file type: {suffix}.",
            details={"supported_types": sorted(SUPPORTED_FILE_EXTENSIONS)},
        )
    return filename


async def save_upload(upload: UploadFile, subdir: str) -> tuple[Path, bytes, int]:
    """Stream an uploaded file to disk and return its path and sniff bytes."""

    settings = get_settings()
    filename = validate_filename(upload.filename)
    target_dir = settings.uploads_path / subdir
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / f"{secrets.token_hex(8)}-{Path(filename).name}"
    sniff_bytes = bytearray()
    total_size = 0
    chunk_size = 1024 * 1024

    try:
        with target_path.open("wb") as handle:
            while True:
                chunk = await upload.read(chunk_size)
                if not chunk:
                    break
                total_size += len(chunk)
                if total_size > settings.max_upload_size_bytes:
                    raise ApiException(
                        status_code=413,
                        code="file_too_large",
                        message="Uploaded file exceeds the maximum allowed size.",
                        details={"max_size_bytes": settings.max_upload_size_bytes},
                    )
                if len(sniff_bytes) < 50000:
                    remaining = 50000 - len(sniff_bytes)
                    sniff_bytes.extend(chunk[:remaining])
                handle.write(chunk)
    except Exception:
        if target_path.exists():
            target_path.unlink()
        raise
    finally:
        await upload.close()

    return target_path, bytes(sniff_bytes), total_size
