"""Robust file parsers for universal file ingestion."""

from __future__ import annotations

import io
import json
import logging
import re
from pathlib import Path
from typing import Any, Generator

import chardet
import pandas as pd
from fastapi import UploadFile

logger = logging.getLogger(__name__)

MAX_FILE_SIZE = 100 * 1024 * 1024  # 100MB
PREVIEW_ROWS = 100
CHUNK_SIZE = 10000


class FileParseError(Exception):
    """Custom exception for file parsing errors."""

    def __init__(self, message: str, details: dict[str, Any] | None = None):
        self.message = message
        self.details = details or {}
        super().__init__(self.message)


class EncodingDetector:
    """Auto-detect file encoding."""

    ENCODINGS_TO_TRY = ["utf-8", "latin-1", "cp1252", "iso-8859-1", "utf-16"]

    @classmethod
    def detect(cls, raw_bytes: bytes) -> str:
        """Detect encoding from raw bytes."""
        try:
            result = chardet.detect(raw_bytes[:50000])
            encoding = result.get("encoding") if result else None
            if encoding and encoding.lower() in [
                e.lower() for e in cls.ENCODINGS_TO_TRY
            ]:
                return encoding
        except Exception as e:
            logger.warning(f"Encoding detection failed: {e}")
        return "utf-8"

    @classmethod
    def safe_decode(cls, raw_bytes: bytes) -> str:
        """Safely decode bytes to string."""
        encoding = cls.detect(raw_bytes)
        for enc in [encoding, "latin-1", "cp1252"]:
            try:
                return raw_bytes.decode(enc)
            except (UnicodeDecodeError, LookupError):
                continue
        return raw_bytes.decode("utf-8", errors="replace")


class DelimiterDetector:
    """Auto-detect CSV delimiter."""

    DELIMITERS = [",", ";", "\t", "|"]

    @classmethod
    def detect(cls, sample: str) -> str:
        """Detect delimiter from sample text."""
        lines = sample.strip().split("\n")[:5]
        if not lines:
            return ","

        counts: dict[str, int] = {}
        for delim in cls.DELIMITERS:
            counts[delim] = sum(line.count(delim) for line in lines)

        if not counts or max(counts.values()) == 0:
            return ","

        return max(counts, key=counts.get)


class BaseParser:
    """Base class for file parsers."""

    def __init__(self, file: UploadFile, filename: str):
        self.file = file
        self.filename = filename
        self.content_bytes: bytes | None = None

    async def read_bytes(self, size: int = CHUNK_SIZE) -> bytes:
        """Read file bytes."""
        if self.content_bytes is None:
            self.content_bytes = await self.file.read(size)
            await self.file.seek(0)
        return self.content_bytes

    async def read_all_bytes(self) -> bytes:
        """Read all file bytes."""
        if self.content_bytes is None:
            self.content_bytes = await self.file.read()
            await self.file.seek(0)
        return self.content_bytes


class CSVParser(BaseParser):
    """Robust CSV parser with auto-detection."""

    async def parse(self) -> dict[str, Any]:
        """Parse CSV file with auto-detection."""
        try:
            raw_bytes = await self.read_all_bytes()
            encoding = EncodingDetector.detect(raw_bytes)
            content = EncodingDetector.safe_decode(raw_bytes)

            delimiter = DelimiterDetector.detect(content)

            skipped_rows = 0
            valid_rows = []

            try:
                for chunk in pd.read_csv(
                    io.BytesIO(raw_bytes),
                    encoding=encoding,
                    delimiter=delimiter,
                    chunksize=CHUNK_SIZE,
                    on_bad_lines="skip",
                    engine="python",
                    quotechar='"',
                    escapechar="\\",
                ):
                    skipped_rows += chunk.attrs.get("skipped_rows", 0)
                    valid_rows.extend(chunk.to_dicts())

            except Exception as e:
                logger.warning(f"Chunked CSV reading failed, trying fallback: {e}")
                df = pd.read_csv(
                    io.BytesIO(raw_bytes),
                    encoding=encoding,
                    delimiter=delimiter,
                    on_bad_lines="skip",
                    engine="c",
                )
                valid_rows = df.to_dicts()

            if not valid_rows:
                raise FileParseError(
                    message="No valid data rows found in CSV",
                    details={"skipped_rows": skipped_rows},
                )

            return {
                "data": valid_rows,
                "encoding": encoding,
                "delimiter": delimiter,
                "skipped_rows": skipped_rows,
                "row_count": len(valid_rows),
            }

        except FileParseError:
            raise
        except Exception as e:
            logger.error(f"CSV parsing error: {e}")
            raise FileParseError(
                message=f"Failed to parse CSV: {str(e)}",
                details={"filename": self.filename},
            )


class ExcelParser(BaseParser):
    """Robust Excel parser with sheet support."""

    async def parse(self, sheet_name: str | None = None) -> dict[str, Any]:
        """Parse Excel file with optional sheet selection."""
        try:
            raw_bytes = await self.read_all_bytes()
            file_obj = io.BytesIO(raw_bytes)

            excel_file = pd.ExcelFile(file_obj)
            all_sheets = excel_file.sheet_names

            if sheet_name and sheet_name not in all_sheets:
                raise FileParseError(
                    message=f"Sheet '{sheet_name}' not found",
                    details={"available_sheets": all_sheets},
                )

            sheets_to_process = [sheet_name] if sheet_name else all_sheets
            result_data: list[dict[str, Any]] = []
            sheet_metadata: list[dict[str, Any]] = []

            for sheet in sheets_to_process:
                try:
                    df = pd.read_excel(
                        file_obj,
                        sheet_name=sheet,
                        engine="openpyxl"
                        if self.filename.endswith(".xlsx")
                        else "xlrd",
                    )

                    df = self._handle_merged_cells(df)
                    df = self._convert_dates(df)
                    df = self._convert_bools(df)

                    sheet_data = df.to_dicts()
                    result_data.extend(sheet_data)

                    sheet_metadata.append(
                        {
                            "sheet_name": sheet,
                            "row_count": len(df),
                            "column_count": len(df.columns),
                        }
                    )

                except Exception as sheet_error:
                    logger.warning(f"Error reading sheet '{sheet}': {sheet_error}")
                    continue

            if not result_data:
                raise FileParseError(
                    message="No valid data found in Excel file",
                    details={"sheets": all_sheets},
                )

            return {
                "data": result_data,
                "sheets": sheet_metadata,
                "sheet_count": len(all_sheets),
                "skipped_rows": 0,
                "row_count": len(result_data),
            }

        except FileParseError:
            raise
        except Exception as e:
            logger.error(f"Excel parsing error: {e}")
            raise FileParseError(
                message=f"Failed to parse Excel: {str(e)}",
                details={"filename": self.filename},
            )

    def _handle_merged_cells(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle merged cells by forward-filling values."""
        try:
            df = df.ffill()
        except Exception:
            pass
        return df

    def _convert_dates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert date columns to ISO format."""
        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    dates = pd.to_datetime(df[col], errors="coerce")
                    if dates.notna().mean() > 0.8:
                        df[col] = dates.dt.isoformat().where(dates.notna(), df[col])
                except Exception:
                    pass
        return df

    def _convert_bools(self, df: pd.DataFrame) -> pd.DataFrame:
        """Convert boolean-like columns."""
        bool_patterns = {
            "true": True,
            "false": False,
            "yes": True,
            "no": False,
            "1": True,
            "0": False,
            "y": True,
            "n": False,
        }

        for col in df.columns:
            if df[col].dtype == "object":
                try:
                    lowered = df[col].astype(str).str.lower().str.strip()
                    if lowered.isin(bool_patterns.keys()).mean() > 0.9:
                        df[col] = lowered.map(bool_patterns).where(
                            lowered.isin(bool_patterns.keys()), df[col]
                        )
                except Exception:
                    pass
        return df


class JSONParser(BaseParser):
    """Robust JSON parser with flattening support."""

    async def parse(self, flatten: bool = True) -> dict[str, Any]:
        """Parse JSON file with optional flattening."""
        try:
            raw_bytes = await self.read_all_bytes()
            content = EncodingDetector.safe_decode(raw_bytes)

            try:
                data = json.loads(content)
            except json.JSONDecodeError as e:
                raise FileParseError(
                    message=f"Invalid JSON format: {str(e)}",
                    details={"position": e.pos},
                )

            normalized = self._normalize_json(data, flatten=flatten)

            if not normalized:
                raise FileParseError(message="No valid data found in JSON")

            return {
                "data": normalized,
                "skipped_rows": 0,
                "row_count": len(normalized),
            }

        except FileParseError:
            raise
        except Exception as e:
            logger.error(f"JSON parsing error: {e}")
            raise FileParseError(
                message=f"Failed to parse JSON: {str(e)}",
                details={"filename": self.filename},
            )

    def _normalize_json(self, data: Any, flatten: bool = True) -> list[dict[str, Any]]:
        """Normalize JSON data to list of dictionaries."""
        if isinstance(data, list):
            if flatten:
                return [
                    self._flatten_dict(item)
                    if isinstance(item, dict)
                    else {"value": item}
                    for item in data
                ]
            return [
                item if isinstance(item, dict) else {"value": item} for item in data
            ]

        if isinstance(data, dict):
            if "data" in data and isinstance(data["data"], list):
                return self._normalize_json(data["data"], flatten)

            if "records" in data and isinstance(data["records"], list):
                return self._normalize_json(data["records"], flatten)

            if "items" in data and isinstance(data["items"], list):
                return self._normalize_json(data["items"], flatten)

            if flatten:
                return [self._flatten_dict(data)]
            return [data]

        return [{"value": data}]

    def _flatten_dict(self, data: dict, parent_key: str = "", sep: str = ".") -> dict:
        """Flatten nested dictionary using dot notation."""
        items: dict[str, Any] = {}

        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key

            if isinstance(value, dict):
                items.update(self._flatten_dict(value, new_key, sep))
            elif isinstance(value, list):
                if value and isinstance(value[0], dict):
                    for i, item in enumerate(value):
                        items.update(self._flatten_dict(item, f"{new_key}[{i}]", sep))
                else:
                    items[new_key] = json.dumps(value)
            else:
                items[new_key] = value

        return items


class ParquetParser(BaseParser):
    """Robust Parquet parser using PyArrow/Pandas."""

    async def parse(self) -> dict[str, Any]:
        """Parse Parquet file."""
        try:
            raw_bytes = await self.read_all_bytes()
            file_obj = io.BytesIO(raw_bytes)

            try:
                import pyarrow.parquet as pq

                parquet_file = pq.ParquetFile(file_obj)
                metadata = {
                    "num_rows": parquet_file.metadata.num_rows,
                    "num_columns": parquet_file.metadata.num_columns,
                    "num_streams": parquet_file.metadata.num_row_groups,
                    "schema": str(parquet_file.schema),
                }
                df = parquet_file.read().to_pandas()
            except ImportError:
                df = pd.read_parquet(file_obj)
                metadata = {
                    "num_rows": len(df),
                    "num_columns": len(df.columns),
                }

            df = self._handle_nulls(df)

            data = df.to_dicts()

            return {
                "data": data,
                "metadata": metadata,
                "skipped_rows": 0,
                "row_count": len(data),
            }

        except Exception as e:
            logger.error(f"Parquet parsing error: {e}")
            raise FileParseError(
                message=f"Failed to parse Parquet: {str(e)}",
                details={"filename": self.filename},
            )

    def _handle_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Handle null values in DataFrame."""
        null_patterns = ["", "NULL", "N/A", "NA", "null", "none", "None", "NaN"]

        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].replace(null_patterns, None)

        return df


class FileParserFactory:
    """Factory for creating appropriate parser."""

    PARSERS = {
        "csv": CSVParser,
        "xlsx": ExcelParser,
        "xls": ExcelParser,
        "json": JSONParser,
        "parquet": ParquetParser,
    }

    @classmethod
    def get_parser(cls, file: UploadFile) -> BaseParser:
        """Get appropriate parser for file type."""
        filename = file.filename or ""
        ext = Path(filename).suffix.lower().lstrip(".")

        if ext not in cls.PARSERS:
            raise FileParseError(
                message=f"Unsupported file type: {ext}",
                details={
                    "supported_types": list(cls.PARSERS.keys()),
                    "filename": filename,
                },
            )

        return cls.PARSERS[ext](file, filename)

    @classmethod
    def is_supported(cls, filename: str) -> bool:
        """Check if file type is supported."""
        ext = Path(filename).suffix.lower().lstrip(".")
        return ext in cls.PARSERS
