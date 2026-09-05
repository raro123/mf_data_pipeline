#!/usr/bin/env python3
"""Transform immutable AMFI market-cap workbooks to raw Parquet."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from decimal import Decimal
from hashlib import sha256
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any

import openpyxl
import pyarrow as pa
import pyarrow.parquet as pq
import xlrd

from config.settings import R2
from scripts.fetch_amfi_mcap_sources import (
    SOURCE_AREA,
    SOURCE_OBJECT_PATTERN,
    _is_canonical_manifest_key,
    _manifest_identity,
    manifest_key_for_source,
)
from utils.logging_setup import (
    get_transform_amfi_mcap_sources_logger,
    log_script_end,
    log_script_start,
)

logger = get_transform_amfi_mcap_sources_logger(__name__)

RAW_AREA = "market_cap/raw"
RAW_SCHEMA_VERSION = "1"
MANIFEST_FIELDS = frozenset(
    {
        "manifest_schema_version",
        "period_end",
        "source_year",
        "source_period_label",
        "source_page_url",
        "source_url",
        "source_file_name",
        "source_file_extension",
        "source_fetched_at",
        "source_content_sha256",
        "source_content_length_bytes",
        "source_content_type",
        "r2_source_object_key",
    }
)
BUSINESS_COLUMNS = (
    "serial_number",
    "company_name",
    "isin",
    "bse_symbol",
    "bse_six_month_average_market_cap_crore",
    "nse_symbol",
    "nse_six_month_average_market_cap_crore",
    "msei_symbol",
    "msei_six_month_average_market_cap_crore",
    "all_exchanges_average_market_cap_crore",
    "market_cap_category",
)
PROVENANCE_COLUMNS = (
    "period_end",
    "source_year",
    "source_period_label",
    "source_sheet_name",
    "source_row_number",
    "source_manifest_schema_version",
    "raw_schema_version",
    "source_page_url",
    "source_url",
    "source_workbook_name",
    "source_workbook_extension",
    "source_fetched_at",
    "source_content_sha256",
    "source_content_length_bytes",
    "source_content_type",
    "source_object_key",
    "source_manifest_object_key",
)
RAW_ARROW_SCHEMA = pa.schema(
    [
        *((name, pa.string()) for name in BUSINESS_COLUMNS),
        ("period_end", pa.date32()),
        ("source_year", pa.int32()),
        ("source_period_label", pa.string()),
        ("source_sheet_name", pa.string()),
        ("source_row_number", pa.int64()),
        ("source_manifest_schema_version", pa.string()),
        ("raw_schema_version", pa.string()),
        ("source_page_url", pa.string()),
        ("source_url", pa.string()),
        ("source_workbook_name", pa.string()),
        ("source_workbook_extension", pa.string()),
        ("source_fetched_at", pa.timestamp("us", tz="UTC")),
        ("source_content_sha256", pa.string()),
        ("source_content_length_bytes", pa.int64()),
        ("source_content_type", pa.string()),
        ("source_object_key", pa.string()),
        ("source_manifest_object_key", pa.string()),
    ]
)

_RAW_OBJECT_PATTERN = re.compile(r"^market_cap_[0-9]{8}_[0-9]{8}T[0-9]{6}Z\.parquet$")


@dataclass(frozen=True)
class SourcePair:
    """One verified Phase 0 manifest and workbook pair."""

    manifest_key: str
    source_key: str
    manifest: dict[str, Any]
    workbook_content: bytes


@dataclass(frozen=True)
class ParsedRows:
    """Retained workbook rows and their source worksheet."""

    sheet_name: str
    rows: tuple[tuple[int, tuple[str | None, ...]], ...]


@dataclass(frozen=True)
class TransformSummary:
    """Counts produced by one complete transformation run."""

    discovered: int
    uploaded: int
    skipped: int
    rows: int
    errors: tuple[str, ...] = ()


# ------------------------------------------------------------------
# Phase 0 pair discovery and verification
# ------------------------------------------------------------------


def source_prefix(r2: R2) -> str:
    """Return the Phase 0 source prefix."""
    return f"{r2.asset_class}/{SOURCE_AREA}/"


def raw_prefix(r2: R2) -> str:
    """Return the Phase 1 raw Parquet prefix."""
    return f"{r2.asset_class}/{RAW_AREA}/"


def _list_keys(s3_client: Any, r2: R2, prefix: str) -> set[str]:
    """List every object key below one R2 prefix."""
    keys: set[str] = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=r2.bucket_name, Prefix=prefix):
        keys.update(item["Key"] for item in page.get("Contents", []))
    return keys


def _decode_manifest(body: bytes, manifest_key: str) -> dict[str, Any]:
    """Decode and structurally validate one Phase 0 manifest."""
    try:
        manifest = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid market-cap manifest JSON: {manifest_key}") from exc
    if not isinstance(manifest, dict):
        raise TypeError(f"Market-cap manifest is not an object: {manifest_key}")
    if set(manifest) != MANIFEST_FIELDS:
        missing = sorted(MANIFEST_FIELDS - set(manifest))
        unknown = sorted(set(manifest) - MANIFEST_FIELDS)
        raise ValueError(
            f"Market-cap manifest schema mismatch: {manifest_key}; "
            f"missing={missing}, unknown={unknown}"
        )

    _manifest_identity(manifest)
    source_key = manifest["r2_source_object_key"]
    if manifest_key_for_source(source_key) != manifest_key:
        raise ValueError(f"Market-cap manifest/object key mismatch: {manifest_key}")

    expected_types = {
        "source_year": int,
        "source_period_label": str,
        "source_page_url": str,
        "source_url": str,
        "source_file_name": str,
        "source_file_extension": str,
        "source_content_length_bytes": int,
        "source_content_type": str,
    }
    for field, expected_type in expected_types.items():
        value = manifest[field]
        if type(value) is not expected_type:
            raise ValueError(f"Market-cap manifest field {field!r} has invalid type")

    match = SOURCE_OBJECT_PATTERN.fullmatch(PurePosixPath(source_key).name)
    if match is None:
        raise ValueError(f"Invalid market-cap source object key: {source_key}")
    fetched_at = _manifest_datetime(manifest)
    if match.group("fetched") != fetched_at.strftime("%Y%m%dT%H%M%SZ"):
        raise ValueError("Market-cap manifest timestamp does not match its object key")
    period_end = date.fromisoformat(manifest["period_end"])
    if (period_end.month, period_end.day) not in {(6, 30), (12, 31)}:
        raise ValueError("Market-cap manifest period_end is not half-year end")
    if manifest["source_year"] != period_end.year:
        raise ValueError("Market-cap manifest source year does not match period")
    expected_label = "Jan - June" if period_end.month == 6 else "Jul - Dec"
    if manifest["source_period_label"] != expected_label:
        raise ValueError("Market-cap manifest period label does not match period")
    if (
        not manifest["source_file_name"]
        .lower()
        .endswith(f".{manifest['source_file_extension']}")
    ):
        raise ValueError(
            "Market-cap manifest workbook name does not match its extension"
        )
    for field in ("source_page_url", "source_url", "source_content_type"):
        if not manifest[field]:
            raise ValueError(f"Market-cap manifest field {field!r} is empty")
    if manifest["source_content_length_bytes"] < 1:
        raise ValueError("Market-cap manifest content length must be positive")
    return manifest


def _manifest_datetime(manifest: dict[str, Any]) -> datetime:
    """Parse a manifest timestamp and normalize it to UTC."""
    value = manifest.get("source_fetched_at")
    if not isinstance(value, str):
        raise TypeError("Market-cap manifest has no source_fetched_at")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError("Market-cap manifest has invalid source_fetched_at") from exc
    if parsed.tzinfo is None:
        raise ValueError("Market-cap manifest timestamp is not timezone-aware")
    return parsed.astimezone(timezone.utc)


def _read_verified_workbook(
    s3_client: Any,
    r2: R2,
    manifest: dict[str, Any],
) -> bytes:
    """Read a workbook only when its length and SHA-256 match the manifest."""
    source_key = manifest["r2_source_object_key"]
    response = s3_client.get_object(Bucket=r2.bucket_name, Key=source_key)
    content = bytes(response["Body"].read())
    expected_length = manifest["source_content_length_bytes"]
    if len(content) != expected_length:
        raise ValueError(
            f"Workbook length mismatch for {source_key}: "
            f"expected {expected_length}, got {len(content)}"
        )
    actual_hash = sha256(content).hexdigest()
    if actual_hash != manifest["source_content_sha256"]:
        raise ValueError(f"Workbook SHA-256 mismatch for {source_key}")
    return content


def load_source_pairs(
    s3_client: Any,
    r2: R2,
) -> tuple[list[SourcePair], list[str]]:
    """Load independently verified canonical Phase 0 source pairs."""
    object_keys = _list_keys(s3_client, r2, source_prefix(r2))
    manifest_keys = sorted(
        key for key in object_keys if _is_canonical_manifest_key(key)
    )
    pairs: list[SourcePair] = []
    errors: list[str] = []
    for manifest_key in manifest_keys:
        try:
            response = s3_client.get_object(
                Bucket=r2.bucket_name,
                Key=manifest_key,
            )
            manifest = _decode_manifest(response["Body"].read(), manifest_key)
            source_key = manifest["r2_source_object_key"]
            if source_key not in object_keys:
                raise ValueError(
                    f"Manifest references a missing workbook: {source_key}"
                )
            content = _read_verified_workbook(s3_client, r2, manifest)
            pairs.append(
                SourcePair(
                    manifest_key=manifest_key,
                    source_key=source_key,
                    manifest=manifest,
                    workbook_content=content,
                )
            )
        # Each manifest is an isolation boundary; report any source-specific
        # parser, storage, or schema failure after processing the others.
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Failed source manifest {manifest_key}: {exc}")
    return pairs, errors


# ------------------------------------------------------------------
# Workbook adapters and structural mapping
# ------------------------------------------------------------------


def _header_text(value: Any) -> str:
    """Normalize a header label for structural matching only."""
    if value is None:
        return ""
    text = str(value).casefold().replace("&", " and ")
    text = text.replace("capitalisation", "capitalization")
    text = text.replace("categorisation", "categorization")
    return re.sub(r"[^a-z0-9]+", "", text)


def _is_market_cap_phrase(value: str) -> bool:
    """Return whether a normalized label describes average market cap."""
    has_period = "6month" in value or "sixmonth" in value
    has_average = "average" in value or "avg" in value
    has_market_cap = "marketcap" in value
    return has_period and has_average and has_market_cap


def _matches_header(position: int, value: Any) -> bool:
    """Match one header cell to its fixed business-column position."""
    text = _header_text(value)
    if position == 0:
        return text in {"srno", "sno", "serialno", "serialnumber"}
    if position == 1:
        return text in {"companyname", "nameofcompany"}
    if position == 2:
        return text in {"isin", "isincode"}
    if position == 3:
        return text in {"bsesymbol", "symbolbse"}
    if position == 4:
        return text.startswith("bse") and _is_market_cap_phrase(text)
    if position == 5:
        return text in {"nsesymbol", "symbolnse"}
    if position == 6:
        return text.startswith("nse") and _is_market_cap_phrase(text)
    if position == 7:
        return text in {
            "msesymbol",
            "mseisymbol",
            "symbolmse",
            "symbolmsei",
        }
    if position == 8:
        return text.startswith(("mse", "msei")) and _is_market_cap_phrase(text)
    if position == 9:
        has_average = "average" in text or "avg" in text
        return (
            has_average
            and "exchange" in text
            and ("marketcap" in text or "allexchanges" in text)
        )
    if position == 10:
        return ("category" in text or "categorization" in text) and (
            "marketcap" in text or "sebi" in text
        )
    return False


def _header_matches(values: Sequence[Any]) -> bool:
    """Return whether a row is exactly the supported 11-column header."""
    retained = list(values)
    while retained and retained[-1] is None:
        retained.pop()
    return len(retained) == len(BUSINESS_COLUMNS) and all(
        _matches_header(position, value) for position, value in enumerate(retained)
    )


def _to_raw_string(value: Any) -> str | None:
    """Convert a cached cell value deterministically without cleaning strings."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    if isinstance(value, (date, time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "nan"
        if math.isinf(value):
            return "inf" if value > 0 else "-inf"
        if value.is_integer():
            return str(int(value))
        return repr(value)
    return str(value)


def _select_header(
    sheets: Iterable[tuple[str, Sequence[Sequence[Any]]]],
) -> tuple[str, int]:
    """Locate exactly one worksheet and row containing the known header."""
    matches: list[tuple[str, int]] = []
    for sheet_name, rows in sheets:
        for row_number, values in enumerate(rows, start=1):
            if _header_matches(values):
                matches.append((sheet_name, row_number))
    sheet_names = {sheet_name for sheet_name, _ in matches}
    if len(matches) != 1 or len(sheet_names) != 1:
        raise ValueError(
            "Expected exactly one worksheet with one recognized 11-column "
            f"header; found {matches}"
        )
    return matches[0]


def _parse_xlsx(content: bytes) -> ParsedRows:
    """Read XLSX cached values and reject missing retained formula caches."""
    formulas = openpyxl.load_workbook(
        BytesIO(content),
        data_only=False,
        read_only=False,
    )
    cached = openpyxl.load_workbook(
        BytesIO(content),
        data_only=True,
        read_only=False,
    )
    try:
        sheets = [
            (
                worksheet.title,
                [tuple(cell.value for cell in row) for row in worksheet.iter_rows()],
            )
            for worksheet in cached.worksheets
        ]
        sheet_name, header_row = _select_header(sheets)
        cached_sheet = cached[sheet_name]
        formula_sheet = formulas[sheet_name]
        if any(
            formula_sheet.cell(row, column).value is not None
            for row in range(1, formula_sheet.max_row + 1)
            for column in range(
                len(BUSINESS_COLUMNS) + 1,
                formula_sheet.max_column + 1,
            )
        ):
            raise ValueError(f"Worksheet {sheet_name!r} has nonblank trailing columns")
        rows: list[tuple[int, tuple[str | None, ...]]] = []
        for row_number in range(header_row + 1, cached_sheet.max_row + 1):
            cached_values = tuple(
                cached_sheet.cell(row_number, column).value
                for column in range(1, len(BUSINESS_COLUMNS) + 1)
            )
            formula_values = tuple(
                formula_sheet.cell(row_number, column).value
                for column in range(1, len(BUSINESS_COLUMNS) + 1)
            )
            if all(value is None for value in formula_values):
                continue
            missing_formula_columns = [
                column
                for column in range(1, len(BUSINESS_COLUMNS) + 1)
                if formula_sheet.cell(row_number, column).data_type == "f"
                and cached_values[column - 1] is None
            ]
            if missing_formula_columns:
                raise ValueError(
                    f"XLSX formula has no cached value in {sheet_name!r} "
                    f"row {row_number}, column(s) {missing_formula_columns}"
                )
            rows.append(
                (
                    row_number,
                    tuple(_to_raw_string(value) for value in cached_values),
                )
            )
        if not rows:
            raise ValueError("Market-cap workbook contains no retained data rows")
        return ParsedRows(sheet_name=sheet_name, rows=tuple(rows))
    finally:
        formulas.close()
        cached.close()


def _xls_cell_value(book: Any, cell: Any) -> Any:
    """Return one cached XLS cell value with deterministic special types."""
    if cell.ctype in (xlrd.XL_CELL_EMPTY, xlrd.XL_CELL_BLANK):
        return None
    if cell.ctype == xlrd.XL_CELL_BOOLEAN:
        return bool(cell.value)
    if cell.ctype == xlrd.XL_CELL_DATE:
        return xlrd.xldate.xldate_as_datetime(cell.value, book.datemode)
    if cell.ctype == xlrd.XL_CELL_ERROR:
        return xlrd.error_text_from_code.get(cell.value, str(cell.value))
    return cell.value


def _parse_xls(content: bytes) -> ParsedRows:
    """Read cached values from a legacy XLS workbook using xlrd."""
    book = xlrd.open_workbook(file_contents=content, on_demand=True)
    try:
        sheet_rows: dict[str, list[tuple[Any, ...]]] = {}
        for sheet in book.sheets():
            sheet_rows[sheet.name] = [
                tuple(
                    _xls_cell_value(book, sheet.cell(row, column))
                    for column in range(sheet.ncols)
                )
                for row in range(sheet.nrows)
            ]
        sheet_name, header_row = _select_header(sheet_rows.items())
        sheet = book.sheet_by_name(sheet_name)
        if any(
            _xls_cell_value(book, sheet.cell(row, column)) is not None
            for row in range(sheet.nrows)
            for column in range(len(BUSINESS_COLUMNS), sheet.ncols)
        ):
            raise ValueError(f"Worksheet {sheet_name!r} has nonblank trailing columns")
        rows: list[tuple[int, tuple[str | None, ...]]] = []
        for zero_based_row in range(header_row, sheet.nrows):
            values = tuple(
                _xls_cell_value(book, sheet.cell(zero_based_row, column))
                if column < sheet.ncols
                else None
                for column in range(len(BUSINESS_COLUMNS))
            )
            if all(value is None for value in values):
                continue
            rows.append(
                (
                    zero_based_row + 1,
                    tuple(_to_raw_string(value) for value in values),
                )
            )
        if not rows:
            raise ValueError("Market-cap workbook contains no retained data rows")
        return ParsedRows(sheet_name=sheet_name, rows=tuple(rows))
    finally:
        book.release_resources()


def parse_workbook(pair: SourcePair) -> ParsedRows:
    """Parse one verified workbook through its declared Excel adapter."""
    extension = pair.manifest["source_file_extension"]
    if extension == "xlsx":
        return _parse_xlsx(pair.workbook_content)
    if extension == "xls":
        return _parse_xls(pair.workbook_content)
    raise ValueError(f"Unsupported market-cap workbook extension: {extension}")


# ------------------------------------------------------------------
# Raw Parquet construction and immutable publication
# ------------------------------------------------------------------


def raw_object_key(r2: R2, manifest: dict[str, Any]) -> str:
    """Return the deterministic raw object key for one source revision."""
    period = date.fromisoformat(manifest["period_end"]).strftime("%Y%m%d")
    fetched = _manifest_datetime(manifest).strftime("%Y%m%dT%H%M%SZ")
    return r2.get_object_key(
        RAW_AREA,
        f"market_cap_{period}_{fetched}",
    )


def build_raw_table(pair: SourcePair, parsed: ParsedRows) -> pa.Table:
    """Build an exact-schema Arrow table from retained values and provenance."""
    manifest = pair.manifest
    fetched_at = _manifest_datetime(manifest)
    period_end = date.fromisoformat(manifest["period_end"])
    rows: list[dict[str, Any]] = []
    provenance = {
        "period_end": period_end,
        "source_year": manifest["source_year"],
        "source_period_label": manifest["source_period_label"],
        "source_sheet_name": parsed.sheet_name,
        "source_manifest_schema_version": manifest["manifest_schema_version"],
        "raw_schema_version": RAW_SCHEMA_VERSION,
        "source_page_url": manifest["source_page_url"],
        "source_url": manifest["source_url"],
        "source_workbook_name": manifest["source_file_name"],
        "source_workbook_extension": manifest["source_file_extension"],
        "source_fetched_at": fetched_at,
        "source_content_sha256": manifest["source_content_sha256"],
        "source_content_length_bytes": manifest["source_content_length_bytes"],
        "source_content_type": manifest["source_content_type"],
        "source_object_key": pair.source_key,
        "source_manifest_object_key": pair.manifest_key,
    }
    for source_row_number, values in parsed.rows:
        row = dict(zip(BUSINESS_COLUMNS, values, strict=True))
        row.update(provenance)
        row["source_row_number"] = source_row_number
        rows.append(row)
    return pa.Table.from_pylist(rows, schema=RAW_ARROW_SCHEMA)


def serialize_parquet(table: pa.Table) -> bytes:
    """Serialize a raw table as Zstandard-compressed Parquet bytes."""
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


def _expected_metadata(
    pair: SourcePair,
    row_count: int,
    parquet_hash: str,
) -> dict[str, str]:
    """Return the exact immutable raw-object metadata contract."""
    return {
        "source-object-key": pair.source_key,
        "source-content-sha256": pair.manifest["source_content_sha256"],
        "source-manifest-object-key": pair.manifest_key,
        "raw-schema-version": RAW_SCHEMA_VERSION,
        "row-count": str(row_count),
        "parquet-sha256": parquet_hash,
    }


def _verify_raw_object(
    s3_client: Any,
    r2: R2,
    object_key: str,
    pair: SourcePair,
    expected_row_count: int,
) -> None:
    """Verify metadata, bytes, exact schema, and row count for one raw object."""
    head = s3_client.head_object(Bucket=r2.bucket_name, Key=object_key)
    metadata = {
        str(key).lower(): str(value) for key, value in head.get("Metadata", {}).items()
    }
    for key, expected in {
        "source-object-key": pair.source_key,
        "source-content-sha256": pair.manifest["source_content_sha256"],
        "source-manifest-object-key": pair.manifest_key,
        "raw-schema-version": RAW_SCHEMA_VERSION,
        "row-count": str(expected_row_count),
    }.items():
        if metadata.get(key) != expected:
            raise ValueError(f"Raw metadata mismatch for {object_key}: {key}")
    stored_hash = metadata.get("parquet-sha256")
    if not stored_hash or not re.fullmatch(r"[0-9a-f]{64}", stored_hash):
        raise ValueError(f"Raw Parquet hash metadata is invalid: {object_key}")

    response = s3_client.get_object(Bucket=r2.bucket_name, Key=object_key)
    body = bytes(response["Body"].read())
    if sha256(body).hexdigest() != stored_hash:
        raise ValueError(f"Raw Parquet SHA-256 mismatch: {object_key}")
    table = pq.read_table(pa.BufferReader(body))
    if table.schema != RAW_ARROW_SCHEMA:
        raise ValueError(
            f"Raw Parquet schema mismatch for {object_key}: {table.schema}"
        )
    if table.num_rows != expected_row_count:
        raise ValueError(
            f"Raw Parquet row-count mismatch for {object_key}: "
            f"expected {expected_row_count}, got {table.num_rows}"
        )


def publish_pair(
    s3_client: Any,
    r2: R2,
    raw_keys: set[str],
    pair: SourcePair,
) -> tuple[bool, int]:
    """Publish or verify one immutable raw Parquet source revision."""
    parsed = parse_workbook(pair)
    table = build_raw_table(pair, parsed)
    object_key = raw_object_key(r2, pair.manifest)
    if not _RAW_OBJECT_PATTERN.fullmatch(PurePosixPath(object_key).name):
        raise ValueError(f"Invalid generated raw object key: {object_key}")

    if object_key in raw_keys:
        _verify_raw_object(
            s3_client,
            r2,
            object_key,
            pair,
            table.num_rows,
        )
        return False, table.num_rows

    parquet_bytes = serialize_parquet(table)
    parquet_hash = sha256(parquet_bytes).hexdigest()
    s3_client.put_object(
        Bucket=r2.bucket_name,
        Key=object_key,
        Body=parquet_bytes,
        ContentType="application/vnd.apache.parquet",
        StorageClass="STANDARD",
        IfNoneMatch="*",
        Metadata=_expected_metadata(pair, table.num_rows, parquet_hash),
    )
    raw_keys.add(object_key)
    _verify_raw_object(
        s3_client,
        r2,
        object_key,
        pair,
        table.num_rows,
    )
    return True, table.num_rows


def transform_sources(s3_client: Any, r2: R2) -> TransformSummary:
    """Transform sources independently and retain all completed uploads."""
    pairs, errors = load_source_pairs(s3_client, r2)
    raw_keys = _list_keys(s3_client, r2, raw_prefix(r2))
    uploaded = 0
    skipped = 0
    rows = 0
    for pair in pairs:
        try:
            was_uploaded, row_count = publish_pair(
                s3_client,
                r2,
                raw_keys,
                pair,
            )
            uploaded += int(was_uploaded)
            skipped += int(not was_uploaded)
            rows += row_count
        # A failed revision must not roll back successful immutable uploads.
        except Exception as exc:  # noqa: BLE001
            errors.append(f"Failed source {pair.source_key}: {exc}")
    return TransformSummary(
        discovered=len(pairs),
        uploaded=uploaded,
        skipped=skipped,
        rows=rows,
        errors=tuple(errors),
    )


def main() -> int:
    """Transform every complete archived source revision to raw Parquet."""
    log_script_start(
        logger,
        "AMFI Market-Cap Raw Transformer",
        "Creating immutable values-only raw Parquet snapshots in R2",
    )
    s3_client = None
    succeeded = False
    try:
        r2 = R2()
        s3_client = r2.setup_s3_client()
        summary = transform_sources(s3_client, r2)
        logger.info(
            "Transform summary: discovered=%s uploaded=%s skipped=%s rows=%s",
            summary.discovered,
            summary.uploaded,
            summary.skipped,
            summary.rows,
        )
        if not summary.discovered and not summary.errors:
            raise RuntimeError("No complete AMFI market-cap source pairs found")
        if summary.errors:
            for error in summary.errors:
                logger.error("Unresolved transformation error: %s", error)
        else:
            succeeded = True
    except Exception as exc:  # noqa: BLE001 - command boundary logs all failures
        logger.error("AMFI market-cap raw transformation failed: %s", exc)
    finally:
        if s3_client is not None:
            s3_client.close()

    log_script_end(logger, "AMFI Market-Cap Raw Transformer", succeeded)
    return 0 if succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
