#!/usr/bin/env python3
"""Fetch and publish AMFI monthly Total Expense Ratio snapshots.

The AMFI workbook changed its expense-component disclosure from April 2026.
This module keeps both source regimes in one nullable, source-faithful
Parquet schema and deliberately defers TER modelling to downstream systems.
"""

import argparse
from datetime import date, datetime, timezone
from hashlib import sha256
from io import BytesIO
from pathlib import PurePosixPath
import re
import time
from typing import Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
import zipfile

import pandas as pd
import requests

from config.settings import API, R2
from utils.logging_setup import (
    get_fetch_ter_logger,
    log_script_end,
    log_script_start,
)
from utils.nav_helpers import save_to_parquet


logger = get_fetch_ter_logger(__name__)

MIN_TER_MONTH = date(2020, 4, 1)
SCHEMA_PRE_APRIL_2026 = "pre_april_2026"
SCHEMA_APRIL_2026_ONWARD = "april_2026_onward"
TER_PARQUET_COMPRESSION = "zstd"

TER_OBJECT_PATTERN = re.compile(
    r"^ter_(?P<month>\d{6})_snapshot_(?P<snapshot_date>\d{8})\.parquet$"
)
MONTH_PATTERN = re.compile(r"^(?P<year>\d{4})-(?P<month>\d{2})$")

COMMON_SOURCE_COLUMNS = frozenset({
    "nsdl_scheme_code",
    "ter_date",
    "scheme_name",
    "regular_total_ter",
    "direct_total_ter",
})

OLD_COMPONENT_COLUMNS = frozenset({
    "regular_base_ter",
    "regular_additional_expense_b",
    "regular_additional_expense_c",
    "regular_gst",
    "direct_base_ter",
    "direct_additional_expense_b",
    "direct_additional_expense_c",
    "direct_gst",
})

NEW_COMPONENT_COLUMNS = frozenset({
    "regular_base_expense_ratio",
    "regular_brokerage_cost",
    "regular_transaction_cost",
    "regular_statutory_levies",
    "direct_base_expense_ratio",
    "direct_brokerage_cost",
    "direct_transaction_cost",
    "direct_statutory_levies",
})

OPTIONAL_SOURCE_COLUMNS = (
    "amfi_scheme_code",
    "mutual_fund",
    "scheme_type",
    "scheme_category",
    "sub_category",
    "isin",
    "financial_year",
    "mf_id",
    "reported_month",
)

NUMERIC_TER_COLUMNS = frozenset(
    OLD_COMPONENT_COLUMNS | NEW_COMPONENT_COLUMNS | {
        "regular_total_ter",
        "direct_total_ter",
    }
)

TER_DATA_COLUMNS = (
    "nsdl_scheme_code",
    "amfi_scheme_code",
    "mutual_fund",
    "scheme_name",
    "scheme_type",
    "scheme_category",
    "sub_category",
    "isin",
    "financial_year",
    "mf_id",
    "reported_month",
    "ter_date",
    "regular_base_ter",
    "regular_additional_expense_b",
    "regular_additional_expense_c",
    "regular_gst",
    "regular_base_expense_ratio",
    "regular_brokerage_cost",
    "regular_transaction_cost",
    "regular_statutory_levies",
    "regular_total_ter",
    "direct_base_ter",
    "direct_additional_expense_b",
    "direct_additional_expense_c",
    "direct_gst",
    "direct_base_expense_ratio",
    "direct_brokerage_cost",
    "direct_transaction_cost",
    "direct_statutory_levies",
    "direct_total_ter",
)

TER_METADATA_COLUMNS = (
    "source_month",
    "source_schema_version",
    "source_fetched_at",
    "source_url",
    "source_content_sha256",
    "source_row_number",
)

TER_OUTPUT_COLUMNS = TER_DATA_COLUMNS + TER_METADATA_COLUMNS


def _header_key(value: object) -> str:
    """Normalize a workbook header for exact known-header matching."""
    if value is None or pd.isna(value):
        return ""
    text = str(value).replace("\xa0", " ").replace("\n", " ")
    text = re.sub(r"\s+", " ", text).strip()
    # AMFI/AMC tables sometimes append footnote numbers to percentage labels.
    text = re.sub(r"(?<=%)\s*[0-9¹²³⁴⁵⁶⁷⁸⁹]+$", "", text)
    return text.lower()


HEADER_DEFINITIONS = {
    "nsdl_scheme_code": (
        "NSDL Scheme Code",
        "NSDLSchemeCode",
        "NSDL_Scheme_Code",
        "Scheme Code (NSDL issued)",
    ),
    "amfi_scheme_code": ("AMFI Scheme Code", "Scheme Code"),
    "ter_date": ("Date", "TER Date", "TER_Date"),
    "scheme_name": ("Scheme Name", "Scheme_Name"),
    "mutual_fund": (
        "Mutual Fund",
        "MutualFund",
        "MF_Name",
        "MF Name",
        "AMC Name",
    ),
    "scheme_type": (
        "Scheme Type",
        "SchemeType",
        "SchemeType_Desc",
        "Fund Type",
    ),
    "scheme_category": (
        "Scheme Category",
        "SchemeCat",
        "SchemeCat_Desc",
        "Category",
    ),
    "sub_category": ("Sub Category", "Sub_Category"),
    "isin": ("ISIN", "ISIN Code"),
    "financial_year": ("Financial Year", "TER Year", "TER_Year"),
    "mf_id": ("MF ID", "MF_ID"),
    "reported_month": ("Month",),
    "regular_total_ter": (
        "Regular Plan - Total TER (%)",
        "Regular Plan - Total Expense Ratio (%)",
    ),
    "direct_total_ter": (
        "Direct Plan - Total TER (%)",
        "Direct Plan - Total Expense Ratio (%)",
    ),
    "regular_base_ter": ("Regular Plan - Base TER (%)",),
    "regular_additional_expense_b": (
        "Regular Plan - Additional expense as per Regulation 52(6A)(b) (%)",
    ),
    "regular_additional_expense_c": (
        "Regular Plan - Additional expense as per Regulation 52(6A)(c) (%)",
    ),
    "regular_gst": ("Regular Plan - GST (%)",),
    "direct_base_ter": ("Direct Plan - Base TER (%)",),
    "direct_additional_expense_b": (
        "Direct Plan - Additional expense as per Regulation 52(6A)(b) (%)",
    ),
    "direct_additional_expense_c": (
        "Direct Plan - Additional expense as per Regulation 52(6A)(c) (%)",
    ),
    "direct_gst": ("Direct Plan - GST (%)",),
    "regular_base_expense_ratio": (
        "Regular Plan - Base Expense Ratio (BER) (%)",
    ),
    "regular_brokerage_cost": (
        "Regular Plan - Brokerage Cost (%)",
        "Regular Plan - Brokerage cost (%)",
    ),
    "regular_transaction_cost": (
        "Regular Plan - Transaction Cost incurred for the purpose of execution "
        "of trade (%)",
    ),
    "regular_statutory_levies": (
        "Regular Plan - Statutory Levies (including GST) (%)",
    ),
    "direct_base_expense_ratio": (
        "Direct Plan - Base Expense Ratio (BER) (%)",
    ),
    "direct_brokerage_cost": (
        "Direct Plan - Brokerage Cost (%)",
        "Direct Plan - Brokerage cost (%)",
    ),
    "direct_transaction_cost": (
        "Direct Plan - Transaction Cost incurred for the purpose of execution "
        "of trade (%)",
    ),
    "direct_statutory_levies": (
        "Direct Plan - Statutory Levies (including GST) (%)",
    ),
}

HEADER_ALIASES: Dict[str, str] = {
    _header_key(alias): canonical
    for canonical, aliases in HEADER_DEFINITIONS.items()
    for alias in (canonical, *aliases)
}


def parse_month(value: object) -> date:
    """Parse and validate an inclusive TER month label.

    Args:
        value: Month in ``YYYY-MM`` format.

    Returns:
        First day of the requested month.

    Raises:
        ValueError: If the label is malformed or before April 2020.
    """
    if isinstance(value, datetime):
        parsed = value.date().replace(day=1)
    elif isinstance(value, date):
        parsed = value.replace(day=1)
    else:
        match = MONTH_PATTERN.fullmatch(str(value).strip())
        if not match:
            raise ValueError(f"Invalid TER month {value!r}; expected YYYY-MM")
        try:
            parsed = date(int(match.group("year")), int(match.group("month")), 1)
        except ValueError as exc:
            raise ValueError(
                f"Invalid TER month {value!r}; expected a real calendar month"
            ) from exc

    if parsed < MIN_TER_MONTH:
        raise ValueError("TER extraction supports months from 2020-04 onward")
    return parsed


def month_label(value: object) -> str:
    """Return a validated month as ``YYYY-MM``."""
    return parse_month(value).strftime("%Y-%m")


def iter_months(start_month: object, end_month: object) -> List[date]:
    """Return every validated month in an inclusive range."""
    start = parse_month(start_month)
    end = parse_month(end_month)
    if start > end:
        raise ValueError("start month must not be after end month")

    result = []
    current = start
    while current <= end:
        result.append(current)
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)
    return result


def resolve_months(
    args: argparse.Namespace,
    today: Optional[date] = None,
) -> List[date]:
    """Resolve CLI arguments into one or more requested TER months."""
    month = getattr(args, "month", None)
    scheduled = bool(getattr(args, "scheduled", False))
    start_month = getattr(args, "start_month", None)
    end_month = getattr(args, "end_month", None)

    if (start_month is None) != (end_month is None):
        raise ValueError("--start-month and --end-month must be supplied together")
    if (start_month is not None or end_month is not None) and (
        month is not None or scheduled
    ):
        raise ValueError(
            "Backfill arguments cannot be combined with --month or --scheduled"
        )
    if month is not None and scheduled:
        raise ValueError("--month cannot be combined with --scheduled")

    if start_month is not None:
        return iter_months(start_month, end_month)
    if month is not None:
        return [parse_month(month)]

    current_date = today or datetime.now(timezone.utc).date()
    if isinstance(current_date, datetime):
        current_date = current_date.date()
    current_month = current_date.replace(day=1)
    if scheduled and current_date.day <= 10:
        if current_month.month == 1:
            current_month = date(current_month.year - 1, 12, 1)
        else:
            current_month = date(current_month.year, current_month.month - 1, 1)
    return [parse_month(current_month)]


def _canonical_header_mapping(columns: Iterable[object]) -> Dict[object, str]:
    """Map known source headers to output names and reject unknown headers."""
    mapping: Dict[object, str] = {}
    seen_outputs = set()
    unknown = []
    for column in columns:
        if str(column).startswith("_"):
            continue
        output_name = HEADER_ALIASES.get(_header_key(column))
        if output_name is None:
            unknown.append(str(column))
            continue
        if output_name in seen_outputs:
            raise ValueError(f"Duplicate TER source column for {output_name!r}")
        mapping[column] = output_name
        seen_outputs.add(output_name)

    if unknown:
        raise ValueError(f"Unknown TER source columns: {unknown}")
    return mapping


def _detect_schema_from_columns(present: set[str]) -> str:
    """Return the source schema version for canonical column names."""
    missing_common = sorted(COMMON_SOURCE_COLUMNS - present)
    if missing_common:
        raise ValueError(f"TER source is missing required columns: {missing_common}")

    has_old = bool(present & OLD_COMPONENT_COLUMNS)
    has_new = bool(present & NEW_COMPONENT_COLUMNS)
    if OLD_COMPONENT_COLUMNS.issubset(present) and not has_new:
        return SCHEMA_PRE_APRIL_2026
    if NEW_COMPONENT_COLUMNS.issubset(present) and not has_old:
        return SCHEMA_APRIL_2026_ONWARD
    if not has_old and not has_new:
        raise ValueError("TER source is missing all regime-specific expense columns")
    raise ValueError("TER source has a missing or mixed disclosure schema")


def detect_schema_version(columns: Iterable[object]) -> str:
    """Identify a complete known schema and reject layout changes."""
    mapping = _canonical_header_mapping(columns)
    return _detect_schema_from_columns(set(mapping.values()))


def _cell_text(value: object) -> str:
    """Convert a header cell into a normalized display string."""
    if value is None or pd.isna(value):
        return ""
    return re.sub(r"\s+", " ", str(value).replace("\xa0", " ").strip())


def _candidate_frame(
    raw: pd.DataFrame,
    headers: Sequence[str],
    data_start: int,
) -> pd.DataFrame:
    """Build a canonical candidate frame for a discovered header row."""
    nonempty_columns = [index for index, name in enumerate(headers) if name]
    if not nonempty_columns:
        raise ValueError("TER workbook has no usable headers")
    candidate_headers = [headers[index] for index in nonempty_columns]
    mapping = _canonical_header_mapping(candidate_headers)
    schema_version = _detect_schema_from_columns(set(mapping.values()))
    frame = raw.iloc[data_start:, nonempty_columns].copy()
    frame.columns = candidate_headers

    # AMFI separates its disclaimer from data with one fully blank row.
    blank_rows = frame.index[frame.isna().all(axis=1)]
    if not blank_rows.empty:
        boundary = blank_rows[0]
        trailing_rows = frame.loc[frame.index > boundary]
        if trailing_rows.notna().sum(axis=1).gt(1).any():
            raise ValueError("TER workbook contains data rows after a blank row")
        frame = frame.loc[frame.index < boundary].copy()
    if frame.empty:
        raise ValueError("TER workbook contains no data rows")

    frame = frame.rename(columns=mapping)
    source_row_numbers = [int(index) + 1 for index in frame.index]
    frame = frame.reset_index(drop=True)
    frame["_source_row_number"] = source_row_numbers
    frame.attrs["source_schema_version"] = schema_version
    return frame


def validate_xlsx_content(content: bytes) -> None:
    """Reject HTML/error bodies and corrupt/non-XLSX ZIP files."""
    if not content or not zipfile.is_zipfile(BytesIO(content)):
        raise ValueError("AMFI response is not a valid XLSX file")
    try:
        with zipfile.ZipFile(BytesIO(content)) as workbook:
            if workbook.testzip() is not None:
                raise ValueError("AMFI XLSX archive is corrupt")
            names = set(workbook.namelist())
            required = {"[Content_Types].xml", "xl/workbook.xml"}
            if not required.issubset(names):
                raise ValueError("AMFI response is a ZIP but not an XLSX workbook")
    except zipfile.BadZipFile as exc:
        raise ValueError("AMFI response is not a valid XLSX file") from exc


def parse_ter_workbook(content: bytes) -> pd.DataFrame:
    """Read an AMFI XLSX workbook and identify its complete source schema."""
    validate_xlsx_content(content)
    try:
        raw = pd.read_excel(
            BytesIO(content),
            header=None,
            engine="openpyxl",
            dtype=object,
        )
    except Exception as exc:
        raise ValueError(f"Unable to read AMFI TER XLSX: {exc}") from exc

    if raw.empty:
        raise ValueError("AMFI TER workbook is empty")

    headers = [_cell_text(value) for value in raw.iloc[0].tolist()]
    try:
        return _candidate_frame(raw, headers, data_start=1)
    except ValueError as exc:
        raise ValueError(f"AMFI TER workbook is invalid: {exc}") from exc


def _as_utc_timestamp(value: object) -> pd.Timestamp:
    """Return a timezone-aware UTC timestamp."""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        return timestamp.tz_localize("UTC")
    return timestamp.tz_convert("UTC")


def _empty_series(length: int, dtype: str) -> pd.Series:
    """Create a nullable series that remains Parquet-compatible when empty."""
    return pd.Series([pd.NA] * length, dtype=dtype)


def _canonicalize_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, str]:
    """Map raw or parsed source columns into canonical names."""
    mapping = _canonical_header_mapping(df.columns)
    schema_version = _detect_schema_from_columns(set(mapping.values()))
    canonical = df.rename(columns=mapping).copy()
    return canonical, schema_version


def build_ter_dataframe(
    raw_df: pd.DataFrame,
    requested_month: object,
    *,
    source_fetched_at: Optional[object] = None,
    source_url: str = "",
    source_content_sha256: str = "",
) -> pd.DataFrame:
    """Validate and enrich one raw TER workbook DataFrame.

    Args:
        raw_df: Parsed source rows, either with AMFI headers or canonical names.
        requested_month: Requested month in ``YYYY-MM`` format.
        source_fetched_at: UTC timestamp for the HTTP fetch.
        source_url: Fully parameterized AMFI request URL.
        source_content_sha256: SHA-256 of the downloaded XLSX bytes.

    Returns:
        Validated source-faithful TER DataFrame ready for Parquet writing.

    Raises:
        ValueError: If the source is empty, incomplete, out of range, or has
            nonnumeric TER values.
    """
    requested = parse_month(requested_month)
    if raw_df.empty:
        raise ValueError(f"AMFI TER source for {month_label(requested)} is empty")

    canonical, schema_version = _canonicalize_dataframe(raw_df)

    row_count = len(canonical)
    for column in TER_DATA_COLUMNS:
        if column not in canonical:
            dtype = "Float64" if column in NUMERIC_TER_COLUMNS else "string"
            canonical[column] = _empty_series(row_count, dtype)

    # Preserve identifiers and attributes as strings. In particular, NSDL
    # codes may be null or contain separators/leading zeroes.
    text_columns = set(TER_DATA_COLUMNS) - NUMERIC_TER_COLUMNS - {"ter_date"}
    for column in text_columns:
        canonical[column] = canonical[column].astype("string")

    parsed_dates = pd.to_datetime(
        canonical["ter_date"],
        errors="coerce",
        dayfirst=True,
        utc=True,
    )
    if parsed_dates.isna().any():
        bad_rows = canonical.index[parsed_dates.isna()].tolist()
        raise ValueError(f"TER source contains unparseable dates at rows {bad_rows}")

    next_month = (
        date(requested.year + 1, 1, 1)
        if requested.month == 12
        else date(requested.year, requested.month + 1, 1)
    )
    lower = pd.Timestamp(requested, tz="UTC")
    upper = pd.Timestamp(next_month, tz="UTC")
    outside = (parsed_dates < lower) | (parsed_dates >= upper)
    if outside.any():
        bad_rows = canonical.index[outside].tolist()
        raise ValueError(
            f"TER dates fall outside requested month {month_label(requested)} "
            f"at rows {bad_rows}"
        )
    canonical["ter_date"] = parsed_dates.dt.normalize()

    for column in NUMERIC_TER_COLUMNS:
        original = canonical[column]
        converted = pd.to_numeric(original, errors="coerce")
        nonempty = original.notna() & original.astype("string").str.strip().ne("")
        invalid = nonempty & converted.isna()
        if invalid.any():
            bad_rows = canonical.index[invalid].tolist()
            raise ValueError(
                f"TER field {column!r} contains nonnumeric values at rows {bad_rows}"
            )
        canonical[column] = converted.astype("Float64")

    if "_source_row_number" in raw_df:
        source_rows = pd.to_numeric(raw_df["_source_row_number"], errors="raise")
    else:
        source_rows = pd.Series(range(2, row_count + 2), index=canonical.index)
    source_rows = pd.Series(source_rows, index=canonical.index).astype("Int64")

    fetched_at = _as_utc_timestamp(
        source_fetched_at
        if source_fetched_at is not None
        else datetime.now(timezone.utc)
    )
    metadata = {
        "source_month": month_label(requested),
        "source_schema_version": schema_version,
        "source_fetched_at": fetched_at,
        "source_url": source_url,
        "source_content_sha256": source_content_sha256,
        "source_row_number": source_rows,
    }
    result = canonical.reindex(columns=TER_DATA_COLUMNS).copy()
    for column, value in metadata.items():
        result[column] = value
    return result.reindex(columns=TER_OUTPUT_COLUMNS)


def build_ter_request(requested_month: object) -> Tuple[str, Mapping[str, str]]:
    """Build the all-fund, all-category/type Excel request."""
    requested = parse_month(requested_month)
    params = {
        "MF_ID": "All",
        "Month": requested.strftime("%m-%Y"),
        "strCat": "-1",
        "strType": "-1",
        "excel": "true",
    }
    prepared = requests.Request("GET", API.AMFI_TER_URL, params=params).prepare()
    return prepared.url, params


def fetch_ter_xlsx(
    requested_month: object,
    session: Optional[requests.Session] = None,
) -> Tuple[bytes, str]:
    """Fetch one validated XLSX response with configured retries."""
    request_url, params = build_ter_request(requested_month)
    request_get = session.get if session is not None else requests.get
    max_retries = max(1, API.MAX_RETRIES)
    last_error: Optional[Exception] = None

    for attempt in range(1, max_retries + 1):
        try:
            response = request_get(
                API.AMFI_TER_URL,
                params=params,
                timeout=API.AMFI_TER_TIMEOUT,
            )
            response.raise_for_status()
            content = bytes(response.content or b"")
            validate_xlsx_content(content)
            response_url = response.url or request_url
            return content, response_url
        except (
            requests.RequestException,
            ValueError,
            OSError,
        ) as exc:
            last_error = exc
            logger.warning(
                "TER fetch failed for %s (attempt %s/%s): %s",
                month_label(requested_month),
                attempt,
                max_retries,
                exc,
            )
            if attempt < max_retries:
                time.sleep(API.RETRY_DELAY)

    raise RuntimeError(
        f"Failed to fetch valid TER XLSX for {month_label(requested_month)} "
        f"after {max_retries} attempts"
    ) from last_error


def _utc_now() -> datetime:
    """Return the current UTC time."""
    return datetime.now(timezone.utc)


def ter_object_name(requested_month: object, snapshot_at: object) -> str:
    """Return the immutable canonical TER object name without its prefix."""
    requested = parse_month(requested_month)
    timestamp = _as_utc_timestamp(snapshot_at)
    return f"ter_{requested:%Y%m}_snapshot_{timestamp:%Y%m%d}"


def ter_object_path(r2: R2, requested_month: object, snapshot_at: object) -> str:
    """Return the canonical R2 path for a TER snapshot."""
    return r2.get_full_path("ter", ter_object_name(requested_month, snapshot_at))


def _canonical_ter_object(path: str, month_key: str) -> bool:
    """Check whether a path is a canonical TER snapshot for a month."""
    match = TER_OBJECT_PATTERN.fullmatch(PurePosixPath(path).name)
    return bool(match and match.group("month") == month_key)


def get_existing_ter_snapshots(
    connection,
    r2: R2,
    requested_month: object,
) -> List[str]:
    """List canonical TER snapshots for one month, ignoring unrelated names."""
    label = parse_month(requested_month).strftime("%Y%m")
    glob_path = r2.get_full_path("ter", f"ter_{label}_snapshot_*")
    object_paths = [
        row[0]
        for row in connection.execute(
            "SELECT file FROM glob(?)", [glob_path]
        ).fetchall()
    ]
    return [
        path for path in object_paths
        if _canonical_ter_object(path, label)
    ]


def _write_month(
    connection,
    r2: R2,
    requested_month: date,
    session: requests.Session,
    *,
    skip_existing_month: bool,
) -> Optional[str]:
    """Fetch, validate, and write one month; return its path or ``None``."""
    check_at = _utc_now()
    existing = get_existing_ter_snapshots(connection, r2, requested_month)
    expected_path = ter_object_path(r2, requested_month, check_at)
    if (skip_existing_month and existing) or expected_path in existing:
        logger.info(
            "Skipping existing TER snapshot for %s",
            month_label(requested_month),
        )
        return None

    content, source_url = fetch_ter_xlsx(requested_month, session)
    raw_df = parse_ter_workbook(content)
    fetched_at = _utc_now()
    snapshot = build_ter_dataframe(
        raw_df,
        requested_month,
        source_fetched_at=fetched_at,
        source_url=source_url,
        source_content_sha256=sha256(content).hexdigest(),
    )

    # Avoid a redundant write if another serialized workflow run completed
    # while this process was fetching and validating the workbook.
    target_path = ter_object_path(r2, requested_month, fetched_at)
    if target_path in get_existing_ter_snapshots(connection, r2, requested_month):
        logger.info("Skipping TER snapshot created concurrently: %s", target_path)
        return None

    save_to_parquet(
        connection,
        f"ter_snapshot_{parse_month(requested_month):%Y%m}",
        snapshot,
        target_path,
        compression=TER_PARQUET_COMPRESSION,
    )
    logger.info("Saved %s TER rows to %s", len(snapshot), target_path)
    return target_path


def run_months(
    months: Sequence[date],
    connection,
    r2: R2,
    session: requests.Session,
    *,
    backfill: bool = False,
) -> List[str]:
    """Process months in order, stopping on the first failed month."""
    written_paths = []
    for requested_month in months:
        path = _write_month(
            connection,
            r2,
            requested_month,
            session,
            skip_existing_month=backfill,
        )
        if path is not None:
            written_paths.append(path)
    return written_paths


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse TER extraction command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Fetch AMFI monthly TER workbooks and publish R2 Parquet"
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--scheduled", action="store_true")
    mode.add_argument("--month", help="One month in YYYY-MM format")
    parser.add_argument("--start-month", help="Inclusive backfill start YYYY-MM")
    parser.add_argument("--end-month", help="Inclusive backfill end YYYY-MM")
    return parser.parse_args(argv)


def main() -> int:
    """Fetch requested TER month(s) and publish validated immutable snapshots."""
    log_script_start(
        logger,
        "AMFI TER Extractor",
        "Fetching monthly AMFI TER XLSX data and saving validated R2 Parquet",
    )
    connection = None
    session = None
    try:
        args = parse_args()
        months = resolve_months(args)
        is_backfill = getattr(args, "start_month", None) is not None
        logger.info(
            "Requested TER month(s): %s",
            ", ".join(month_label(month) for month in months),
        )
        r2 = R2()
        connection = r2.setup_connection()
        session = requests.Session()
        run_months(
            months,
            connection,
            r2,
            session,
            backfill=is_backfill,
        )
    except Exception as exc:
        logger.error("TER extraction failed: %s", exc)
        log_script_end(logger, "AMFI TER Extractor", False)
        return 1
    finally:
        if session is not None:
            session.close()
        if connection is not None:
            connection.close()

    log_script_end(logger, "AMFI TER Extractor", True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
