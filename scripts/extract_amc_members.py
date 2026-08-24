"""Fetch AMFI member records and write an immutable raw snapshot to R2."""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from typing import Any

import pandas as pd
import requests

from config.settings import API, R2
from utils.logging_setup import (
    get_extract_amc_members_logger,
    log_script_end,
    log_script_start,
)
from utils.nav_helpers import save_to_parquet

logger = get_extract_amc_members_logger(__name__)


def fetch_json(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, str] | None = None,
) -> Any:
    """Fetch and decode JSON, retrying according to the shared API settings.

    Args:
        session: Shared requests session.
        url: Endpoint URL.
        params: Optional query parameters.

    Returns:
        The decoded JSON value.

    Raises:
        RuntimeError: If all request attempts fail.
    """
    attempts = max(1, API.MAX_RETRIES)
    last_error: Exception | None = None

    for attempt in range(1, attempts + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=API.AMFI_MEMBERS_TIMEOUT,
            )
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            if attempt == attempts:
                break
            logger.warning(
                "Request failed for %s (attempt %s/%s): %s",
                url,
                attempt,
                attempts,
                exc,
            )
            time.sleep(API.RETRY_DELAY)

    raise RuntimeError(f"Failed to fetch {url}: {last_error}")


def normalize_member_id(value: object) -> str:
    """Return a non-empty AMFI member ID as a string.

    Args:
        value: Source member identifier.

    Returns:
        Normalized member identifier.

    Raises:
        ValueError: If the identifier is null or empty.
    """
    if value is None:
        raise ValueError("AMFI member ID is missing")

    member_id = str(value).strip()
    if not member_id:
        raise ValueError("AMFI member ID is empty")
    return member_id


def validate_listing_payload(payload: object) -> list[dict[str, Any]]:
    """Extract and validate member records from the listing response.

    Args:
        payload: Decoded AMFI listing response.

    Returns:
        Listing records in source order.
    """
    if not isinstance(payload, dict):
        raise ValueError("AMFI members response must be an object")

    records = payload.get("AMFIMembers")
    if not isinstance(records, list) or not records:
        raise ValueError("AMFI members response contains no member records")
    if not all(isinstance(record, dict) for record in records):
        raise ValueError("AMFI members response contains a non-object record")

    member_ids = [normalize_member_id(record.get("mf_id")) for record in records]
    if len(member_ids) != len(set(member_ids)):
        raise ValueError("AMFI members response contains duplicate member IDs")
    return records


def validate_social_payload(payload: object) -> list[dict[str, Any]]:
    """Extract social-media records from the AMFI response.

    Args:
        payload: Decoded AMFI social-media response.

    Returns:
        Social-media records.
    """
    if not isinstance(payload, dict):
        raise ValueError("AMFI social-media response must be an object")

    records = payload.get("data")
    if not isinstance(records, list):
        raise ValueError("AMFI social-media response has no data list")
    if not all(isinstance(record, dict) for record in records):
        raise ValueError("AMFI social-media response contains a non-object record")
    return records


def index_social_records(
    records: list[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """Index social records by member ID and reject duplicates.

    Args:
        records: AMFI social-media records.

    Returns:
        Records keyed by normalized member ID.
    """
    indexed: dict[str, dict[str, Any]] = {}
    for record in records:
        raw_member_id = record.get("MF_ID") or record.get("MF_Id")
        member_id = normalize_member_id(raw_member_id)
        if member_id in indexed:
            raise ValueError(
                f"AMFI social-media response duplicates member ID {member_id}"
            )
        indexed[member_id] = record
    return indexed


def serialize_record(record: dict[str, Any] | None) -> str | None:
    """Serialize one source record without selecting business fields."""
    if record is None:
        return None
    return json.dumps(record, ensure_ascii=False, separators=(",", ":"))


def build_snapshot_dataframe(
    listing_records: list[dict[str, Any]],
    detail_records: dict[str, dict[str, Any]],
    social_records: list[dict[str, Any]],
    snapshot_fetched_at: datetime,
) -> pd.DataFrame:
    """Build and validate a member-grain raw snapshot DataFrame.

    Args:
        listing_records: Validated member-list records.
        detail_records: Detail records keyed by normalized member ID.
        social_records: Validated social-media records.
        snapshot_fetched_at: UTC timestamp shared by every snapshot row.

    Returns:
        One raw row per listed AMFI member.
    """
    social_by_id = index_social_records(social_records)
    listed_ids = {
        normalize_member_id(record.get("mf_id")) for record in listing_records
    }
    unmatched_social_ids = sorted(set(social_by_id) - listed_ids)
    if unmatched_social_ids:
        logger.warning(
            "Ignoring %s social record(s) outside the current member listing: %s",
            len(unmatched_social_ids),
            ", ".join(unmatched_social_ids),
        )

    timestamp = pd.Timestamp(snapshot_fetched_at)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")

    rows: list[dict[str, Any]] = []
    for listing_record in listing_records:
        member_id = normalize_member_id(listing_record.get("mf_id"))
        detail_record = detail_records.get(member_id)
        if not isinstance(detail_record, dict):
            raise ValueError(f"Missing detail record for member ID {member_id}")

        detail_member_id = normalize_member_id(detail_record.get("mf_id"))
        if detail_member_id != member_id:
            raise ValueError(
                "Detail member ID mismatch: "
                f"expected {member_id}, received {detail_member_id}"
            )

        rows.append(
            {
                "member_id": member_id,
                "snapshot_fetched_at": timestamp,
                "listing_record_json": serialize_record(listing_record),
                "detail_record_json": serialize_record(detail_record),
                "social_record_json": serialize_record(
                    social_by_id.get(member_id)
                ),
            }
        )

    return pd.DataFrame(rows)


def fetch_amc_member_snapshot(
    session: requests.Session,
    snapshot_fetched_at: datetime,
) -> pd.DataFrame:
    """Fetch all AMFI member sources and assemble one complete snapshot.

    Args:
        session: Shared requests session.
        snapshot_fetched_at: UTC timestamp for the complete run.

    Returns:
        Validated member-grain raw snapshot.
    """
    listing_records = validate_listing_payload(
        fetch_json(session, API.AMFI_MEMBERS_URL)
    )
    social_records = validate_social_payload(
        fetch_json(session, API.AMFI_SOCIAL_MEDIA_URL)
    )

    detail_records: dict[str, dict[str, Any]] = {}
    for position, listing_record in enumerate(listing_records, start=1):
        member_id = normalize_member_id(listing_record.get("mf_id"))
        logger.info(
            "Fetching member detail %s/%s (member_id=%s)",
            position,
            len(listing_records),
            member_id,
        )
        detail = fetch_json(
            session,
            API.AMFI_MEMBER_DETAIL_URL,
            params={"MF_ID": member_id},
        )
        if not isinstance(detail, dict):
            raise ValueError(f"Missing detail record for member ID {member_id}")
        detail_records[member_id] = detail

    return build_snapshot_dataframe(
        listing_records,
        detail_records,
        social_records,
        snapshot_fetched_at,
    )


def save_snapshot_to_r2(
    snapshot: pd.DataFrame,
    snapshot_fetched_at: datetime,
) -> str:
    """Write a validated snapshot to a unique R2 Parquet object.

    Args:
        snapshot: Complete snapshot DataFrame.
        snapshot_fetched_at: Timestamp used in the immutable object name.

    Returns:
        R2 path of the uploaded object.
    """
    timestamp = pd.Timestamp(snapshot_fetched_at)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize("UTC")
    else:
        timestamp = timestamp.tz_convert("UTC")

    file_timestamp = timestamp.strftime("%Y%m%dT%H%M%SZ")
    r2 = R2()
    r2_path = r2.get_full_path(
        "amc_members",
        f"amc_members_{file_timestamp}",
    )
    connection = r2.setup_connection()
    try:
        save_to_parquet(connection, "amc_member_snapshot", snapshot, r2_path)
    finally:
        connection.close()

    logger.info("Saved %s member rows to %s", len(snapshot), r2_path)
    return r2_path


def main() -> int:
    """Fetch, validate, and upload one complete AMC member snapshot."""
    log_script_start(
        logger,
        "AMC Member Extractor",
        "Fetching AMFI member records and saving a raw R2 snapshot",
    )
    snapshot_fetched_at = datetime.now(timezone.utc)

    try:
        with requests.Session() as session:
            snapshot = fetch_amc_member_snapshot(session, snapshot_fetched_at)
        save_snapshot_to_r2(snapshot, snapshot_fetched_at)
    except Exception as exc:
        logger.error("AMC member extraction failed: %s", exc)
        log_script_end(logger, "AMC Member Extractor", False)
        return 1

    log_script_end(logger, "AMC Member Extractor", True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
