#!/usr/bin/env python3
"""Archive immutable AMFI market-cap categorisation workbooks in R2."""

from __future__ import annotations

import json
import re
import time
import zipfile
from dataclasses import dataclass
from datetime import date, datetime, timezone
from hashlib import sha256
from html.parser import HTMLParser
from io import BytesIO
from pathlib import PurePosixPath
from typing import Any
from urllib.parse import quote, unquote, urljoin, urlsplit

import requests

from config.settings import API, R2
from utils.logging_setup import (
    get_fetch_amfi_mcap_sources_logger,
    log_script_end,
    log_script_start,
)

logger = get_fetch_amfi_mcap_sources_logger(__name__)

ALLOWED_AMFI_HOSTS = frozenset({
    "www.amfiindia.com",
    "portal.amfiindia.com",
})
SOURCE_AREA = "market_cap/source"
MANIFEST_SCHEMA_VERSION = "1"
XLS_SIGNATURE = bytes.fromhex("D0CF11E0A1B11AE1")
SOURCE_OBJECT_PATTERN = re.compile(
    r"^market_cap_(?P<period>\d{8})_"
    r"(?P<fetched>\d{8}T\d{6}Z)\.(?P<extension>xlsx|xls)$"
)


@dataclass(frozen=True)
class SourceLink:
    """One Excel source discovered on the AMFI listing page."""

    source_year: int
    source_period_label: str
    period_end: date
    source_url: str
    source_file_name: str
    source_file_extension: str


@dataclass(frozen=True)
class DownloadedSource:
    """One technically verified source workbook held in memory."""

    link: SourceLink
    content: bytes
    source_url: str
    source_fetched_at: datetime
    source_content_type: str
    source_content_sha256: str


@dataclass(frozen=True)
class ArchiveSummary:
    """Counts produced by one complete archive synchronization."""

    discovered: int
    uploaded: int
    skipped: int
    repaired_objects: int
    errors: tuple[str, ...] = ()


@dataclass
class ArchiveInventory:
    """Canonical source objects and manifests already present in R2."""

    object_keys: set[str]
    source_keys: set[str]
    manifests: dict[str, dict[str, Any]]
    complete_identities: set[tuple[str, str]]


@dataclass(frozen=True)
class _RawExcelLink:
    """Excel anchor plus the surrounding listing context."""

    year: int | None
    period_text: str
    href: str | None


class _CategorisationPageParser(HTMLParser):
    """Collect Excel anchors with their nearest year and list-item text."""

    def __init__(self) -> None:
        super().__init__()
        self.links: list[_RawExcelLink] = []
        self._current_year: int | None = None
        self._heading_tag: str | None = None
        self._heading_text: list[str] = []
        self._list_depth = 0
        self._list_text: list[str] = []
        self._list_excel_hrefs: list[str | None] = []
        self._anchor_active = False
        self._anchor_href: str | None = None
        self._anchor_text: list[str] = []
        self._anchor_in_list = False

    # ------ HTML events

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        """Track headings, list items, and anchor labels."""
        attributes = dict(attrs)
        if tag in {"h1", "h2", "h3", "h4", "h5", "h6"}:
            self._heading_tag = tag
            self._heading_text = []

        if tag == "li":
            if self._list_depth == 0:
                self._list_text = []
                self._list_excel_hrefs = []
            self._list_depth += 1

        if tag == "a":
            self._anchor_active = True
            self._anchor_href = attributes.get("href")
            self._anchor_text = []
            self._anchor_in_list = self._list_depth > 0

        if tag == "img" and self._anchor_active:
            alt_text = attributes.get("alt")
            if alt_text:
                self._anchor_text.append(alt_text)

    def handle_data(self, data: str) -> None:
        """Collect visible text for the active structural elements."""
        if self._heading_tag is not None:
            self._heading_text.append(data)
        if self._list_depth > 0:
            self._list_text.append(data)
        if self._anchor_active:
            self._anchor_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        """Finalize headings, Excel anchors, and list-item records."""
        if tag == self._heading_tag:
            heading = _normalized_text("".join(self._heading_text))
            if re.fullmatch(r"20\d{2}", heading):
                self._current_year = int(heading)
            self._heading_tag = None
            self._heading_text = []

        if tag == "a" and self._anchor_active:
            anchor_text = _normalized_text(" ".join(self._anchor_text))
            href_path = urlsplit((self._anchor_href or "").strip()).path.lower()
            is_excel = "excel" in anchor_text.lower() or href_path.endswith(
                (".xlsx", ".xls")
            )
            if is_excel:
                if self._anchor_in_list:
                    self._list_excel_hrefs.append(self._anchor_href)
                else:
                    self.links.append(
                        _RawExcelLink(self._current_year, "", self._anchor_href)
                    )
            self._anchor_href = None
            self._anchor_text = []
            self._anchor_in_list = False
            self._anchor_active = False

        if tag == "li" and self._list_depth > 0:
            self._list_depth -= 1
            if self._list_depth == 0:
                period_text = _normalized_text(" ".join(self._list_text))
                self.links.extend(
                    _RawExcelLink(self._current_year, period_text, href)
                    for href in self._list_excel_hrefs
                )
                self._list_text = []
                self._list_excel_hrefs = []


# ------ Discovery and download


def _normalized_text(value: str) -> str:
    """Collapse whitespace and normalize dash characters."""
    return re.sub(r"\s+", " ", value.replace("–", "-").replace("—", "-")).strip()


def period_from_label(year: int, label: str) -> tuple[str, date]:
    """Map an AMFI half-year label to its canonical label and period end."""
    normalized = _normalized_text(label).lower()
    if re.search(r"\bjan(?:uary)?\s*-\s*jun(?:e)?\b", normalized):
        return "Jan - June", date(year, 6, 30)
    if re.search(r"\bjul(?:y)?\s*-\s*dec(?:ember)?\b", normalized):
        return "Jul - Dec", date(year, 12, 31)
    raise ValueError(f"Unknown AMFI market-cap period label: {label!r}")


def _normalized_amfi_url(raw_url: str, page_url: str) -> str:
    """Return a safe absolute AMFI URL for one discovered link."""
    url = urljoin(page_url, raw_url.strip())
    parsed = urlsplit(url)
    if parsed.scheme.lower() != "https" or parsed.hostname not in ALLOWED_AMFI_HOSTS:
        raise ValueError(f"Unsupported AMFI market-cap source URL: {url!r}")
    return url


def discover_source_links(html: str, page_url: str) -> list[SourceLink]:
    """Discover and validate every Excel link in the AMFI listing HTML."""
    parser = _CategorisationPageParser()
    parser.feed(html)
    parser.close()
    if not parser.links:
        raise ValueError("AMFI market-cap page contains no Excel links")

    sources: list[SourceLink] = []
    seen_urls: set[str] = set()
    for raw_link in parser.links:
        if raw_link.year is None:
            raise ValueError("AMFI Excel link has no associated four-digit year")
        if raw_link.href is None or not raw_link.href.strip():
            raise ValueError("AMFI Excel link has an empty URL")

        source_url = _normalized_amfi_url(raw_link.href, page_url)
        if source_url in seen_urls:
            continue

        canonical_label, period_end = period_from_label(
            raw_link.year,
            raw_link.period_text,
        )
        path = urlsplit(source_url).path
        extension = PurePosixPath(path).suffix.lower().lstrip(".")
        if extension not in {"xlsx", "xls"}:
            raise ValueError(
                f"Unsupported Excel extension for AMFI URL: {source_url}"
            )
        source_file_name = unquote(PurePosixPath(path).name).strip()
        if not source_file_name:
            raise ValueError(f"AMFI URL has no source filename: {source_url}")

        sources.append(
            SourceLink(
                source_year=raw_link.year,
                source_period_label=canonical_label,
                period_end=period_end,
                source_url=source_url,
                source_file_name=source_file_name,
                source_file_extension=extension,
            )
        )
        seen_urls.add(source_url)

    return sources


def validate_excel_content(content: bytes, extension: str) -> None:
    """Perform container-level integrity checks without reading worksheets."""
    if not content:
        raise ValueError("AMFI Excel response is empty")
    if extension == "xlsx":
        if not zipfile.is_zipfile(BytesIO(content)):
            raise ValueError("AMFI XLSX response is not a valid ZIP container")
        with zipfile.ZipFile(BytesIO(content)) as workbook:
            corrupt_member = workbook.testzip()
        if corrupt_member is not None:
            raise ValueError(
                f"AMFI XLSX response has a corrupt ZIP member: {corrupt_member}"
            )
        return
    if extension == "xls":
        if not content.startswith(XLS_SIGNATURE):
            raise ValueError("AMFI XLS response has an invalid binary signature")
        return
    raise ValueError(f"Unsupported Excel extension: {extension!r}")


def fetch_listing_html(
    session: requests.Session,
    page_url: str = API.AMFI_MCAP_CATEGORISATION_URL,
) -> str:
    """Fetch the AMFI listing page using the shared retry policy."""
    attempts = max(1, API.MAX_RETRIES)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(page_url, timeout=API.AMFI_MCAP_TIMEOUT)
            response.raise_for_status()
            html = response.text
            if not html.strip():
                raise ValueError("AMFI market-cap listing response is empty")
            return html
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            logger.warning(
                "Market-cap listing fetch failed (attempt %s/%s): %s",
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                time.sleep(API.RETRY_DELAY)
    raise RuntimeError("Failed to fetch the AMFI market-cap listing") from last_error


def fetch_source(
    session: requests.Session,
    source: SourceLink,
) -> DownloadedSource:
    """Download and technically verify one source using configured retries."""
    attempts = max(1, API.MAX_RETRIES)
    last_error: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(
                source.source_url,
                timeout=API.AMFI_MCAP_TIMEOUT,
            )
            response.raise_for_status()
            final_url = _normalized_amfi_url(
                response.url or source.source_url,
                API.AMFI_MCAP_CATEGORISATION_URL,
            )
            content = bytes(response.content or b"")
            validate_excel_content(content, source.source_file_extension)
            fetched_at = datetime.now(timezone.utc).replace(microsecond=0)
            content_type = response.headers.get("Content-Type")
            return DownloadedSource(
                link=source,
                content=content,
                source_url=final_url,
                source_fetched_at=fetched_at,
                source_content_type=(
                    content_type.strip()
                    if content_type
                    else "application/octet-stream"
                ),
                source_content_sha256=sha256(content).hexdigest(),
            )
        except (requests.RequestException, ValueError, OSError) as exc:
            last_error = exc
            logger.warning(
                "Market-cap source fetch failed for %s (attempt %s/%s): %s",
                source.period_end,
                attempt,
                attempts,
                exc,
            )
            if attempt < attempts:
                time.sleep(API.RETRY_DELAY)
    raise RuntimeError(
        f"Failed to fetch valid AMFI workbook for {source.period_end}"
    ) from last_error


def download_all_sources(
    session: requests.Session,
) -> tuple[list[DownloadedSource], list[str]]:
    """Discover sources and isolate failures to individual downloads."""
    html = fetch_listing_html(session)
    links = discover_source_links(html, API.AMFI_MCAP_CATEGORISATION_URL)
    logger.info("Discovered %s AMFI market-cap workbook(s)", len(links))
    downloads: list[DownloadedSource] = []
    errors: list[str] = []
    for source in links:
        try:
            downloads.append(fetch_source(session, source))
        except RuntimeError as exc:
            message = (
                f"Failed to download {source.period_end} from "
                f"{source.source_url}: {exc}"
            )
            logger.error(message)
            errors.append(message)
    return downloads, errors


# ------ R2 inventory and manifests


def source_prefix(r2: R2) -> str:
    """Return the flat R2 prefix used by Phase 0 source objects."""
    return f"{r2.asset_class}/{SOURCE_AREA}/"


def source_object_keys(r2: R2, source: DownloadedSource) -> tuple[str, str]:
    """Return timestamped workbook and sidecar keys for a download."""
    base_name = (
        f"market_cap_{source.link.period_end:%Y%m%d}_"
        f"{source.source_fetched_at:%Y%m%dT%H%M%SZ}"
    )
    workbook_key = r2.get_object_key(
        SOURCE_AREA,
        base_name,
        source.link.source_file_extension,
    )
    manifest_key = r2.get_object_key(
        SOURCE_AREA,
        base_name,
        "manifest.json",
    )
    return workbook_key, manifest_key


def manifest_key_for_source(source_key: str) -> str:
    """Return the expected sidecar key for a canonical source object."""
    stem, _, _extension = source_key.rpartition(".")
    return f"{stem}.manifest.json"


def build_manifest(
    source: DownloadedSource,
    source_key: str,
    *,
    fetched_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the versioned provenance sidecar for one source object."""
    archive_time = fetched_at or source.source_fetched_at
    archive_time = archive_time.astimezone(timezone.utc).replace(microsecond=0)
    return {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "period_end": source.link.period_end.isoformat(),
        "source_year": source.link.source_year,
        "source_period_label": source.link.source_period_label,
        "source_page_url": API.AMFI_MCAP_CATEGORISATION_URL,
        "source_url": source.source_url,
        "source_file_name": source.link.source_file_name,
        "source_file_extension": source.link.source_file_extension,
        "source_fetched_at": archive_time.isoformat().replace("+00:00", "Z"),
        "source_content_sha256": source.source_content_sha256,
        "source_content_length_bytes": len(source.content),
        "source_content_type": source.source_content_type,
        "r2_source_object_key": source_key,
    }


def _manifest_identity(manifest: dict[str, Any]) -> tuple[str, str]:
    """Validate an archive manifest and return its deduplication identity."""
    if manifest.get("manifest_schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError("Unsupported market-cap manifest schema version")
    period_end = manifest.get("period_end")
    content_hash = manifest.get("source_content_sha256")
    source_key = manifest.get("r2_source_object_key")
    if not isinstance(period_end, str):
        raise ValueError("Market-cap manifest has no period_end")
    try:
        date.fromisoformat(period_end)
    except ValueError as exc:
        raise ValueError("Market-cap manifest has invalid period_end") from exc
    if not isinstance(content_hash, str) or not re.fullmatch(
        r"[0-9a-f]{64}", content_hash
    ):
        raise ValueError("Market-cap manifest has invalid source SHA-256")
    source_match = (
        SOURCE_OBJECT_PATTERN.fullmatch(PurePosixPath(source_key).name)
        if isinstance(source_key, str)
        else None
    )
    if source_match is None:
        raise ValueError("Market-cap manifest has invalid source object key")
    parsed_period = date.fromisoformat(period_end)
    if source_match.group("period") != parsed_period.strftime("%Y%m%d"):
        raise ValueError("Market-cap manifest period does not match its object key")
    if manifest.get("source_file_extension") != source_match.group("extension"):
        raise ValueError("Market-cap manifest extension does not match its object key")
    fetched_at = manifest.get("source_fetched_at")
    if not isinstance(fetched_at, str):
        raise ValueError("Market-cap manifest has no source_fetched_at")
    try:
        parsed_fetched_at = datetime.fromisoformat(
            fetched_at.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("Market-cap manifest has invalid source_fetched_at") from exc
    if parsed_fetched_at.tzinfo is None:
        raise ValueError("Market-cap manifest source_fetched_at is not timezone-aware")
    return period_end, content_hash


def _decode_manifest(body: bytes, manifest_key: str) -> dict[str, Any]:
    """Decode one JSON manifest from R2 and validate its object pairing."""
    try:
        manifest = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(f"Invalid market-cap manifest: {manifest_key}") from exc
    if not isinstance(manifest, dict):
        raise ValueError(f"Market-cap manifest is not an object: {manifest_key}")
    _manifest_identity(manifest)
    if manifest_key_for_source(manifest["r2_source_object_key"]) != manifest_key:
        raise ValueError(f"Market-cap manifest/object key mismatch: {manifest_key}")
    return manifest


def _is_canonical_manifest_key(key: str) -> bool:
    """Return whether a key can pair with a canonical XLSX or XLS object."""
    if not key.endswith(".manifest.json"):
        return False
    base_name = key.removesuffix(".manifest.json")
    return any(
        SOURCE_OBJECT_PATTERN.fullmatch(
            PurePosixPath(f"{base_name}.{extension}").name
        )
        for extension in ("xlsx", "xls")
    )


def load_archive_inventory(s3_client: Any, r2: R2) -> ArchiveInventory:
    """List canonical market-cap objects and load all provenance manifests."""
    object_keys: set[str] = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=r2.bucket_name, Prefix=source_prefix(r2)):
        object_keys.update(item["Key"] for item in page.get("Contents", []))

    source_keys = {
        key
        for key in object_keys
        if SOURCE_OBJECT_PATTERN.fullmatch(PurePosixPath(key).name)
    }
    manifest_keys = {key for key in object_keys if _is_canonical_manifest_key(key)}

    manifests: dict[str, dict[str, Any]] = {}
    complete_identities: set[tuple[str, str]] = set()
    for manifest_key in sorted(manifest_keys):
        response = s3_client.get_object(Bucket=r2.bucket_name, Key=manifest_key)
        manifest = _decode_manifest(response["Body"].read(), manifest_key)
        manifests[manifest_key] = manifest
        if manifest["r2_source_object_key"] in source_keys:
            complete_identities.add(_manifest_identity(manifest))

    return ArchiveInventory(
        object_keys=object_keys,
        source_keys=source_keys,
        manifests=manifests,
        complete_identities=complete_identities,
    )


def _serialize_manifest(manifest: dict[str, Any]) -> bytes:
    """Serialize a manifest deterministically as UTF-8 JSON."""
    return (json.dumps(manifest, indent=2, sort_keys=True) + "\n").encode("utf-8")


def _put_source(
    s3_client: Any,
    r2: R2,
    source: DownloadedSource,
    source_key: str,
    fetched_at: datetime,
) -> None:
    """Create one immutable workbook object using a conditional PUT."""
    s3_client.put_object(
        Bucket=r2.bucket_name,
        Key=source_key,
        Body=source.content,
        ContentType=source.source_content_type or "application/octet-stream",
        StorageClass="STANDARD",
        IfNoneMatch="*",
        Metadata={
            "period-end": source.link.period_end.isoformat(),
            "source-content-sha256": source.source_content_sha256,
            "source-fetched-at": fetched_at.isoformat().replace("+00:00", "Z"),
            "source-url": quote(source.source_url, safe=""),
            "source-page-url": quote(
                API.AMFI_MCAP_CATEGORISATION_URL,
                safe="",
            ),
            "source-file-name": quote(source.link.source_file_name, safe=""),
            "source-period-label": quote(
                source.link.source_period_label,
                safe="",
            ),
            "source-year": str(source.link.source_year),
        },
    )


def _put_manifest(
    s3_client: Any,
    r2: R2,
    manifest_key: str,
    manifest: dict[str, Any],
) -> None:
    """Create one immutable manifest object using a conditional PUT."""
    s3_client.put_object(
        Bucket=r2.bucket_name,
        Key=manifest_key,
        Body=_serialize_manifest(manifest),
        ContentType="application/json",
        StorageClass="STANDARD",
        IfNoneMatch="*",
    )


def _timestamp_from_source_key(source_key: str) -> datetime:
    """Read the UTC ingestion timestamp encoded in a source object key."""
    match = SOURCE_OBJECT_PATTERN.fullmatch(PurePosixPath(source_key).name)
    if match is None:
        raise ValueError(f"Invalid market-cap source object key: {source_key}")
    return datetime.strptime(
        match.group("fetched"),
        "%Y%m%dT%H%M%SZ",
    ).replace(tzinfo=timezone.utc)


def _download_identity(source: DownloadedSource) -> tuple[str, str]:
    """Return the full-hash deduplication identity for a download."""
    return source.link.period_end.isoformat(), source.source_content_sha256


def _manifest_from_source_object(
    s3_client: Any,
    r2: R2,
    source_key: str,
    head: dict[str, Any],
) -> dict[str, Any]:
    """Reconstruct exact provenance for an orphan workbook object."""
    match = SOURCE_OBJECT_PATTERN.fullmatch(PurePosixPath(source_key).name)
    if match is None:
        raise ValueError(f"Invalid market-cap source object key: {source_key}")

    metadata = head.get("Metadata", {})
    required_metadata = (
        "period-end",
        "source-content-sha256",
        "source-url",
        "source-page-url",
        "source-file-name",
        "source-period-label",
        "source-year",
    )
    missing = [key for key in required_metadata if not metadata.get(key)]
    if missing:
        raise ValueError(
            "source object lacks repair metadata: " + ", ".join(missing)
        )

    period_end = date.fromisoformat(metadata["period-end"])
    if match.group("period") != period_end.strftime("%Y%m%d"):
        raise ValueError("source metadata period does not match its object key")
    content_hash = metadata["source-content-sha256"]
    if not re.fullmatch(r"[0-9a-f]{64}", content_hash):
        raise ValueError("source object has invalid SHA-256 metadata")

    response = s3_client.get_object(Bucket=r2.bucket_name, Key=source_key)
    content = response["Body"].read()
    if sha256(content).hexdigest() != content_hash:
        raise ValueError("source object content does not match its SHA-256 metadata")
    extension = match.group("extension")
    validate_excel_content(content, extension)

    fetched_at = _timestamp_from_source_key(source_key)
    return {
        "manifest_schema_version": MANIFEST_SCHEMA_VERSION,
        "period_end": period_end.isoformat(),
        "source_year": int(metadata["source-year"]),
        "source_period_label": unquote(metadata["source-period-label"]),
        "source_page_url": unquote(metadata["source-page-url"]),
        "source_url": unquote(metadata["source-url"]),
        "source_file_name": unquote(metadata["source-file-name"]),
        "source_file_extension": extension,
        "source_fetched_at": fetched_at.isoformat().replace("+00:00", "Z"),
        "source_content_sha256": content_hash,
        "source_content_length_bytes": len(content),
        "source_content_type": head.get(
            "ContentType",
            "application/octet-stream",
        ),
        "r2_source_object_key": source_key,
    }


def repair_incomplete_pairs(
    s3_client: Any,
    r2: R2,
    inventory: ArchiveInventory,
    downloads_by_identity: dict[tuple[str, str], DownloadedSource],
) -> tuple[int, list[str]]:
    """Repair safe incomplete pairs and report unrecoverable ones."""
    repaired = 0
    errors: list[str] = []

    for manifest_key, manifest in sorted(inventory.manifests.items()):
        source_key = manifest["r2_source_object_key"]
        if source_key in inventory.source_keys:
            continue
        identity = _manifest_identity(manifest)
        source = downloads_by_identity.get(identity)
        if source is None:
            errors.append(
                f"Cannot repair missing workbook for manifest {manifest_key}"
            )
            continue
        fetched_at = datetime.fromisoformat(
            manifest["source_fetched_at"].replace("Z", "+00:00")
        )
        _put_source(s3_client, r2, source, source_key, fetched_at)
        inventory.source_keys.add(source_key)
        inventory.object_keys.add(source_key)
        inventory.complete_identities.add(identity)
        repaired += 1

    referenced_sources = {
        manifest["r2_source_object_key"]
        for manifest in inventory.manifests.values()
    }
    for source_key in sorted(inventory.source_keys - referenced_sources):
        try:
            head = s3_client.head_object(Bucket=r2.bucket_name, Key=source_key)
            manifest = _manifest_from_source_object(
                s3_client,
                r2,
                source_key,
                head,
            )
            manifest_key = manifest_key_for_source(source_key)
            _put_manifest(s3_client, r2, manifest_key, manifest)
            inventory.manifests[manifest_key] = manifest
            inventory.object_keys.add(manifest_key)
            inventory.complete_identities.add(_manifest_identity(manifest))
            repaired += 1
        except (KeyError, TypeError, ValueError) as exc:
            errors.append(
                f"Cannot repair missing manifest for {source_key}: {exc}"
            )

    return repaired, errors


# ------ Orchestration


def archive_downloads(
    downloads: list[DownloadedSource],
    s3_client: Any,
    r2: R2,
) -> ArchiveSummary:
    """Archive new content versions and repair incomplete R2 pairs."""
    downloads_by_identity: dict[tuple[str, str], DownloadedSource] = {}
    for source in downloads:
        downloads_by_identity.setdefault(_download_identity(source), source)

    inventory = load_archive_inventory(s3_client, r2)
    repaired, errors = repair_incomplete_pairs(
        s3_client,
        r2,
        inventory,
        downloads_by_identity,
    )

    uploaded = 0
    skipped = 0
    for source in downloads:
        identity = _download_identity(source)
        if identity in inventory.complete_identities:
            logger.info(
                "Skipping existing market-cap source for %s (%s)",
                source.link.period_end,
                source.source_content_sha256,
            )
            skipped += 1
            continue

        source_key, manifest_key = source_object_keys(r2, source)
        if source_key in inventory.object_keys or manifest_key in inventory.object_keys:
            raise RuntimeError(
                f"Timestamped market-cap archive key already exists: {source_key}"
            )
        manifest = build_manifest(source, source_key)
        _put_source(
            s3_client,
            r2,
            source,
            source_key,
            source.source_fetched_at,
        )
        _put_manifest(s3_client, r2, manifest_key, manifest)
        inventory.object_keys.update({source_key, manifest_key})
        inventory.source_keys.add(source_key)
        inventory.manifests[manifest_key] = manifest
        inventory.complete_identities.add(identity)
        uploaded += 1
        logger.info(
            "Archived AMFI market-cap source to r2://%s/%s",
            r2.bucket_name,
            source_key,
        )

    return ArchiveSummary(
        discovered=len(downloads),
        uploaded=uploaded,
        skipped=skipped,
        repaired_objects=repaired,
        errors=tuple(errors),
    )


def main() -> int:
    """Discover, verify, and archive all AMFI market-cap workbooks."""
    log_script_start(
        logger,
        "AMFI Market-Cap Source Archiver",
        "Archiving source Excel bytes and provenance manifests in R2",
    )
    session: requests.Session | None = None
    s3_client = None
    succeeded = False
    try:
        session = requests.Session()
        downloads, download_errors = download_all_sources(session)
        if not downloads:
            raise RuntimeError("No valid AMFI market-cap workbooks were downloaded")
        r2 = R2()
        s3_client = r2.setup_s3_client()
        summary = archive_downloads(downloads, s3_client, r2)
        logger.info(
            "Archive summary: discovered=%s uploaded=%s skipped=%s repaired=%s",
            summary.discovered,
            summary.uploaded,
            summary.skipped,
            summary.repaired_objects,
        )
        errors = [*download_errors, *summary.errors]
        if errors:
            logger.error(
                "Archive completed valid work with %s unresolved error(s)",
                len(errors),
            )
            for error in errors:
                logger.error("Unresolved archive error: %s", error)
        else:
            succeeded = True
    except Exception as exc:
        logger.error("AMFI market-cap source archiving failed: %s", exc)
    finally:
        if session is not None:
            session.close()
        if s3_client is not None:
            s3_client.close()

    log_script_end(logger, "AMFI Market-Cap Source Archiver", succeeded)
    return 0 if succeeded else 1


if __name__ == "__main__":
    raise SystemExit(main())
