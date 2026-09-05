"""Focused tests for immutable AMFI market-cap source archiving."""

import json
import zipfile
from datetime import date, datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

import requests

from scripts import fetch_amfi_mcap_sources as mcap


def xlsx_bytes(payload: bytes = b"workbook") -> bytes:
    """Return a small valid ZIP container representing an XLSX response."""
    output = BytesIO()
    with zipfile.ZipFile(output, "w") as workbook:
        workbook.writestr("xl/workbook.xml", payload)
    return output.getvalue()


def source_link(period_end: date = date(2026, 6, 30)) -> mcap.SourceLink:
    """Build one source-link fixture."""
    return mcap.SourceLink(
        source_year=period_end.year,
        source_period_label=(
            "Jan - June" if period_end.month == 6 else "Jul - Dec"
        ),
        period_end=period_end,
        source_url="https://www.amfiindia.com/files/source.xlsx",
        source_file_name="source.xlsx",
        source_file_extension="xlsx",
    )


def downloaded_source(
    payload: bytes = b"workbook",
    *,
    period_end: date = date(2026, 6, 30),
    fetched_at: datetime = datetime(2026, 9, 5, 8, 30, 15, tzinfo=timezone.utc),
) -> mcap.DownloadedSource:
    """Build one verified downloaded-source fixture."""
    content = xlsx_bytes(payload)
    return mcap.DownloadedSource(
        link=source_link(period_end),
        content=content,
        source_url="https://www.amfiindia.com/files/source.xlsx",
        source_fetched_at=fetched_at,
        source_content_type=(
            "application/vnd.openxmlformats-officedocument."
            "spreadsheetml.sheet"
        ),
        source_content_sha256=mcap.sha256(content).hexdigest(),
    )


class FakePaginator:
    """Paginate the current keys held by an in-memory S3 client."""

    def __init__(self, client):
        self.client = client

    def paginate(self, *, Bucket, Prefix):
        del Bucket
        keys = sorted(key for key in self.client.objects if key.startswith(Prefix))
        midpoint = len(keys) // 2
        pages = [keys[:midpoint], keys[midpoint:]] if keys else [[]]
        return [{"Contents": [{"Key": key} for key in page]} for page in pages]


class FakeS3Client:
    """Minimal in-memory S3 client supporting archive operations."""

    def __init__(self):
        self.objects = {}
        self.put_calls = []
        self.closed = False

    def get_paginator(self, operation):
        if operation != "list_objects_v2":
            raise AssertionError(operation)
        return FakePaginator(self)

    def put_object(self, **kwargs):
        key = kwargs["Key"]
        if kwargs.get("IfNoneMatch") == "*" and key in self.objects:
            raise AssertionError(f"attempted overwrite: {key}")
        self.objects[key] = {
            "Body": bytes(kwargs["Body"]),
            "Metadata": dict(kwargs.get("Metadata", {})),
            "ContentType": kwargs.get("ContentType"),
        }
        self.put_calls.append(kwargs)

    def get_object(self, *, Bucket, Key):
        del Bucket
        return {"Body": BytesIO(self.objects[Key]["Body"])}

    def head_object(self, *, Bucket, Key):
        del Bucket
        return {"Metadata": self.objects[Key]["Metadata"]}

    def close(self):
        self.closed = True


class FakeR2:
    """R2 path and client fixture."""

    bucket_name = "bucket"
    asset_class = "mutual_funds"

    def __init__(self, client=None):
        self.client = client

    def get_object_key(self, area, file_name, file_extension="parquet"):
        return f"{self.asset_class}/{area}/{file_name}.{file_extension}"

    def setup_s3_client(self):
        return self.client


class DiscoveryTests(TestCase):
    def test_discovers_periods_and_normalizes_relative_or_spaced_urls(self):
        html = """
        <h6>2026</h6>
        <li><p>Jan – June</p>
          <a href=" /files/june.xlsx "><img alt="Excel"></a>
        </li>
        <li><p>Jul - Dec</p>
          <a href="https://portal.amfiindia.com/files/dec.xls">Excel</a>
        </li>
        """

        sources = mcap.discover_source_links(
            html,
            "https://www.amfiindia.com/otherdata/categorisation-of-stocks",
        )

        self.assertEqual([item.period_end for item in sources], [
            date(2026, 6, 30),
            date(2026, 12, 31),
        ])
        self.assertEqual(
            sources[0].source_url,
            "https://www.amfiindia.com/files/june.xlsx",
        )
        self.assertEqual(sources[1].source_file_extension, "xls")

    def test_duplicate_urls_are_ignored(self):
        html = """
        <h6>2026</h6><li><p>Jan - June</p>
        <a href="/same.xlsx">Excel</a><a href="/same.xlsx">Excel</a></li>
        """

        sources = mcap.discover_source_links(html, "https://www.amfiindia.com/page")

        self.assertEqual(len(sources), 1)

    def test_unknown_period_or_missing_context_fails(self):
        cases = (
            '<h6>2026</h6><li><p>First half</p><a href="/x.xlsx">Excel</a></li>',
            '<a href="/x.xlsx">Excel</a>',
        )
        for html in cases:
            with self.subTest(html=html), self.assertRaises(ValueError):
                mcap.discover_source_links(html, "https://www.amfiindia.com/page")

    def test_external_or_unsupported_excel_link_fails(self):
        cases = (
            "https://example.com/source.xlsx",
            "https://www.amfiindia.com/source.csv",
        )
        for url in cases:
            html = (
                f'<h6>2026</h6><li><p>Jan - June</p>'
                f'<a href="{url}">Excel</a></li>'
            )
            with self.subTest(url=url), self.assertRaises(ValueError):
                mcap.discover_source_links(html, "https://www.amfiindia.com/page")


class TechnicalValidationTests(TestCase):
    def test_accepts_valid_xlsx_and_xls_containers(self):
        mcap.validate_excel_content(xlsx_bytes(), "xlsx")
        mcap.validate_excel_content(mcap.XLS_SIGNATURE + b"content", "xls")

    def test_rejects_empty_html_corrupt_xlsx_and_invalid_xls(self):
        cases = (
            (b"", "xlsx"),
            (b"<html>blocked</html>", "xlsx"),
            (b"not an OLE file", "xls"),
        )
        for content, extension in cases:
            with self.subTest(extension=extension), self.assertRaises(ValueError):
                mcap.validate_excel_content(content, extension)

    def test_source_fetch_retries_invalid_responses(self):
        good = xlsx_bytes()
        session = Mock()
        session.get.side_effect = [
            SimpleNamespace(
                content=b"<html>blocked</html>",
                url="https://www.amfiindia.com/files/source.xlsx",
                headers={"Content-Type": "text/html"},
                raise_for_status=lambda: None,
            ),
            SimpleNamespace(
                content=good,
                url="https://www.amfiindia.com/files/source.xlsx",
                headers={"Content-Type": "application/octet-stream"},
                raise_for_status=lambda: None,
            ),
        ]
        with patch.object(mcap.API, "MAX_RETRIES", 2), patch.object(
            mcap.API, "RETRY_DELAY", 0
        ):
            result = mcap.fetch_source(session, source_link())

        self.assertEqual(result.content, good)
        self.assertEqual(session.get.call_count, 2)

    def test_retry_exhaustion_raises_runtime_error(self):
        response = SimpleNamespace(
            content=b"bad",
            url="https://www.amfiindia.com/files/source.xlsx",
            headers={},
            raise_for_status=lambda: None,
        )
        session = Mock()
        session.get.return_value = response
        with patch.object(mcap.API, "MAX_RETRIES", 2), patch.object(
            mcap.API, "RETRY_DELAY", 0
        ), self.assertRaises(RuntimeError):
            mcap.fetch_source(session, source_link())


class ArchiveTests(TestCase):
    def setUp(self):
        self.client = FakeS3Client()
        self.r2 = FakeR2(self.client)

    def test_timestamped_keys_and_manifest_fields(self):
        source = downloaded_source()
        source_key, manifest_key = mcap.source_object_keys(self.r2, source)
        manifest = mcap.build_manifest(source, source_key)

        self.assertEqual(
            source_key,
            "mutual_funds/market_cap/source/"
            "market_cap_20260630_20260905T083015Z.xlsx",
        )
        self.assertEqual(manifest_key, source_key[:-5] + ".manifest.json")
        self.assertEqual(
            manifest["source_content_sha256"],
            source.source_content_sha256,
        )
        self.assertEqual(manifest["source_fetched_at"], "2026-09-05T08:30:15Z")

    def test_new_version_uploads_source_before_manifest_and_rerun_skips(self):
        source = downloaded_source()

        first = mcap.archive_downloads([source], self.client, self.r2)
        second = mcap.archive_downloads([source], self.client, self.r2)

        self.assertEqual(first, mcap.ArchiveSummary(1, 1, 0, 0))
        self.assertEqual(second, mcap.ArchiveSummary(1, 0, 1, 0))
        self.assertEqual(len(self.client.objects), 2)
        self.assertTrue(self.client.put_calls[0]["Key"].endswith(".xlsx"))
        self.assertTrue(self.client.put_calls[1]["Key"].endswith(".manifest.json"))
        self.assertEqual(self.client.put_calls[0]["IfNoneMatch"], "*")

    def test_changed_content_creates_a_timestamped_revision(self):
        first = downloaded_source(payload=b"first")
        second = downloaded_source(
            payload=b"second",
            fetched_at=datetime(2026, 9, 6, 8, 30, 15, tzinfo=timezone.utc),
        )

        mcap.archive_downloads([first], self.client, self.r2)
        result = mcap.archive_downloads([second], self.client, self.r2)

        self.assertEqual(result.uploaded, 1)
        self.assertEqual(len(self.client.objects), 4)

    def test_repairs_missing_manifest_then_skips_complete_version(self):
        source = downloaded_source()
        mcap.archive_downloads([source], self.client, self.r2)
        source_key, manifest_key = mcap.source_object_keys(self.r2, source)
        del self.client.objects[manifest_key]

        result = mcap.archive_downloads([source], self.client, self.r2)

        self.assertEqual(result, mcap.ArchiveSummary(1, 0, 1, 1))
        manifest = json.loads(self.client.objects[manifest_key]["Body"])
        self.assertEqual(manifest["r2_source_object_key"], source_key)

    def test_repairs_missing_workbook_then_skips_complete_version(self):
        source = downloaded_source()
        mcap.archive_downloads([source], self.client, self.r2)
        source_key, _manifest_key = mcap.source_object_keys(self.r2, source)
        del self.client.objects[source_key]

        result = mcap.archive_downloads([source], self.client, self.r2)

        self.assertEqual(result, mcap.ArchiveSummary(1, 0, 1, 1))
        self.assertEqual(self.client.objects[source_key]["Body"], source.content)

    def test_unrecoverable_incomplete_pair_fails(self):
        source = downloaded_source()
        mcap.archive_downloads([source], self.client, self.r2)
        source_key, _manifest_key = mcap.source_object_keys(self.r2, source)
        del self.client.objects[source_key]

        with self.assertRaisesRegex(RuntimeError, "Cannot repair"):
            mcap.archive_downloads([], self.client, self.r2)

    def test_main_closes_http_and_s3_clients(self):
        source = downloaded_source()
        session = Mock()
        with patch.object(mcap.requests, "Session", return_value=session), patch.object(
            mcap, "download_all_sources", return_value=[source]
        ), patch.object(mcap, "R2", return_value=self.r2):
            result = mcap.main()

        self.assertEqual(result, 0)
        session.close.assert_called_once()
        self.assertTrue(self.client.closed)


class FetchErrorTests(TestCase):
    def test_http_errors_are_retryable(self):
        session = Mock()
        session.get.side_effect = requests.Timeout("timeout")
        with patch.object(mcap.API, "MAX_RETRIES", 2), patch.object(
            mcap.API, "RETRY_DELAY", 0
        ), self.assertRaises(RuntimeError):
            mcap.fetch_listing_html(session)
        self.assertEqual(session.get.call_count, 2)
