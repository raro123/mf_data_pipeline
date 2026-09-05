"""Focused tests for AMFI market-cap raw transformation."""

from __future__ import annotations

import json
from dataclasses import replace
from datetime import date, datetime, timezone
from hashlib import sha256
from io import BytesIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

import openpyxl
import pyarrow.parquet as pq
import xlrd

from scripts import fetch_amfi_mcap_sources as archive
from scripts import transform_amfi_mcap_sources as transform
from tests.test_fetch_amfi_mcap_sources import FakeR2, FakeS3Client

HEADER = (
    "S. No.",
    "Company Name",
    "ISIN",
    "BSE Symbol",
    "BSE 6 Months Average Market Capitalisation (Rs. Crore)",
    "NSE Symbol",
    "NSE 6 Months Average Market Capitalisation (Rs. Crore)",
    "MSEI Symbol",
    "MSEI 6 Months Average Market Capitalisation (Rs. Crore)",
    "Average of All Exchanges (Rs. Cr.)",
    "Categorization as per SEBI Circular dated Oct 6, 2017",
)


def workbook_bytes(
    rows,
    *,
    header=HEADER,
    sheet_name="Average MCap",
    second_sheet_header=False,
):
    """Build a small XLSX workbook fixture."""
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = sheet_name
    sheet.append(["AMFI categorisation"])
    sheet.append(list(header))
    for row in rows:
        sheet.append(list(row))
    if second_sheet_header:
        other = workbook.create_sheet("Other")
        other.append(list(header))
        other.append([1, "Other"])
    output = BytesIO()
    workbook.save(output)
    workbook.close()
    return output.getvalue()


def manifest_for(
    content: bytes,
    *,
    fetched_at=datetime(2026, 9, 5, 8, 30, 15, tzinfo=timezone.utc),
    extension="xlsx",
):
    """Build one valid Phase 0 manifest fixture."""
    period_end = date(2026, 6, 30)
    source_key = (
        "mutual_funds/market_cap/source/"
        f"market_cap_{period_end:%Y%m%d}_{fetched_at:%Y%m%dT%H%M%SZ}."
        f"{extension}"
    )
    manifest_key = archive.manifest_key_for_source(source_key)
    manifest = {
        "manifest_schema_version": archive.MANIFEST_SCHEMA_VERSION,
        "period_end": period_end.isoformat(),
        "source_year": 2026,
        "source_period_label": "Jan - June",
        "source_page_url": (
            "https://www.amfiindia.com/otherdata/categorisation-of-stocks"
        ),
        "source_url": "https://www.amfiindia.com/files/source.xlsx",
        "source_file_name": f"source.{extension}",
        "source_file_extension": extension,
        "source_fetched_at": fetched_at.isoformat().replace("+00:00", "Z"),
        "source_content_sha256": sha256(content).hexdigest(),
        "source_content_length_bytes": len(content),
        "source_content_type": "application/octet-stream",
        "r2_source_object_key": source_key,
    }
    return manifest_key, source_key, manifest


def source_pair(content: bytes, **kwargs) -> transform.SourcePair:
    """Build one verified source-pair fixture."""
    manifest_key, source_key, manifest = manifest_for(content, **kwargs)
    return transform.SourcePair(
        manifest_key=manifest_key,
        source_key=source_key,
        manifest=manifest,
        workbook_content=content,
    )


class PairVerificationTests(TestCase):
    def setUp(self):
        self.client = FakeS3Client()
        self.r2 = FakeR2(self.client)
        self.content = workbook_bytes([[1, "Company"]])
        self.manifest_key, self.source_key, self.manifest = manifest_for(self.content)

    def store_pair(self, manifest=None, include_source=True):
        """Put one manifest/source fixture into the fake object store."""
        payload = manifest or self.manifest
        if include_source:
            self.client.objects[self.source_key] = {
                "Body": self.content,
                "Metadata": {},
                "ContentType": "application/octet-stream",
            }
        self.client.objects[self.manifest_key] = {
            "Body": json.dumps(payload).encode(),
            "Metadata": {},
            "ContentType": "application/json",
        }

    def test_loads_only_complete_canonical_verified_pairs(self):
        self.store_pair()
        self.client.objects["mutual_funds/market_cap/source/notes.json"] = {
            "Body": b"{}",
            "Metadata": {},
            "ContentType": "application/json",
        }

        pairs, errors = transform.load_source_pairs(self.client, self.r2)

        self.assertEqual(errors, [])
        self.assertEqual(len(pairs), 1)
        self.assertEqual(pairs[0].workbook_content, self.content)

    def test_missing_source_length_hash_and_manifest_schema_fail_independently(self):
        cases = []

        self.store_pair(include_source=False)
        pairs, errors = transform.load_source_pairs(self.client, self.r2)
        cases.append((pairs, errors))

        self.client = FakeS3Client()
        self.r2 = FakeR2(self.client)
        bad_length = dict(self.manifest, source_content_length_bytes=1)
        self.store_pair(bad_length)
        cases.append(transform.load_source_pairs(self.client, self.r2))

        self.client = FakeS3Client()
        self.r2 = FakeR2(self.client)
        bad_hash = dict(self.manifest, source_content_sha256="0" * 64)
        self.store_pair(bad_hash)
        cases.append(transform.load_source_pairs(self.client, self.r2))

        self.client = FakeS3Client()
        self.r2 = FakeR2(self.client)
        bad_schema = dict(self.manifest)
        bad_schema["unexpected"] = "field"
        self.store_pair(bad_schema)
        cases.append(transform.load_source_pairs(self.client, self.r2))

        for pairs, errors in cases:
            with self.subTest(error=errors):
                self.assertEqual(pairs, [])
                self.assertEqual(len(errors), 1)


class WorkbookParsingTests(TestCase):
    def test_header_alias_blank_trailing_columns_and_exact_values(self):
        header = list(HEADER)
        header[7] = "MSE Symbol"
        header[8] = "MSE Six Month Avg Market Capitalisation (Rs Crore)"
        header.extend([None, None])
        first = [
            1,
            "  ACME – Ltd  ",
            "INE000A01001",
            "--",
            123.5,
            None,
            120,
            " MSE ",
            0,
            121.25,
            " Large Cap ",
        ]
        partial = [None, "Note: source footnote"]
        content = workbook_bytes([first, [None] * 11, partial, first], header=header)

        parsed = transform._parse_xlsx(content)

        self.assertEqual(parsed.sheet_name, "Average MCap")
        self.assertEqual([row[0] for row in parsed.rows], [3, 5, 6])
        self.assertEqual(parsed.rows[0][1][1], "  ACME – Ltd  ")
        self.assertEqual(parsed.rows[0][1][3], "--")
        self.assertEqual(parsed.rows[0][1][4], "123.5")
        self.assertIsNone(parsed.rows[0][1][5])
        self.assertEqual(parsed.rows[1][1][1], "Note: source footnote")
        self.assertEqual(parsed.rows[0][1], parsed.rows[2][1])

    def test_unknown_ambiguous_headers_and_empty_data_fail(self):
        unknown = list(HEADER)
        unknown[4] = "Unknown Column"
        trailing = workbook_bytes([[1, "Company"]])
        trailing_book = openpyxl.load_workbook(BytesIO(trailing))
        trailing_book.active.cell(3, 12, "unexpected")
        trailing_output = BytesIO()
        trailing_book.save(trailing_output)
        trailing_book.close()
        cases = (
            workbook_bytes([[1, "Company"]], header=unknown),
            workbook_bytes([[1, "Company"]], second_sheet_header=True),
            workbook_bytes([]),
            trailing_output.getvalue(),
        )
        for content in cases:
            with self.subTest(), self.assertRaises(ValueError):
                transform._parse_xlsx(content)

    def test_xlsx_formula_requires_cached_value_in_retained_row(self):
        values = [None] * 11
        values[4] = "=1+2"
        content = workbook_bytes([values])

        with self.assertRaisesRegex(ValueError, "formula has no cached value"):
            transform._parse_xlsx(content)

    def test_xlsx_reads_cached_formula_value_not_formula_text(self):
        formula_book = openpyxl.Workbook()
        cached_book = openpyxl.Workbook()
        for workbook in (formula_book, cached_book):
            workbook.active.title = "Data"
            workbook.active.append(list(HEADER))
        formula_book.active.append([1, "Company", None, None, "=1+2"])
        cached_book.active.append([1, "Company", None, None, 3])

        with patch.object(
            transform.openpyxl,
            "load_workbook",
            side_effect=[formula_book, cached_book],
        ):
            parsed = transform._parse_xlsx(b"fixture")

        self.assertEqual(parsed.rows[0][1][4], "3")

    def test_xls_adapter_uses_cached_values_and_preserves_rows(self):
        rows = [
            list(HEADER),
            [1.0, "  Company  "] + [None] * 9,
            [None] * 11,
            [None, "Note"] + [None] * 9,
        ]

        class FakeCell:
            def __init__(self, value):
                self.value = value
                if value is None:
                    self.ctype = xlrd.XL_CELL_EMPTY
                elif isinstance(value, str):
                    self.ctype = xlrd.XL_CELL_TEXT
                else:
                    self.ctype = xlrd.XL_CELL_NUMBER

        class FakeSheet:
            name = "Legacy"
            nrows = len(rows)
            ncols = 11

            def cell(self, row, column):
                return FakeCell(rows[row][column])

        sheet = FakeSheet()
        book = SimpleNamespace(
            datemode=0,
            sheets=lambda: [sheet],
            sheet_by_name=lambda _name: sheet,
            release_resources=Mock(),
        )
        with patch.object(transform.xlrd, "open_workbook", return_value=book):
            parsed = transform._parse_xls(b"legacy")

        self.assertEqual([row[0] for row in parsed.rows], [2, 4])
        self.assertEqual(parsed.rows[0][1][0], "1")
        self.assertEqual(parsed.rows[0][1][1], "  Company  ")
        self.assertEqual(parsed.rows[1][1][1], "Note")
        book.release_resources.assert_called_once()


class PublicationTests(TestCase):
    def setUp(self):
        self.client = FakeS3Client()
        self.r2 = FakeR2(self.client)
        self.content = workbook_bytes([[1, "Company"], [None, "Note"]])
        self.pair = source_pair(self.content)

    def test_naming_schema_provenance_upload_and_verified_rerun_skip(self):
        uploaded, row_count = transform.publish_pair(
            self.client,
            self.r2,
            set(),
            self.pair,
        )
        object_key = transform.raw_object_key(self.r2, self.pair.manifest)
        stored = self.client.objects[object_key]
        table = pq.read_table(BytesIO(stored["Body"]))

        self.assertTrue(uploaded)
        self.assertEqual(row_count, 2)
        self.assertEqual(
            object_key,
            "mutual_funds/market_cap/raw/market_cap_20260630_20260905T083015Z.parquet",
        )
        self.assertEqual(table.schema, transform.RAW_ARROW_SCHEMA)
        self.assertEqual(table["source_row_number"].to_pylist(), [3, 4])
        self.assertEqual(table["raw_schema_version"].to_pylist(), ["1", "1"])
        call = self.client.put_calls[-1]
        self.assertEqual(call["IfNoneMatch"], "*")
        self.assertEqual(call["StorageClass"], "STANDARD")
        self.assertEqual(call["Metadata"]["row-count"], "2")
        self.assertEqual(
            call["Metadata"]["source-object-key"],
            self.pair.source_key,
        )

        skipped, skipped_rows = transform.publish_pair(
            self.client,
            self.r2,
            {object_key},
            self.pair,
        )
        self.assertFalse(skipped)
        self.assertEqual(skipped_rows, 2)
        self.assertEqual(len(self.client.put_calls), 1)

    def test_existing_object_with_bad_metadata_or_hash_fails(self):
        transform.publish_pair(self.client, self.r2, set(), self.pair)
        object_key = transform.raw_object_key(self.r2, self.pair.manifest)
        self.client.objects[object_key]["Metadata"]["row-count"] = "99"

        with self.assertRaisesRegex(ValueError, "metadata mismatch"):
            transform.publish_pair(
                self.client,
                self.r2,
                {object_key},
                self.pair,
            )

        self.client.objects[object_key]["Metadata"]["row-count"] = "2"
        self.client.objects[object_key]["Body"] += b"tampered"
        with self.assertRaisesRegex(ValueError, "SHA-256 mismatch"):
            transform.publish_pair(
                self.client,
                self.r2,
                {object_key},
                self.pair,
            )

    def test_revisions_create_distinct_immutable_objects(self):
        second_pair = source_pair(
            self.content,
            fetched_at=datetime(2026, 9, 6, 8, 30, 15, tzinfo=timezone.utc),
        )
        raw_keys = set()

        transform.publish_pair(self.client, self.r2, raw_keys, self.pair)
        transform.publish_pair(self.client, self.r2, raw_keys, second_pair)

        self.assertEqual(len(raw_keys), 2)
        self.assertTrue(all(key.endswith(".parquet") for key in raw_keys))

    def test_partial_success_is_retained_but_summary_fails(self):
        second_pair = replace(self.pair, source_key="bad-source")
        with (
            patch.object(
                transform,
                "load_source_pairs",
                return_value=([self.pair, second_pair], []),
            ),
            patch.object(
                transform,
                "_list_keys",
                return_value=set(),
            ),
            patch.object(
                transform,
                "publish_pair",
                side_effect=[(True, 2), ValueError("bad workbook")],
            ),
        ):
            summary = transform.transform_sources(self.client, self.r2)

        self.assertEqual(summary.uploaded, 1)
        self.assertEqual(summary.rows, 2)
        self.assertEqual(len(summary.errors), 1)

        fake_client = FakeS3Client()
        with (
            patch.object(
                transform,
                "R2",
                return_value=FakeR2(fake_client),
            ),
            patch.object(
                transform,
                "transform_sources",
                return_value=summary,
            ),
        ):
            self.assertEqual(transform.main(), 1)
        self.assertTrue(fake_client.closed)
