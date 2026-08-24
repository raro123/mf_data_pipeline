"""Focused tests for the AMFI TER extraction workflow."""

from datetime import date, datetime, timezone
from io import BytesIO
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import Mock, patch

import pandas as pd
import requests

from scripts import fetch_ter_data as ter


OLD_HEADERS = [
    "NSDL Scheme Code",
    "Scheme Name",
    "Scheme Type",
    "Scheme Category",
    "TER Date",
    "Regular Plan - Base TER (%)",
    "Regular Plan - Additional expense as per Regulation 52(6A)(b) (%)",
    "Regular Plan - Additional expense as per Regulation 52(6A)(c) (%)",
    "Regular Plan - GST (%)",
    "Regular Plan - Total TER (%)",
    "Direct Plan - Base TER (%)",
    "Direct Plan - Additional expense as per Regulation 52(6A)(b) (%)",
    "Direct Plan - Additional expense as per Regulation 52(6A)(c) (%)",
    "Direct Plan - GST (%)",
    "Direct Plan - Total TER (%)",
]

NEW_HEADERS = [
    "NSDL Scheme Code",
    "Scheme Name",
    "Scheme Type",
    "Scheme Category",
    "TER Date",
    "Regular Plan - Base Expense Ratio (BER) (%)",
    "Regular Plan - Brokerage Cost (%)",
    "Regular Plan - Transaction Cost incurred for the purpose of execution "
    "of trade (%)",
    "Regular Plan - Statutory Levies (including GST) (%)",
    "Regular Plan - Total TER (%)",
    "Direct Plan - Base Expense Ratio (BER) (%)",
    "Direct Plan - Brokerage Cost (%)",
    "Direct Plan - Transaction Cost incurred for the purpose of execution "
    "of trade (%)",
    "Direct Plan - Statutory Levies (including GST) (%)",
    "Direct Plan - Total TER (%)",
]


def workbook_bytes(headers, rows, *, include_disclaimer=False):
    """Build a small in-memory XLSX fixture."""
    output = BytesIO()
    values = [headers, *rows]
    if include_disclaimer:
        values.extend(
            [
                [None] * len(headers),
                ["Disclaimer :"] + [None] * (len(headers) - 1),
                ["Percentages mentioned above are annualized."]
                + [None] * (len(headers) - 1),
            ]
        )
    pd.DataFrame(values).to_excel(output, index=False, header=False)
    return output.getvalue()


def old_row(date_value="30-Apr-2020"):
    """Return one old-regime fixture row."""
    return [[
        None,
        "Example Fund",
        "Open Ended",
        "Other Scheme - FoF Domestic",
        date_value,
        1.0,
        0.1,
        0.2,
        0.3,
        1.6,
        0.5,
        0.1,
        0.2,
        0.1,
        0.9,
    ]]


def new_row(date_value="30-Apr-2026"):
    """Return one new-regime fixture row."""
    return [[
        "AMC/O/E/ABC/20/01/0001",
        "Example Fund",
        "Open Ended",
        "Equity Scheme - Flexi Cap Fund",
        date_value,
        0.8,
        0.01,
        0.02,
        0.15,
        0.98,
        0.4,
        0.01,
        0.02,
        0.08,
        0.51,
    ]]


class FakeResponse:
    """Minimal response object for request tests."""

    def __init__(self, content=b"not xlsx", error=None, url=None):
        self.content = content
        self.error = error
        self.url = url

    def raise_for_status(self):
        if self.error:
            raise self.error


class FakeConnection:
    """R2 stand-in supporting object discovery and write assertions."""

    def __init__(self, objects=()):
        self.objects = list(objects)
        self.closed = False
        self.writes = []

    def execute(self, query, params):
        self.glob_pattern = params[0]
        return SimpleNamespace(
            fetchall=lambda: [(path,) for path in self.objects]
        )

    def close(self):
        self.closed = True


class FakeR2:
    """R2 path builder for local unit tests."""

    def get_full_path(self, area, name, file_extension="parquet"):
        return f"r2://bucket/mutual_funds/{area}/{name}.{file_extension}"

    def setup_connection(self):
        return self.connection


class TerSchemaTests(TestCase):
    def test_old_and_new_schemas_have_nullable_superset_columns(self):
        old = ter.parse_ter_workbook(workbook_bytes(OLD_HEADERS, old_row()))
        old_output = ter.build_ter_dataframe(old, "2020-04")
        new = ter.parse_ter_workbook(workbook_bytes(NEW_HEADERS, new_row()))
        new_output = ter.build_ter_dataframe(new, "2026-04")

        self.assertEqual(
            old.attrs["source_schema_version"], ter.SCHEMA_PRE_APRIL_2026
        )
        self.assertEqual(
            new.attrs["source_schema_version"], ter.SCHEMA_APRIL_2026_ONWARD
        )
        self.assertEqual(list(old_output.columns), list(ter.TER_OUTPUT_COLUMNS))
        self.assertTrue(pd.isna(old_output.loc[0, "regular_base_expense_ratio"]))
        self.assertTrue(pd.isna(new_output.loc[0, "regular_base_ter"]))
        self.assertTrue(pd.isna(new_output.loc[0, "nsdl_scheme_code"]) is False)

    def test_real_layout_stops_before_disclaimer_footer(self):
        for headers, rows, expected_schema in (
            (OLD_HEADERS, old_row(), ter.SCHEMA_PRE_APRIL_2026),
            (NEW_HEADERS, new_row(), ter.SCHEMA_APRIL_2026_ONWARD),
        ):
            with self.subTest(schema=expected_schema):
                parsed = ter.parse_ter_workbook(
                    workbook_bytes(headers, rows, include_disclaimer=True)
                )

                self.assertEqual(len(parsed), 1)
                self.assertEqual(
                    parsed.attrs["source_schema_version"], expected_schema
                )
                self.assertEqual(parsed.loc[0, "scheme_name"], "Example Fund")

    def test_unknown_or_partial_schema_fails(self):
        partial = OLD_HEADERS[:-1]
        with self.assertRaisesRegex(ValueError, "missing|required|partial"):
            ter.detect_schema_version(partial)

        unknown = OLD_HEADERS[:-1] + ["Unexpected TER column"]
        with self.assertRaisesRegex(ValueError, "Unknown|missing|required"):
            ter.detect_schema_version(unknown)

    def test_blank_row_cannot_truncate_later_data(self):
        rows = [
            new_row("01-Apr-2026")[0],
            [None] * len(NEW_HEADERS),
            new_row("02-Apr-2026")[0],
        ]

        with self.assertRaisesRegex(ValueError, "data rows after a blank row"):
            ter.parse_ter_workbook(workbook_bytes(NEW_HEADERS, rows))


class TerValidationTests(TestCase):
    def test_rejects_invalid_dates_out_of_month_and_nonnumeric_values(self):
        invalid_date = pd.DataFrame([old_row("01-May-2020")[0]], columns=OLD_HEADERS)
        with self.assertRaisesRegex(ValueError, "outside"):
            ter.build_ter_dataframe(invalid_date, "2020-04")

        invalid_number = pd.DataFrame(
            [old_row()[0]], columns=OLD_HEADERS, dtype=object
        )
        invalid_number.loc[0, "Regular Plan - Base TER (%)"] = "not-a-number"
        with self.assertRaisesRegex(ValueError, "nonnumeric"):
            ter.build_ter_dataframe(invalid_number, "2020-04")

        with self.assertRaisesRegex(ValueError, "empty"):
            ter.build_ter_dataframe(pd.DataFrame(columns=OLD_HEADERS), "2020-04")

    def test_null_nsdl_code_is_preserved(self):
        source = pd.DataFrame(old_row(), columns=OLD_HEADERS)
        result = ter.build_ter_dataframe(source, "2020-04")

        self.assertTrue(pd.isna(result.loc[0, "nsdl_scheme_code"]))
        self.assertEqual(result.loc[0, "source_month"], "2020-04")
        self.assertEqual(result.loc[0, "source_row_number"], 2)

    def test_no_write_is_possible_after_validation_failure(self):
        connection = FakeConnection()
        r2 = FakeR2()
        session = Mock()
        with patch.object(
            ter,
            "fetch_ter_xlsx",
            return_value=(b"bad", "https://example/ter.xlsx"),
        ), patch.object(
            ter, "parse_ter_workbook", side_effect=ValueError("bad schema")
        ), patch.object(ter, "save_to_parquet") as save:
            with self.assertRaisesRegex(ValueError, "bad schema"):
                ter._write_month(
                    connection,
                    r2,
                    date(2026, 4, 1),
                    session,
                    skip_existing_month=False,
                )
        save.assert_not_called()


class TerFetchTests(TestCase):
    def test_request_uses_all_funds_categories_types_and_excel(self):
        url, params = ter.build_ter_request("2026-04")

        self.assertEqual(params, {
            "MF_ID": "All",
            "Month": "04-2026",
            "strCat": "-1",
            "strType": "-1",
            "excel": "true",
        })
        self.assertIn("MF_ID=All", url)

    def test_retries_timeout_http_error_corrupt_and_non_xlsx(self):
        session = Mock()
        good = workbook_bytes(NEW_HEADERS, new_row())
        session.get.side_effect = [
            requests.Timeout("timeout"),
            FakeResponse(error=requests.HTTPError("500")),
            FakeResponse(content=b"<html>blocked</html>"),
            FakeResponse(content=good, url="https://example/good.xlsx"),
        ]
        with patch.object(ter.API, "MAX_RETRIES", 4), patch.object(
            ter.API, "RETRY_DELAY", 0
        ):
            content, source_url = ter.fetch_ter_xlsx("2026-04", session)

        self.assertEqual(content, good)
        self.assertEqual(source_url, "https://example/good.xlsx")
        self.assertEqual(session.get.call_count, 4)

    def test_all_invalid_responses_fail_after_retries(self):
        session = Mock()
        session.get.return_value = FakeResponse(content=b"not xlsx")
        with patch.object(ter.API, "MAX_RETRIES", 2), patch.object(
            ter.API, "RETRY_DELAY", 0
        ):
            with self.assertRaisesRegex(RuntimeError, "2026-04"):
                ter.fetch_ter_xlsx("2026-04", session)
        self.assertEqual(session.get.call_count, 2)


class TerResolutionTests(TestCase):
    def args(self, **overrides):
        values = {
            "month": None,
            "scheduled": False,
            "start_month": None,
            "end_month": None,
        }
        values.update(overrides)
        return SimpleNamespace(**values)

    def test_scheduled_resolution_handles_month_and_year_boundaries(self):
        self.assertEqual(
            ter.resolve_months(self.args(scheduled=True), date(2026, 5, 5)),
            [date(2026, 4, 1)],
        )
        self.assertEqual(
            ter.resolve_months(self.args(scheduled=True), date(2026, 5, 20)),
            [date(2026, 5, 1)],
        )
        self.assertEqual(
            ter.resolve_months(self.args(scheduled=True), date(2026, 1, 5)),
            [date(2025, 12, 1)],
        )

    def test_backfill_is_inclusive_and_rejects_invalid_ranges(self):
        self.assertEqual(
            ter.resolve_months(
                self.args(start_month="2020-04", end_month="2020-06")
            ),
            [date(2020, 4, 1), date(2020, 5, 1), date(2020, 6, 1)],
        )
        with self.assertRaises(ValueError):
            ter.resolve_months(
                self.args(start_month="2020-06", end_month="2020-04")
            )
        with self.assertRaises(ValueError):
            ter.resolve_months(self.args(month="2020-03"))


class TerStorageTests(TestCase):
    def test_backfill_ignores_unrelated_objects_and_skips_existing_month(self):
        connection = FakeConnection([
            "r2://bucket/mutual_funds/ter/ter_202004_snapshot_20260824.parquet",
            "r2://bucket/mutual_funds/ter/ter_202004_notes.parquet",
            "r2://bucket/mutual_funds/ter/other_202004_snapshot_20260824.parquet",
        ])
        existing = ter.get_existing_ter_snapshots(connection, FakeR2(), "2020-04")

        self.assertEqual(existing, [connection.objects[0]])

    def test_run_months_stops_on_first_failure(self):
        connection = FakeConnection()
        r2 = FakeR2()
        session = Mock()
        with patch.object(ter, "get_existing_ter_snapshots", return_value=[]), \
                patch.object(
                    ter,
                    "_write_month",
                    side_effect=["first", RuntimeError("failed month")],
                ) as write:
            with self.assertRaisesRegex(RuntimeError, "failed month"):
                ter.run_months(
                    [date(2020, 4, 1), date(2020, 5, 1), date(2020, 6, 1)],
                    connection,
                    r2,
                    session,
                    backfill=True,
                )
        self.assertEqual(write.call_count, 2)

    def test_save_writer_receives_zstandard(self):
        connection = FakeConnection()
        r2 = FakeR2()
        session = Mock()
        content = workbook_bytes(NEW_HEADERS, new_row())
        with patch.object(ter, "get_existing_ter_snapshots", return_value=[]), \
                patch.object(
                    ter, "fetch_ter_xlsx", return_value=(content, "url")
                ), patch.object(ter, "save_to_parquet") as save, patch.object(
                    ter,
                    "_utc_now",
                    return_value=datetime(2026, 8, 24, tzinfo=timezone.utc),
                ):
            path = ter._write_month(
                connection,
                r2,
                date(2026, 4, 1),
                session,
                skip_existing_month=False,
            )

        self.assertIn("ter_202604_snapshot_20260824.parquet", path)
        self.assertEqual(save.call_args.kwargs["compression"], "zstd")

    def test_same_day_object_is_idempotent(self):
        connection = FakeConnection([
            "r2://bucket/mutual_funds/ter/ter_202604_snapshot_20260824.parquet"
        ])
        session = Mock()
        with patch.object(
            ter,
            "_utc_now",
            return_value=datetime(2026, 8, 24, tzinfo=timezone.utc),
        ), patch.object(ter, "fetch_ter_xlsx") as fetch, patch.object(
            ter, "save_to_parquet"
        ) as save:
            result = ter._write_month(
                connection,
                FakeR2(),
                date(2026, 4, 1),
                session,
                skip_existing_month=False,
            )

        self.assertIsNone(result)
        fetch.assert_not_called()
        save.assert_not_called()

    def test_main_closes_r2_and_http_connections_on_success_and_failure(self):
        for run_error in (None, RuntimeError("failure")):
            connection = FakeConnection()
            r2 = FakeR2()
            r2.connection = connection
            session = Mock()
            args = SimpleNamespace(
                month="2026-04",
                scheduled=False,
                start_month=None,
                end_month=None,
            )
            with patch.object(ter, "parse_args", return_value=args), patch.object(
                ter, "R2", return_value=r2
            ), patch.object(
                ter.requests, "Session", return_value=session
            ), patch.object(ter, "run_months", side_effect=run_error):
                result = ter.main()

            self.assertEqual(result, 0 if run_error is None else 1)
            self.assertTrue(connection.closed)
            session.close.assert_called_once()
