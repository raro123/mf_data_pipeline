from datetime import date
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from scripts import fetch_daily_nav


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def fetchall(self):
        return self.rows


class FakeConnection:
    def __init__(self, object_paths=()):
        self.object_paths = object_paths
        self.closed = False

    def execute(self, query, params):
        if query != "SELECT file FROM glob(?)":
            raise AssertionError(f"Unexpected query: {query}")
        return FakeResult([(path,) for path in self.object_paths])

    def close(self):
        self.closed = True


class FakeR2:
    def __init__(self, connection=None):
        self.connection = connection

    def get_full_path(self, area, name, file_extension='parquet'):
        return f"r2://bucket/mutual_funds/{area}/{name}.{file_extension}"

    def setup_connection(self):
        return self.connection


class FetchDailyNavTests(TestCase):
    def test_parse_raw_nav_date_accepts_only_canonical_filenames(self):
        self.assertEqual(
            fetch_daily_nav.parse_raw_nav_date(
                "r2://bucket/mutual_funds/raw/nav_daily_20260612.parquet"
            ),
            pd.Timestamp("2026-06-12"),
        )
        self.assertIsNone(
            fetch_daily_nav.parse_raw_nav_date(
                "r2://bucket/mutual_funds/raw/nav_historical.parquet"
            )
        )
        self.assertIsNone(
            fetch_daily_nav.parse_raw_nav_date(
                "r2://bucket/mutual_funds/raw/nav_daily_20261340.parquet"
            )
        )
        self.assertIsNone(
            fetch_daily_nav.parse_raw_nav_date(
                "r2://bucket/mutual_funds/raw/nav_daily_latest.parquet"
            )
        )

    def test_latest_raw_date_ignores_unrelated_and_unordered_objects(self):
        connection = FakeConnection([
            "r2://bucket/mutual_funds/raw/nav_daily_20260610.parquet",
            "r2://bucket/mutual_funds/raw/nav_historical.parquet",
            "r2://bucket/mutual_funds/raw/nav_daily_20260612.parquet",
            "r2://bucket/mutual_funds/raw/nav_daily_20260611.parquet",
        ])

        result = fetch_daily_nav.get_latest_raw_nav_date(connection, FakeR2())

        self.assertEqual(result, pd.Timestamp("2026-06-12"))

    def test_missing_dates_skip_weekend_between_friday_and_monday(self):
        result = fetch_daily_nav.get_missing_dates(
            pd.Timestamp("2026-06-12"),
            through_date=date(2026, 6, 15),
        )

        self.assertEqual(result, ["20260615"])

    def test_explicit_date_bypasses_raw_object_discovery(self):
        args = SimpleNamespace(date="20260610", bootstrap_date=None)
        connection = FakeConnection()

        result = fetch_daily_nav.resolve_dates_to_fetch(
            args, connection, FakeR2(), through_date=date(2026, 6, 15)
        )

        self.assertEqual(result, ["20260610"])

    def test_empty_prefix_requires_bootstrap_date(self):
        args = SimpleNamespace(date=None, bootstrap_date=None)

        with self.assertRaisesRegex(RuntimeError, "--bootstrap-date"):
            fetch_daily_nav.resolve_dates_to_fetch(
                args, FakeConnection(), FakeR2(),
                through_date=date(2026, 6, 15)
            )

    def test_bootstrap_date_is_inclusive_and_skips_weekend(self):
        args = SimpleNamespace(date=None, bootstrap_date="20260612")

        result = fetch_daily_nav.resolve_dates_to_fetch(
            args, FakeConnection(), FakeR2(), through_date=date(2026, 6, 15)
        )

        self.assertEqual(result, ["20260612", "20260615"])

    def test_no_missing_dates_does_not_call_amfi(self):
        today_str = date.today().strftime("%Y%m%d")
        connection = FakeConnection([
            f"r2://bucket/mutual_funds/raw/nav_daily_{today_str}.parquet"
        ])
        r2 = FakeR2(connection)
        args = SimpleNamespace(date=None, bootstrap_date=None)

        with patch.object(fetch_daily_nav, "parse_args", return_value=args), \
                patch.object(fetch_daily_nav, "R2", return_value=r2), \
                patch.object(fetch_daily_nav, "fetch_daily_nav_data") as fetch:
            result = fetch_daily_nav.main()

        self.assertTrue(result)
        fetch.assert_not_called()
        self.assertTrue(connection.closed)

    def test_default_cutoff_does_not_request_today(self):
        yesterday = pd.Timestamp(date.today()) - pd.Timedelta(days=1)

        result = fetch_daily_nav.get_missing_dates(yesterday)

        self.assertEqual(result, [])

    def test_failed_fetch_stops_before_cleaning_or_writing(self):
        connection = FakeConnection()
        r2 = FakeR2(connection)
        args = SimpleNamespace(date="20260612", bootstrap_date=None)

        with patch.object(fetch_daily_nav, "parse_args", return_value=args), \
                patch.object(fetch_daily_nav, "R2", return_value=r2), \
                patch.object(
                    fetch_daily_nav, "fetch_daily_nav_data", return_value=None
                ), \
                patch.object(fetch_daily_nav, "clean_nav_dataframe") as clean, \
                patch.object(fetch_daily_nav, "save_to_parquet") as save:
            result = fetch_daily_nav.main()

        self.assertFalse(result)
        clean.assert_not_called()
        save.assert_not_called()
        self.assertTrue(connection.closed)
