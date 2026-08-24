from datetime import date, datetime, timezone
from types import SimpleNamespace
from unittest import TestCase
from unittest.mock import patch

import pandas as pd

from scripts import repair_missing_nav


def raw_nav_frame(rows):
    return pd.DataFrame(
        rows,
        columns=[
            "Scheme Code",
            "ISIN Div Payout/ISIN Growth",
            "ISIN Div Reinvestment",
            "Net Asset Value",
            "Date",
        ],
    )


def candidate_frame(rows):
    return pd.DataFrame(
        rows,
        columns=["scheme_code", "isin_growth", "isin_dividend", "nav", "date"],
    ).assign(date=lambda frame: pd.to_datetime(frame["date"]))


class FakeConnection:
    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True

    def read_parquet(self, path):
        return SimpleNamespace(
            count=lambda expression: SimpleNamespace(fetchone=lambda: (1,))
        )


class FakeGlobConnection:
    def __init__(self, paths):
        self.paths = paths

    def execute(self, query, params):
        if query != "SELECT file FROM glob(?)":
            raise AssertionError(f"Unexpected query: {query}")
        if params != ["r2://bucket/raw/*.parquet"]:
            raise AssertionError(f"Unexpected parameters: {params}")
        return SimpleNamespace(fetchall=lambda: [(path,) for path in self.paths])


class FakeR2:
    def __init__(self, connection):
        self.connection = connection

    def setup_connection(self):
        return self.connection

    def get_full_path(self, area, name, file_extension="parquet"):
        return f"r2://bucket/mutual_funds/{area}/{name}.{file_extension}"


class RepairMissingNavTests(TestCase):
    def test_repair_range_must_end_before_today(self):
        with self.assertRaisesRegex(ValueError, "no later than yesterday"):
            repair_missing_nav.parse_repair_range(
                "20260801", "20260824", today=date(2026, 8, 24)
            )

    def test_repair_range_rejects_reversed_dates(self):
        with self.assertRaisesRegex(ValueError, "must not be after"):
            repair_missing_nav.parse_repair_range(
                "20260823", "20260801", today=date(2026, 8, 24)
            )

    def test_candidates_exclude_weekends(self):
        raw = raw_nav_frame([
            ["122639", "INF879O01027", "-", "91.0", "14-Aug-2026"],
            ["122639", "INF879O01027", "-", "92.0", "15-Aug-2026"],
            ["122639", "INF879O01027", "-", "93.0", "17-Aug-2026"],
        ])

        result, invalid_count = repair_missing_nav.prepare_repair_candidates(
            raw, pd.Timestamp("2026-08-14"), pd.Timestamp("2026-08-17")
        )

        self.assertEqual(
            result["date"].dt.strftime("%Y-%m-%d").tolist(),
            ["2026-08-14", "2026-08-17"],
        )
        self.assertEqual(invalid_count, 0)

    def test_candidates_exclude_invalid_nav_observations(self):
        raw = raw_nav_frame([
            ["1", "A", "-", "10.0", "14-Aug-2026"],
            ["2", "B", "-", "0", "14-Aug-2026"],
            ["3", "C", "-", "-", "14-Aug-2026"],
        ])

        result, invalid_count = repair_missing_nav.prepare_repair_candidates(
            raw, pd.Timestamp("2026-08-14"), pd.Timestamp("2026-08-14")
        )

        self.assertEqual(result["scheme_code"].tolist(), ["1"])
        self.assertEqual(invalid_count, 2)

    def test_candidates_reject_dates_outside_requested_range(self):
        raw = raw_nav_frame([
            ["122639", "INF879O01027", "-", "91.0", "31-Jul-2026"],
        ])

        with self.assertRaisesRegex(ValueError, "outside the requested range"):
            repair_missing_nav.prepare_repair_candidates(
                raw, pd.Timestamp("2026-08-01"), pd.Timestamp("2026-08-23")
            )

    def test_candidates_reject_duplicate_scheme_date_keys(self):
        raw = raw_nav_frame([
            ["122639", "INF879O01027", "-", "91.0", "14-Aug-2026"],
            ["122639", "INF879O01027", "-", "91.0", "14-Aug-2026"],
        ])

        with self.assertRaisesRegex(ValueError, "duplicate scheme/date"):
            repair_missing_nav.prepare_repair_candidates(
                raw, pd.Timestamp("2026-08-01"), pd.Timestamp("2026-08-23")
            )

    def test_classification_repairs_only_absent_keys(self):
        candidates = candidate_frame([
            ["1", "A", "-", 10.0, "2026-08-17"],
            ["2", "B", "-", 20.0, "2026-08-17"],
            ["3", "C", "-", 31.0, "2026-08-17"],
        ])
        existing = pd.DataFrame([
            ["2", date(2026, 8, 17), 20.0],
            ["3", date(2026, 8, 17), 30.0],
        ], columns=["scheme_code", "date", "nav"])

        missing, conflicts, exact_count = repair_missing_nav.classify_candidates(
            candidates, existing
        )

        self.assertEqual(missing["scheme_code"].tolist(), ["1"])
        self.assertEqual(conflicts["scheme_code"].tolist(), ["3"])
        self.assertEqual(exact_count, 1)

    def test_repair_filename_is_unique_and_not_a_daily_watermark(self):
        result = repair_missing_nav.build_repair_object_name(
            pd.Timestamp("2026-08-01"),
            pd.Timestamp("2026-08-23"),
            datetime(2026, 8, 24, 12, 30, tzinfo=timezone.utc),
        )

        self.assertEqual(
            result,
            "nav_repair_missing_20260801_20260823_20260824T123000Z",
        )
        self.assertIsNone(
            repair_missing_nav.parse_raw_nav_date(f"{result}.parquet")
        )

    def test_object_exists_checks_parent_listing_for_exact_path(self):
        target = "r2://bucket/raw/nav_repair.parquet"

        self.assertTrue(
            repair_missing_nav.object_exists(
                FakeGlobConnection([target]), target
            )
        )
        self.assertFalse(
            repair_missing_nav.object_exists(
                FakeGlobConnection(["r2://bucket/raw/other.parquet"]), target
            )
        )

    def test_repair_requires_existing_daily_snapshots(self):
        paths = [
            "r2://bucket/mutual_funds/raw/nav_daily_20260817.parquet",
            "r2://bucket/mutual_funds/raw/nav_daily_20260819.parquet",
        ]

        with self.assertRaisesRegex(RuntimeError, "2026-08-18"):
            repair_missing_nav.validate_daily_snapshot_coverage(
                paths, pd.Timestamp("2026-08-17"), pd.Timestamp("2026-08-19")
            )

    def test_comparison_selects_only_in_range_daily_and_repair_objects(self):
        paths = [
            "r2://bucket/raw/nav_daily_20260731.parquet",
            "r2://bucket/raw/nav_daily_20260817.parquet",
            "r2://bucket/raw/nav_daily_20260818.parquet",
            "r2://bucket/raw/nav_repair_missing_20260801_20260823_run.parquet",
            "r2://bucket/raw/nav_historical.parquet",
        ]

        with patch.object(
            repair_missing_nav, "object_overlaps_range", return_value=False
        ):
            result = repair_missing_nav.select_comparison_objects(
                object(),
                paths,
                pd.Timestamp("2026-08-17"),
                pd.Timestamp("2026-08-18"),
            )

        self.assertEqual(result, paths[1:4])

    def test_dry_run_does_not_write(self):
        connection = FakeConnection()
        candidates = candidate_frame([
            ["1", "A", "-", 10.0, "2026-08-17"],
        ])
        args = SimpleNamespace(start="20260801", end="20260823", write=False)

        with patch.object(repair_missing_nav, "parse_args", return_value=args), \
                patch.object(
                    repair_missing_nav, "R2", return_value=FakeR2(connection)
                ), \
                patch.object(
                    repair_missing_nav,
                    "fetch_repair_source",
                    return_value=pd.DataFrame(),
                ), \
                patch.object(
                    repair_missing_nav,
                    "prepare_repair_candidates",
                    return_value=(candidates, 0),
                ), \
                patch.object(
                    repair_missing_nav, "get_raw_nav_objects", return_value=["raw"]
                ), \
                patch.object(
                    repair_missing_nav, "validate_daily_snapshot_coverage"
                ), \
                patch.object(
                    repair_missing_nav,
                    "select_comparison_objects",
                    return_value=["raw"],
                ), \
                patch.object(
                    repair_missing_nav,
                    "read_existing_nav_rows",
                    return_value=pd.DataFrame(
                        columns=["scheme_code", "date", "nav"]
                    ),
                ), \
                patch.object(repair_missing_nav, "save_to_parquet") as save:
            result = repair_missing_nav.main()

        self.assertTrue(result)
        save.assert_not_called()
        self.assertTrue(connection.closed)

    def test_write_with_no_missing_rows_is_a_noop(self):
        connection = FakeConnection()
        candidates = candidate_frame([
            ["1", "A", "-", 10.0, "2026-08-17"],
        ])
        existing = pd.DataFrame([
            ["1", date(2026, 8, 17), 10.0],
        ], columns=["scheme_code", "date", "nav"])
        args = SimpleNamespace(start="20260801", end="20260823", write=True)

        with patch.object(repair_missing_nav, "parse_args", return_value=args), \
                patch.object(
                    repair_missing_nav, "R2", return_value=FakeR2(connection)
                ), \
                patch.object(
                    repair_missing_nav,
                    "fetch_repair_source",
                    return_value=pd.DataFrame(),
                ), \
                patch.object(
                    repair_missing_nav,
                    "prepare_repair_candidates",
                    return_value=(candidates, 0),
                ), \
                patch.object(
                    repair_missing_nav, "get_raw_nav_objects", return_value=["raw"]
                ), \
                patch.object(
                    repair_missing_nav, "validate_daily_snapshot_coverage"
                ), \
                patch.object(
                    repair_missing_nav,
                    "select_comparison_objects",
                    return_value=["raw"],
                ), \
                patch.object(
                    repair_missing_nav,
                    "read_existing_nav_rows",
                    return_value=existing,
                ), \
                patch.object(repair_missing_nav, "save_to_parquet") as save:
            result = repair_missing_nav.main()

        self.assertTrue(result)
        save.assert_not_called()
        self.assertTrue(connection.closed)

    def test_write_uploads_only_missing_rows(self):
        connection = FakeConnection()
        candidates = candidate_frame([
            ["1", "A", "-", 10.0, "2026-08-17"],
            ["2", "B", "-", 20.0, "2026-08-17"],
        ])
        existing = pd.DataFrame([
            ["2", date(2026, 8, 17), 20.0],
        ], columns=["scheme_code", "date", "nav"])
        args = SimpleNamespace(start="20260801", end="20260823", write=True)

        with patch.object(repair_missing_nav, "parse_args", return_value=args), \
                patch.object(
                    repair_missing_nav, "R2", return_value=FakeR2(connection)
                ), \
                patch.object(
                    repair_missing_nav,
                    "fetch_repair_source",
                    return_value=pd.DataFrame(),
                ), \
                patch.object(
                    repair_missing_nav,
                    "prepare_repair_candidates",
                    return_value=(candidates, 0),
                ), \
                patch.object(
                    repair_missing_nav, "get_raw_nav_objects", return_value=["raw"]
                ), \
                patch.object(
                    repair_missing_nav, "validate_daily_snapshot_coverage"
                ), \
                patch.object(
                    repair_missing_nav,
                    "select_comparison_objects",
                    return_value=["raw"],
                ), \
                patch.object(
                    repair_missing_nav,
                    "read_existing_nav_rows",
                    return_value=existing,
                ), \
                patch.object(
                    repair_missing_nav,
                    "build_repair_object_name",
                    return_value="nav_repair_test",
                ), \
                patch.object(
                    repair_missing_nav, "object_exists", return_value=False
                ), \
                patch.object(repair_missing_nav, "save_to_parquet") as save:
            result = repair_missing_nav.main()

        self.assertTrue(result)
        written = save.call_args.args[2]
        self.assertEqual(written["scheme_code"].tolist(), ["1"])
        self.assertEqual(
            save.call_args.args[3],
            "r2://bucket/mutual_funds/raw/nav_repair_test.parquet",
        )
        self.assertTrue(connection.closed)
