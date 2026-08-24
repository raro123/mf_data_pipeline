"""Tests for the weekly AMFI AMC-member snapshot extractor."""

import json
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import Mock, patch

import pandas as pd
import requests

from scripts import extract_amc_members


SNAPSHOT_AT = datetime(2026, 8, 24, 10, 30, tzinfo=timezone.utc)


class FakeResponse:
    """Minimal requests response used by fetch retry tests."""

    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error

    def json(self):
        return self.payload


class AmcMemberSnapshotTests(TestCase):
    def setUp(self):
        self.listing = [
            {"mf_id": "62", "mf_name": "First Fund", "new_field": "kept"},
            {"mf_id": "90", "mf_name": "Second Fund"},
        ]
        self.details = {
            "62": {"mf_id": 62, "address": "Mumbai"},
            "90": {"mf_id": 90, "address": "Bengaluru"},
        }
        self.social = [{"MF_ID": "62", "x_name": "first"}]

    def test_builds_member_rows_and_preserves_source_json(self):
        result = extract_amc_members.build_snapshot_dataframe(
            self.listing,
            self.details,
            self.social,
            SNAPSHOT_AT,
        )

        self.assertEqual(result["member_id"].tolist(), ["62", "90"])
        self.assertEqual(result["snapshot_fetched_at"].nunique(), 1)
        self.assertEqual(
            result["snapshot_fetched_at"].iloc[0],
            pd.Timestamp(SNAPSHOT_AT),
        )
        self.assertEqual(
            json.loads(result["listing_record_json"].iloc[0])["new_field"],
            "kept",
        )
        self.assertEqual(
            json.loads(result["detail_record_json"].iloc[0])["address"],
            "Mumbai",
        )
        self.assertIsNone(result["social_record_json"].iloc[1])

    def test_duplicate_listing_ids_are_rejected(self):
        payload = {"AMFIMembers": [{"mf_id": "62"}, {"mf_id": 62}]}

        with self.assertRaisesRegex(ValueError, "duplicate member IDs"):
            extract_amc_members.validate_listing_payload(payload)

    def test_duplicate_social_ids_are_rejected(self):
        social = [{"MF_ID": "62"}, {"MF_Id": 62}]

        with self.assertRaisesRegex(ValueError, "duplicates member ID 62"):
            extract_amc_members.build_snapshot_dataframe(
                self.listing,
                self.details,
                social,
                SNAPSHOT_AT,
            )

    def test_missing_detail_is_rejected(self):
        with self.assertRaisesRegex(ValueError, "member ID 90"):
            extract_amc_members.build_snapshot_dataframe(
                self.listing,
                {"62": self.details["62"]},
                self.social,
                SNAPSHOT_AT,
            )

    def test_mismatched_detail_id_is_rejected(self):
        details = {**self.details, "62": {"mf_id": 90}}

        with self.assertRaisesRegex(ValueError, "expected 62, received 90"):
            extract_amc_members.build_snapshot_dataframe(
                self.listing,
                details,
                self.social,
                SNAPSHOT_AT,
            )

    def test_fetch_json_retries_transient_failure(self):
        session = Mock()
        session.get.side_effect = [
            requests.ConnectionError("temporary"),
            FakeResponse({"ok": True}),
        ]

        with patch.object(extract_amc_members.API, "MAX_RETRIES", 2), \
                patch.object(extract_amc_members.API, "RETRY_DELAY", 0):
            result = extract_amc_members.fetch_json(session, "https://example")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(session.get.call_count, 2)

    def test_main_does_not_upload_an_invalid_snapshot(self):
        with patch.object(
            extract_amc_members,
            "fetch_amc_member_snapshot",
            side_effect=ValueError("invalid detail"),
        ), patch.object(extract_amc_members, "save_snapshot_to_r2") as save:
            result = extract_amc_members.main()

        self.assertEqual(result, 1)
        save.assert_not_called()

    def test_r2_filename_uses_utc_snapshot_timestamp(self):
        connection = Mock()
        r2 = Mock()
        r2.get_full_path.return_value = "r2://bucket/amc_members/file.parquet"
        r2.setup_connection.return_value = connection
        snapshot = pd.DataFrame({"member_id": ["62"]})

        with patch.object(extract_amc_members, "R2", return_value=r2), \
                patch.object(extract_amc_members, "save_to_parquet") as save:
            result = extract_amc_members.save_snapshot_to_r2(
                snapshot,
                SNAPSHOT_AT,
            )

        self.assertEqual(result, "r2://bucket/amc_members/file.parquet")
        r2.get_full_path.assert_called_once_with(
            "amc_members",
            "amc_members_20260824T103000Z",
        )
        save.assert_called_once_with(
            connection,
            "amc_member_snapshot",
            snapshot,
            "r2://bucket/amc_members/file.parquet",
        )
        connection.close.assert_called_once()
