#!/usr/bin/env python3
"""Repair NAV observations that AMFI published after their daily snapshot.

The command is dry-run only unless ``--write`` is supplied. Repair objects use
noncanonical filenames so they are ingested by the datalake without advancing
the daily raw-object watermark.
"""

import argparse
from datetime import date, datetime, timezone
from pathlib import PurePosixPath
from typing import Optional

import numpy as np
import pandas as pd

from config.settings import R2
from scripts.fetch_daily_nav import parse_raw_nav_date
from scripts.fetch_historical_nav import daterange_chunks, fetch_nav_data
from utils.nav_helpers import NAV_COLUMN_MAPPING, clean_nav_dataframe, save_to_parquet


KEY_COLUMNS = ["scheme_code", "date"]
NAV_OUTPUT_COLUMNS = list(NAV_COLUMN_MAPPING.values())


def parse_args():
    """Parse repair date bounds and the explicit write opt-in."""
    parser = argparse.ArgumentParser(
        description="Find and optionally repair NAV rows reported late by AMFI."
    )
    parser.add_argument(
        "--start", required=True, help="First NAV date to inspect (YYYYMMDD)"
    )
    parser.add_argument(
        "--end", required=True, help="Last NAV date to inspect (YYYYMMDD)"
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="Upload missing rows to R2; otherwise run a read-only preview",
    )
    return parser.parse_args()


def parse_repair_range(start: str, end: str, today: Optional[date] = None):
    """Parse and validate an inclusive repair range.

    Args:
        start: First date in YYYYMMDD format.
        end: Last date in YYYYMMDD format.
        today: Date used to enforce the completed-publication cutoff.

    Returns:
        The inclusive start and end dates as pandas timestamps.

    Raises:
        ValueError: If the values are invalid, reversed, or include today.
    """
    try:
        start_date = pd.Timestamp(datetime.strptime(start, "%Y%m%d").date())
        end_date = pd.Timestamp(datetime.strptime(end, "%Y%m%d").date())
    except ValueError as exc:
        raise ValueError("Repair dates must use YYYYMMDD format") from exc

    if start_date > end_date:
        raise ValueError("Repair start date must not be after end date")

    latest_allowed = pd.Timestamp((today or date.today())) - pd.Timedelta(days=1)
    if end_date > latest_allowed:
        raise ValueError(
            f"Repair end date must be no later than yesterday "
            f"({latest_allowed:%Y%m%d})"
        )

    return start_date, end_date


def fetch_repair_source(start: pd.Timestamp, end: pd.Timestamp) -> pd.DataFrame:
    """Fetch an AMFI range using the configured historical chunk size."""
    frames = []
    for chunk_start, chunk_end in daterange_chunks(
        start.strftime("%Y%m%d"), end.strftime("%Y%m%d")
    ):
        frame = fetch_nav_data(chunk_start, chunk_end)
        if frame is None or frame.empty:
            raise RuntimeError(
                f"AMFI returned no valid data for {chunk_start}-{chunk_end}"
            )
        frames.append(frame)

    return pd.concat(frames, ignore_index=True)


def prepare_repair_candidates(
    raw_df: pd.DataFrame,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> tuple[pd.DataFrame, int]:
    """Normalize AMFI rows and return valid weekday candidates plus exclusions."""
    if raw_df is None or raw_df.empty:
        raise ValueError("AMFI repair response is empty")

    clean_df = clean_nav_dataframe(raw_df)
    if clean_df["date"].isna().any():
        raise ValueError("AMFI repair response contains invalid date values")

    outside_range = clean_df["date"].lt(start) | clean_df["date"].gt(end)
    if outside_range.any():
        invalid_dates = sorted(
            clean_df.loc[outside_range, "date"].dt.strftime("%Y-%m-%d").unique()
        )
        raise ValueError(
            "AMFI repair response contains dates outside the requested range: "
            + ", ".join(invalid_dates)
        )

    invalid_nav = (
        clean_df["nav"].isna()
        | ~np.isfinite(clean_df["nav"])
        | clean_df["nav"].le(0)
    )
    valid_df = clean_df.loc[~invalid_nav]

    duplicates = valid_df.duplicated(KEY_COLUMNS, keep=False)
    if duplicates.any():
        duplicate_count = (
            valid_df.loc[duplicates, KEY_COLUMNS].drop_duplicates().shape[0]
        )
        raise ValueError(
            "AMFI repair response contains "
            f"{duplicate_count} duplicate scheme/date key(s)"
        )

    candidates = (
        valid_df.loc[valid_df["date"].dt.weekday.lt(5), NAV_OUTPUT_COLUMNS]
        .sort_values(KEY_COLUMNS)
        .reset_index(drop=True)
    )
    return candidates, int(invalid_nav.sum())


def get_raw_nav_objects(connection, r2: R2) -> list[str]:
    """Return every raw Parquet object used by the datalake NAV loader."""
    raw_glob = r2.get_full_path("raw", "*")
    return [
        row[0]
        for row in connection.execute(
            "SELECT file FROM glob(?)", [raw_glob]
        ).fetchall()
    ]


def validate_daily_snapshot_coverage(
    object_paths: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> None:
    """Require an existing canonical daily snapshot for every repair weekday."""
    expected_dates = set(pd.bdate_range(start, end))
    available_dates = {
        parsed_date
        for path in object_paths
        if (parsed_date := parse_raw_nav_date(path)) is not None
    }
    missing_dates = sorted(expected_dates - available_dates)
    if missing_dates:
        formatted = ", ".join(
            nav_date.strftime("%Y-%m-%d") for nav_date in missing_dates
        )
        raise RuntimeError(
            "Canonical daily snapshots are missing for: "
            f"{formatted}. Use fetch_daily_nav gap recovery instead."
        )


def object_overlaps_range(
    connection,
    object_path: str,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> bool:
    """Check a noncanonical Parquet object's date bounds from footer metadata."""
    minimum, maximum = connection.execute(
        """
        SELECT
            MIN(TRY_CAST(stats_min_value AS DATE)),
            MAX(TRY_CAST(stats_max_value AS DATE))
        FROM parquet_metadata(?)
        WHERE path_in_schema = 'date'
        """,
        [object_path],
    ).fetchone()
    if minimum is None or maximum is None:
        return True
    return minimum <= end.date() and maximum >= start.date()


def select_comparison_objects(
    connection,
    object_paths: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> list[str]:
    """Select only raw objects that can contain dates in the repair range."""
    selected = []
    for path in object_paths:
        snapshot_date = parse_raw_nav_date(path)
        if snapshot_date is not None:
            if start <= snapshot_date <= end:
                selected.append(path)
            continue

        filename = PurePosixPath(path).name
        if filename.startswith("nav_repair_missing_") or object_overlaps_range(
            connection, path, start, end
        ):
            selected.append(path)

    return selected


def read_existing_nav_rows(
    connection,
    object_paths: list[str],
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> pd.DataFrame:
    """Read existing raw NAV keys and values for the bounded repair range."""
    if not object_paths:
        raise RuntimeError("No raw NAV objects found; use daily bootstrap instead")

    return connection.execute(
        """
        SELECT DISTINCT
            scheme_code::VARCHAR AS scheme_code,
            date::DATE AS date,
            nav::DOUBLE AS nav
        FROM read_parquet(?, union_by_name=true)
        WHERE date::DATE BETWEEN ? AND ?
          AND scheme_code IS NOT NULL
          AND date IS NOT NULL
          AND nav IS NOT NULL
        """,
        [object_paths, start.date(), end.date()],
    ).fetchdf()


def classify_candidates(
    candidates: pd.DataFrame,
    existing_rows: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, int]:
    """Separate missing rows from existing rows and NAV restatements.

    Args:
        candidates: Validated current AMFI observations.
        existing_rows: Raw observations already stored for the same period.

    Returns:
        Missing rows, restatement details, and the exact-existing row count.
    """
    existing = existing_rows.copy()
    if existing.empty:
        return candidates.copy(), pd.DataFrame(), 0

    existing["date"] = pd.to_datetime(existing["date"])
    existing["scheme_code"] = existing["scheme_code"].astype(str)
    summaries = (
        existing.groupby(KEY_COLUMNS, as_index=False)["nav"]
        .agg(lambda values: tuple(sorted(set(values))))
        .rename(columns={"nav": "existing_navs"})
    )
    classified = candidates.merge(summaries, on=KEY_COLUMNS, how="left")
    key_exists = classified["existing_navs"].notna()
    exact_match = classified.apply(
        lambda row: (
            row["nav"] in row["existing_navs"]
            if isinstance(row["existing_navs"], tuple)
            else False
        ),
        axis=1,
    )

    missing = classified.loc[~key_exists, NAV_OUTPUT_COLUMNS].reset_index(drop=True)
    conflict_mask = key_exists & ~exact_match
    conflicts = classified.loc[
        conflict_mask, KEY_COLUMNS + ["nav", "existing_navs"]
    ].rename(columns={"nav": "amfi_nav"}).reset_index(drop=True)
    exact_count = int((key_exists & exact_match).sum())
    return missing, conflicts, exact_count


def build_repair_object_name(
    start: pd.Timestamp,
    end: pd.Timestamp,
    generated_at: Optional[datetime] = None,
) -> str:
    """Build a unique noncanonical repair object name without extension."""
    timestamp = generated_at or datetime.now(timezone.utc)
    stamp = timestamp.astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"nav_repair_missing_{start:%Y%m%d}_{end:%Y%m%d}_{stamp}"


def object_exists(connection, object_path: str) -> bool:
    """Return whether an exact R2 object path already exists."""
    parent_glob = f"{object_path.rsplit('/', 1)[0]}/*.parquet"
    paths = connection.execute(
        "SELECT file FROM glob(?)", [parent_glob]
    ).fetchall()
    return any(path == object_path for path, in paths)


def print_summary(
    candidates: pd.DataFrame,
    missing: pd.DataFrame,
    conflicts: pd.DataFrame,
    exact_count: int,
    excluded_invalid_nav_count: int,
) -> None:
    """Print a concise repair preview."""
    print(f"AMFI weekday observations: {len(candidates):,}")
    print(
        "Invalid/non-positive NAV observations excluded: "
        f"{excluded_invalid_nav_count:,}"
    )
    print(f"Already stored with the same NAV: {exact_count:,}")
    print(f"Missing observations: {len(missing):,}")
    print(f"Existing keys with a different NAV (skipped): {len(conflicts):,}")

    if not missing.empty:
        counts = missing.groupby("date").size()
        print("Missing observations by date:")
        for nav_date, count in counts.items():
            print(f"  {nav_date:%Y-%m-%d}: {count:,}")

    if not conflicts.empty:
        print("Restatement sample (not written):")
        print(conflicts.head(10).to_string(index=False))


def main() -> bool:
    """Preview or write a bounded missing-only NAV repair."""
    args = parse_args()
    connection = None

    try:
        start, end = parse_repair_range(args.start, args.end)
        r2 = R2()
        connection = r2.setup_connection()

        raw_df = fetch_repair_source(start, end)
        candidates, excluded_invalid_nav_count = prepare_repair_candidates(
            raw_df, start, end
        )
        print(f"Validated {len(candidates):,} weekday NAV observations.", flush=True)
        object_paths = get_raw_nav_objects(connection, r2)
        print(f"Discovered {len(object_paths):,} raw NAV objects.", flush=True)
        validate_daily_snapshot_coverage(object_paths, start, end)
        print("Canonical daily snapshot coverage is complete.", flush=True)
        comparison_paths = select_comparison_objects(
            connection, object_paths, start, end
        )
        print(
            f"Selected {len(comparison_paths):,} overlapping objects for comparison.",
            flush=True,
        )
        existing_rows = read_existing_nav_rows(
            connection, comparison_paths, start, end
        )
        print(
            f"Read {len(existing_rows):,} existing observations for comparison.",
            flush=True,
        )
        missing, conflicts, exact_count = classify_candidates(
            candidates, existing_rows
        )
        print_summary(
            candidates,
            missing,
            conflicts,
            exact_count,
            excluded_invalid_nav_count,
        )

        if not args.write:
            print("Dry run only. Re-run with --write to upload missing rows.")
            return True

        if missing.empty:
            print("No missing observations to write.")
            return True

        object_name = build_repair_object_name(start, end)
        repair_path = r2.get_full_path("raw", object_name)
        if object_exists(connection, repair_path):
            raise RuntimeError(f"Repair object already exists: {repair_path}")

        save_to_parquet(connection, object_name, missing, repair_path)
        written_count = (
            connection.read_parquet(repair_path).count("*").fetchone()[0]
        )
        if written_count != len(missing):
            raise RuntimeError(
                f"Repair verification failed: wrote {written_count:,} of "
                f"{len(missing):,} rows"
            )

        print(f"Uploaded {written_count:,} missing observations to {repair_path}")
        print("Run the datalake mf-daily workflow to ingest the repair object.")
        return True

    except Exception as exc:
        print(f"Repair failed: {exc}")
        return False
    finally:
        if connection is not None:
            connection.close()


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
