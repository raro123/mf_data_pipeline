#!/usr/bin/env python3
"""
Daily NAV Data Fetcher

Fetches current NAV data from AMFI with gap-filling and weekend skip logic.
This script has been refactored to use centralized configuration and logging.
"""

import argparse
import re
import time
from datetime import datetime, date, timedelta
from io import StringIO
from pathlib import PurePosixPath

import pandas as pd
import requests

from config.settings import R2, API
from utils.nav_helpers import clean_nav_dataframe, save_to_parquet


RAW_NAV_FILENAME_PATTERN = re.compile(r"^nav_daily_(\d{8})\.parquet$")


def parse_args():
    parser = argparse.ArgumentParser(description='Fetch daily NAV data from AMFI')
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument(
        '--date', type=str,
        help='Specific date to fetch (YYYYMMDD format); bypasses gap detection')
    date_group.add_argument(
        '--bootstrap-date', type=str,
        help='First date to fetch when no raw daily NAV objects exist (YYYYMMDD)')
    return parser.parse_args()


def parse_raw_nav_date(object_path: str):
    """Extract a date from a canonical raw daily NAV object path."""
    filename = PurePosixPath(object_path).name
    match = RAW_NAV_FILENAME_PATTERN.fullmatch(filename)
    if not match:
        return None
    try:
        return pd.Timestamp(datetime.strptime(match.group(1), '%Y%m%d').date())
    except ValueError:
        return None


def get_latest_raw_nav_date(connection, r2: R2):
    """Return the latest date represented by raw daily NAV object names."""
    raw_glob = r2.get_full_path('raw', 'nav_daily_*')
    object_paths = [
        row[0]
        for row in connection.execute(
            "SELECT file FROM glob(?)", [raw_glob]
        ).fetchall()
    ]
    raw_dates = [
        parsed_date
        for path in object_paths
        if (parsed_date := parse_raw_nav_date(path)) is not None
    ]
    return max(raw_dates, default=None)


def fetch_daily_nav_data(start_date_str: str) -> pd.DataFrame:
    """
    Fetch NAV data for a date range from AMFI API.

    Args:
        start_date_str: Start date in YYYYMMDD format

    Returns:
        pandas.DataFrame: NAV data or None if failed
    """
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'
    params = {'frmdt': start_date.strftime('%d-%b-%Y')}

    retries = 0
    max_retries = API.MAX_RETRIES

    while retries < max_retries:
        try:
            print(f"Fetching data: {start_date_str} (attempt {retries + 1})")
            response = requests.get(url, params=params, timeout=API.AMFI_NAV_TIMEOUT)
            response.raise_for_status()
            df = pd.read_csv(StringIO(response.text), sep=";")

            if df.empty or len(df.columns) < 3:
                print(f"No valid data for {start_date_str}")
                return None

            print(f"Fetched {len(df):,} records for {start_date_str}")
            return df

        except requests.exceptions.Timeout:
            print(f"Timeout for {start_date_str} (attempt {retries + 1})")
        except requests.exceptions.RequestException as e:
            print(f"Request failed for {start_date_str}: {e} (attempt {retries + 1})")
        except Exception as e:
            print(f"Unexpected error for {start_date_str}: {e}")
            break

        retries += 1
        if retries < max_retries:
            print(f"Retrying in {API.RETRY_DELAY} seconds...")
            time.sleep(API.RETRY_DELAY)

    print(f"Failed to fetch data after {max_retries} attempts: {start_date_str}")
    return None


def is_weekend(check_date):
    """Check if the given date is a weekend (Saturday=5, Sunday=6)."""
    return check_date.weekday() >= 5


def get_missing_dates(latest_historical_date, through_date=None):
    """
    Get missing weekdays through the latest expected published date.

    By default, the cutoff is yesterday because the scheduled morning run
    collects the prior day's completed AMFI NAV data.

    Excludes weekends as markets are closed.

    Args:
        latest_historical_date: Latest date in historical data

    Returns:
        list: List of missing dates (excluding weekends)
    """
    if latest_historical_date is None:
        raise ValueError("latest_historical_date is required")

    missing_dates = []
    current_date = pd.Timestamp(latest_historical_date) + timedelta(days=1)
    cutoff = pd.Timestamp(through_date or (date.today() - timedelta(days=1)))

    while current_date <= cutoff:
        if not is_weekend(current_date):
            missing_dates.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    return missing_dates


def resolve_dates_to_fetch(args, connection, r2: R2, through_date=None):
    """Resolve explicit or gap-filled dates without reading clean NAV output."""
    if args.date:
        datetime.strptime(args.date, '%Y%m%d')
        return [args.date]

    latest_raw_date = get_latest_raw_nav_date(connection, r2)
    if latest_raw_date is None:
        if not args.bootstrap_date:
            raise RuntimeError(
                "No raw daily NAV objects found. Re-run with "
                "--bootstrap-date YYYYMMDD to initialize the prefix."
            )
        bootstrap_date = pd.Timestamp(
            datetime.strptime(args.bootstrap_date, '%Y%m%d').date()
        )
        latest_raw_date = bootstrap_date - timedelta(days=1)
        print(f"No raw watermark found; bootstrapping from {args.bootstrap_date}")
    else:
        print(f"Latest raw NAV watermark: {latest_raw_date:%Y-%m-%d}")

    return get_missing_dates(latest_raw_date, through_date=through_date)


def main():
    args = parse_args()
    conn = None

    try:
        r2 = R2()
        conn = r2.setup_connection()

        dates = resolve_dates_to_fetch(args, conn, r2)
        if not dates:
            print("No missing weekday NAV dates to fetch.")
            return True

        print(f"Dates to fetch ({len(dates)}): {', '.join(dates)}")

        for date_str in dates:
            raw_df = fetch_daily_nav_data(start_date_str=date_str)
            if raw_df is None or raw_df.empty:
                print(f"Stopping at {date_str}; no raw NAV data was fetched.")
                return False

            clean_df = clean_nav_dataframe(raw_df)
            if clean_df.empty:
                print(f"Stopping at {date_str}; fetched NAV data contained no valid rows.")
                return False

            daily_path = r2.get_full_path('raw', f'nav_daily_{date_str}')
            save_to_parquet(conn, f'nav_daily_raw_{date_str}', clean_df, daily_path)
            print(f"Successfully created daily NAV Parquet file at {daily_path}")
            print(conn.read_parquet(daily_path).limit(5))

    except Exception as e:
        print(f"Error during processing: {e}")
        return False
    finally:
        if conn is not None:
            conn.close()
    return True


if __name__ == "__main__":
    raise SystemExit(0 if main() else 1)
