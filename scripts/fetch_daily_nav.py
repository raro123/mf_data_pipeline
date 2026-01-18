#!/usr/bin/env python3
"""
Daily NAV Data Fetcher

Fetches current NAV data from AMFI with gap-filling and weekend skip logic.
This script has been refactored to use centralized configuration and logging.
"""

import argparse
import requests
import pandas as pd
import time
from datetime import datetime, date, timedelta
from io import StringIO
from config.settings import R2, API
from utils.nav_helpers import NAV_COLUMNS, clean_nav_dataframe, save_to_parquet


def parse_args():
    parser = argparse.ArgumentParser(description='Fetch daily NAV data from AMFI')
    parser.add_argument('--date', type=str, help='Specific date to fetch (YYYYMMDD format)')
    return parser.parse_args()


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


def get_missing_dates(latest_historical_date):
    """
    Get list of missing dates between latest historical and today.
    Excludes weekends as markets are closed.

    Args:
        latest_historical_date: Latest date in historical data

    Returns:
        list: List of missing dates (excluding weekends)
    """
    if latest_historical_date is None:
        return [pd.Timestamp(date.today())]

    missing_dates = []
    current_date = latest_historical_date + timedelta(days=1)
    today = pd.Timestamp(date.today())

    while current_date <= today:
        if not is_weekend(current_date):
            missing_dates.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    return missing_dates


def main():
    args = parse_args()

    try:
        r2 = R2()
        conn = r2.setup_connection()

        if args.date:
            dates = [args.date]  # use specific date provided
        else:
            # existing logic: find missing dates
            historical_path = r2.get_full_path('clean', 'nav_daily_growth_plan')
            max_date_available = conn.read_parquet(
                historical_path).max('date').execute().df().iloc[0, 0]
            dates = get_missing_dates(max_date_available)

        for date_str in dates:
            raw_df = fetch_daily_nav_data(start_date_str=date_str)
            clean_df = clean_nav_dataframe(raw_df)
            daily_path = r2.get_full_path('raw', f'nav_daily_{date_str}')
            save_to_parquet(conn, f'nav_daily_raw_{date_str}', clean_df, daily_path)
            print(f"Successfully created daily NAV Parquet file at {daily_path}")
            print(conn.read_parquet(daily_path).limit(5))

    except Exception as e:
        print(f"Error during processing: {e}")
        return False
    return True


if __name__ == "__main__":
    main()
