#!/usr/bin/env python3
"""
Scheme-wise Average AUM Data Fetcher

Fetches scheme-wise Average AUM data from AMFI API with configurable historical depth.
Supports fetching specific FY/period combinations or a range of years.

Usage:
    python -m scripts.fetch_aum_data              # Fetch last 5 years (default)
    python -m scripts.fetch_aum_data --years 3    # Fetch last 3 years
    python -m scripts.fetch_aum_data --fy 1 --period 1  # Specific FY and quarter
"""

import argparse
import requests
import pandas as pd
import time
from datetime import datetime
from typing import Optional

from config.settings import R2, API, Paths
from utils.nav_helpers import save_to_parquet


def parse_args():
    parser = argparse.ArgumentParser(description='Fetch scheme-wise AUM data from AMFI')
    parser.add_argument('--years', type=int, default=5,
                        help='Number of years to fetch (default: 5)')
    parser.add_argument('--fy', type=int, help='Specific financial year ID (1=current, 2=previous)')
    parser.add_argument('--period', type=int, help='Specific period/quarter ID (1-4)')
    return parser.parse_args()


def fetch_aum_api(fy_id: int, period_id: Optional[int] = None) -> dict:
    """
    Fetch AUM data from AMFI API.

    Args:
        fy_id: Financial year ID (1=current, 2=previous, etc.)
        period_id: Optional period/quarter ID. If None, fetches mapping info only.

    Returns:
        JSON response as dict, or empty dict on failure
    """
    params = {
        'fyId': fy_id,
        'strType': 'Typewise',
        'MF_ID': 0
    }
    if period_id is not None:
        params['periodId'] = period_id

    retries = 0
    while retries < API.MAX_RETRIES:
        try:
            period_desc = f"FY={fy_id}" + (f", Period={period_id}" if period_id else " (mapping)")
            print(f"Fetching AUM: {period_desc} (attempt {retries + 1})")

            response = requests.get(
                API.AMFI_AUM_URL,
                params=params,
                timeout=API.AMFI_AUM_TIMEOUT
            )
            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout:
            print(f"Timeout for {period_desc} (attempt {retries + 1})")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e} (attempt {retries + 1})")
        except Exception as e:
            print(f"Unexpected error: {e}")
            break

        retries += 1
        if retries < API.MAX_RETRIES:
            print(f"Retrying in {API.RETRY_DELAY} seconds...")
            time.sleep(API.RETRY_DELAY)

    print(f"Failed after {API.MAX_RETRIES} attempts")
    return {}


def flatten_aum_response(data: dict, fy_label: str, period_label: str) -> pd.DataFrame:
    """
    Flatten nested AUM JSON response into a DataFrame.

    Args:
        data: API response containing nested scheme data
        fy_label: Financial year label (e.g., "April 2024 - March 2025")
        period_label: Period label (e.g., "October - December 2024")

    Returns:
        DataFrame with flattened scheme AUM data
    """
    rows = []
    aum_data = data.get('data', [])

    for mf_entry in aum_data:
        mf_name = mf_entry.get('Mfname', '')
        scheme_type = mf_entry.get('SchemeType_Desc', '')

        for scheme in mf_entry.get('schemes', []):
            aum_values = scheme.get('AverageAumForTheMonth', {})
            rows.append({
                'scheme_code': str(scheme.get('AMFI_Code', '')),
                'scheme_name': scheme.get('SchemeNAVName', ''),
                'mf_name': mf_name,
                'scheme_type': scheme_type,
                'aum_excl_fof': aum_values.get('ExcludingFundOfFundsDomesticButIncludingFundOfFundsOverseas', 0.0),
                'aum_fof_domestic': aum_values.get('FundOfFundsDomestic', 0.0),
                'financial_year': fy_label,
                'period': period_label,
            })

    return pd.DataFrame(rows)


def fetch_all_aum_data(num_years: int, specific_fy: Optional[int] = None,
                       specific_period: Optional[int] = None) -> pd.DataFrame:
    """
    Fetch AUM data for specified range of financial years.

    Makes one initial mapping call to build FY label lookup, then fetches
    periods per FY and uses selectedPeriod from data responses as period labels.

    Args:
        num_years: Number of years to fetch
        specific_fy: If provided, fetch only this FY
        specific_period: If provided, fetch only this period

    Returns:
        Combined DataFrame with all AUM data
    """
    all_data = []

    # Determine FY range
    if specific_fy is not None:
        fy_ids = [specific_fy]
    else:
        fy_ids = list(range(1, num_years + 1))

    # One initial mapping call to build FY label lookup and cache FY1's periods
    initial_mapping = fetch_aum_api(1, period_id=None)
    if not initial_mapping:
        print("Could not fetch initial mapping, aborting.")
        return pd.DataFrame()

    fy_labels = {fy['id']: fy['financial_year'] for fy in initial_mapping.get('years', [])}
    # Cache FY1's periods from the initial call
    periods_cache = {1: initial_mapping.get('data', {}).get('periods', [])}

    for fy_id in fy_ids:
        fy_label = fy_labels.get(fy_id)
        if not fy_label:
            print(f"No FY label for ID {fy_id}, skipping...")
            continue

        # Get periods: use cache for FY1, otherwise fetch mapping
        if fy_id in periods_cache:
            periods = periods_cache[fy_id]
        else:
            mapping = fetch_aum_api(fy_id, period_id=None)
            if not mapping:
                print(f"Could not fetch periods for {fy_label}, skipping...")
                continue
            periods = mapping.get('data', {}).get('periods', [])

        if not periods:
            print(f"No periods available for {fy_label}, skipping...")
            continue

        print(f"Processing: {fy_label} ({len(periods)} periods available)")

        # Determine which periods to fetch
        if specific_period is not None:
            # Validate the requested period exists
            if not any(p['id'] == specific_period for p in periods):
                print(f"Period {specific_period} not available for {fy_label}, skipping...")
                continue
            period_ids = [specific_period]
        else:
            period_ids = [p['id'] for p in periods]

        for period_id in period_ids:
            data = fetch_aum_api(fy_id, period_id)
            if not data or 'data' not in data:
                print(f"  No data for period {period_id}, may not be available yet")
                continue

            period_label = data.get('selectedPeriod', f'Period {period_id}')
            print(f"  Fetching: {period_label}")

            df = flatten_aum_response(data, fy_label, period_label)
            if not df.empty:
                print(f"  Retrieved {len(df):,} schemes")
                all_data.append(df)
            else:
                print(f"  No schemes found for {period_label}")

        time.sleep(0.5)  # brief pause between FYs

    if not all_data:
        return pd.DataFrame()

    return (pd.concat(all_data, ignore_index=True)
            .assign(fetched_at=datetime.now()))


def main():
    args = parse_args()

    print("=" * 60)
    print("Scheme-wise AUM Data Fetcher")
    print("=" * 60)

    # Fetch AUM data
    if args.fy is not None:
        print(f"Fetching specific FY ID: {args.fy}" +
              (f", Period: {args.period}" if args.period else ""))
        df = fetch_all_aum_data(1, specific_fy=args.fy, specific_period=args.period)
    else:
        print(f"Fetching last {args.years} years of AUM data...")
        df = fetch_all_aum_data(args.years)

    if df.empty:
        print("No data retrieved. Exiting.")
        return False

    # Summary statistics
    print("\n" + "=" * 60)
    print("Summary")
    print("=" * 60)
    print(f"Total records: {len(df):,}")
    print(f"Unique schemes: {df['scheme_code'].nunique():,}")
    print(f"AMCs covered: {df['mf_name'].nunique()}")
    print(f"FY-Period combinations: {df.groupby(['financial_year', 'period']).ngroups}")

    print("\nRecords by Financial Year:")
    print(df.groupby('financial_year').size().to_string())

    # Save locally with date stamp
    Paths.create_directories()
    date_stamp = datetime.now().strftime('%Y%m%d')
    local_path = Paths.AUM_SCHEMEWISE.parent / f"aum_schemewise_{date_stamp}.parquet"
    df.to_parquet(local_path, index=False)
    print(f"\nSaved locally: {local_path}")

    # Upload to R2
    try:
        r2 = R2()
        conn = r2.setup_connection()
        r2_path = r2.get_full_path('aum', f'aum_schemewise_{date_stamp}')
        save_to_parquet(conn, 'aum_schemewise', df, r2_path)
        print(f"Uploaded to R2: {r2_path}")
    except Exception as e:
        print(f"R2 upload failed: {e}")
        print("Data saved locally only.")

    print("\nDone!")
    return True


if __name__ == "__main__":
    main()
