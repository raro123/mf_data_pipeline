#!/usr/bin/env python3
"""
Daily NAV Data Fetcher

Fetches current NAV data from AMFI with gap-filling and weekend skip logic.
This script has been refactored to use centralized configuration and logging.
"""

from config.settings import Paths, R2, API
import requests
import pandas as pd
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from io import StringIO
import time



# Import centralized configuration

col_select = ['Scheme Code', 'ISIN Div Payout/ISIN Growth',
              'ISIN Div Reinvestment', 'Net Asset Value', 'Date']


def fetch_daily_nav_data(start_date_str: str) -> pd.DataFrame:
    """
    Fetch NAV data for a date range from AMFI API.
    
    Args:
        start_date_str: Start date in YYYYMMDD format
        end_date_str: End date in YYYYMMDD format
        
    Returns:
        pandas.DataFrame: NAV data or None if failed
    """
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    #end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    # Use configured API settings
    url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'
    params = {
        #'tp': '1',
        'frmdt': start_date.strftime('%d-%b-%Y'),
        #'todt': end_date.strftime('%d-%b-%Y'),
    }
    
    retries = 0
    max_retries = API.MAX_RETRIES
    
    while retries < max_retries:
        try:
            print(f"📡 Fetching data: {start_date_str}  (attempt {retries + 1})")
            
            response = requests.get(url, params=params, timeout=API.AMFI_NAV_TIMEOUT)
            response.raise_for_status()
            
            # Parse CSV response
            df = pd.read_csv(StringIO(response.text), sep=";")
            
            # Basic validation
            if df.empty or len(df.columns) < 3:
                print(f"⚠️ No valid data for {start_date_str}")
                return None
                
            print(f"✅ Fetched {len(df):,} records for {start_date_str}")
            return df
            
        except requests.exceptions.Timeout:
            print(f"⏰ Timeout for {start_date_str}  (attempt {retries + 1})")
        except requests.exceptions.RequestException as e:
            print(f"🌐 Request failed for {start_date_str} : {e} (attempt {retries + 1})")
        except Exception as e:
            print(f"❌ Unexpected error for {start_date_str} to : {e}")
            break
        
        retries += 1
        if retries < max_retries:
            print(f"🔄 Retrying in {API.RETRY_DELAY} seconds...")
            time.sleep(API.RETRY_DELAY)
    
    print(f"❌ Failed to fetch data after {max_retries} attempts: {start_date_str} ")
    return None

def generate_timestamp():
    """
    Generate timestamp in the format expected by AMFI API.
    
    Returns:
        str: Timestamp in DDMMYYYYHHMMSS format
    """
    now = datetime.now()
    return now.strftime('%d%m%Y%H%M%S')



def clean_daily_nav(df, col_select=col_select):
    clean_df = (df[col_select]
                .rename(columns={
                    'Scheme Code': 'scheme_code',
                    'ISIN Div Payout/ISIN Growth': 'isin_growth',
                    'ISIN Div Reinvestment': 'isin_dividend',
                    'Net Asset Value': 'nav',
                    'Date': 'date'
                })
                .query('scheme_code.notnull() & nav.notnull() & date.notnull()')
                .assign(scheme_code=lambda x: x['scheme_code'].astype(str),
                        date=lambda x: pd.to_datetime(
                    x['date'], format='%d-%b-%Y', errors='coerce'),
                    nav=lambda x: pd.to_numeric(x['nav'], errors='coerce'),
                )
                )
    return clean_df


def load_daily_nav(table_name, table_df, path, connection):
    # Register your DataFrame with DuckDB
    connection.register(table_name, table_df)
    connection.execute(f"""
    COPY {table_name} TO '{path}' 
    (FORMAT PARQUET)
    """)


def is_weekend(check_date):
    """
    Check if the given date is a weekend (Saturday=5, Sunday=6).
    
    Args:
        check_date (datetime.date): Date to check
        
    Returns:
        bool: True if weekend, False otherwise
    """
    return check_date.weekday() >= 5


def get_missing_dates(latest_historical_date):
    """
    Get list of missing dates between latest historical and today.
    Excludes weekends as markets are closed.
    
    Args:
        latest_historical_date (datetime.date): Latest date in historical data
        
    Returns:
        list: List of missing dates (excluding weekends)
    """
    if latest_historical_date is None:
        return [pd.Timestamp(date.today())]

    missing_dates = []
    current_date = latest_historical_date + timedelta(days=1)
    today = pd.Timestamp(date.today())

    while current_date <= today:
        # Skip weekends
        if not is_weekend(current_date):
            missing_dates.append(current_date.strftime('%Y%m%d'))
        current_date += timedelta(days=1)

    return missing_dates


def main():
    try:
        r2 = R2()
        conn = r2.setup_connection()
        historical_path = r2.get_full_path('clean', 'nav_daily_growth_plan')
        max_date_available = conn.read_parquet(
            historical_path).max('date').execute().df().iloc[0, 0]
        dates = get_missing_dates(max_date_available)
        for date in dates:
            raw_df = fetch_daily_nav_data(start_date_str=date)
            clean_df = clean_daily_nav(raw_df,col_select=col_select)
            daily_path = r2.get_full_path('raw', f'nav_daily_{date}')
            load_daily_nav(
                table_name=f'nav_daily_raw_{date}', table_df=clean_df, path=daily_path, connection=conn)
            print(
                f"✅ Successfully created daily NAV Parquet file at {daily_path}")
            print(conn.read_parquet(daily_path).limit(5))

    except Exception as e:
        print(f"❌ Error during processing: {e}")
        return False
    return True

if __name__ == "__main__":
    main()