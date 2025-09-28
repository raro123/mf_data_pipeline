#!/usr/bin/env python3
"""
Daily NAV Data Fetcher

Fetches current NAV data from AMFI with gap-filling and weekend skip logic.
This script has been refactored to use centralized configuration and logging.
"""

import requests
import pandas as pd
import sys
from datetime import datetime, date, timedelta
from pathlib import Path
from io import StringIO

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, API, Validation, get_daily_nav_file_path
from utils.logging_setup import get_daily_fetch_logger, log_script_start, log_script_end, log_data_summary, log_file_operation

# Initialize logger
logger = get_daily_fetch_logger(__name__)

def generate_timestamp():
    """
    Generate timestamp in the format expected by AMFI API.
    
    Returns:
        str: Timestamp in DDMMYYYYHHMMSS format
    """
    now = datetime.now()
    return now.strftime('%d%m%Y%H%M%S')

def fetch_daily_nav_data(nav_date=None):
    """
    Fetch current NAV data from AMFI API.
    
    Args:
        nav_date (datetime.date, optional): Specific date to fetch. Defaults to today.
        
    Returns:
        pandas.DataFrame: NAV data or None if failed
    """
    if nav_date is None:
        nav_date = date.today()
    
    timestamp = generate_timestamp()
    
    # Use configured API settings
    url = f"{API.AMFI_NAV_BASE_URL}?t={timestamp}"
    
    logger.info(f"📡 Fetching daily NAV data for {nav_date}")
    logger.info(f"🌐 API URL: {url}")
    
    try:
        response = requests.get(url, timeout=API.AMFI_NAV_TIMEOUT)
        response.raise_for_status()
        
        logger.info(f"✅ Fetched {len(response.content):,} bytes")
        
        # Read the data
        nav_data = StringIO(response.text)
        df = pd.read_csv(nav_data, sep=';', header=None)
        
        logger.info(f"📊 Fetched {len(df):,} raw records")
        
        # Basic validation
        if df.empty:
            logger.warning("⚠️ No data received from API")
            return None
            
        return df
        
    except requests.exceptions.Timeout:
        logger.error(f"⏰ Request timeout after {API.AMFI_NAV_TIMEOUT}s")
        return None
    except requests.exceptions.RequestException as e:
        logger.error(f"🌐 Request failed: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error: {e}")
        return None

def clean_daily_nav_data(df, nav_date):
    """
    Clean and standardize daily NAV data.
    
    Args:
        df (pandas.DataFrame): Raw NAV data
        nav_date (datetime.date): Target date for the data
        
    Returns:
        pandas.DataFrame: Cleaned NAV data or None if failed
    """
    if df is None or df.empty:
        logger.warning("⚠️ No data to clean")
        return None
    
    try:
        # Set column names based on expected format
        expected_cols = ['Scheme Code', 'ISIN Div Payout/ ISIN Growth', 'ISIN Div Reinvestment', 
                        'Scheme Name', 'Net Asset Value', 'Date']
        
        if len(df.columns) >= len(expected_cols):
            df.columns = expected_cols + [f'extra_{i}' for i in range(len(df.columns) - len(expected_cols))]
        else:
            logger.error(f"❌ Unexpected data format: {len(df.columns)} columns")
            return None
        
        # Drop rows with missing NAV
        initial_count = len(df)
        df = df.dropna(subset=['Net Asset Value'])
        dropped_nav = initial_count - len(df)
        
        if dropped_nav > 0:
            logger.info(f"🗑️ Dropped {dropped_nav:,} records with missing NAV")
        
        # Convert NAV to numeric and filter invalid values
        df['Net Asset Value'] = pd.to_numeric(df['Net Asset Value'], errors='coerce')
        df = df.dropna(subset=['Net Asset Value'])
        
        # Apply validation rules from config
        valid_nav = (
            (df['Net Asset Value'] >= Validation.MIN_NAV_VALUE) & 
            (df['Net Asset Value'] <= Validation.MAX_NAV_VALUE)
        )
        df = df[valid_nav]
        
        if df.empty:
            logger.error("❌ No valid NAV records after filtering")
            return None
        
        # Parse and validate dates
        df['Date'] = pd.to_datetime(df['Date'], errors='coerce')
        
        # Check date consistency
        unique_dates = df['Date'].dt.date.unique()
        unique_dates = unique_dates[pd.notna(unique_dates)]
        
        if len(unique_dates) > 1:
            logger.warning(f"⚠️ Data contains multiple dates: {unique_dates}")
        
        # Standardize column names
        clean_df = df.rename(columns={
            'Scheme Code': 'scheme_code',
            'Scheme Name': 'scheme_name', 
            'ISIN Div Payout/ ISIN Growth': 'isin_growth',
            'ISIN Div Reinvestment': 'isin_dividend',
            'Net Asset Value': 'nav',
            'Date': 'date'
        })
        
        # Add repurchase and sale prices (same as NAV for daily data)
        clean_df['repurchase_price'] = clean_df['nav']
        clean_df['sale_price'] = clean_df['nav']
        
        # Set consistent date
        clean_df['date'] = pd.to_datetime(nav_date)
        
        final_count = len(clean_df)
        logger.info(f"✅ Cleaned daily data: {final_count:,} valid records")
        logger.info(f"📊 Successfully processed {final_count:,} valid NAV records")
        
        return clean_df
        
    except Exception as e:
        logger.error(f"❌ Error cleaning daily NAV data: {e}")
        return None

def save_daily_nav_data(df, nav_date):
    """
    Save daily NAV data to Parquet file using configured paths.
    
    Args:
        df (pandas.DataFrame): Cleaned NAV data
        nav_date (datetime.date): Date for the data
        
    Returns:
        str: Path to saved file or None if failed
    """
    if df is None or df.empty:
        logger.warning("⚠️ No data to save")
        return None
    
    # Use configured file path
    date_str = nav_date.strftime('%Y%m%d')
    file_path = get_daily_nav_file_path(date_str)
    
    # Ensure output directory exists
    file_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Use configured compression
        from config.settings import Processing
        df.to_parquet(file_path, index=False, compression=Processing.PARQUET_COMPRESSION)
        
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", file_path, True, file_size_mb)
        
        logger.info(f"📊 Records: {len(df):,}")
        logger.info(f"📦 Size: {file_size_mb:.2f} MB")
        
        return str(file_path)
        
    except Exception as e:
        logger.error(f"❌ Failed to save daily NAV data: {e}")
        return None

def get_latest_historical_date():
    """
    Get the latest date from historical NAV data.
    
    Returns:
        datetime.date: Latest historical date or None if not found
    """
    try:
        # Check configured historical directory
        history_dir = Paths.RAW_NAV_HISTORICAL
        
        if not history_dir.exists():
            logger.warning(f"⚠️ Historical directory not found: {history_dir}")
            return None
        
        batch_files = list(history_dir.glob("batch_*.parquet"))
        if not batch_files:
            logger.warning("⚠️ No historical batch files found")
            return None
        
        # Read the last batch file to get latest date
        latest_batch = sorted(batch_files)[-1]
        df = pd.read_parquet(latest_batch)
        
        latest_date = df['date'].max().date()
        logger.info(f"📅 Latest historical date: {latest_date}")
        
        return latest_date
        
    except Exception as e:
        logger.error(f"❌ Error checking historical data: {e}")
        return None

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
        return [date.today()]
    
    missing_dates = []
    current_date = latest_historical_date + timedelta(days=1)
    today = date.today()
    
    while current_date <= today:
        # Skip weekends
        if not is_weekend(current_date):
            missing_dates.append(current_date)
        current_date += timedelta(days=1)
    
    return missing_dates

def main():
    """Main function to fetch and save daily NAV data."""
    
    log_script_start(logger, "Daily NAV Fetcher", 
                    "Fetching daily NAV data with gap-filling and weekend skip")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Get latest historical date
    latest_historical = get_latest_historical_date()
    logger.info(f"📅 Latest historical date: {latest_historical}")
    
    # Get missing dates (excluding weekends)
    missing_dates = get_missing_dates(latest_historical)
    
    if not missing_dates:
        logger.info("✅ No missing dates to process")
        log_script_end(logger, "Daily NAV Fetcher", True)
        return 0
    
    logger.info(f"📅 Missing dates to process: {len(missing_dates)}")
    for missing_date in missing_dates:
        logger.info(f"   - {missing_date} ({missing_date.strftime('%A')})")
    
    # Process each missing date
    success_count = 0
    failed_dates = []
    
    for target_date in missing_dates:
        logger.info(f"\n🔄 Processing {target_date} ({target_date.strftime('%A')})...")
        
        # Skip if weekend (double-check)
        if is_weekend(target_date):
            logger.info(f"⏭️  Skipping {target_date} (Weekend)")
            continue
        
        # Check if file already exists
        date_str = target_date.strftime('%Y%m%d')
        file_path = get_daily_nav_file_path(date_str)
        
        if file_path.exists():
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"📄 File already exists: {file_path} ({file_size_mb:.2f} MB)")
            success_count += 1
            continue
        
        # Fetch and process NAV data
        nav_data = fetch_daily_nav_data(target_date)
        
        if nav_data is None:
            logger.error(f"❌ Failed to fetch data for {target_date}")
            failed_dates.append(target_date)
            continue
        
        # Clean the data
        clean_data = clean_daily_nav_data(nav_data, target_date)
        if clean_data is None:
            logger.error(f"❌ Failed to clean data for {target_date}")
            failed_dates.append(target_date)
            continue
        
        # Save the data
        saved_path = save_daily_nav_data(clean_data, target_date)
        
        if saved_path:
            success_count += 1
            unique_schemes = clean_data['scheme_code'].nunique()
            logger.info(f"✅ Saved {target_date}: {unique_schemes:,} schemes")
        else:
            logger.error(f"❌ Failed to save data for {target_date}")
            failed_dates.append(target_date)
    
    # Final summary
    logger.info(f"\n📊 Processing Summary:")
    logger.info(f"   Total dates: {len(missing_dates)}")
    logger.info(f"   Successfully processed: {success_count}")
    logger.info(f"   Failed: {len(failed_dates)}")
    
    if failed_dates:
        logger.warning("⚠️ Failed dates:")
        for failed_date in failed_dates:
            logger.warning(f"   - {failed_date}")
    else:
        logger.info("🎊 All dates processed successfully!")
    
    success = len(failed_dates) == 0
    log_script_end(logger, "Daily NAV Fetcher", success)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)