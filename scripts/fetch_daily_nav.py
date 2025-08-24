#!/usr/bin/env python3

import requests
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from io import StringIO
import logging
import os
from dotenv import load_dotenv

# Configuration
load_dotenv()

# Simple logging setup
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/daily_nav_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def generate_timestamp():
    """Generate timestamp in the format expected by AMFI API."""
    now = datetime.now()
    # Format: DDMMYYYYHHMMSS (based on the example t=2408202509470)
    return now.strftime('%d%m%Y%H%M%S')

def fetch_daily_nav_data(nav_date=None):
    """
    Fetch current NAV data from AMFI.
    
    Args:
        nav_date (datetime.date, optional): Specific date to fetch. Defaults to today.
        
    Returns:
        pandas.DataFrame: Cleaned NAV data or None if failed
    """
    if nav_date is None:
        nav_date = date.today()
    
    # Generate timestamp for API call
    timestamp = generate_timestamp()
    url = f"https://www.amfiindia.com/spages/NAVAll.txt?t={timestamp}"
    
    logger.info(f"Fetching daily NAV data for {nav_date}")
    logger.info(f"API URL: {url}")
    
    try:
        # Fetch data
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        
        # Check if we got data
        if len(response.text.strip()) < 100:
            logger.warning("Response too short, likely no data")
            return None
        
        # Parse CSV data
        df = pd.read_csv(StringIO(response.text), sep=";", dtype=str)
        logger.info(f"Fetched {len(df)} raw records")
        
        # Clean and validate data using same logic as historical processor
        cleaned_df = clean_daily_nav_data(df, nav_date)
        
        if cleaned_df is not None and not cleaned_df.empty:
            logger.info(f"Successfully processed {len(cleaned_df)} valid NAV records")
            return cleaned_df
        else:
            logger.warning("No valid NAV data after cleaning")
            return None
            
    except requests.RequestException as e:
        logger.error(f"Failed to fetch NAV data: {e}")
        return None
    except Exception as e:
        logger.error(f"Error processing NAV data: {e}")
        return None

def clean_daily_nav_data(df, nav_date):
    """
    Clean daily NAV data using same logic as historical processor.
    
    Args:
        df (pandas.DataFrame): Raw NAV data
        nav_date (datetime.date): Expected NAV date
        
    Returns:
        pandas.DataFrame: Cleaned data
    """
    try:
        if df is None or df.empty:
            return None
        
        # Filter out header rows and category separators (same as historical)
        clean_df = df[
            df['Scheme Code'].notna() & 
            df['Date'].notna() & 
            ~df['Scheme Code'].str.contains('Open Ended|Close Ended|Interval Fund|Fund of Funds', na=False) &
            ~df['Scheme Code'].str.contains('Mutual Fund', na=False)
        ].copy()
        
        if clean_df.empty:
            logger.warning("No valid records after filtering")
            return None
        
        # Rename columns to match our standard schema
        clean_df = clean_df.rename(columns={
            'Scheme Code': 'scheme_code',
            'Scheme Name': 'scheme_name',
            'ISIN Div Payout/ ISIN Growth': 'isin_growth',  # Note the space in daily data
            'ISIN Div Reinvestment': 'isin_dividend',
            'Net Asset Value': 'nav',
            'Repurchase Price': 'repurchase_price',
            'Sale Price': 'sale_price',
            'Date': 'date'
        })
        
        # Handle missing columns (daily data may not have repurchase/sale prices)
        for col in ['repurchase_price', 'sale_price']:
            if col not in clean_df.columns:
                clean_df[col] = None
        
        # Convert data types
        clean_df['scheme_code'] = clean_df['scheme_code'].astype(str)
        clean_df['nav'] = pd.to_numeric(clean_df['nav'], errors='coerce')
        clean_df['repurchase_price'] = pd.to_numeric(clean_df['repurchase_price'], errors='coerce')
        clean_df['sale_price'] = pd.to_numeric(clean_df['sale_price'], errors='coerce')
        
        # Convert date - daily format might be different
        clean_df['date'] = pd.to_datetime(clean_df['date'], format='%d-%b-%Y', errors='coerce')
        
        # Remove rows where NAV is missing
        initial_count = len(clean_df)
        clean_df = clean_df[clean_df['nav'].notna()]
        dropped = initial_count - len(clean_df)
        
        if dropped > 0:
            logger.info(f"Dropped {dropped} records with missing NAV")
        
        # Validate date matches expected
        if not clean_df.empty:
            actual_dates = clean_df['date'].dropna().dt.date.unique()
            if len(actual_dates) == 1 and actual_dates[0] != nav_date:
                logger.warning(f"Expected date {nav_date}, but data contains {actual_dates[0]}")
            elif len(actual_dates) > 1:
                logger.warning(f"Data contains multiple dates: {actual_dates}")
        
        # Select final columns in correct order
        final_columns = [
            'scheme_code', 'scheme_name', 'isin_growth', 'isin_dividend', 
            'nav', 'repurchase_price', 'sale_price', 'date'
        ]
        
        result_df = clean_df[final_columns]
        
        logger.info(f"Cleaned daily data: {len(result_df)} valid records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error cleaning daily NAV data: {e}")
        return None

def save_daily_nav_data(df, nav_date, output_dir="raw/amfi_nav_daily"):
    """
    Save daily NAV data to file.
    
    Args:
        df (pandas.DataFrame): Cleaned NAV data
        nav_date (datetime.date): NAV date
        output_dir (str): Output directory
        
    Returns:
        str: Path to saved file or None if failed
    """
    if df is None or df.empty:
        logger.error("Cannot save empty DataFrame")
        return None
    
    # Create output directory
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    # Generate filename
    filename = f"daily_nav_{nav_date.strftime('%Y%m%d')}.parquet"
    file_path = output_path / filename
    
    # Check if file already exists
    if file_path.exists():
        logger.info(f"File {file_path} already exists")
        
        # Load existing and compare
        try:
            existing_df = pd.read_parquet(file_path)
            if len(existing_df) == len(df):
                logger.info("Same record count, skipping save")
                return str(file_path)
        except Exception as e:
            logger.warning(f"Could not read existing file: {e}")
    
    try:
        # Save as Parquet
        df.to_parquet(file_path, index=False, compression='snappy')
        file_size_mb = file_path.stat().st_size / (1024 * 1024)
        
        logger.info(f"✅ Saved daily NAV data: {file_path}")
        logger.info(f"📊 Records: {len(df):,}")
        logger.info(f"📦 Size: {file_size_mb:.2f} MB")
        
        return str(file_path)
        
    except Exception as e:
        logger.error(f"Failed to save NAV data: {e}")
        return None

def get_latest_historical_date():
    """
    Get the latest date from historical data to avoid duplicates.
    
    Returns:
        datetime.date: Latest date in historical data or None
    """
    try:
        history_dir = Path("raw/amfi_nav_history")
        if not history_dir.exists():
            logger.warning("Historical data directory not found")
            return None
        
        # Check the last batch file for latest date
        batch_files = sorted(history_dir.glob("batch_*.parquet"))
        if not batch_files:
            logger.warning("No historical batch files found")
            return None
        
        # Read the last batch to get the latest date
        last_batch = pd.read_parquet(batch_files[-1])
        latest_date = last_batch['date'].max().date()
        
        logger.info(f"Latest historical date: {latest_date}")
        return latest_date
        
    except Exception as e:
        logger.error(f"Error checking historical data: {e}")
        return None

def is_weekend(check_date):
    """Check if the given date is a weekend (Saturday=5, Sunday=6)."""
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
    logger.info("🚀 Starting daily NAV extraction...")
    
    # Get latest historical date
    latest_historical = get_latest_historical_date()
    logger.info(f"Latest historical date: {latest_historical}")
    
    # Get missing dates (excluding weekends)
    missing_dates = get_missing_dates(latest_historical)
    
    if not missing_dates:
        logger.info("✅ No missing dates to process")
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
        output_dir = Path("raw/amfi_nav_daily")
        filename = f"daily_nav_{target_date.strftime('%Y%m%d')}.parquet"
        file_path = output_dir / filename
        
        if file_path.exists():
            logger.info(f"✅ File already exists: {file_path}")
            success_count += 1
            continue
        
        # Fetch and process NAV data
        nav_data = fetch_daily_nav_data(target_date)
        
        if nav_data is None:
            logger.error(f"❌ Failed to fetch data for {target_date}")
            failed_dates.append(target_date)
            continue
        
        # Save the data
        saved_path = save_daily_nav_data(nav_data, target_date)
        
        if saved_path:
            success_count += 1
            unique_schemes = nav_data['scheme_code'].nunique()
            logger.info(f"✅ Saved {target_date}: {unique_schemes:,} schemes")
        else:
            logger.error(f"❌ Failed to save data for {target_date}")
            failed_dates.append(target_date)
    
    # Final summary
    logger.info(f"\n🎉 Processing complete!")
    logger.info(f"✅ Successfully processed: {success_count}/{len(missing_dates)} dates")
    
    if failed_dates:
        logger.warning(f"❌ Failed dates: {len(failed_dates)}")
        for failed_date in failed_dates:
            logger.warning(f"   - {failed_date}")
        return 1
    else:
        logger.info("🎊 All dates processed successfully!")
        return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)