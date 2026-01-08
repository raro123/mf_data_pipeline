#!/usr/bin/env python3
"""
Historical NAV Data Fetcher

Fetches historical NAV data from AMFI in 90-day chunks and saves to CSV files.
This script has been refactored to use centralized configuration and logging.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
import time

# Import centralized configuration
from config.settings import Paths, API, Processing
from utils.logging_setup import get_historical_fetch_logger, log_script_start, log_script_end, log_file_operation

# Initialize logger
logger = get_historical_fetch_logger(__name__)

def daterange_chunks(start_date_str: str, end_date_str: str, chunk_days: int = None) -> tuple:
    """
    Generate date ranges in chunks of specified days.
    
    Args:
        start_date_str: Start date in YYYYMMDD format
        end_date_str: End date in YYYYMMDD format  
        chunk_days: Days per chunk (from config if not provided)
        
    Yields:
        tuple: (start_date_str, end_date_str) for each chunk
    """
    chunk_days = chunk_days or Processing.HISTORICAL_FETCH_DAYS
    
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days-1), end_date)
        yield current.strftime('%Y%m%d'), chunk_end.strftime('%Y%m%d')
        current = chunk_end + timedelta(days=1)

def fetch_nav_data(start_date_str: str, end_date_str: str) -> pd.DataFrame:
    """
    Fetch NAV data for a date range from AMFI API.
    
    Args:
        start_date_str: Start date in YYYYMMDD format
        end_date_str: End date in YYYYMMDD format
        
    Returns:
        pandas.DataFrame: NAV data or None if failed
    """
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    # Use configured API settings
    url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'
    params = {
        'tp': '1',
        'frmdt': start_date.strftime('%d-%b-%Y'),
        'todt': end_date.strftime('%d-%b-%Y'),
    }
    
    retries = 0
    max_retries = API.MAX_RETRIES
    
    while retries < max_retries:
        try:
            logger.info(f"📡 Fetching data: {start_date_str} to {end_date_str} (attempt {retries + 1})")
            
            response = requests.get(url, params=params, timeout=API.AMFI_NAV_TIMEOUT)
            response.raise_for_status()
            
            # Parse CSV response
            df = pd.read_csv(StringIO(response.text), sep=";")
            
            # Basic validation
            if df.empty or len(df.columns) < 3:
                logger.warning(f"⚠️ No valid data for {start_date_str} to {end_date_str}")
                return None
                
            logger.info(f"✅ Fetched {len(df):,} records for {start_date_str} to {end_date_str}")
            return df
            
        except requests.exceptions.Timeout:
            logger.warning(f"⏰ Timeout for {start_date_str} to {end_date_str} (attempt {retries + 1})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"🌐 Request failed for {start_date_str} to {end_date_str}: {e} (attempt {retries + 1})")
        except Exception as e:
            logger.error(f"❌ Unexpected error for {start_date_str} to {end_date_str}: {e}")
            break
        
        retries += 1
        if retries < max_retries:
            logger.info(f"🔄 Retrying in {API.RETRY_DELAY} seconds...")
            time.sleep(API.RETRY_DELAY)
    
    logger.error(f"❌ Failed to fetch data after {max_retries} attempts: {start_date_str} to {end_date_str}")
    return None

def save_to_csv(df: pd.DataFrame, start_date_str: str, end_date_str: str) -> str:
    """
    Save DataFrame to CSV file using configured paths.
    
    Args:
        df: DataFrame to save
        start_date_str: Start date string
        end_date_str: End date string
        
    Returns:
        str: Path to saved file or None if failed
    """
    if df is None or df.empty:
        logger.warning("⚠️ No data to save")
        return None
    
    # Use configured output directory
    output_dir = Paths.RAW_NAV_CSV
    output_dir.mkdir(parents=True, exist_ok=True)
    
    filename = f"amfi_raw_nav_{start_date_str}_{end_date_str}.csv"
    filepath = output_dir / filename
    
    # Skip if file already exists
    if filepath.exists():
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        logger.info(f"📄 File already exists: {filepath} ({file_size_mb:.2f} MB)")
        return str(filepath)
    
    try:
        # Use configured encoding
        df.to_csv(filepath, index=False, encoding=Processing.CSV_ENCODING)
        
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", filepath, True, file_size_mb)
        
        return str(filepath)
        
    except Exception as e:
        logger.error(f"❌ Failed to save {filename}: {e}")
        return None

def main():
    """Main function - fetch historical NAV data in chunks."""
    
    log_script_start(logger, "Historical NAV Fetcher", 
                    "Fetching historical NAV data from AMFI in 90-day chunks")
    
    # Use configurable date range
    start_date = '20060101'  # Could be made configurable
    end_date = datetime.now().strftime('%Y%m%d')
    
    logger.info(f"📅 Date range: {start_date} to {end_date}")
    logger.info(f"⚙️ Chunk size: {Processing.HISTORICAL_FETCH_DAYS} days")
    logger.info(f"📁 Output directory: {Paths.RAW_NAV_CSV}")
    
    # Ensure directories exist
    Paths.create_directories()
    
    successful_files = []
    failed_chunks = []
    
    # Process in chunks
    chunk_count = 0
    for start, end in daterange_chunks(start_date, end_date):
        chunk_count += 1
        logger.info(f"🔄 Processing chunk {chunk_count}: {start} to {end}")
        
        # Fetch data
        df = fetch_nav_data(start, end)
        
        # Save if successful
        if df is not None:
            filepath = save_to_csv(df, start, end)
            if filepath:
                successful_files.append(filepath)
            else:
                failed_chunks.append((start, end))
        else:
            failed_chunks.append((start, end))
            
        # Be nice to the API - configurable delay
        time.sleep(1)  # Could be made configurable
    
    # Summary
    logger.info("📊 Processing Summary:")
    logger.info(f"   Total chunks: {chunk_count}")
    logger.info(f"   Successful: {len(successful_files)}")
    logger.info(f"   Failed: {len(failed_chunks)}")
    
    if failed_chunks:
        logger.warning("⚠️ Failed chunks:")
        for start, end in failed_chunks:
            logger.warning(f"   {start} to {end}")
    
    # Calculate total data size
    if successful_files:
        total_size = sum(Path(f).stat().st_size for f in successful_files) / (1024 * 1024)
        logger.info(f"💾 Total data downloaded: {total_size:.2f} MB")
    
    success = len(failed_chunks) == 0
    log_script_end(logger, "Historical NAV Fetcher", success)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)