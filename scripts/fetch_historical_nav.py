#!/usr/bin/env python3
"""
Historical NAV Data Fetcher

Fetches historical NAV data from AMFI in 90-day chunks and saves to CSV files.
Uses centralized configuration and supports resuming or forcing updates.
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
import time
import argparse

# Import centralized configuration
from config.settings import Paths, API, Processing
from utils.logging_setup import get_historical_fetch_logger, log_script_start, log_script_end, log_file_operation

# Initialize logger
logger = get_historical_fetch_logger(__name__)

def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='Fetch historical NAV data from AMFI.')
    
    # Default start date (magic date when MF data started becoming available/relevant)
    default_start = '20060101'
    default_end = datetime.now().strftime('%Y%m%d')
    
    parser.add_argument('--start', type=str, default=default_start,
                        help=f'Start date (YYYYMMDD). Default: {default_start}')
    parser.add_argument('--end', type=str, default=default_end,
                        help=f'End date (YYYYMMDD). Default: {default_end}')
    parser.add_argument('--force', action='store_true',
                        help='Force re-download even if file exists')
    
    return parser.parse_args()

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
    while current <= end_date:
        chunk_end = min(current + timedelta(days=chunk_days-1), end_date)
        yield current.strftime('%Y%m%d'), chunk_end.strftime('%Y%m%d')
        current = chunk_end + timedelta(days=1)

def get_output_path(start_date_str: str, end_date_str: str) -> Path:
    """Generate the output filepath for a given date range."""
    filename = f"amfi_raw_nav_{start_date_str}_{end_date_str}.csv"
    return Paths.RAW_NAV_CSV / filename

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
    url = API.AMFI_NAV_HISTORY_URL
    params = {
        'tp': '1',
        'frmdt': start_date.strftime('%d-%b-%Y'),
        'todt': end_date.strftime('%d-%b-%Y'),
    }
    
    retries = 0
    max_retries = API.MAX_RETRIES
    
    while retries < max_retries:
        try:
            logger.info(f"FETCH: {start_date_str} to {end_date_str} (attempt {retries + 1})")
            
            response = requests.get(url, params=params, timeout=API.AMFI_NAV_TIMEOUT)
            response.raise_for_status()
            
            # Parse CSV response
            # Using ; as separator as per AMFI format
            df = pd.read_csv(StringIO(response.text), sep=";")
            
            # Basic validation
            if df.empty or len(df.columns) < 3:
                logger.warning(f"WARN: No valid data for {start_date_str} to {end_date_str}")
                return None
                
            logger.info(f"SUCCESS: Fetched {len(df):,} records for {start_date_str} to {end_date_str}")
            return df
            
        except requests.exceptions.Timeout:
            logger.warning(f"TIMEOUT: {start_date_str} to {end_date_str} (attempt {retries + 1})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"ERROR: Request failed for {start_date_str} to {end_date_str}: {e} (attempt {retries + 1})")
        except Exception as e:
            logger.error(f"FATAL: Unexpected error for {start_date_str} to {end_date_str}: {e}")
            break
        
        retries += 1
        if retries < max_retries:
            logger.info(f"RETRY: Waiting {API.RETRY_DELAY} seconds...")
            time.sleep(API.RETRY_DELAY)
    
    logger.error(f"FAILED: After {max_retries} attempts: {start_date_str} to {end_date_str}")
    return None

def save_to_csv(df: pd.DataFrame, filepath: Path) -> bool:
    """
    Save DataFrame to CSV file.
    
    Args:
        df: DataFrame to save
        filepath: Destination path
        
    Returns:
        bool: True if successful
    """
    if df is None or df.empty:
        return False
        
    try:
        # Use configured encoding
        df.to_csv(filepath, index=False, encoding=Processing.CSV_ENCODING)
        
        file_size_mb = filepath.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", filepath, True, file_size_mb)
        return True
        
    except Exception as e:
        logger.error(f"ERROR: Failed to save {filepath.name}: {e}")
        return False

def main():
    """Main function - fetch historical NAV data in chunks."""
    args = parse_args()
    
    log_script_start(logger, "Historical NAV Fetcher", 
                    "Fetching historical NAV data from AMFI")
    
    logger.info(f"Date range: {args.start} to {args.end}")
    logger.info(f"Chunk size: {Processing.HISTORICAL_FETCH_DAYS} days")
    logger.info(f"Output directory: {Paths.RAW_NAV_CSV}")
    logger.info(f"Force update: {args.force}")
    
    # Ensure directories exist
    Paths.create_directories()
    
    successful_files = []
    failed_chunks = []
    skipped_chunks = 0
    
    # Process in chunks
    chunk_count = 0
    
    for start, end in daterange_chunks(args.start, args.end):
        chunk_count += 1
        filepath = get_output_path(start, end)
        
        # Check if exists (unless forced)
        if filepath.exists() and not args.force:
            file_size_mb = filepath.stat().st_size / (1024 * 1024)
            logger.info(f"SKIP: Chunk {chunk_count} ({start}-{end}): File exists ({file_size_mb:.2f} MB)")
            successful_files.append(str(filepath))
            skipped_chunks += 1
            continue
            
        logger.info(f"PROCESS: Chunk {chunk_count}: {start} to {end}")
        
        # Fetch data
        df = fetch_nav_data(start, end)
        
        # Save if successful
        if df is not None:
            if save_to_csv(df, filepath):
                successful_files.append(str(filepath))
            else:
                failed_chunks.append((start, end))
        else:
            failed_chunks.append((start, end))
            
        # Be nice to the API
        time.sleep(1) 
    
    # Summary
    logger.info("SUMMARY: Processing Details:")
    logger.info(f"   Total chunks: {chunk_count}")
    logger.info(f"   Skipped (Exists): {skipped_chunks}")
    logger.info(f"   Fetched & Saved: {len(successful_files) - skipped_chunks}")
    logger.info(f"   Failed: {len(failed_chunks)}")
    
    if failed_chunks:
        logger.warning("FAILED CHUNKS:")
        for start, end in failed_chunks:
            logger.warning(f"   {start} to {end}")
    
    # Calculate total data size
    if successful_files:
        total_size = sum(Path(f).stat().st_size for f in successful_files) / (1024 * 1024)
        logger.info(f"DATA SIZE: {total_size:.2f} MB")
    
    success = len(failed_chunks) == 0
    log_script_end(logger, "Historical NAV Fetcher", success)
    
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)