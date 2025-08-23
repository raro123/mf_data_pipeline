#!/usr/bin/env python3

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
import time
import logging

# Simple logging setup
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/nav_fetch_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def daterange_chunks(start_date_str, end_date_str, chunk_days=90):
    """Generate date ranges in chunks of specified days."""
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days-1), end_date)
        yield current.strftime('%Y%m%d'), chunk_end.strftime('%Y%m%d')
        current = chunk_end + timedelta(days=1)

def fetch_nav_data(start_date_str, end_date_str):
    """Fetch NAV data for a date range."""
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    
    url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'
    params = {
        'tp': '1',
        'frmdt': start_date.strftime('%d-%b-%Y'),
        'todt': end_date.strftime('%d-%b-%Y'),
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        
        df = pd.read_csv(StringIO(response.text), sep=";")
        
        # Basic validation - just check if we got data
        if df.empty or len(df.columns) < 3:
            logger.warning(f"No valid data for {start_date_str} to {end_date_str}")
            return None
            
        logger.info(f"Fetched {len(df)} records for {start_date_str} to {end_date_str}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to fetch data for {start_date_str} to {end_date_str}: {e}")
        return None

def save_to_csv(df, start_date_str, end_date_str, output_dir="raw/amfi_nav"):
    """Save DataFrame to CSV."""
    if df is None or df.empty:
        return None
        
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    filename = f"amfi_raw_nav_{start_date_str}_{end_date_str}.csv"
    filepath = Path(output_dir) / filename
    
    # Skip if file already exists
    if filepath.exists():
        logger.info(f"File {filepath} already exists, skipping")
        return str(filepath)
    
    try:
        df.to_csv(filepath, index=False)
        logger.info(f"Saved {filename}")
        return str(filepath)
    except Exception as e:
        logger.error(f"Failed to save {filename}: {e}")
        return None

def main():
    """Main function - fetch NAV data in chunks."""
    start_date = '20060101'
    end_date = datetime.now().strftime('%Y%m%d')
    
    logger.info(f"Starting NAV data fetch from {start_date} to {end_date}")
    
    successful_files = []
    failed_chunks = []
    
    for start, end in daterange_chunks(start_date, end_date):
        logger.info(f"Processing {start} to {end}")
        
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
            
        # Be nice to the API
        time.sleep(1)
    
    # Summary
    logger.info(f"Completed: {len(successful_files)} successful, {len(failed_chunks)} failed")
    if failed_chunks:
        logger.warning(f"Failed chunks: {failed_chunks}")

if __name__ == "__main__":
    main()