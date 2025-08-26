#!/usr/bin/env python3

import requests
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from io import StringIO
import time
import logging
import os
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from typing import Optional, Tuple, List, Generator
#from src.common.storage import CloudflareR2, generate_object_name

# Configuration
load_dotenv()

# Constants
FOLDER_NAME = "raw/amfi_nav"
AMFI_NAV_FILENAME_PREFIX = "amfi_nav_data"
CHUNK_DAYS = 90
DEFAULT_RETRY_COUNT = 3
DEFAULT_REQUEST_TIMEOUT = 30
REQUEST_DELAY = 1
EXPECTED_COLUMNS = ['Scheme Code', 'ISIN Div Payout/ISIN Growth', 'ISIN Div Reinvestment', 'Scheme Name', 'Net Asset Value', 'Date']

# Simple logging setup with file output
log_dir = Path("logs")
log_dir.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_dir / f"nav_fetch_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()  # Also log to console
    ]
)

logger = logging.getLogger(__name__)

# Custom exceptions
class NAVDataError(Exception):
    """Raised when NAV data is invalid or corrupted."""
    pass

class APIError(Exception):
    """Raised when API request fails."""
    pass

def create_session() -> requests.Session:
    """Create a requests session with retry strategy and timeout."""
    session = requests.Session()
    retry_strategy = Retry(
        total=DEFAULT_RETRY_COUNT,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

def validate_nav_data(df: pd.DataFrame) -> bool:
    """Validate NAV data structure and content."""
    if df is None or df.empty:
        return False
    
    # Check if we have minimum required columns
    if len(df.columns) < 3:
        logger.warning(f"DataFrame has only {len(df.columns)} columns, expected at least 3")
        return False
    
    # Check for common column patterns in AMFI data
    has_scheme_info = any('scheme' in col.lower() or 'isin' in col.lower() for col in df.columns)
    has_nav_info = any('nav' in col.lower() or 'value' in col.lower() for col in df.columns)
    has_date_info = any('date' in col.lower() for col in df.columns)
    
    if not (has_scheme_info and has_nav_info and has_date_info):
        logger.warning("DataFrame missing expected AMFI NAV data columns")
        return False
    
    return True

def daterange_chunks(start_date_str: str, end_date_str: str, chunk_days: int = CHUNK_DAYS) -> Generator[Tuple[str, str], None, None]:
    """Generate date ranges in chunks of specified days, returned as (YYYYMMDD, YYYYMMDD) strings."""
        # Convert string dates to datetime
    start_date = datetime.strptime(start_date_str, '%Y%m%d')
    end_date = datetime.strptime(end_date_str, '%Y%m%d')
    current = start_date
    while current < end_date:
        chunk_end = min(current + timedelta(days=chunk_days-1), end_date)
        yield current.strftime('%Y%m%d'), chunk_end.strftime('%Y%m%d')
        current = chunk_end + timedelta(days=1)


def fetch_nav_data_90_days_max(start_date_str: str, end_date_str: str, session: Optional[requests.Session] = None) -> Optional[pd.DataFrame]:
    """Fetch NAV data for a specific date range with proper error handling.

    Args:
        start_date_str (str): Start date in YYYYMMDD format (e.g., '20060101')
        end_date_str (str): End date in YYYYMMDD format (e.g., '20060331')
        session (requests.Session, optional): Session to use for requests. Creates new if None.

    Returns:
        pandas.DataFrame: DataFrame with NAV data, or None if all attempts fail
        
    Raises:
        APIError: When API request fails after all retries
        NAVDataError: When received data is invalid
    """
    try:
        # Convert YYYYMMDD to datetime
        start_date = datetime.strptime(start_date_str, '%Y%m%d')
        end_date = datetime.strptime(end_date_str, '%Y%m%d')
    except ValueError as e:
        raise ValueError(f"Invalid date format: {e}")
    
    # Validate date range
    if (end_date - start_date).days > CHUNK_DAYS:
        raise ValueError(f"Date range exceeds {CHUNK_DAYS} days limit")
    
    url = 'https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx'
    params = {
        'tp': '1',
        'frmdt': start_date.strftime('%d-%b-%Y'),
        'todt': end_date.strftime('%d-%b-%Y'),
    }
    
    if session is None:
        session = create_session()
    
    try:
        logger.info(f"API_REQUEST_START: {start_date_str} to {end_date_str}")
        request_start = datetime.now()
        response = session.get(url, params=params, timeout=DEFAULT_REQUEST_TIMEOUT)
        response.raise_for_status()
        
        # Check if response contains actual data
        if len(response.text.strip()) < 100:  # Arbitrary minimum size check
            raise NAVDataError(f"Response too short, likely no data for period {start_date_str} to {end_date_str}")
        
        try:
            df = pd.read_csv(StringIO(response.text), sep=";")
        except Exception as e:
            raise NAVDataError(f"Failed to parse CSV data: {e}")
        
        # Validate the data
        if not validate_nav_data(df):
            raise NAVDataError(f"Invalid NAV data structure for period {start_date_str} to {end_date_str}")
        
        request_duration = (datetime.now() - request_start).total_seconds()
        logger.info(f"API_REQUEST_SUCCESS: {start_date_str} to {end_date_str} - {len(df)} records in {request_duration:.2f}s")
        return df
        
    except requests.RequestException as e:
        logger.error(f"API request failed for {start_date_str} to {end_date_str}: {e}")
        raise APIError(f"Failed to fetch data: {e}")
    except (NAVDataError, ValueError) as e:
        logger.error(f"Data validation failed for {start_date_str} to {end_date_str}: {e}")
        raise

def save_to_csv(df: pd.DataFrame, start_date_str: str, end_date_str: str, savepath: str) -> str:
    """
    Save DataFrame to CSV file with standardized naming and duplicate checking.
    
    Args:
        df (pandas.DataFrame): DataFrame to save
        start_date_str (str): Start date in YYYYMMDD format
        end_date_str (str): End date in YYYYMMDD format
        savepath (str): Directory path to save the CSV file
        
    Returns:
        str: Path to the saved CSV file
        
    Raises:
        ValueError: If DataFrame is empty or invalid
        OSError: If file operations fail
    """
    if df is None or df.empty:
        raise ValueError("Cannot save empty DataFrame")
    
    # Create output directory if it doesn't exist
    Path(savepath).mkdir(parents=True, exist_ok=True)
    
    # Generate object name
    object_name = f"amfi_raw_nav_{start_date_str}_{end_date_str}"
    csv_path = Path(savepath) / f"{object_name}.csv"
    
    # Check if file already exists and has similar size
    if csv_path.exists():
        try:
            existing_df = pd.read_csv(csv_path)
            if len(existing_df) == len(df):
                logger.info(f"File {csv_path} already exists with same record count, skipping")
                return str(csv_path)
        except Exception as e:
            logger.warning(f"Could not read existing file {csv_path}: {e}")
    
    try:
        # Save DataFrame to CSV
        df.to_csv(csv_path, index=False)
        logger.info(f"Saved {len(df)} records to {csv_path}")
        return str(csv_path)
    except Exception as e:
        logger.error(f"Failed to save CSV to {csv_path}: {e}")
        raise OSError(f"Could not save file: {e}")
    
def fetch_and_save_nav_90_days(start_date_str: str, end_date_str: str, savepath: str = 'data/raw/amfi_nav', session: Optional[requests.Session] = None) -> Optional[str]:
    """
    Fetches NAV data for a chunk and saves it to CSV.
    
    Args:
        start_date_str (str): Start date in YYYYMMDD format
        end_date_str (str): End date in YYYYMMDD format
        savepath (str): Directory path to save the CSV file
        session (requests.Session, optional): Session to use for requests
        
    Returns:
        str: Path to saved CSV file if successful, None if failed
    """
    try:
        df = fetch_nav_data_90_days_max(start_date_str, end_date_str, session)
        if df is not None:
            csv_path = save_to_csv(df, start_date_str, end_date_str, savepath)
            return csv_path
        return None
    except (APIError, NAVDataError, ValueError, OSError) as e:
        logger.error(f"Failed to fetch and save data for {start_date_str} to {end_date_str}: {e}")
        return None




def fetch_nav_data(start_date_str: str, end_date_str: str, savepath: str) -> List[str]:
    """
    Fetch NAV data for any date range by breaking it into 90-day chunks.
    
    Args:
        start_date_str (str): Start date in YYYYMMDD format (e.g., '20060101')
        end_date_str (str): End date in YYYYMMDD format (e.g., '20231231')
        savepath (str): Directory path to save CSV files
    
    Returns:
        list: List of paths to successfully generated CSV files
    """
    successful_files = []
    failed_chunks = []
    
    # Create session for reuse across requests
    session = create_session()
    
    try:
        # Create list of all date chunks
        chunks = list(daterange_chunks(start_date_str, end_date_str))
        total_chunks = len(chunks)
        logger.info(f"Processing {total_chunks} date chunks from {start_date_str} to {end_date_str}")
        
        for i, (start, end) in enumerate(chunks, 1):
            logger.info(f"Processing chunk {i}/{total_chunks}: {start} to {end}")
            
            try:
                csv_path = fetch_and_save_nav_90_days(start, end, savepath, session)
                if csv_path:
                    successful_files.append(csv_path)
                    logger.info(f"✓ Successfully processed {start} to {end}")
                else:
                    failed_chunks.append((start, end))
                    logger.warning(f"✗ Failed to process {start} to {end}")
                    
                # Add delay between requests to be respectful to the API
                if i < total_chunks:  # Don't sleep after the last request
                    time.sleep(REQUEST_DELAY)
                    
            except Exception as e:
                failed_chunks.append((start, end))
                logger.error(f"✗ Exception processing {start} to {end}: {e}")
        
        # Summary
        logger.info(f"Completed processing: {len(successful_files)} successful, {len(failed_chunks)} failed")
        if failed_chunks:
            logger.warning(f"Failed chunks: {failed_chunks}")
            
    finally:
        session.close()
    
    return successful_files



def main():
    """Main execution function."""
    # Define date range - can be overridden by environment variables
    start_date = os.getenv('START_DATE', '20060101')  # January 1, 2006
    end_date = os.getenv('END_DATE', datetime.now().strftime('%Y%m%d'))  # Today in YYYYMMDD format
    output_folder = os.getenv('OUTPUT_FOLDER', FOLDER_NAME)
    
    logger.info(f"Starting NAV data fetch from {start_date} to {end_date}")
    logger.info(f"Output directory: {output_folder}")
    
    # Create output directory if it doesn't exist
    output_dir = Path(output_folder)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    try:
        output_files = fetch_nav_data(start_date, end_date, output_folder)
        
        if output_files:
            logger.info(f"Successfully completed data fetch.")
            logger.info(f"Generated {len(output_files)} files:")
            for file_path in output_files:
                logger.info(f"- {file_path}")
        else:
            logger.error("Failed to fetch any data")
            return 1  # Exit code for failure
            
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        return 1
    
    return 0  # Success

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
