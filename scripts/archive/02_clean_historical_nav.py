#!/usr/bin/env python3
"""
Historical NAV Data Cleaner with DuckDB

Cleans raw historical NAV CSV files and creates a single merged Parquet file using DuckDB.
This approach is memory-efficient and avoids the complexity of batch processing.
"""

import duckdb
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing, Validation
from utils.logging_setup import get_historical_clean_logger, log_script_start, log_script_end, log_file_operation

# Initialize logger
logger = get_historical_clean_logger(__name__)

def clean_nav_file(file_path):
    """
    Clean a single NAV file and return cleaned DataFrame.
    
    Args:
        file_path (Path): Path to raw CSV file
        
    Returns:
        pandas.DataFrame: Cleaned NAV data or None if failed
    """
    try:
        # Use configured encoding
        df = pd.read_csv(file_path, dtype=str, encoding=Processing.CSV_ENCODING)
        logger.info(f"Processing {file_path.name} - {len(df):,} raw records")
        
        # Filter out header rows and category separators
        clean_df = (df[
            df['Scheme Code'].notna() & 
            df['Date'].notna() & 
            ~df['Scheme Code'].str.contains('Open Ended|Close Ended|Interval Fund|Fund of Funds', na=False) &
            ~df['Scheme Code'].str.contains('Mutual Fund', na=False)
        ]
         .rename(columns={
            'Scheme Code': 'scheme_code',
            'Scheme Name': 'scheme_name',
            'ISIN Div Payout/ISIN Growth': 'isin_growth', 
            'ISIN Div Reinvestment': 'isin_dividend',
            'Net Asset Value': 'nav',
            'Repurchase Price': 'repurchase_price',
            'Sale Price': 'sale_price',
            'Date': 'date'
        })
         .assign(scheme_code=lambda x: x['scheme_code'].astype(str),
                 date = lambda x: pd.to_datetime(x['date'], format='%d-%b-%Y', errors='coerce'),
                 nav = lambda x: pd.to_numeric(x['nav'], errors='coerce'),
                 repurchase_price = lambda x: pd.to_numeric(x['repurchase_price'], errors='coerce'),
                 sale_price = lambda x: pd.to_numeric(x['sale_price'], errors='coerce')             
        )
        )
        
        return clean_df
        
    except Exception as e:
        logger.error(f"❌ Failed to process {file_path}: {e}")
        return None

def process_batch(file_paths, batch_num):
    """
    Process a batch of files using parallel processing.
    
    Args:
        file_paths (list): List of file paths to process
        batch_num (int): Batch number for logging
        
    Returns:
        pandas.DataFrame: Combined cleaned data
    """
    logger.info(f"🔄 Processing batch {batch_num}: {len(file_paths)} files")
    
    # Use configured number of processes
    max_workers = min(len(file_paths), multiprocessing.cpu_count())
    
    cleaned_dfs = []
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all file processing jobs
        future_to_file = {executor.submit(clean_nav_file, fp): fp for fp in file_paths}
        
        # Collect results as they complete
        for future in as_completed(future_to_file):
            file_path = future_to_file[future]
            try:
                result = future.result()
                if result is not None:
                    cleaned_dfs.append(result)
            except Exception as e:
                logger.error(f"❌ Error processing {file_path}: {e}")
    
    if not cleaned_dfs:
        logger.error(f"❌ No valid data in batch {batch_num}")
        return None
    
    # Combine all DataFrames
    logger.info(f"🔗 Combining {len(cleaned_dfs)} DataFrames from batch {batch_num}")
    combined_df = pd.concat(cleaned_dfs, ignore_index=True)
    
    # Free memory
    del cleaned_dfs
    gc.collect()
    
    log_data_summary(logger, combined_df, f"batch {batch_num}")
    return combined_df

def combine_all_nav_files_memory_efficient():
    """
    Combine all NAV files using memory-efficient batch processing.
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_script_start(logger, "Historical NAV Cleaner", 
                    "Processing raw CSV files into optimized Parquet batches")
    
    # Use configured paths
    input_dir = Paths.RAW_NAV_CSV
    output_dir = Paths.RAW_NAV_HISTORICAL
    
    logger.info(f"📁 Input directory: {input_dir}")
    logger.info(f"📁 Output directory: {output_dir}")
    
    # Ensure directories exist
    Paths.create_directories()
    
    if not input_dir.exists():
        logger.error(f"❌ Input directory not found: {input_dir}")
        return False
    
    # Get all CSV files
    csv_files = sorted(list(input_dir.glob("*.csv")))
    
    if not csv_files:
        logger.error(f"❌ No CSV files found in {input_dir}")
        return False
    
    logger.info(f"📊 Found {len(csv_files)} CSV files to process")
    
    combined_df = pd.concat((clean_nav_file(Path(fp)) for fp in csv_files), ignore_index=True)
    return combined_df
    #to_parquet('.data/processed/nav_historical/nav_historical.parquet', index=False, compression=Processing.PARQUET_COMPRESSION)

def main():
    """Main function to process all NAV files."""
    
    success = combine_all_nav_files_memory_efficient()
    
    log_script_end(logger, "Historical NAV Cleaner", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)