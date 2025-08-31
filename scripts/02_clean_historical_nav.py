#!/usr/bin/env python3
"""
Historical NAV Data Cleaner

Cleans raw historical NAV CSV files and converts to optimized Parquet batches.
This script has been refactored to use centralized configuration and logging.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import gc

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing, Validation
from utils.logging_setup import get_historical_clean_logger, log_script_start, log_script_end, log_data_summary, log_file_operation

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
        clean_df = df[
            df['Scheme Code'].notna() & 
            df['Date'].notna() & 
            ~df['Scheme Code'].str.contains('Open Ended|Close Ended|Interval Fund|Fund of Funds', na=False) &
            ~df['Scheme Code'].str.contains('Mutual Fund', na=False)
        ].copy()
        
        if clean_df.empty:
            logger.warning(f"⚠️ No valid data found in {file_path}")
            return None
        
        # Create standardized column names
        clean_df = clean_df.rename(columns={
            'Scheme Code': 'scheme_code',
            'Scheme Name': 'scheme_name',
            'ISIN Div Payout/ISIN Growth': 'isin_growth', 
            'ISIN Div Reinvestment': 'isin_dividend',
            'Net Asset Value': 'nav',
            'Repurchase Price': 'repurchase_price',
            'Sale Price': 'sale_price',
            'Date': 'date'
        })
        
        # Convert data types
        clean_df['scheme_code'] = clean_df['scheme_code'].astype(str)
        clean_df['date'] = pd.to_datetime(clean_df['date'], format='%d-%b-%Y', errors='coerce')
        
        # Convert numeric columns with validation
        numeric_cols = ['nav', 'repurchase_price', 'sale_price']
        for col in numeric_cols:
            clean_df[col] = pd.to_numeric(clean_df[col], errors='coerce')
        
        # Apply validation rules from config
        before_count = len(clean_df)
        
        # Filter invalid NAV values
        valid_nav = (
            (clean_df['nav'] >= Validation.MIN_NAV_VALUE) & 
            (clean_df['nav'] <= Validation.MAX_NAV_VALUE)
        )
        clean_df = clean_df[valid_nav]
        
        # Filter invalid dates  
        valid_dates = clean_df['date'].notna()
        clean_df = clean_df[valid_dates]
        
        after_count = len(clean_df)
        
        if before_count != after_count:
            filtered = before_count - after_count
            logger.info(f"   Filtered {filtered:,} invalid records ({filtered/before_count*100:.1f}%)")
        
        logger.info(f"✅ Cleaned {file_path.name}: {after_count:,} valid records")
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
    
    # Use configured batch size
    batch_size = Processing.HISTORICAL_BATCH_SIZE
    logger.info(f"⚙️ Processing in batches of {batch_size} files")
    
    # Process files in batches
    batch_num = 1
    file_index = 0
    
    while file_index < len(csv_files):
        # Get files for this batch
        batch_files = csv_files[file_index:file_index + batch_size]
        
        # Process batch
        batch_df = process_batch(batch_files, batch_num)
        
        if batch_df is None:
            logger.error(f"❌ Failed to process batch {batch_num}")
            return False
        
        # Save batch to Parquet
        from config.settings import get_batch_file_path
        batch_file = get_batch_file_path(batch_num)
        
        try:
            # Use configured compression
            batch_df.to_parquet(
                batch_file, 
                index=False, 
                compression=Processing.PARQUET_COMPRESSION
            )
            
            file_size_mb = batch_file.stat().st_size / (1024 * 1024)
            log_file_operation(logger, "saved", batch_file, True, file_size_mb)
            
            logger.info(f"📊 Batch {batch_num} summary:")
            logger.info(f"   Files processed: {len(batch_files)}")
            logger.info(f"   Records: {len(batch_df):,}")
            logger.info(f"   Date range: {batch_df['date'].min().date()} to {batch_df['date'].max().date()}")
            logger.info(f"   Unique schemes: {batch_df['scheme_code'].nunique():,}")
            
        except Exception as e:
            logger.error(f"❌ Failed to save batch {batch_num}: {e}")
            return False
        
        # Clean up memory
        del batch_df
        gc.collect()
        
        # Move to next batch
        file_index += batch_size
        batch_num += 1
    
    logger.info("🎉 All batches processed successfully!")
    
    # Final summary
    total_batches = batch_num - 1
    logger.info("📊 Processing Summary:")
    logger.info(f"   Total files: {len(csv_files)}")
    logger.info(f"   Total batches: {total_batches}")
    logger.info(f"   Output directory: {output_dir}")
    
    # Calculate total output size
    total_size = sum(f.stat().st_size for f in output_dir.glob("batch_*.parquet")) / (1024 * 1024)
    logger.info(f"   Total output size: {total_size:.1f} MB")
    
    return True

def main():
    """Main function to process all NAV files."""
    
    success = combine_all_nav_files_memory_efficient()
    
    log_script_end(logger, "Historical NAV Cleaner", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)