#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

# Simple logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def combine_batch_files():
    """Combine existing batch Parquet files."""
    temp_dir = Path("raw/amfi_nav_history/temp_batches")
    output_file = Path("raw/amfi_nav_history/historical_nav_data.parquet")
    
    batch_files = list(temp_dir.glob("batch_*.parquet"))
    batch_files.sort()
    
    if not batch_files:
        logger.error("No batch files found!")
        return False
    
    logger.info(f"Found {len(batch_files)} batch files to combine")
    
    try:
        # Read all batch files and combine
        logger.info("Reading batch files...")
        dfs = []
        total_records = 0
        
        for batch_file in batch_files:
            logger.info(f"Reading {batch_file.name}...")
            df = pd.read_parquet(batch_file)
            dfs.append(df)
            total_records += len(df)
            logger.info(f"  {len(df):,} records")
        
        logger.info(f"Total records to combine: {total_records:,}")
        
        # Combine all dataframes
        logger.info("Combining dataframes...")
        combined_df = pd.concat(dfs, ignore_index=True)
        
        # Sort by date and scheme code  
        logger.info("Sorting by date and scheme code...")
        combined_df = combined_df.sort_values(['date', 'scheme_code'])
        
        # Save final file
        logger.info("Saving final Parquet file...")
        combined_df.to_parquet(output_file, index=False, compression='snappy')
        
        # Get file info
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        date_range = f"{combined_df['date'].min().date()} to {combined_df['date'].max().date()}"
        unique_schemes = combined_df['scheme_code'].nunique()
        
        logger.info("✅ SUCCESS!")
        logger.info(f"📦 File: {output_file}")
        logger.info(f"📊 Records: {len(combined_df):,}")
        logger.info(f"📅 Date range: {date_range}")
        logger.info(f"🏢 Unique schemes: {unique_schemes:,}")
        logger.info(f"💾 File size: {file_size_mb:.1f} MB")
        
        # Clean up batch files
        logger.info("Cleaning up batch files...")
        for batch_file in batch_files:
            batch_file.unlink()
        temp_dir.rmdir()
        
        return True
        
    except Exception as e:
        logger.error(f"Error combining files: {e}")
        return False

if __name__ == "__main__":
    success = combine_batch_files()
    exit(0 if success else 1)