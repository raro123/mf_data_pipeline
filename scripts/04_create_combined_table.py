#!/usr/bin/env python3
"""
Combined NAV Table Creator

Creates a unified NAV table by combining historical batches and daily data.
This script has been refactored to use centralized configuration and logging.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import gc

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing, Validation
from utils.logging_setup import get_combine_table_logger, log_script_start, log_script_end, log_data_summary, log_file_operation

# Initialize logger
logger = get_combine_table_logger(__name__)

def create_combined_table_memory_efficient():
    """
    Create combined NAV table using memory-efficient processing.
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_script_start(logger, "Combined NAV Table Creator", 
                    "Creating unified table from historical batches and daily data")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Step 1: Process historical data in chunks and write immediately
    logger.info("📚 Processing historical data in chunks...")
    hist_dir = Paths.RAW_NAV_HISTORICAL
    batch_files = sorted(hist_dir.glob("batch_*.parquet"))
    
    if not batch_files:
        logger.error("❌ No historical batch files found")
        return False
    
    # Create output file path
    output_file = Paths.COMBINED_NAV_TABLE
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Process first batch to establish schema
    logger.info(f"📂 Processing {batch_files[0].name} (establishing schema)...")
    try:
        first_batch = pd.read_parquet(batch_files[0])
        
        # Use categorical types for memory efficiency
        first_batch['scheme_code'] = first_batch['scheme_code'].astype('category')
        first_batch['scheme_name'] = first_batch['scheme_name'].astype('category')
        
        # Write first batch
        first_batch.to_parquet(output_file, index=False, compression=Processing.PARQUET_COMPRESSION)
        total_records = len(first_batch)
        logger.info(f"✅ Written {len(first_batch):,} records")
        
        # Clear memory
        del first_batch
        gc.collect()
        
    except Exception as e:
        logger.error(f"❌ Failed to process first batch: {e}")
        return False
    
    # Process remaining batches and append
    for batch_file in batch_files[1:]:
        logger.info(f"📂 Processing {batch_file.name}...")
        try:
            batch_df = pd.read_parquet(batch_file)
            batch_df['scheme_code'] = batch_df['scheme_code'].astype('category')
            batch_df['scheme_name'] = batch_df['scheme_name'].astype('category')
            
            # Read existing data, combine with batch, write back
            existing_df = pd.read_parquet(output_file)
            combined_chunk = pd.concat([existing_df, batch_df], ignore_index=True)
            combined_chunk.to_parquet(output_file, index=False, compression=Processing.PARQUET_COMPRESSION)
            
            total_records += len(batch_df)
            logger.info(f"✅ Added {len(batch_df):,} records (total: {total_records:,})")
            
            # Clear memory
            del batch_df, existing_df, combined_chunk
            gc.collect()
            
        except Exception as e:
            logger.error(f"❌ Failed to process {batch_file.name}: {e}")
            return False
    
    # Step 2: Add daily data if available
    daily_dir = Paths.RAW_NAV_DAILY
    daily_files = sorted(daily_dir.glob("daily_nav_*.parquet"))
    
    if daily_files:
        logger.info("📅 Adding daily data...")
        for daily_file in daily_files:
            logger.info(f"📂 Processing {daily_file.name}...")
            try:
                daily_df = pd.read_parquet(daily_file)
                daily_df['scheme_code'] = daily_df['scheme_code'].astype('category')
                daily_df['scheme_name'] = daily_df['scheme_name'].astype('category')
                
                # Read existing, combine, write
                existing_df = pd.read_parquet(output_file)
                combined_chunk = pd.concat([existing_df, daily_df], ignore_index=True)
                combined_chunk.to_parquet(output_file, index=False, compression=Processing.PARQUET_COMPRESSION)
                
                total_records += len(daily_df)
                logger.info(f"✅ Added {len(daily_df):,} records (total: {total_records:,})")
                
                # Clear memory
                del daily_df, existing_df, combined_chunk
                gc.collect()
                
            except Exception as e:
                logger.error(f"❌ Failed to process {daily_file.name}: {e}")
                return False
    else:
        logger.info("📅 No daily data files found to add")
    
    # Step 3: Final deduplication pass
    logger.info("🔍 Final deduplication pass...")
    try:
        final_df = pd.read_parquet(output_file)
        initial_count = len(final_df)
        
        final_clean = final_df.drop_duplicates(subset=['scheme_code', 'date'], keep='first')
        final_count = len(final_clean)
        
        if initial_count != final_count:
            duplicates_removed = initial_count - final_count
            duplicate_percentage = (duplicates_removed / initial_count) * 100
            
            logger.info(f"🗑️ Removed {duplicates_removed:,} duplicates ({duplicate_percentage:.2f}%)")
            
            # Check against validation threshold
            if duplicate_percentage > Validation.MAX_DUPLICATE_PERCENTAGE * 100:
                logger.warning(f"⚠️ Duplicate percentage exceeds threshold: {Validation.MAX_DUPLICATE_PERCENTAGE*100}%")
            
            final_clean.to_parquet(output_file, index=False, compression=Processing.PARQUET_COMPRESSION)
        else:
            logger.info("✅ No duplicates found")
        
        # Validate final data
        logger.info("✅ Validating final data...")
        
        # Check NAV values
        nav_null_count = final_clean['nav'].isna().sum()
        nav_null_percentage = (nav_null_count / len(final_clean)) * 100 if len(final_clean) > 0 else 0
        
        if nav_null_percentage > Validation.MAX_NULL_PERCENTAGE * 100:
            logger.warning(f"⚠️ Null NAV percentage: {nav_null_percentage:.2f}% (threshold: {Validation.MAX_NULL_PERCENTAGE*100}%)")
        
        # Final summary
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        log_file_operation(logger, "created", output_file, True, file_size_mb)
        
        logger.info("📊 Final Summary:")
        logger.info(f"   Records: {final_count:,}")
        logger.info(f"   Unique schemes: {final_clean['scheme_code'].nunique():,}")
        logger.info(f"   Null NAV values: {nav_null_count:,} ({nav_null_percentage:.2f}%)")
        
        if not final_clean.empty:
            logger.info(f"   Date range: {final_clean['date'].min().date()} to {final_clean['date'].max().date()}")
            logger.info(f"   NAV range: ₹{final_clean['nav'].min():.2f} to ₹{final_clean['nav'].max():.2f}")
        
        # Clear final memory
        del final_df, final_clean
        gc.collect()
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed in final processing: {e}")
        return False

def main():
    """Main function to create combined NAV table."""
    
    success = create_combined_table_memory_efficient()
    
    log_script_end(logger, "Combined NAV Table Creator", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)