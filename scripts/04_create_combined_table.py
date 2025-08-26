#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Configuration
load_dotenv()

# Logging setup
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/raw_nav_table_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def load_historical_data():
    """
    Load all historical batch files.
    
    Returns:
        pandas.DataFrame: Combined historical data
    """
    logger.info("📚 Loading historical data...")
    
    hist_dir = Path("raw/amfi_nav_history")
    if not hist_dir.exists():
        logger.error("Historical data directory not found")
        return None
    
    batch_files = sorted(hist_dir.glob("batch_*.parquet"))
    if not batch_files:
        logger.error("No historical batch files found")
        return None
    
    logger.info(f"Found {len(batch_files)} historical batch files")
    
    dataframes = []
    total_records = 0
    
    for batch_file in batch_files:
        logger.info(f"  Loading {batch_file.name}...")
        try:
            df = pd.read_parquet(batch_file)
            dataframes.append(df)
            total_records += len(df)
            logger.info(f"    ✅ {len(df):,} records")
        except Exception as e:
            logger.error(f"    ❌ Failed to load {batch_file}: {e}")
            return None
    
    # Combine all historical data
    logger.info("🔄 Combining historical batches...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    logger.info(f"✅ Historical data loaded: {len(combined_df):,} records")
    logger.info(f"📅 Date range: {combined_df['date'].min().date()} to {combined_df['date'].max().date()}")
    
    return combined_df

def load_daily_data():
    """
    Load all daily NAV files.
    
    Returns:
        pandas.DataFrame: Combined daily data
    """
    logger.info("📅 Loading daily data...")
    
    daily_dir = Path("raw/amfi_nav_daily")
    if not daily_dir.exists():
        logger.warning("Daily data directory not found")
        return pd.DataFrame()
    
    daily_files = sorted(daily_dir.glob("daily_nav_*.parquet"))
    if not daily_files:
        logger.warning("No daily files found")
        return pd.DataFrame()
    
    logger.info(f"Found {len(daily_files)} daily files")
    
    dataframes = []
    total_records = 0
    
    for daily_file in daily_files:
        logger.info(f"  Loading {daily_file.name}...")
        try:
            df = pd.read_parquet(daily_file)
            dataframes.append(df)
            total_records += len(df)
            logger.info(f"    ✅ {len(df):,} records")
        except Exception as e:
            logger.error(f"    ❌ Failed to load {daily_file}: {e}")
            continue
    
    if not dataframes:
        logger.warning("No valid daily files loaded")
        return pd.DataFrame()
    
    # Combine all daily data
    logger.info("🔄 Combining daily files...")
    combined_df = pd.concat(dataframes, ignore_index=True)
    
    logger.info(f"✅ Daily data loaded: {len(combined_df):,} records")
    logger.info(f"📅 Date range: {combined_df['date'].min().date()} to {combined_df['date'].max().date()}")
    
    return combined_df

def deduplicate_data(df):
    """
    Remove duplicate records based on scheme_code and date.
    
    Args:
        df (pandas.DataFrame): Combined data
        
    Returns:
        pandas.DataFrame: Deduplicated data
    """
    logger.info("🔍 Checking for duplicates...")
    
    initial_count = len(df)
    
    # Check for duplicates
    duplicates = df.duplicated(subset=['scheme_code', 'date'], keep='first')
    duplicate_count = duplicates.sum()
    
    if duplicate_count > 0:
        logger.warning(f"Found {duplicate_count:,} duplicate records")
        
        # Show sample duplicates
        duplicate_rows = df[duplicates].head(3)
        for _, row in duplicate_rows.iterrows():
            logger.warning(f"  Duplicate: {row['scheme_code']} on {row['date'].date()}")
        
        # Remove duplicates (keep first occurrence)
        df_clean = df.drop_duplicates(subset=['scheme_code', 'date'], keep='first')
        logger.info(f"✅ Removed {duplicate_count:,} duplicates")
    else:
        logger.info("✅ No duplicates found")
        df_clean = df
    
    final_count = len(df_clean)
    logger.info(f"📊 Records: {initial_count:,} → {final_count:,}")
    
    return df_clean

def validate_combined_data(df):
    """
    Validate the combined dataset.
    
    Args:
        df (pandas.DataFrame): Combined data
        
    Returns:
        bool: True if validation passes
    """
    logger.info("✅ Validating combined data...")
    
    # Check required columns
    required_columns = ['scheme_code', 'scheme_name', 'isin_growth', 'isin_dividend', 
                       'nav', 'repurchase_price', 'sale_price', 'date']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        logger.error(f"Missing columns: {missing_columns}")
        return False
    
    # Check for empty dataframe
    if df.empty:
        logger.error("Combined dataset is empty")
        return False
    
    # Check data types
    logger.info("📋 Data validation:")
    logger.info(f"  Total records: {len(df):,}")
    logger.info(f"  Unique schemes: {df['scheme_code'].nunique():,}")
    logger.info(f"  Date range: {df['date'].min().date()} to {df['date'].max().date()}")
    logger.info(f"  Null NAV values: {df['nav'].isna().sum():,}")
    
    # Check for critical issues
    if df['nav'].isna().all():
        logger.error("All NAV values are null")
        return False
    
    if df['scheme_code'].isna().sum() > len(df) * 0.1:  # More than 10% missing
        logger.warning("High percentage of missing scheme codes")
    
    logger.info("✅ Data validation passed")
    return True

def save_combined_table(df, output_path="data/raw_nav_table.parquet"):
    """
    Save the combined NAV table.
    
    Args:
        df (pandas.DataFrame): Combined data
        output_path (str): Output file path
        
    Returns:
        str: Path to saved file or None if failed
    """
    logger.info(f"💾 Saving combined table to {output_path}...")
    
    # Create output directory
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Use categorical types for memory efficiency (no sorting to avoid memory issues)
        df['scheme_code'] = df['scheme_code'].astype('category')
        df['scheme_name'] = df['scheme_name'].astype('category')
        
        # Save as Parquet without sorting
        df.to_parquet(output_file, index=False, compression='snappy')
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        
        logger.info(f"✅ Saved combined NAV table: {output_file}")
        logger.info(f"📊 Records: {len(df):,}")
        logger.info(f"📦 Size: {file_size_mb:.2f} MB")
        logger.info(f"📅 Date range: {df['date'].min().date()} to {df['date'].max().date()}")
        logger.info(f"🏢 Unique schemes: {df['scheme_code'].nunique():,}")
        
        return str(output_file)
        
    except Exception as e:
        logger.error(f"Failed to save combined table: {e}")
        return None

def main():
    """Main function to create combined raw NAV table with memory-efficient processing."""
    logger.info("🚀 Starting raw NAV table creation...")
    
    # Step 1: Process historical data in chunks and write immediately
    logger.info("📚 Processing historical data in chunks...")
    hist_dir = Path("raw/amfi_nav_history")
    batch_files = sorted(hist_dir.glob("batch_*.parquet"))
    
    if not batch_files:
        logger.error("No historical batch files found")
        return 1
    
    # Create output directory
    output_file = Path("data/raw_nav_table.parquet")
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Process first batch to establish schema
    logger.info(f"Processing {batch_files[0].name} (establishing schema)...")
    first_batch = pd.read_parquet(batch_files[0])
    first_batch['scheme_code'] = first_batch['scheme_code'].astype('category')
    first_batch['scheme_name'] = first_batch['scheme_name'].astype('category')
    
    # Write first batch
    first_batch.to_parquet(output_file, index=False, compression='snappy')
    total_records = len(first_batch)
    logger.info(f"✅ Written {len(first_batch):,} records")
    
    # Process remaining batches and append
    for batch_file in batch_files[1:]:
        logger.info(f"Processing {batch_file.name}...")
        batch_df = pd.read_parquet(batch_file)
        batch_df['scheme_code'] = batch_df['scheme_code'].astype('category')
        batch_df['scheme_name'] = batch_df['scheme_name'].astype('category')
        
        # Read existing data, combine with batch, write back
        existing_df = pd.read_parquet(output_file)
        combined_chunk = pd.concat([existing_df, batch_df], ignore_index=True)
        combined_chunk.to_parquet(output_file, index=False, compression='snappy')
        
        total_records += len(batch_df)
        logger.info(f"✅ Added {len(batch_df):,} records (total: {total_records:,})")
        
        # Clear memory
        del batch_df, existing_df, combined_chunk
    
    # Step 2: Add daily data if available
    daily_dir = Path("raw/amfi_nav_daily")
    daily_files = sorted(daily_dir.glob("daily_nav_*.parquet"))
    
    if daily_files:
        logger.info("📅 Adding daily data...")
        for daily_file in daily_files:
            logger.info(f"Processing {daily_file.name}...")
            daily_df = pd.read_parquet(daily_file)
            daily_df['scheme_code'] = daily_df['scheme_code'].astype('category')
            daily_df['scheme_name'] = daily_df['scheme_name'].astype('category')
            
            # Read existing, combine, write
            existing_df = pd.read_parquet(output_file)
            combined_chunk = pd.concat([existing_df, daily_df], ignore_index=True)
            combined_chunk.to_parquet(output_file, index=False, compression='snappy')
            
            total_records += len(daily_df)
            logger.info(f"✅ Added {len(daily_df):,} records (total: {total_records:,})")
            
            # Clear memory
            del daily_df, existing_df, combined_chunk
    
    # Step 3: Final deduplication pass
    logger.info("🔍 Final deduplication pass...")
    final_df = pd.read_parquet(output_file)
    initial_count = len(final_df)
    
    final_clean = final_df.drop_duplicates(subset=['scheme_code', 'date'], keep='first')
    final_count = len(final_clean)
    
    if initial_count != final_count:
        logger.info(f"🗑️ Removed {initial_count - final_count:,} duplicates")
        final_clean.to_parquet(output_file, index=False, compression='snappy')
    
    # Final summary
    file_size_mb = output_file.stat().st_size / (1024 * 1024)
    
    logger.info(f"🎉 Successfully created raw NAV table: {output_file}")
    logger.info(f"📊 Final records: {final_count:,}")
    logger.info(f"📦 Size: {file_size_mb:.2f} MB")
    logger.info(f"📅 Date range: {final_clean['date'].min().date()} to {final_clean['date'].max().date()}")
    logger.info(f"🏢 Unique schemes: {final_clean['scheme_code'].nunique():,}")
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)