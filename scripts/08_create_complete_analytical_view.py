#!/usr/bin/env python3
"""
Complete Analytical NAV Data Creator

Creates analytical view by combining available historical data with daily NAV files.
This handles the case where historical batches weren't fully processed due to memory constraints.
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
from utils.logging_setup import get_analytical_nav_logger, log_script_start, log_script_end, log_data_summary, log_file_operation

# Initialize logger
logger = get_analytical_nav_logger(__name__)

def load_all_nav_data():
    """
    Load all available NAV data (historical + daily files).
    
    Returns:
        pandas.DataFrame: Complete NAV data or None if failed
    """
    logger.info("📂 Loading all available NAV data...")
    all_dfs = []
    total_records = 0
    
    # Step 1: Load historical combined data if available
    historical_file = Paths.COMBINED_NAV_TABLE
    if historical_file.exists():
        logger.info(f"📂 Loading historical data from {historical_file}...")
        try:
            hist_df = pd.read_parquet(historical_file)
            logger.info(f"✅ Historical: {len(hist_df):,} records ({hist_df['date'].min().date()} to {hist_df['date'].max().date()})")
            all_dfs.append(hist_df)
            total_records += len(hist_df)
        except Exception as e:
            logger.error(f"❌ Failed to load historical data: {e}")
    
    # Step 2: Load daily NAV files
    daily_dir = Paths.RAW_NAV_DAILY
    daily_files = sorted(daily_dir.glob("daily_nav_*.parquet"))
    
    if daily_files:
        logger.info(f"📂 Found {len(daily_files)} daily NAV files...")
        for daily_file in daily_files:
            try:
                daily_df = pd.read_parquet(daily_file)
                logger.info(f"✅ Daily {daily_file.name}: {len(daily_df):,} records ({daily_df['date'].iloc[0].date()})")
                all_dfs.append(daily_df)
                total_records += len(daily_df)
            except Exception as e:
                logger.warning(f"⚠️ Skipped {daily_file.name}: {e}")
    
    # Step 3: Combine all data
    if not all_dfs:
        logger.error("❌ No NAV data found to process")
        return None
    
    logger.info(f"🔗 Combining {len(all_dfs)} datasets ({total_records:,} total records)...")
    try:
        combined_df = pd.concat(all_dfs, ignore_index=True)
        
        # Sort by date and scheme_code for consistency
        combined_df = combined_df.sort_values(['scheme_code', 'date']).reset_index(drop=True)
        
        # Remove duplicates (in case of overlap between historical and daily)
        before_dedup = len(combined_df)
        combined_df = combined_df.drop_duplicates(subset=['scheme_code', 'date'], keep='last')
        after_dedup = len(combined_df)
        
        if before_dedup != after_dedup:
            logger.info(f"🧹 Removed {before_dedup - after_dedup:,} duplicate records")
        
        log_data_summary(logger, combined_df, "complete NAV dataset")
        logger.info(f"📅 Complete date range: {combined_df['date'].min().date()} to {combined_df['date'].max().date()}")
        logger.info(f"🏢 Unique schemes: {combined_df['scheme_code'].nunique():,}")
        
        # Clean up memory
        del all_dfs
        gc.collect()
        
        return combined_df
        
    except Exception as e:
        logger.error(f"❌ Failed to combine NAV data: {e}")
        return None

def load_scheme_metadata():
    """
    Load processed scheme metadata.
    
    Returns:
        pandas.DataFrame: Scheme metadata or None if failed
    """
    metadata_file = Paths.SCHEME_METADATA_CLEAN
    
    logger.info(f"📂 Loading scheme metadata from {metadata_file}...")
    
    if not metadata_file.exists():
        logger.error(f"❌ Scheme metadata file not found: {metadata_file}")
        logger.info("💡 Run 05_extract_scheme_metadata.py and 06_clean_scheme_metadata.py first")
        return None
    
    try:
        df = pd.read_parquet(metadata_file)
        log_data_summary(logger, df, "scheme metadata")
        
        logger.info(f"🏢 Unique AMCs: {df['amc_name'].nunique():,}")
        logger.info(f"📊 Scheme types: {df['scheme_type'].nunique():,}")
        logger.info(f"📈 Scheme categories: {df['scheme_category'].nunique():,}")
        
        return df
        
    except Exception as e:
        logger.error(f"❌ Failed to load scheme metadata: {e}")
        return None

def create_analytical_nav_daily(nav_df, metadata_df):
    """
    Create analytical NAV daily dataset by joining NAV data with metadata.
    
    Args:
        nav_df (pandas.DataFrame): Complete NAV data
        metadata_df (pandas.DataFrame): Scheme metadata
        
    Returns:
        pandas.DataFrame: Analytical dataset or None if failed
    """
    logger.info("🔗 Creating complete analytical NAV daily dataset...")
    
    try:
        # Log initial record counts
        logger.info(f"📊 Input data:")
        logger.info(f"   NAV records: {len(nav_df):,}")
        logger.info(f"   Metadata records: {len(metadata_df):,}")
        logger.info(f"   NAV unique schemes: {nav_df['scheme_code'].nunique():,}")
        logger.info(f"   Metadata unique schemes: {metadata_df['scheme_code'].nunique():,}")
        
        # Prepare metadata columns for join
        metadata_cols = ['scheme_code', 'scheme_name', 'amc_name', 'scheme_type', 
                        'scheme_category', 'launch_date', 'minimum_amount']
        
        # Add enhanced categorization columns if they exist
        if 'scheme_category_level1' in metadata_df.columns:
            metadata_cols.append('scheme_category_level1')
            logger.info("✅ Found enhanced categorization: scheme_category_level1")
        if 'scheme_category_level2' in metadata_df.columns:
            metadata_cols.append('scheme_category_level2')
            logger.info("✅ Found enhanced categorization: scheme_category_level2")
        
        # Add Direct/Regular plan indicator if available
        if 'is_direct' in metadata_df.columns:
            metadata_cols.append('is_direct')
            logger.info("✅ Found Direct/Regular plan indicator: is_direct")
        
        # Add Growth/Dividend plan indicator if available
        if 'is_growth_plan' in metadata_df.columns:
            metadata_cols.append('is_growth_plan')
            logger.info("✅ Found Growth/Dividend plan indicator: is_growth_plan")
        
        # Perform inner join on scheme_code
        analytical_df = pd.merge(
            nav_df,
            metadata_df[metadata_cols],
            on='scheme_code',
            how='inner',
            suffixes=('_nav', '_meta')
        )
        
        # Clean up column names - prefer metadata scheme name
        if 'scheme_name_nav' in analytical_df.columns and 'scheme_name_meta' in analytical_df.columns:
            analytical_df['scheme_name'] = analytical_df['scheme_name_meta']
            analytical_df = analytical_df.drop(['scheme_name_nav', 'scheme_name_meta'], axis=1)
        
        # Log join results
        logger.info(f"✅ Join completed:")
        logger.info(f"   Result records: {len(analytical_df):,}")
        logger.info(f"   Join coverage: {len(analytical_df)/len(nav_df)*100:.1f}% of NAV data")
        logger.info(f"   Unique schemes in result: {analytical_df['scheme_code'].nunique():,}")
        
        # Add derived columns for analysis
        logger.info("📊 Adding derived columns...")
        analytical_df['year'] = analytical_df['date'].dt.year
        analytical_df['month'] = analytical_df['date'].dt.month
        analytical_df['quarter'] = analytical_df['date'].dt.quarter
        analytical_df['weekday'] = analytical_df['date'].dt.day_name()
        
        # Calculate performance metrics where possible
        analytical_df['nav_change_pct'] = analytical_df.groupby('scheme_code')['nav'].pct_change()
        
        # Optimize column types for storage efficiency  
        logger.info("🔧 Optimizing column types...")
        categorical_cols = ['scheme_name', 'amc_name', 'scheme_type', 'scheme_category', 'weekday']
        
        # Add enhanced categorization columns if they exist
        if 'scheme_category_level1' in analytical_df.columns:
            categorical_cols.append('scheme_category_level1')
        if 'scheme_category_level2' in analytical_df.columns:
            categorical_cols.append('scheme_category_level2')
        
        for col in categorical_cols:
            if col in analytical_df.columns:
                analytical_df[col] = analytical_df[col].astype('category')
                logger.info(f"   Made {col} categorical ({analytical_df[col].nunique()} categories)")
        
        # Integer columns for better compression
        analytical_df['year'] = analytical_df['year'].astype('int16')
        analytical_df['month'] = analytical_df['month'].astype('int8') 
        analytical_df['quarter'] = analytical_df['quarter'].astype('int8')
        
        # Final dataset summary
        log_data_summary(logger, analytical_df, "complete analytical NAV daily dataset")
        
        logger.info("📋 Final dataset columns:")
        for col in analytical_df.columns:
            logger.info(f"   {col}: {analytical_df[col].dtype}")
        
        return analytical_df
        
    except Exception as e:
        logger.error(f"❌ Failed to create analytical dataset: {e}")
        return None

def save_analytical_data(df):
    """
    Save analytical dataset to parquet file.
    
    Args:
        df (pandas.DataFrame): Analytical dataset
        
    Returns:
        bool: True if successful, False otherwise
    """
    if df is None or df.empty:
        logger.error("❌ No data to save")
        return False
    
    output_file = Paths.ANALYTICAL / "complete_nav_daily_data.parquet"
    
    logger.info(f"💾 Saving complete analytical dataset to {output_file}...")
    
    try:
        # Ensure output directory exists
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Save with compression for efficiency
        df.to_parquet(
            output_file,
            index=False,
            compression=Processing.PARQUET_COMPRESSION
        )
        
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", output_file, True, file_size_mb)
        
        logger.info("📊 Final dataset summary:")
        logger.info(f"   Records: {len(df):,}")
        logger.info(f"   Columns: {len(df.columns)}")
        logger.info(f"   Date range: {df['date'].min().date()} to {df['date'].max().date()}")
        logger.info(f"   Unique schemes: {df['scheme_code'].nunique():,}")
        logger.info(f"   Unique AMCs: {df['amc_name'].nunique():,}")
        logger.info(f"   File size: {file_size_mb:.2f} MB")
        
        # Memory usage summary
        memory_usage = df.memory_usage(deep=True).sum() / (1024 * 1024)
        logger.info(f"   Memory usage: {memory_usage:.1f} MB")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to save analytical dataset: {e}")
        return False

def validate_analytical_data(df):
    """
    Validate the analytical dataset for quality and completeness.
    
    Args:
        df (pandas.DataFrame): Analytical dataset
        
    Returns:
        bool: True if validation passes, False otherwise
    """
    logger.info("✅ Validating complete analytical dataset...")
    
    try:
        # Check for required columns
        required_columns = ['date', 'scheme_code', 'nav', 'scheme_name', 'amc_name', 'scheme_category']
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            logger.error(f"❌ Missing required columns: {missing_columns}")
            return False
        
        # Check for null values in critical columns
        for col in ['date', 'scheme_code', 'nav']:
            null_count = df[col].isnull().sum()
            if null_count > 0:
                logger.error(f"❌ Found {null_count} null values in critical column: {col}")
                return False
        
        # Check NAV value ranges
        nav_min = df['nav'].min()
        nav_max = df['nav'].max()
        
        if nav_min < Validation.MIN_NAV_VALUE or nav_max > Validation.MAX_NAV_VALUE:
            logger.warning(f"⚠️ NAV values outside expected range: {nav_min:.2f} to {nav_max:.2f}")
        
        # Check date range
        date_min = df['date'].min()
        date_max = df['date'].max()
        
        if date_max < pd.Timestamp.now() - pd.Timedelta(days=30):
            logger.warning(f"⚠️ Latest date seems old: {date_max.date()}")
        
        logger.info("✅ Validation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"❌ Validation failed: {e}")
        return False

def main():
    """Main function to create complete analytical NAV daily dataset."""
    
    log_script_start(logger, "Complete Analytical NAV Creator", 
                    "Creating complete analytical dataset from all available NAV data")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Load input data
    logger.info("📥 Loading all available NAV data...")
    nav_data = load_all_nav_data()
    if nav_data is None:
        log_script_end(logger, "Complete Analytical NAV Creator", False)
        return 1
    
    metadata = load_scheme_metadata()
    if metadata is None:
        log_script_end(logger, "Complete Analytical NAV Creator", False) 
        return 1
    
    # Create analytical dataset
    analytical_data = create_analytical_nav_daily(nav_data, metadata)
    if analytical_data is None:
        log_script_end(logger, "Complete Analytical NAV Creator", False)
        return 1
    
    # Validate data quality
    if not validate_analytical_data(analytical_data):
        log_script_end(logger, "Complete Analytical NAV Creator", False)
        return 1
    
    # Save analytical dataset
    if not save_analytical_data(analytical_data):
        log_script_end(logger, "Complete Analytical NAV Creator", False)
        return 1
    
    # Clean up memory
    del nav_data, metadata, analytical_data
    gc.collect()
    
    logger.info("🎉 Successfully created complete analytical NAV daily dataset!")
    logger.info("🔍 Dataset includes all available historical + daily NAV data")
    logger.info("📊 Ready for comprehensive analysis, reporting, and ML applications")
    
    log_script_end(logger, "Complete Analytical NAV Creator", True)
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)