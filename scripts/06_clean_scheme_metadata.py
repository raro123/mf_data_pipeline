#!/usr/bin/env python3
"""
Scheme Metadata Cleaner

Processes raw scheme metadata CSV and creates clean Parquet/CSV files.
This script has been refactored to use centralized configuration and logging.
"""

import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing, Validation
from utils.logging_setup import get_clean_metadata_logger, log_script_start, log_script_end, log_data_summary, log_file_operation

# Initialize logger
logger = get_clean_metadata_logger(__name__)

def load_raw_metadata():
    """
    Load the latest raw scheme metadata from timestamped CSV files.
        
    Returns:
        pandas.DataFrame: Raw metadata or None if failed
    """
    # Get the latest raw metadata file
    from config.settings import get_latest_raw_metadata_file
    
    try:
        input_file = get_latest_raw_metadata_file()
        logger.info(f"📂 Loading raw metadata from {input_file}...")
    except FileNotFoundError as e:
        logger.error(f"❌ {e}")
        logger.info("💡 Run 05_extract_scheme_metadata.py first to extract raw data")
        return None
    
    try:
        # Use configured encoding
        df = pd.read_csv(input_file, encoding=Processing.CSV_ENCODING)
        
        log_data_summary(logger, df, "raw scheme metadata")
        logger.info(f"📋 Columns: {list(df.columns)}")
        
        # Show sample data
        logger.info("📊 Sample data:")
        for i, row in df.head(2).iterrows():
            logger.info(f"   Row {i}: {dict(row)}")
        
        return df
        
    except pd.errors.ParserError as e:
        logger.error(f"❌ Failed to parse CSV: {e}")
        return None
    except Exception as e:
        logger.error(f"❌ Failed to load raw metadata: {e}")
        return None

def clean_scheme_metadata(df):
    """
    Clean and standardize scheme metadata.
    
    Args:
        df (pandas.DataFrame): Raw metadata
        
    Returns:
        pandas.DataFrame: Cleaned metadata
    """
    logger.info("🧹 Cleaning scheme metadata...")
    
    if df is None or df.empty:
        logger.error("No data to clean")
        return None
    
    # Make a copy to avoid modifying original
    clean_df = df.copy()
    initial_count = len(clean_df)
    
    # Standardize column names
    column_mapping = {
        'AMC': 'amc_name',
        'Code': 'scheme_code', 
        'Scheme Name': 'scheme_name',
        'Scheme Type': 'scheme_type',
        'Scheme Category': 'scheme_category',
        'Scheme NAV Name': 'scheme_nav_name',
        'Scheme Minimum Amount': 'minimum_amount',
        'Launch Date': 'launch_date',
        'Closure Date': 'closure_date',
        'ISIN Div Payout/ ISIN Growth': 'isin_growth',
        'ISIN Div Reinvestment': 'isin_dividend'
    }
    
    # Handle potential column name variations
    actual_columns = list(clean_df.columns)
    logger.info(f"📋 Actual columns: {actual_columns}")
    
    # Flexible column mapping
    flexible_mapping = {}
    for old_name in actual_columns:
        for expected, new_name in column_mapping.items():
            if expected.lower() in old_name.lower() or old_name.lower() in expected.lower():
                flexible_mapping[old_name] = new_name
                break
    
    if flexible_mapping:
        clean_df = clean_df.rename(columns=flexible_mapping)
        logger.info(f"📝 Renamed columns: {len(flexible_mapping)}")
        for old, new in flexible_mapping.items():
            logger.info(f"   {old} → {new}")
    
    # Clean scheme_code - ensure it's string and strip whitespace
    if 'scheme_code' in clean_df.columns:
        clean_df['scheme_code'] = clean_df['scheme_code'].astype(str).str.strip()
        # Remove any non-numeric scheme codes if they exist
        numeric_codes = pd.to_numeric(clean_df['scheme_code'], errors='coerce')
        valid_codes = numeric_codes.notna()
        if not valid_codes.all():
            invalid_count = (~valid_codes).sum()
            logger.warning(f"⚠️ Found {invalid_count} non-numeric scheme codes")
            # Keep them as strings but log the issue
    
    # Clean text columns - strip whitespace and handle empty strings
    text_columns = ['amc_name', 'scheme_name', 'scheme_type', 'scheme_category', 'scheme_nav_name']
    for col in text_columns:
        if col in clean_df.columns:
            clean_df[col] = clean_df[col].astype(str).str.strip()
            # Replace empty strings with NaN
            clean_df[col] = clean_df[col].replace('', None)
    
    # Clean dates
    date_columns = ['launch_date', 'closure_date']
    for col in date_columns:
        if col in clean_df.columns:
            try:
                # Handle various date formats
                clean_df[col] = pd.to_datetime(clean_df[col], errors='coerce', dayfirst=True)
                valid_dates = clean_df[col].notna().sum()
                logger.info(f"✅ Cleaned {col}: {valid_dates:,} valid dates")
                
                if valid_dates > 0:
                    min_date = clean_df[col].min()
                    max_date = clean_df[col].max()
                    logger.info(f"   Date range: {min_date.date()} to {max_date.date()}")
            except Exception as e:
                logger.warning(f"⚠️ Could not parse {col}: {e}")
    
    # Clean minimum amount
    if 'minimum_amount' in clean_df.columns:
        try:
            # Convert to numeric, handling various formats
            clean_df['minimum_amount'] = pd.to_numeric(clean_df['minimum_amount'], errors='coerce')
            valid_amounts = clean_df['minimum_amount'].notna().sum()
            logger.info(f"✅ Cleaned minimum_amount: {valid_amounts:,} valid amounts")
            
            if valid_amounts > 0:
                min_amt = clean_df['minimum_amount'].min()
                max_amt = clean_df['minimum_amount'].max()
                logger.info(f"   Amount range: ₹{min_amt:,.0f} to ₹{max_amt:,.0f}")
        except Exception as e:
            logger.warning(f"⚠️ Could not clean minimum_amount: {e}")
    
    # Split ISIN codes if they're combined in one column
    if 'isin_codes' in clean_df.columns and 'isin_growth' not in clean_df.columns:
        logger.info("🔄 Splitting combined ISIN codes...")
        # This would need specific logic based on the actual data format
        # For now, just rename to isin_growth
        clean_df = clean_df.rename(columns={'isin_codes': 'isin_growth'})
    
    # Remove completely empty rows
    clean_df = clean_df.dropna(how='all')
    final_count = len(clean_df)
    
    if initial_count != final_count:
        logger.info(f"🗑️ Removed {initial_count - final_count:,} empty rows")
    
    logger.info(f"✅ Cleaned data shape: {clean_df.shape}")
    
    return clean_df

def validate_scheme_metadata(df):
    """
    Validate the cleaned scheme metadata.
    
    Args:
        df (pandas.DataFrame): Cleaned metadata
        
    Returns:
        bool: True if validation passes
    """
    logger.info("✅ Validating scheme metadata...")
    
    if df is None or df.empty:
        logger.error("No data to validate")
        return False
    
    # Check required columns
    required_columns = ['scheme_code', 'scheme_name', 'amc_name']
    missing_cols = [col for col in required_columns if col not in df.columns]
    
    if missing_cols:
        logger.error(f"❌ Missing required columns: {missing_cols}")
        return False
    
    # Validation checks
    logger.info("📋 Validation summary:")
    logger.info(f"  Total schemes: {len(df):,}")
    
    # Check scheme codes
    if 'scheme_code' in df.columns:
        unique_codes = df['scheme_code'].nunique()
        null_codes = df['scheme_code'].isna().sum()
        logger.info(f"  Unique scheme codes: {unique_codes:,}")
        logger.info(f"  Null scheme codes: {null_codes:,}")
        
        if null_codes > 0:
            logger.warning(f"  ⚠️ Found {null_codes:,} null scheme codes")
        
        # Check for duplicates
        duplicates = df['scheme_code'].duplicated().sum()
        if duplicates > 0:
            logger.warning(f"  ⚠️ Found {duplicates:,} duplicate scheme codes")
    
    # Check AMCs
    if 'amc_name' in df.columns:
        unique_amcs = df['amc_name'].nunique()
        null_amcs = df['amc_name'].isna().sum()
        logger.info(f"  Unique AMCs: {unique_amcs:,}")
        if null_amcs > 0:
            logger.warning(f"  ⚠️ Null AMC names: {null_amcs:,}")
    
    # Check scheme types and categories
    if 'scheme_type' in df.columns:
        logger.info(f"  Scheme types: {df['scheme_type'].nunique():,}")
    
    if 'scheme_category' in df.columns:
        logger.info(f"  Scheme categories: {df['scheme_category'].nunique():,}")
    
    # Check launch dates
    if 'launch_date' in df.columns:
        valid_launch_dates = df['launch_date'].notna().sum()
        logger.info(f"  Valid launch dates: {valid_launch_dates:,}")
        
        if valid_launch_dates > 0:
            earliest = df['launch_date'].min()
            latest = df['launch_date'].max()
            logger.info(f"  Launch date range: {earliest.date()} to {latest.date()}")
    
    logger.info("✅ Validation completed")
    return True

def save_scheme_metadata(df):
    """
    Save the cleaned scheme metadata using configured paths.
    
    Args:
        df (pandas.DataFrame): Cleaned metadata
        
    Returns:
        str: Path to saved file or None if failed
    """
    # Use configured output paths
    parquet_file = Paths.SCHEME_METADATA_CLEAN
    csv_file = Paths.SCHEME_METADATA_CSV
    
    logger.info(f"💾 Saving cleaned scheme metadata...")
    
    if df is None or df.empty:
        logger.error("No data to save")
        return None
    
    # Create output directory
    parquet_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Use categorical types for memory efficiency
        categorical_columns = ['amc_name', 'scheme_name', 'scheme_type', 'scheme_category', 'scheme_nav_name']
        
        df_save = df.copy()
        for col in categorical_columns:
            if col in df_save.columns:
                df_save[col] = df_save[col].astype('category')
                logger.info(f"   Made {col} categorical ({df_save[col].nunique()} categories)")
        
        # Save as Parquet with configured compression
        df_save.to_parquet(parquet_file, index=False, compression=Processing.PARQUET_COMPRESSION)
        
        parquet_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", parquet_file, True, parquet_size_mb)
        
        # Also save as CSV for easy viewing
        df.to_csv(csv_file, index=False, encoding=Processing.CSV_ENCODING)
        csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", csv_file, True, csv_size_mb)
        
        logger.info(f"📊 Records: {len(df_save):,}")
        
        return str(parquet_file)
        
    except Exception as e:
        logger.error(f"Failed to save scheme metadata: {e}")
        return None

def main():
    """Main function to clean and save scheme metadata."""
    
    log_script_start(logger, "Scheme Metadata Cleaner", 
                    "Processing raw scheme metadata into clean Parquet/CSV files")
    
    # Check if processing is needed
    from config.settings import should_process_metadata, get_latest_raw_metadata_file
    
    if not should_process_metadata():
        try:
            latest_raw = get_latest_raw_metadata_file()
            processed_file = Paths.PROCESSED_SCHEME_METADATA / "amfi_scheme_metadata.parquet"
            
            logger.info("✅ Processed metadata is up-to-date")
            logger.info(f"📂 Latest raw: {latest_raw.name}")
            logger.info(f"📄 Processed file: {processed_file.name}")
            logger.info("💡 No processing needed - metadata already current")
            
            log_script_end(logger, "Scheme Metadata Cleaner", True)
            return 0
            
        except Exception as e:
            logger.warning(f"⚠️ Could not check file timestamps: {e}")
            logger.info("🔄 Proceeding with processing...")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Load raw data
    raw_data = load_raw_metadata()
    if raw_data is None:
        log_script_end(logger, "Scheme Metadata Cleaner", False)
        return 1
    
    # Clean data
    clean_data = clean_scheme_metadata(raw_data)
    if clean_data is None:
        logger.error("❌ Failed to clean scheme metadata")
        log_script_end(logger, "Scheme Metadata Cleaner", False)
        return 1
    
    # Validate data
    validation_passed = validate_scheme_metadata(clean_data)
    if not validation_passed:
        logger.warning("⚠️ Data validation had warnings, but continuing...")
    
    # Save cleaned data
    saved_path = save_scheme_metadata(clean_data)
    success = saved_path is not None
    
    log_script_end(logger, "Scheme Metadata Cleaner", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)