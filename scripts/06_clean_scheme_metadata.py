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

def enhance_scheme_categorization(df, logger):
    """
    Extract enhanced scheme categorization from scheme_category column.
    
    Creates two new columns:
    - scheme_category_level1: Main category (Equity Scheme, Debt Scheme, etc.)
    - scheme_category_level2: Sub-category (Large Cap Fund, Liquid Fund, etc.)
    
    Args:
        df (pandas.DataFrame): DataFrame with scheme_category column
        logger: Logger instance
        
    Returns:
        pandas.DataFrame: DataFrame with enhanced categorization
    """
    df_enhanced = df.copy()
    
    # Initialize new columns
    df_enhanced['scheme_category_level1'] = 'Others'
    df_enhanced['scheme_category_level2'] = 'Others'
    
    # Process each category
    total_processed = 0
    pattern_stats = {
        'Equity Scheme': 0,
        'Debt Scheme': 0, 
        'Hybrid Scheme': 0,
        'Other Scheme': 0,
        'Others': 0
    }
    
    for idx, category in df_enhanced['scheme_category'].items():
        if pd.isna(category) or category.strip() == '':
            # Handle null/empty categories
            df_enhanced.loc[idx, 'scheme_category_level1'] = 'Others'
            df_enhanced.loc[idx, 'scheme_category_level2'] = 'Others'
            pattern_stats['Others'] += 1
        elif ' - ' in category:
            # Split on first occurrence of ' - '
            parts = category.split(' - ', 1)
            level1 = parts[0].strip()
            level2 = parts[1].strip() if len(parts) > 1 else level1
            
            # Standardize level 1 categories
            if 'equity' in level1.lower():
                level1_clean = 'Equity Scheme'
            elif 'debt' in level1.lower():
                level1_clean = 'Debt Scheme'
            elif 'hybrid' in level1.lower():
                level1_clean = 'Hybrid Scheme'
            elif 'other' in level1.lower():
                level1_clean = 'Other Scheme'
            else:
                level1_clean = 'Others'
            
            # Handle empty or error level 2
            if not level2 or level2.lower() in ['error', 'null', 'na', '']:
                level2 = level1_clean
            
            df_enhanced.loc[idx, 'scheme_category_level1'] = level1_clean
            df_enhanced.loc[idx, 'scheme_category_level2'] = level2
            pattern_stats[level1_clean] += 1
        else:
            # Categories without ' - ' pattern (like 'Income', 'Growth')
            # These are legacy categories, classify them appropriately
            category_lower = category.lower().strip()
            
            if category_lower in ['income', 'growth', 'dividend']:
                # These are old naming conventions, likely mixed types
                df_enhanced.loc[idx, 'scheme_category_level1'] = 'Others'
                df_enhanced.loc[idx, 'scheme_category_level2'] = category.strip()
            else:
                # Other unstructured categories
                df_enhanced.loc[idx, 'scheme_category_level1'] = 'Others'
                df_enhanced.loc[idx, 'scheme_category_level2'] = category.strip()
            
            pattern_stats['Others'] += 1
        
        total_processed += 1
    
    # Log statistics
    logger.info(f"📊 Enhanced categorization statistics:")
    for category, count in pattern_stats.items():
        if count > 0:
            percentage = (count / total_processed) * 100
            logger.info(f"   {category}: {count:,} schemes ({percentage:.1f}%)")
    
    # Log sample categorizations
    logger.info("📋 Sample enhanced categorizations:")
    sample_df = df_enhanced[['scheme_category', 'scheme_category_level1', 'scheme_category_level2']].drop_duplicates()
    for i, row in sample_df.head(10).iterrows():
        original = row['scheme_category'][:50] + '...' if len(str(row['scheme_category'])) > 50 else row['scheme_category']
        logger.info(f"   '{original}' → L1: '{row['scheme_category_level1']}', L2: '{row['scheme_category_level2']}'")
    
    # Get unique counts for new columns
    level1_unique = df_enhanced['scheme_category_level1'].nunique()
    level2_unique = df_enhanced['scheme_category_level2'].nunique()
    
    logger.info(f"✅ Enhanced categorization completed:")
    logger.info(f"   Level 1 categories: {level1_unique}")
    logger.info(f"   Level 2 categories: {level2_unique}")
    logger.info(f"   Total schemes processed: {total_processed:,}")
    
    return df_enhanced

def detect_direct_plan(scheme_nav_name):
    """
    Detect if a scheme is Direct or Regular based on scheme_nav_name.
    
    Args:
        scheme_nav_name (str): Scheme NAV name
        
    Returns:
        bool: True if Direct plan, False if Regular plan or unclear
    """
    if pd.isna(scheme_nav_name) or not isinstance(scheme_nav_name, str):
        return False
    
    name_lower = scheme_nav_name.lower().strip()
    
    # Direct plan indicators (case-insensitive)
    direct_indicators = [
        'direct',
        'dir ',      # with space to avoid matching words like "director"
        ' dir',      # with space prefix
        '-dir-',     # with dashes
        '-direct-',  # with dashes
        '(dir)',     # in parentheses
        '(direct)',  # in parentheses
        'drct',      # abbreviated form
    ]
    
    # Regular plan indicators (case-insensitive)
    regular_indicators = [
        'regular',
        'reg ',      # with space to avoid matching words like "region"
        ' reg',      # with space prefix
        '-reg-',     # with dashes
        '-regular-', # with dashes
        '(reg)',     # in parentheses
        '(regular)', # in parentheses
        'rglr',      # abbreviated form
    ]
    
    # Check for Direct indicators first (higher priority)
    has_direct = any(indicator in name_lower for indicator in direct_indicators)
    has_regular = any(indicator in name_lower for indicator in regular_indicators)
    
    # Priority logic: Direct wins if both are present
    if has_direct:
        return True
    elif has_regular:
        return False
    else:
        # Default: neither found, assume Regular
        return False

def enhance_direct_regular_detection(df, logger):
    """
    Add is_direct column to detect Direct vs Regular mutual fund plans.
    
    Args:
        df (pandas.DataFrame): DataFrame with scheme_nav_name column
        logger: Logger instance
        
    Returns:
        pandas.DataFrame: DataFrame with is_direct column
    """
    df_enhanced = df.copy()
    
    if 'scheme_nav_name' not in df_enhanced.columns:
        logger.warning("⚠️ scheme_nav_name column not found, skipping Direct/Regular detection")
        df_enhanced['is_direct'] = False
        return df_enhanced
    
    logger.info("🎯 Detecting Direct vs Regular plans...")
    
    # Apply detection logic
    df_enhanced['is_direct'] = df_enhanced['scheme_nav_name'].apply(detect_direct_plan)
    
    # Calculate statistics
    total_schemes = len(df_enhanced)
    direct_count = df_enhanced['is_direct'].sum()
    regular_count = total_schemes - direct_count
    
    # Log statistics
    logger.info(f"📊 Direct vs Regular plan distribution:")
    logger.info(f"   Direct plans: {direct_count:,} ({direct_count/total_schemes*100:.1f}%)")
    logger.info(f"   Regular plans: {regular_count:,} ({regular_count/total_schemes*100:.1f}%)")
    
    # Show sample detections for verification
    logger.info("📋 Sample Direct/Regular detections:")
    
    # Sample direct plans
    direct_samples = df_enhanced[df_enhanced['is_direct'] == True]['scheme_nav_name'].head(5)
    logger.info("   Direct plans detected:")
    for i, name in enumerate(direct_samples, 1):
        name_display = name[:70] + '...' if len(name) > 70 else name
        logger.info(f"   {i}. {name_display}")
    
    # Sample regular plans  
    regular_samples = df_enhanced[df_enhanced['is_direct'] == False]['scheme_nav_name'].head(5)
    logger.info("   Regular plans detected:")
    for i, name in enumerate(regular_samples, 1):
        name_display = name[:70] + '...' if len(name) > 70 else name
        logger.info(f"   {i}. {name_display}")
    
    logger.info(f"✅ Direct/Regular detection completed for {total_schemes:,} schemes")
    
    return df_enhanced

def detect_growth_plan(scheme_nav_name):
    """
    Detect if a scheme is Growth or Dividend/IDCW based on scheme_nav_name.
    
    Growth plans reinvest dividends (compounding), while IDCW/Dividend plans
    distribute them (affecting NAV comparability).
    
    Args:
        scheme_nav_name (str): Scheme NAV name
        
    Returns:
        bool: True if Growth plan, False if Dividend/IDCW plan
    """
    if pd.isna(scheme_nav_name) or not isinstance(scheme_nav_name, str):
        return False
    
    name_lower = scheme_nav_name.lower().strip()
    
    # Non-growth (dividend/payout) indicators (higher priority)
    non_growth_indicators = [
        'idcw',           # Income Distribution cum Capital Withdrawal
        'dividend',       # Traditional dividend plans
        'income',         # Income distribution
        'monthly',        # Monthly payout
        'quarterly',      # Quarterly payout  
        'weekly',         # Weekly payout
        'daily',          # Daily payout
        'annual',         # Annual payout
        'payout',         # Generic payout term
        'distribution',   # Distribution plans
        'div ',           # Abbreviated dividend with space
        ' div',           # Abbreviated dividend with space prefix
        'div)',           # Abbreviated dividend in parentheses
        '(div)',          # Abbreviated dividend in full parentheses
    ]
    
    # Growth plan indicators
    growth_indicators = [
        'growth',
        'grwth',          # Abbreviated form
        'gr ',            # Very abbreviated with space
        ' gr',            # Very abbreviated with space prefix  
        'gr)',            # Very abbreviated in parentheses
        '(gr)',           # Very abbreviated in full parentheses
        'growt',          # Partial match for typos
        'accum',          # Accumulation (alternate term for growth)
        'accumulation',   # Full accumulation term
    ]
    
    # Check for non-growth indicators first (higher priority)
    has_non_growth = any(indicator in name_lower for indicator in non_growth_indicators)
    has_growth = any(indicator in name_lower for indicator in growth_indicators)
    
    # Priority logic: Non-growth indicators trump growth indicators
    if has_non_growth:
        return False
    elif has_growth:
        return True
    else:
        # Default: if unclear, assume dividend/IDCW (conservative approach)
        return False

def enhance_growth_plan_detection(df, logger):
    """
    Add is_growth_plan column to detect Growth vs Dividend/IDCW mutual fund plans.
    
    Args:
        df (pandas.DataFrame): DataFrame with scheme_nav_name column
        logger: Logger instance
        
    Returns:
        pandas.DataFrame: DataFrame with is_growth_plan column
    """
    df_enhanced = df.copy()
    
    if 'scheme_nav_name' not in df_enhanced.columns:
        logger.warning("⚠️ scheme_nav_name column not found, skipping Growth plan detection")
        df_enhanced['is_growth_plan'] = False
        return df_enhanced
    
    logger.info("🌱 Detecting Growth vs Dividend/IDCW plans...")
    
    # Apply detection logic
    df_enhanced['is_growth_plan'] = df_enhanced['scheme_nav_name'].apply(detect_growth_plan)
    
    # Calculate statistics
    total_schemes = len(df_enhanced)
    growth_count = df_enhanced['is_growth_plan'].sum()
    dividend_count = total_schemes - growth_count
    
    # Log statistics
    logger.info(f"📊 Growth vs Dividend/IDCW plan distribution:")
    logger.info(f"   Growth plans: {growth_count:,} ({growth_count/total_schemes*100:.1f}%)")
    logger.info(f"   Dividend/IDCW plans: {dividend_count:,} ({dividend_count/total_schemes*100:.1f}%)")
    
    # Show sample detections for verification
    logger.info("📋 Sample Growth/Dividend detections:")
    
    # Sample growth plans
    growth_samples = df_enhanced[df_enhanced['is_growth_plan'] == True]['scheme_nav_name'].head(5)
    logger.info("   Growth plans detected:")
    for i, name in enumerate(growth_samples, 1):
        name_display = name[:70] + '...' if len(name) > 70 else name
        logger.info(f"   {i}. {name_display}")
    
    # Sample dividend/IDCW plans  
    dividend_samples = df_enhanced[df_enhanced['is_growth_plan'] == False]['scheme_nav_name'].head(5)
    logger.info("   Dividend/IDCW plans detected:")
    for i, name in enumerate(dividend_samples, 1):
        name_display = name[:70] + '...' if len(name) > 70 else name
        logger.info(f"   {i}. {name_display}")
    
    logger.info(f"✅ Growth plan detection completed for {total_schemes:,} schemes")
    
    return df_enhanced

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
    
    # Enhanced categorization - extract scheme category levels
    if 'scheme_category' in clean_df.columns:
        logger.info("🏷️ Extracting enhanced scheme categorization...")
        clean_df = enhance_scheme_categorization(clean_df, logger)
    
    # Direct vs Regular plan detection
    if 'scheme_nav_name' in clean_df.columns:
        logger.info("🎯 Detecting Direct vs Regular plans...")
        clean_df = enhance_direct_regular_detection(clean_df, logger)
        
        # Growth vs Dividend/IDCW plan detection
        logger.info("🌱 Detecting Growth vs Dividend/IDCW plans...")
        clean_df = enhance_growth_plan_detection(clean_df, logger)
    
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
        categorical_columns = ['amc_name', 'scheme_name', 'scheme_type', 'scheme_category', 'scheme_nav_name',
                             'scheme_category_level1', 'scheme_category_level2']
        
        # Boolean columns (will be converted to bool, not category)
        boolean_columns = ['is_direct', 'is_growth_plan']
        for col in boolean_columns:
            if col in df.columns:
                df[col] = df[col].astype('bool')
        
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