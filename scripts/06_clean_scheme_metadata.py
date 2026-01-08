#!/usr/bin/env python3
"""
Scheme Metadata Cleaner - Refactored

Processes raw scheme metadata CSV and creates clean Parquet/CSV files.
Refactored for pandas chaining, vectorized operations, and minimal logging.
"""

import pandas as pd
from pathlib import Path

from config.settings import Paths, Processing
from utils.logging_setup import get_clean_metadata_logger, log_script_start, log_script_end, log_file_operation

logger = get_clean_metadata_logger(__name__)

# Pattern indicators for Direct/Regular and Growth/Dividend detection
DIRECT_INDICATORS = ['direct', 'dir ', ' dir', '-dir-', '-direct-', '(dir)', '(direct)', 'drct']
REGULAR_INDICATORS = ['regular', 'reg ', ' reg', '-reg-', '-regular-', '(reg)', '(regular)', 'rglr']
GROWTH_INDICATORS = ['growth', 'grwth', 'gr ', ' gr', 'gr)', '(gr)', 'growt', 'accum', 'accumulation']
NON_GROWTH_INDICATORS = ['idcw', 'dividend', 'income', 'monthly', 'quarterly', 'weekly', 'daily',
                         'annual', 'payout', 'distribution', 'div ', ' div', 'div)', '(div)']

# Column name mappings from raw to processed
COLUMN_MAPPING = {
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


def load_raw_metadata():
    """Load latest raw scheme metadata from timestamped CSV"""
    from config.settings import get_latest_raw_metadata_file

    try:
        input_file = get_latest_raw_metadata_file()
        logger.info(f"Loading raw metadata from {input_file.name}")
        df = pd.read_csv(input_file, encoding=Processing.CSV_ENCODING)
        logger.info(f"Loaded {len(df):,} records")
        return df
    except FileNotFoundError as e:
        logger.error(f"{e}. Run 05_extract_scheme_metadata.py first")
        return None
    except Exception as e:
        logger.error(f"Failed to load: {e}")
        return None


def split_category_levels(df):
    """
    Split scheme_category into level1 and level2 using vectorized operations.

    Preserves exact logic:
    - Split on ' - ' (first occurrence only)
    - Case-insensitive substring matching for level1
    - For categories without ' - ': level1='Others', level2=original_category
    - For categories with ' - ': level2 fallback to level1 if empty/error
    """
    # Check if category contains ' - ' separator
    has_separator = df['scheme_category'].fillna('').str.contains(' - ', regex=False)

    # Split on ' - ' (n=1 for first occurrence only)
    split_df = df['scheme_category'].fillna('').str.split(' - ', n=1, expand=True)

    # Extract and clean
    level1_raw = split_df[0].str.strip() if 0 in split_df.columns else pd.Series([''] * len(df))
    level2_raw = split_df[1].str.strip() if 1 in split_df.columns else pd.Series([''] * len(df))

    # Map level1 with case-insensitive substring matching
    def map_level1(x):
        if pd.isna(x) or x == '':
            return 'Others'
        x_lower = str(x).lower()
        if 'equity' in x_lower:
            return 'Equity Scheme'
        elif 'debt' in x_lower:
            return 'Debt Scheme'
        elif 'hybrid' in x_lower:
            return 'Hybrid Scheme'
        elif 'other' in x_lower:
            return 'Other Scheme'
        else:
            return 'Others'

    df['scheme_category_level1'] = level1_raw.apply(map_level1)

    # Handle level2 based on whether ' - ' exists
    # If no separator: use original category
    # If separator: use extracted level2, fallback to level1 if empty/error
    df['scheme_category_level2'] = level2_raw.copy()

    # For rows WITH separator: replace empty/error values with level1
    mask_with_separator = has_separator
    df.loc[mask_with_separator, 'scheme_category_level2'] = (
        df.loc[mask_with_separator, 'scheme_category_level2']
        .replace(['', 'error', 'null', 'na'], None)
        .fillna(df.loc[mask_with_separator, 'scheme_category_level1'])
    )

    # For rows WITHOUT separator: use the original category
    mask_without_separator = ~has_separator
    df.loc[mask_without_separator, 'scheme_category_level2'] = (
        df.loc[mask_without_separator, 'scheme_category']
        .fillna('Others')
    )

    logger.info(f"Category levels: L1={df['scheme_category_level1'].nunique()}, L2={df['scheme_category_level2'].nunique()}")

    return df


def detect_plan_flags(df):
    """
    Detect Direct/Regular and Growth/Dividend flags using vectorized regex.

    Priority rules:
    - Direct wins over Regular if both match
    - Non-growth (Dividend) wins over Growth if both match
    """
    import re

    # Build regex patterns - escape special characters
    direct_pattern = '|'.join(re.escape(indicator) for indicator in DIRECT_INDICATORS)
    regular_pattern = '|'.join(re.escape(indicator) for indicator in REGULAR_INDICATORS)
    growth_pattern = '|'.join(re.escape(indicator) for indicator in GROWTH_INDICATORS)
    non_growth_pattern = '|'.join(re.escape(indicator) for indicator in NON_GROWTH_INDICATORS)

    name_lower = df['scheme_nav_name'].fillna('').str.lower()

    # Direct/Regular detection - Direct wins, default False (Regular)
    has_direct = name_lower.str.contains(direct_pattern, regex=True, na=False)
    has_regular = name_lower.str.contains(regular_pattern, regex=True, na=False)
    df['is_direct'] = has_direct | (~has_regular & False)

    # Growth/Dividend detection - Non-growth wins, default False (Dividend)
    has_non_growth = name_lower.str.contains(non_growth_pattern, regex=True, na=False)
    has_growth = name_lower.str.contains(growth_pattern, regex=True, na=False)
    df['is_growth_plan'] = has_growth & ~has_non_growth

    direct_count = df['is_direct'].sum()
    growth_count = df['is_growth_plan'].sum()
    logger.info(f"Plan detection: Direct={direct_count:,}, Regular={len(df)-direct_count:,}, "
                f"Growth={growth_count:,}, Dividend={len(df)-growth_count:,}")

    return df


def clean_scheme_metadata(df):
    """Clean and standardize metadata using pandas chaining"""

    logger.info(f"Cleaning {len(df):,} schemes")

    # Flexible column renaming (handle variations in column names)
    actual_columns = list(df.columns)
    flexible_mapping = {}
    for old_name in actual_columns:
        for expected, new_name in COLUMN_MAPPING.items():
            if expected.lower() in old_name.lower() or old_name.lower() in expected.lower():
                flexible_mapping[old_name] = new_name
                break

    return (df
        .rename(columns=flexible_mapping)
        .assign(
            # Clean text columns - strip whitespace, replace '' with None
            scheme_code=lambda x: x['scheme_code'].astype(str).str.strip(),
            amc_name=lambda x: x.get('amc_name', pd.Series([None]*len(x))).astype(str).str.strip().replace('', None),
            scheme_name=lambda x: x.get('scheme_name', pd.Series([None]*len(x))).astype(str).str.strip().replace('', None),
            scheme_type=lambda x: x.get('scheme_type', pd.Series([None]*len(x))).astype(str).str.strip().replace('', None),
            scheme_category=lambda x: x.get('scheme_category', pd.Series([None]*len(x))).astype(str).str.strip().replace('', None),
            scheme_nav_name=lambda x: x.get('scheme_nav_name', pd.Series([None]*len(x))).astype(str).str.strip().replace('', None),
            # Parse dates with dayfirst=True
            launch_date=lambda x: pd.to_datetime(x.get('launch_date'), errors='coerce', dayfirst=True),
            closure_date=lambda x: pd.to_datetime(x.get('closure_date'), errors='coerce', dayfirst=True),
            # Convert minimum_amount to numeric
            minimum_amount=lambda x: pd.to_numeric(x.get('minimum_amount'), errors='coerce')
        )
        .pipe(split_category_levels)
        .pipe(detect_plan_flags)
        .dropna(how='all')
    )


def validate_metadata(df):
    """Minimal validation - log counts and warnings only"""

    logger.info("Validating metadata")

    if df is None or df.empty:
        logger.error("No data to validate")
        return False

    # Check required columns
    required_columns = ['scheme_code', 'scheme_name', 'amc_name']
    missing_cols = [col for col in required_columns if col not in df.columns]

    if missing_cols:
        logger.error(f"Missing required columns: {missing_cols}")
        return False

    # Log counts
    logger.info(f"Total schemes: {len(df):,}")
    logger.info(f"Unique scheme codes: {df['scheme_code'].nunique():,}")
    logger.info(f"Unique AMCs: {df.get('amc_name', pd.Series()).nunique():,}")

    # Check for issues
    null_codes = df['scheme_code'].isna().sum()
    if null_codes > 0:
        logger.warning(f"Found {null_codes:,} null scheme codes")

    duplicates = df['scheme_code'].duplicated().sum()
    if duplicates > 0:
        logger.warning(f"Found {duplicates:,} duplicate scheme codes")

    logger.info("Validation completed")
    return True


def save_metadata(df):
    """Save cleaned metadata with categorical types"""

    if df is None or df.empty:
        logger.error("No data to save")
        return None

    parquet_file = Paths.SCHEME_METADATA_CLEAN
    csv_file = Paths.SCHEME_METADATA_CSV
    parquet_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Optimize dtypes for storage
        df_save = df.copy()

        # Convert to categorical for memory efficiency
        categorical_columns = ['amc_name', 'scheme_name', 'scheme_type', 'scheme_category',
                              'scheme_nav_name', 'scheme_category_level1', 'scheme_category_level2']
        for col in categorical_columns:
            if col in df_save.columns:
                df_save[col] = df_save[col].astype('category')

        # Convert to bool explicitly
        for col in ['is_direct', 'is_growth_plan']:
            if col in df_save.columns:
                df_save[col] = df_save[col].astype('bool')

        # Save Parquet
        df_save.to_parquet(parquet_file, index=False, compression=Processing.PARQUET_COMPRESSION)
        parquet_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", parquet_file, True, parquet_size_mb)

        # Save CSV
        df.to_csv(csv_file, index=False, encoding=Processing.CSV_ENCODING)
        csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", csv_file, True, csv_size_mb)

        logger.info(f"Saved {len(df):,} records")

        return str(parquet_file)

    except Exception as e:
        logger.error(f"Failed to save: {e}")
        return None


def main():
    """Main execution flow"""

    log_script_start(logger, "Scheme Metadata Cleaner",
                    "Processing raw scheme metadata (refactored)")

    # Check if processing is needed
    from config.settings import should_process_metadata

    if not should_process_metadata():
        logger.info("Processed metadata is up-to-date, skipping")
        log_script_end(logger, "Scheme Metadata Cleaner", True)
        return 0

    # Ensure directories exist
    Paths.create_directories()

    # Load raw data
    raw_data = load_raw_metadata()
    if raw_data is None:
        log_script_end(logger, "Scheme Metadata Cleaner", False)
        return 1

    # Clean data
    clean_data = clean_scheme_metadata(raw_data)
    if clean_data is None or clean_data.empty:
        logger.error("Failed to clean metadata")
        log_script_end(logger, "Scheme Metadata Cleaner", False)
        return 1

    # Validate
    validation_passed = validate_metadata(clean_data)
    if not validation_passed:
        logger.warning("Validation had warnings, but continuing")

    # Save
    saved_path = save_metadata(clean_data)
    success = saved_path is not None

    log_script_end(logger, "Scheme Metadata Cleaner", success)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
