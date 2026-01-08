#!/usr/bin/env python3
"""
Scheme Masterdata Builder

Maintains a comprehensive master list of all schemes ever seen.
- Never deletes schemes (tracks inactive ones)
- Adds new schemes as they appear
- Updates attributes to latest values
- Tracks first_seen and last_seen dates
"""

import pandas as pd

from config.settings import Paths, Processing
from utils.logging_setup import get_clean_metadata_logger, log_script_start, log_script_end, log_file_operation

logger = get_clean_metadata_logger(__name__)


def load_existing_masterdata():
    """
    Load existing masterdata if it exists.

    Returns:
        pd.DataFrame or None: Existing masterdata or None if not exists
    """
    masterdata_file = Paths.SCHEME_MASTERDATA

    if not masterdata_file.exists():
        logger.info("No existing masterdata found - will create new")
        return None

    try:
        logger.info(f"Loading existing masterdata from {masterdata_file.name}")
        df = pd.read_parquet(masterdata_file)
        logger.info(f"Loaded {len(df):,} schemes from masterdata")
        logger.info(f"  Active schemes: {df['is_active'].sum():,}")
        logger.info(f"  Inactive schemes: {(~df['is_active']).sum():,}")
        return df
    except Exception as e:
        logger.error(f"Failed to load existing masterdata: {e}")
        return None


def load_latest_cleaned_metadata():
    """
    Load the latest cleaned metadata from 06_clean_scheme_metadata output.

    Returns:
        pd.DataFrame or None: Latest cleaned metadata
    """
    clean_file = Paths.SCHEME_METADATA_CLEAN

    if not clean_file.exists():
        logger.error(f"Cleaned metadata not found at {clean_file}")
        logger.error("Run 06_clean_scheme_metadata.py first")
        return None

    try:
        logger.info(f"Loading latest cleaned metadata from {clean_file.name}")
        df = pd.read_parquet(clean_file)
        logger.info(f"Loaded {len(df):,} schemes from latest data")
        return df
    except Exception as e:
        logger.error(f"Failed to load cleaned metadata: {e}")
        return None


def build_initial_masterdata(latest_df):
    """
    Build initial masterdata from latest cleaned data.
    Used when no existing masterdata exists.

    Strategy:
    - first_seen_date: Use launch_date if available, else today
    - last_seen_date: Today
    - is_active: True (all schemes in latest data are active)

    Args:
        latest_df: Latest cleaned metadata DataFrame

    Returns:
        pd.DataFrame: Initial masterdata
    """
    logger.info("Building initial masterdata from latest data")

    today = pd.Timestamp.now().normalize()

    # Use launch_date for first_seen_date, fallback to today
    first_seen = latest_df['launch_date'].copy()
    first_seen = first_seen.fillna(today)

    masterdata = (latest_df
        .assign(
            first_seen_date=first_seen,
            last_seen_date=today,
            is_active=True,
            attribute_last_updated=today
        )
    )

    logger.info(f"Created initial masterdata with {len(masterdata):,} schemes")
    logger.info(f"  Using launch_date for first_seen: {(~latest_df['launch_date'].isna()).sum():,} schemes")
    logger.info(f"  Using today's date for first_seen: {latest_df['launch_date'].isna().sum():,} schemes")

    return masterdata


def merge_masterdata(existing_df, latest_df):
    """
    Merge existing masterdata with latest cleaned metadata.

    Logic:
    1. New schemes (in latest, not in existing): Add with first_seen_date=today
    2. Existing schemes (in both): Update attributes and last_seen_date
    3. Missing schemes (in existing, not in latest): Mark as inactive

    Args:
        existing_df: Existing masterdata DataFrame
        latest_df: Latest cleaned metadata DataFrame

    Returns:
        pd.DataFrame: Updated masterdata
    """
    logger.info("Merging existing masterdata with latest data")

    today = pd.Timestamp.now().normalize()

    # Identify new, existing, and missing schemes
    existing_codes = set(existing_df['scheme_code'])
    latest_codes = set(latest_df['scheme_code'])

    new_codes = latest_codes - existing_codes
    existing_codes_in_latest = existing_codes & latest_codes
    missing_codes = existing_codes - latest_codes

    logger.info(f"Scheme analysis:")
    logger.info(f"  New schemes: {len(new_codes):,}")
    logger.info(f"  Existing schemes: {len(existing_codes_in_latest):,}")
    logger.info(f"  Missing schemes (will be marked inactive): {len(missing_codes):,}")

    # 1. Handle new schemes
    new_schemes = latest_df[latest_df['scheme_code'].isin(new_codes)].copy()
    if len(new_schemes) > 0:
        # For new schemes, use launch_date as first_seen if available
        first_seen_new = new_schemes['launch_date'].copy()
        first_seen_new = first_seen_new.fillna(today)

        new_schemes = new_schemes.assign(
            first_seen_date=first_seen_new,
            last_seen_date=today,
            is_active=True,
            attribute_last_updated=today
        )

    # 2. Handle existing schemes - update attributes
    existing_schemes = existing_df[existing_df['scheme_code'].isin(existing_codes_in_latest)].copy()
    latest_existing = latest_df[latest_df['scheme_code'].isin(existing_codes_in_latest)].copy()

    # Merge to get latest attributes
    # Keep first_seen_date from existing, update everything else from latest
    existing_schemes = existing_schemes[['scheme_code', 'first_seen_date']].merge(
        latest_existing,
        on='scheme_code',
        how='left'
    ).assign(
        last_seen_date=today,
        is_active=True,
        attribute_last_updated=today
    )

    # 3. Handle missing schemes - mark as inactive
    missing_schemes = existing_df[existing_df['scheme_code'].isin(missing_codes)].copy()
    if len(missing_schemes) > 0:
        missing_schemes = missing_schemes.assign(
            is_active=False
            # Keep last_seen_date and attribute_last_updated as is
        )

    # Combine all
    masterdata = pd.concat([new_schemes, existing_schemes, missing_schemes], ignore_index=True)

    logger.info(f"Merge complete: {len(masterdata):,} total schemes")
    active_count = masterdata['is_active'].sum()
    inactive_count = len(masterdata) - active_count
    logger.info(f"  Active: {active_count:,}")
    logger.info(f"  Inactive: {inactive_count:,}")

    return masterdata


def save_masterdata(df):
    """
    Save masterdata to Parquet and CSV.

    Args:
        df: Masterdata DataFrame

    Returns:
        str or None: Path to saved Parquet file
    """
    if df is None or df.empty:
        logger.error("No data to save")
        return None

    parquet_file = Paths.SCHEME_MASTERDATA
    csv_file = Paths.SCHEME_MASTERDATA_CSV
    parquet_file.parent.mkdir(parents=True, exist_ok=True)

    try:
        # Optimize dtypes for storage
        df_save = df.copy()

        # Convert to categorical for memory efficiency
        categorical_columns = [
            'amc_name', 'scheme_name', 'scheme_type', 'scheme_category',
            'scheme_nav_name', 'scheme_category_level1', 'scheme_category_level2'
        ]
        for col in categorical_columns:
            if col in df_save.columns:
                df_save[col] = df_save[col].astype('category')

        # Convert to bool explicitly
        for col in ['is_direct', 'is_growth_plan', 'is_active']:
            if col in df_save.columns:
                df_save[col] = df_save[col].astype('bool')

        # Save Parquet
        df_save.to_parquet(parquet_file, index=False, compression=Processing.PARQUET_COMPRESSION)
        parquet_size_mb = parquet_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", parquet_file, True, parquet_size_mb)

        # Save CSV (for easy inspection)
        df.to_csv(csv_file, index=False, encoding=Processing.CSV_ENCODING)
        csv_size_mb = csv_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", csv_file, True, csv_size_mb)

        logger.info(f"Saved masterdata with {len(df):,} schemes")

        return str(parquet_file)

    except Exception as e:
        logger.error(f"Failed to save masterdata: {e}")
        return None


def main():
    """Main execution flow"""

    log_script_start(logger, "Scheme Masterdata Builder",
                    "Building comprehensive master list of all schemes")

    # Ensure directories exist
    Paths.create_directories()

    # Load latest cleaned metadata
    latest_df = load_latest_cleaned_metadata()
    if latest_df is None:
        log_script_end(logger, "Scheme Masterdata Builder", False)
        return 1

    # Load existing masterdata
    existing_df = load_existing_masterdata()

    # Build or merge masterdata
    if existing_df is None:
        # Initial build
        masterdata = build_initial_masterdata(latest_df)
    else:
        # Incremental update
        masterdata = merge_masterdata(existing_df, latest_df)

    if masterdata is None or masterdata.empty:
        logger.error("Failed to build masterdata")
        log_script_end(logger, "Scheme Masterdata Builder", False)
        return 1

    # Save masterdata
    saved_path = save_masterdata(masterdata)
    success = saved_path is not None

    if success:
        logger.info("✓ Masterdata build complete")
        logger.info("  Use this file for comprehensive scheme analysis")
        logger.info("  It maintains all schemes ever seen, including inactive ones")

    log_script_end(logger, "Scheme Masterdata Builder", success)
    return 0 if success else 1


if __name__ == "__main__":
    exit(main())
