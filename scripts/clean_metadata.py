
#!/usr/bin/env python3
"""
Scheme Metadata Cleaner (Refactored)

Processes raw scheme metadata CSV and creates clean Parquet/CSV files.
This version uses vectorized pandas operations for simplicity and performance.
"""

import pandas as pd
import numpy as np
from pathlib import Path

# --- Configuration: Define paths directly for simplicity ---
# NOTE: In a real project, reading these from a config file is better.
# For this example, we assume a 'data/raw/' and 'data/processed/' structure.
INPUT_FILE = Path("data/raw/amfi_scheme_metadata_20250928.csv") # Example file name
OUTPUT_PARQUET = Path("data/processed/scheme_metadata_clean.parquet")
OUTPUT_CSV = Path("data/processed/scheme_metadata_clean.csv")

def clean_and_enhance_metadata(df: pd.DataFrame) -> pd.DataFrame:
    """
    Cleans, standardizes, and enhances the raw scheme metadata using vectorized operations.

    Args:
        df (pd.DataFrame): The raw metadata DataFrame.

    Returns:
        pd.DataFrame: The cleaned and enhanced DataFrame.
    """
    print("🧹 Starting data cleaning and enhancement...")

    # 1. Standardize column names
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
    # Rename only the columns that exist in the DataFrame
    df = df.rename(columns={k: v for k, v in column_mapping.items() if k in df.columns})
    print(f"   - Renamed columns. New columns: {list(df.columns)}")

    # 2. Clean data types and values
    text_cols = ['amc_name', 'scheme_name', 'scheme_type', 'scheme_category', 'scheme_nav_name']
    for col in text_cols:
        if col in df.columns:
            # Chain string operations for efficiency
            df[col] = df[col].astype(str).str.strip().replace('', pd.NA)

    if 'scheme_code' in df.columns:
        df['scheme_code'] = pd.to_numeric(df['scheme_code'], errors='coerce').astype('Int64')

    if 'minimum_amount' in df.columns:
        df['minimum_amount'] = pd.to_numeric(df['minimum_amount'], errors='coerce')

    for col in ['launch_date', 'closure_date']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True)

    print("   - Standardized data types (text, numeric, dates).")

    # 3. Feature Engineering: Scheme Categorization (Vectorized)
    # Replaces the original for-loop with efficient string splitting and np.select
    if 'scheme_category' in df.columns:
        parts = df['scheme_category'].str.split(' - ', n=1, expand=True)
        conditions = [
            parts[0].str.contains('equity', case=False, na=False),
            parts[0].str.contains('debt', case=False, na=False),
            parts[0].str.contains('hybrid', case=False, na=False),
            parts[0].str.contains('other', case=False, na=False),
        ]
        choices = ['Equity Scheme', 'Debt Scheme', 'Hybrid Scheme', 'Other Scheme']
        df['scheme_category_level1'] = np.select(conditions, choices, default='Others')
        df['scheme_category_level2'] = parts[1].fillna(parts[0]).str.strip()
        print("   - Created scheme category levels 1 and 2.")

    # 4. Feature Engineering: Plan Type Detection (Vectorized using Regex)
    if 'scheme_nav_name' in df.columns:
        # Define regex patterns for robust matching (e.g., `\b` for whole words)
        direct_pattern = r'\b(direct|dir)\b'
        non_growth_pattern = r'\b(idcw|dividend|payout|distrib|inc)\b'
        growth_pattern = r'\b(growth|gr|accum)\b'

        # Detect Direct vs. Regular plans (default to Regular/False)
        df['is_direct'] = df['scheme_nav_name'].str.contains(direct_pattern, case=False, na=False)

        # Detect Growth vs. Dividend plans (default to Dividend/False)
        # Non-growth keywords take precedence over growth keywords
        conditions = [
            df['scheme_nav_name'].str.contains(non_growth_pattern, case=False, na=False),
            df['scheme_nav_name'].str.contains(growth_pattern, case=False, na=False)
        ]
        choices = [False, True] # False if non-growth, True if growth
        df['is_growth_plan'] = np.select(conditions, choices, default=False)
        print("   - Detected Direct and Growth plan types.")

    # 5. Final Cleanup
    df = df.dropna(how='all')
    print(f"✅ Cleaning complete. Final data shape: {df.shape}")
    return df

def validate_data(df: pd.DataFrame):
    """Performs simple validation checks and prints a summary."""
    print("\n✅ Validating cleaned data...")
    if df.empty:
        print("   - Validation failed: DataFrame is empty.")
        return

    # Check for missing required columns
    required_cols = ['scheme_code', 'scheme_name', 'amc_name']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        print(f"   - WARNING: Missing required columns: {missing_cols}")

    # Summary statistics
    print(f"   - Total schemes processed: {len(df):,}")
    if 'scheme_code' in df.columns:
        print(f"   - Unique scheme codes: {df['scheme_code'].nunique():,}")
        if df['scheme_code'].duplicated().any():
            print(f"   - WARNING: Found {df['scheme_code'].duplicated().sum():,} duplicate scheme codes.")
    if 'amc_name' in df.columns:
        print(f"   - Unique AMCs found: {df['amc_name'].nunique():,}")
    if 'is_direct' in df.columns:
        print(f"   - Direct Plans: {df['is_direct'].sum():,} | Regular Plans: {len(df) - df['is_direct'].sum():,}")
    if 'is_growth_plan' in df.columns:
        print(f"   - Growth Plans: {df['is_growth_plan'].sum():,} | Dividend/IDCW Plans: {len(df) - df['is_growth_plan'].sum():,}")
    print("   - Validation summary complete.")


if __name__ == "__main__":
    print("--- Scheme Metadata Cleaning Script ---")

    # 1. Load Data
    try:
        raw_df = pd.read_csv(INPUT_FILE, encoding='latin1') # Common encoding for such files
        print(f"📂 Successfully loaded raw data from '{INPUT_FILE}'. Shape: {raw_df.shape}")
    except FileNotFoundError:
        print(f"❌ ERROR: Input file not found at '{INPUT_FILE}'.")
        print("💡 Please make sure the file exists and the path is correct.")
        exit(1)
    except Exception as e:
        print(f"❌ ERROR: Failed to load CSV file. Reason: {e}")
        exit(1)

    # 2. Process Data
    clean_df = clean_and_enhance_metadata(raw_df)

    # 3. Validate Data
    validate_data(clean_df)

    # 4. Save Data
    try:
        # Ensure the output directory exists
        OUTPUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
        
        print(f"\n💾 Saving cleaned data...")
        
        # Save to Parquet for efficient storage
        clean_df.to_parquet(OUTPUT_PARQUET, index=False, compression='snappy')
        parquet_size = OUTPUT_PARQUET.stat().st_size / (1024 * 1024)
        print(f"   - Saved Parquet file to '{OUTPUT_PARQUET}' ({parquet_size:.2f} MB)")

        # Save to CSV for easy inspection
        clean_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
        csv_size = OUTPUT_CSV.stat().st_size / (1024 * 1024)
        print(f"   - Saved CSV file to '{OUTPUT_CSV}' ({csv_size:.2f} MB)")

        print("\n🎉 Script finished successfully!")

    except Exception as e:
        print(f"\n❌ ERROR: Failed to save the output files. Reason: {e}")
        exit(1)