#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import os

# Simple logging setup
Path("logs").mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"logs/clean_nav_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def clean_nav_file(file_path):
    """
    Clean a single NAV file and extract key columns.
    
    Returns cleaned DataFrame with:
    - scheme_code
    - scheme_name  
    - isin_growth
    - isin_dividend
    - nav
    - repurchase_price
    - sale_price
    - date
    """
    try:
        df = pd.read_csv(file_path)
        logger.info(f"Processing {file_path} - {len(df)} raw records")
        
        # Filter out header rows and category separators
        clean_df = df[
            df['Scheme Code'].notna() & 
            df['Date'].notna() & 
            ~df['Scheme Code'].str.contains('Open Ended|Close Ended|Interval Fund|Fund of Funds', na=False) &
            ~df['Scheme Code'].str.contains('Mutual Fund', na=False)
        ].copy()
        
        if clean_df.empty:
            logger.warning(f"No valid data found in {file_path}")
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
        clean_df['nav'] = pd.to_numeric(clean_df['nav'], errors='coerce')
        clean_df['repurchase_price'] = pd.to_numeric(clean_df['repurchase_price'], errors='coerce')
        clean_df['sale_price'] = pd.to_numeric(clean_df['sale_price'], errors='coerce')
        
        # Convert date
        clean_df['date'] = pd.to_datetime(clean_df['date'], format='%d-%b-%Y', errors='coerce')
        
        # Remove rows where NAV is missing (key field)
        clean_df = clean_df[clean_df['nav'].notna()]
        
        # Select final columns
        final_columns = [
            'scheme_code', 'scheme_name', 'isin_growth', 'isin_dividend', 
            'nav', 'repurchase_price', 'sale_price', 'date'
        ]
        
        result_df = clean_df[final_columns]
        
        logger.info(f"Cleaned {file_path} - {len(result_df)} valid records")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return None

def combine_all_nav_files(input_dir="raw/amfi_nav", output_file="raw/amfi_nav_history/historical_nav_data.parquet"):
    """
    Clean all raw NAV files and combine into single historical dataset.
    """
    input_path = Path(input_dir)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = list(input_path.glob("amfi_raw_nav_*.csv"))
    csv_files.sort()  # Process chronologically
    
    logger.info(f"Found {len(csv_files)} raw NAV files to process")
    
    # Use parallel processing
    max_workers = min(multiprocessing.cpu_count(), len(csv_files))
    logger.info(f"Using {max_workers} parallel workers")
    
    all_data = []
    processed_count = 0
    failed_count = 0
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # Submit all jobs
        future_to_file = {executor.submit(clean_nav_file, csv_file): csv_file for csv_file in csv_files}
        
        # Collect results
        for future in as_completed(future_to_file):
            csv_file = future_to_file[future]
            try:
                cleaned_df = future.result()
                if cleaned_df is not None and not cleaned_df.empty:
                    all_data.append(cleaned_df)
                    processed_count += 1
                    logger.info(f"✓ Processed {csv_file.name}")
                else:
                    failed_count += 1
                    logger.warning(f"✗ Failed {csv_file.name}")
            except Exception as e:
                failed_count += 1
                logger.error(f"✗ Exception processing {csv_file.name}: {e}")
    
    if not all_data:
        logger.error("No data to combine!")
        return None
    
    # Combine all dataframes
    logger.info("Combining all cleaned data...")
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Sort by date and scheme code
    combined_df = combined_df.sort_values(['date', 'scheme_code'])
    
    # Save combined dataset as Parquet
    combined_df.to_parquet(output_path, index=False, compression='snappy')
    
    # Summary
    total_records = len(combined_df)
    date_range = f"{combined_df['date'].min().date()} to {combined_df['date'].max().date()}"
    unique_schemes = combined_df['scheme_code'].nunique()
    
    # Get file size
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    
    logger.info(f"Historical NAV dataset created: {output_path}")
    logger.info(f"File size: {file_size_mb:.1f} MB")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Date range: {date_range}")
    logger.info(f"Unique schemes: {unique_schemes:,}")
    logger.info(f"Files processed: {processed_count}/{len(csv_files)}")
    logger.info(f"Format: Parquet with Snappy compression")
    
    return str(output_path)

def main():
    """Main function to clean and combine all NAV data."""
    logger.info("Starting NAV data cleaning process...")
    
    output_file = combine_all_nav_files()
    
    if output_file:
        logger.info(f"Successfully created historical NAV dataset: {output_file}")
    else:
        logger.error("Failed to create historical dataset")
        return 1
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)