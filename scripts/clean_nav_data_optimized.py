#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing

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
    """Clean a single NAV file and return cleaned DataFrame."""
    try:
        df = pd.read_csv(file_path, dtype=str)  # Read as strings to avoid dtype issues
        logger.info(f"Processing {file_path.name} - {len(df)} raw records")
        
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
        
        logger.info(f"✓ Cleaned {file_path.name} - {len(result_df)} valid records")
        return result_df
        
    except Exception as e:
        logger.error(f"✗ Error processing {file_path}: {e}")
        return None

def combine_all_nav_files_optimized(input_dir="raw/amfi_nav", output_file="raw/amfi_nav_history/historical_nav_data.parquet"):
    """Clean all raw NAV files and combine efficiently using batches."""
    input_path = Path(input_dir)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Get all CSV files
    csv_files = list(input_path.glob("amfi_raw_nav_*.csv"))
    csv_files.sort()  # Process chronologically
    
    logger.info(f"Found {len(csv_files)} raw NAV files to process")
    
    # Use parallel processing with reduced workers to avoid memory issues
    max_workers = min(8, multiprocessing.cpu_count())  # Limit workers
    logger.info(f"Using {max_workers} parallel workers")
    
    processed_count = 0
    failed_count = 0
    
    # Process in batches and write incrementally
    batch_size = 20  # Process 20 files at a time
    all_batches = []
    
    for i in range(0, len(csv_files), batch_size):
        batch_files = csv_files[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1}/{(len(csv_files)-1)//batch_size + 1} ({len(batch_files)} files)")
        
        batch_data = []
        
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            # Submit batch jobs
            future_to_file = {executor.submit(clean_nav_file, csv_file): csv_file for csv_file in batch_files}
            
            # Collect batch results
            for future in as_completed(future_to_file):
                csv_file = future_to_file[future]
                try:
                    cleaned_df = future.result()
                    if cleaned_df is not None and not cleaned_df.empty:
                        batch_data.append(cleaned_df)
                        processed_count += 1
                    else:
                        failed_count += 1
                        logger.warning(f"✗ Failed {csv_file.name}")
                except Exception as e:
                    failed_count += 1
                    logger.error(f"✗ Exception processing {csv_file.name}: {e}")
        
        # Combine batch data
        if batch_data:
            batch_df = pd.concat(batch_data, ignore_index=True)
            all_batches.append(batch_df)
            logger.info(f"Batch {i//batch_size + 1} complete: {len(batch_df):,} records")
        
        # Clear memory
        del batch_data
    
    if not all_batches:
        logger.error("No data to combine!")
        return None
    
    # Final combine
    logger.info("Combining all batches into final dataset...")
    combined_df = pd.concat(all_batches, ignore_index=True)
    
    # Sort by date and scheme code
    logger.info("Sorting data...")
    combined_df = combined_df.sort_values(['date', 'scheme_code'])
    
    # Save combined dataset as Parquet
    logger.info("Saving to Parquet format...")
    combined_df.to_parquet(output_path, index=False, compression='snappy')
    
    # Get file size
    file_size_mb = output_path.stat().st_size / (1024 * 1024)
    
    # Summary
    total_records = len(combined_df)
    date_range = f"{combined_df['date'].min().date()} to {combined_df['date'].max().date()}"
    unique_schemes = combined_df['scheme_code'].nunique()
    
    logger.info(f"✅ Historical NAV dataset created: {output_path}")
    logger.info(f"📦 File size: {file_size_mb:.1f} MB")
    logger.info(f"📊 Total records: {total_records:,}")
    logger.info(f"📅 Date range: {date_range}")
    logger.info(f"🏢 Unique schemes: {unique_schemes:,}")
    logger.info(f"✅ Files processed: {processed_count}/{len(csv_files)}")
    logger.info(f"💾 Format: Parquet with Snappy compression")
    
    return str(output_path)

def main():
    """Main function to clean and combine all NAV data."""
    logger.info("🚀 Starting optimized NAV data cleaning process...")
    
    output_file = combine_all_nav_files_optimized()
    
    if output_file:
        logger.info(f"🎉 Successfully created historical NAV dataset: {output_file}")
        return 0
    else:
        logger.error("❌ Failed to create historical dataset")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)