#!/usr/bin/env python3

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
from concurrent.futures import ProcessPoolExecutor, as_completed
import multiprocessing
import gc

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
        df = pd.read_csv(file_path, dtype=str)
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

def save_batch_to_parquet(batch_df, batch_num, output_dir):
    """Save a batch DataFrame to individual Parquet file."""
    batch_file = output_dir / f"batch_{batch_num:02d}.parquet"
    try:
        batch_df.to_parquet(batch_file, index=False, compression='snappy')
        logger.info(f"💾 Saved batch {batch_num} to {batch_file} - {len(batch_df):,} records")
        return str(batch_file)
    except Exception as e:
        logger.error(f"❌ Failed to save batch {batch_num}: {e}")
        return None

def combine_parquet_files(output_dir, final_output):
    """Combine individual batch Parquet files into final output."""
    logger.info("🔄 Combining batch Parquet files...")
    
    batch_files = list(output_dir.glob("batch_*.parquet"))
    batch_files.sort()
    
    if not batch_files:
        logger.error("No batch files found to combine!")
        return None
    
    try:
        # Read and combine all batch files
        combined_df = pd.concat([pd.read_parquet(f) for f in batch_files], ignore_index=True)
        
        # Sort by date and scheme code
        logger.info("🔄 Sorting final dataset...")
        combined_df = combined_df.sort_values(['date', 'scheme_code'])
        
        # Save final combined file
        logger.info("💾 Saving final combined dataset...")
        combined_df.to_parquet(final_output, index=False, compression='snappy')
        
        # Clean up batch files
        for batch_file in batch_files:
            batch_file.unlink()
        logger.info("🧹 Cleaned up temporary batch files")
        
        return combined_df
        
    except Exception as e:
        logger.error(f"❌ Error combining files: {e}")
        return None

def combine_all_nav_files_memory_efficient(input_dir="raw/amfi_nav", output_file="raw/amfi_nav_history/historical_nav_data.parquet"):
    """Clean all raw NAV files and combine efficiently with memory management."""
    input_path = Path(input_dir)
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create temp directory for batch files
    temp_dir = output_path.parent / "temp_batches"
    temp_dir.mkdir(exist_ok=True)
    
    # Get all CSV files
    csv_files = list(input_path.glob("amfi_raw_nav_*.csv"))
    csv_files.sort()
    
    logger.info(f"Found {len(csv_files)} raw NAV files to process")
    
    # Use smaller batches to avoid memory issues
    max_workers = min(4, multiprocessing.cpu_count())  # Reduce workers
    logger.info(f"Using {max_workers} parallel workers")
    
    processed_count = 0
    failed_count = 0
    batch_size = 15  # Smaller batch size
    batch_num = 1
    
    # Process in smaller batches and save each batch immediately
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
        
        # Combine and save batch data to Parquet immediately
        if batch_data:
            batch_df = pd.concat(batch_data, ignore_index=True)
            batch_file = save_batch_to_parquet(batch_df, batch_num, temp_dir)
            if batch_file:
                logger.info(f"Batch {batch_num} saved: {len(batch_df):,} records")
            batch_num += 1
            
            # Force garbage collection
            del batch_data, batch_df
            gc.collect()
    
    logger.info(f"All processing complete. Files processed: {processed_count}/{len(csv_files)}")
    
    # Combine all batch Parquet files into final output
    final_df = combine_parquet_files(temp_dir, output_path)
    
    if final_df is not None:
        # Get file size
        file_size_mb = output_path.stat().st_size / (1024 * 1024)
        
        # Summary
        total_records = len(final_df)
        date_range = f"{final_df['date'].min().date()} to {final_df['date'].max().date()}"
        unique_schemes = final_df['scheme_code'].nunique()
        
        logger.info(f"✅ Historical NAV dataset created: {output_path}")
        logger.info(f"📦 File size: {file_size_mb:.1f} MB")
        logger.info(f"📊 Total records: {total_records:,}")
        logger.info(f"📅 Date range: {date_range}")
        logger.info(f"🏢 Unique schemes: {unique_schemes:,}")
        logger.info(f"✅ Files processed: {processed_count}/{len(csv_files)}")
        logger.info(f"💾 Format: Parquet with Snappy compression")
        
        # Clean up temp directory
        temp_dir.rmdir()
        
        return str(output_path)
    
    return None

def main():
    """Main function to clean and combine all NAV data."""
    logger.info("🚀 Starting memory-efficient NAV data cleaning process...")
    
    output_file = combine_all_nav_files_memory_efficient()
    
    if output_file:
        logger.info(f"🎉 Successfully created historical NAV dataset: {output_file}")
        return 0
    else:
        logger.error("❌ Failed to create historical dataset")
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)