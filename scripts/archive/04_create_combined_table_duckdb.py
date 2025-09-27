#!/usr/bin/env python3
"""
Combined NAV Table Creator using DuckDB

Creates a unified NAV table by combining historical CSV files and daily parquet data.
Uses DuckDB for memory-efficient processing of large CSV files.
"""

import duckdb
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime
import gc

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing, Validation
from utils.logging_setup import get_combine_table_logger, log_script_start, log_script_end, log_data_summary, log_file_operation

# Initialize logger
logger = get_combine_table_logger(__name__)

def create_combined_table_with_duckdb():
    """
    Create combined NAV table using DuckDB for efficient CSV processing.
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_script_start(logger, "DuckDB Combined NAV Table Creator", 
                    "Creating unified table from historical CSVs and daily parquet files")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Initialize DuckDB connection
    logger.info("🦆 Initializing DuckDB connection...")
    conn = duckdb.connect()
    
    try:
        # Step 1: Process all historical CSV files
        hist_dir = Paths.RAW_NAV_HISTORICAL.parent / "nav_historical"
        csv_files = sorted(hist_dir.glob("amfi_raw_nav_*.csv"))
        
        if not csv_files:
            logger.error("❌ No historical CSV files found")
            return False
        
        logger.info(f"📚 Found {len(csv_files)} historical CSV files")
        
        # Create a view that combines all CSV files
        logger.info("🔗 Creating unified view from all CSV files...")
        
        # Build the UNION ALL query for all CSV files
        union_queries = []
        for csv_file in csv_files:
            # Use DuckDB's CSV reader with proper column names
            query = f"""
            SELECT 
                "Scheme Code" as scheme_code,
                "ISIN Div Payout/ISIN Growth" as isin_growth,
                "ISIN Div Reinvestment" as isin_dividend,
                "Scheme Name" as scheme_name,
                CAST("Net Asset Value" as DOUBLE) as nav,
                CAST("Repurchase Price" as DOUBLE) as repurchase_price,
                CAST("Sale Price" as DOUBLE) as sale_price,
                strptime("Date", '%d-%b-%Y') as date
            FROM read_csv('{csv_file.absolute()}', header=true, auto_detect=false, 
                         columns={{'Scheme Code': 'VARCHAR', 'ISIN Div Payout/ISIN Growth': 'VARCHAR', 
                                 'ISIN Div Reinvestment': 'VARCHAR', 'Scheme Name': 'VARCHAR',
                                 'Net Asset Value': 'VARCHAR', 'Repurchase Price': 'VARCHAR',
                                 'Sale Price': 'VARCHAR', 'Date': 'VARCHAR'}})
            WHERE "Net Asset Value" IS NOT NULL 
            AND "Net Asset Value" != 'N.A.'
            AND "Date" IS NOT NULL
            """
            union_queries.append(query)
        
        # Combine all queries with UNION ALL
        full_query = " UNION ALL ".join(union_queries)
        
        logger.info("📊 Executing combined query on all CSV files...")
        
        # Execute query and get result
        result = conn.execute(f"""
        CREATE OR REPLACE TABLE historical_nav AS (
            {full_query}
        )
        """)
        
        # Get record count
        record_count = conn.execute("SELECT COUNT(*) FROM historical_nav").fetchone()[0]
        logger.info(f"✅ Processed {record_count:,} records from CSV files")
        
        # Get date range
        date_range = conn.execute("""
        SELECT MIN(date) as min_date, MAX(date) as max_date 
        FROM historical_nav
        """).fetchone()
        logger.info(f"📅 Historical date range: {date_range[0]} to {date_range[1]}")
        
        # Step 2: Add daily parquet files if available
        daily_dir = Paths.RAW_NAV_DAILY
        daily_files = sorted(daily_dir.glob("daily_nav_*.parquet"))
        
        if daily_files:
            logger.info(f"📂 Adding {len(daily_files)} daily parquet files...")
            
            # Create daily table
            daily_queries = []
            for daily_file in daily_files:
                query = f"SELECT * FROM read_parquet('{daily_file.absolute()}')"
                daily_queries.append(query)
            
            daily_union = " UNION ALL ".join(daily_queries)
            
            conn.execute(f"""
            CREATE OR REPLACE TABLE daily_nav AS (
                {daily_union}
            )
            """)
            
            daily_count = conn.execute("SELECT COUNT(*) FROM daily_nav").fetchone()[0]
            logger.info(f"✅ Added {daily_count:,} records from daily files")
            
            # Combine historical and daily data
            logger.info("🔗 Combining historical and daily data...")
            conn.execute("""
            CREATE OR REPLACE TABLE combined_nav AS (
                SELECT * FROM historical_nav
                UNION ALL
                SELECT * FROM daily_nav
            )
            """)
        else:
            logger.info("📝 No daily files found, using only historical data")
            conn.execute("CREATE OR REPLACE TABLE combined_nav AS SELECT * FROM historical_nav")
        
        # Remove duplicates and sort
        logger.info("🧹 Removing duplicates and sorting...")
        conn.execute("""
        CREATE OR REPLACE TABLE final_nav AS (
            SELECT DISTINCT *
            FROM combined_nav
            ORDER BY scheme_code, date
        )
        """)
        
        # Get final statistics
        final_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT scheme_code) as unique_schemes,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM final_nav
        """).fetchone()
        
        logger.info(f"📊 Final dataset statistics:")
        logger.info(f"   Total records: {final_stats[0]:,}")
        logger.info(f"   Unique schemes: {final_stats[1]:,}")
        logger.info(f"   Date range: {final_stats[2]} to {final_stats[3]}")
        
        # Step 3: Export to parquet
        output_file = Paths.COMBINED_NAV_TABLE
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"💾 Exporting to parquet: {output_file}...")
        
        conn.execute(f"""
        COPY final_nav TO '{output_file.absolute()}' 
        (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
        """)
        
        # Verify the output file
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", output_file, True, file_size_mb)
        
        # Final verification by loading with pandas
        logger.info("✅ Verifying output file...")
        verification_df = pd.read_parquet(output_file, nrows=10)
        logger.info(f"📋 Output columns: {list(verification_df.columns)}")
        logger.info(f"📊 Sample data shape: {verification_df.shape}")
        
        logger.info("🎉 Successfully created combined NAV table using DuckDB!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create combined table: {e}")
        return False
    
    finally:
        # Close DuckDB connection
        conn.close()
        logger.info("🦆 DuckDB connection closed")

def main():
    """Main function to create combined NAV table using DuckDB."""
    
    # Check if DuckDB is available
    try:
        import duckdb
        logger.info(f"🦆 DuckDB version: {duckdb.__version__}")
    except ImportError:
        logger.error("❌ DuckDB not installed. Run: pip install duckdb")
        return 1
    
    success = create_combined_table_with_duckdb()
    
    if success:
        logger.info("➡️  Next step: run 07_create_analytical_nav_daily.py to create analytical view")
        log_script_end(logger, "DuckDB Combined NAV Table Creator", True)
        return 0
    else:
        log_script_end(logger, "DuckDB Combined NAV Table Creator", False)
        return 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)