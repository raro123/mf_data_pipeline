#!/usr/bin/env python3
"""
Historical NAV Data Cleaner with DuckDB

Cleans raw historical NAV CSV files and creates a single merged Parquet file using DuckDB.
This approach is memory-efficient and avoids the complexity of batch processing.
"""

import duckdb
import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing, Validation
from utils.logging_setup import get_historical_clean_logger, log_script_start, log_script_end, log_file_operation

# Initialize logger
logger = get_historical_clean_logger(__name__)

def create_merged_historical_nav_with_duckdb():
    """
    Create a single merged historical NAV file using DuckDB for efficient processing.
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_script_start(logger, "Historical NAV Cleaner with DuckDB", 
                    "Creating single merged Parquet file from all historical CSV files")
    
    # Use configured paths
    input_dir = Path("data/raw/nav_historical")
    output_file = Paths.COMBINED_NAV_TABLE
    
    logger.info(f"📁 Input directory: {input_dir}")
    logger.info(f"📁 Output file: {output_file}")
    
    # Ensure directories exist
    Paths.create_directories()
    
    if not input_dir.exists():
        logger.error(f"❌ Input directory not found: {input_dir}")
        return False
    
    # Get all CSV files
    csv_files = sorted(list(input_dir.glob("amfi_raw_nav_*.csv")))
    
    if not csv_files:
        logger.error(f"❌ No CSV files found in {input_dir}")
        return False
    
    logger.info(f"📊 Found {len(csv_files)} CSV files to process")
    
    # Initialize DuckDB connection
    logger.info("🦆 Initializing DuckDB connection...")
    conn = duckdb.connect()
    
    try:
        # Build the UNION ALL query for all CSV files
        logger.info("🔗 Creating unified view from all CSV files...")
        
        union_queries = []
        for csv_file in csv_files:
            logger.info(f"📄 Adding {csv_file.name} to processing queue")
            
            # Use DuckDB's CSV reader with data cleaning
            query = f"""
            SELECT 
                "Scheme Code" as scheme_code,
                "Scheme Name" as scheme_name,
                "ISIN Div Payout/ISIN Growth" as isin_growth,
                "ISIN Div Reinvestment" as isin_dividend,
                CASE 
                    WHEN "Net Asset Value" = 'N.A.' OR "Net Asset Value" IS NULL THEN NULL
                    ELSE TRY_CAST("Net Asset Value" as DOUBLE)
                END as nav,
                CASE 
                    WHEN "Repurchase Price" = 'N.A.' OR "Repurchase Price" IS NULL THEN NULL
                    ELSE TRY_CAST("Repurchase Price" as DOUBLE)
                END as repurchase_price,
                CASE 
                    WHEN "Sale Price" = 'N.A.' OR "Sale Price" IS NULL THEN NULL
                    ELSE TRY_CAST("Sale Price" as DOUBLE)
                END as sale_price,
                TRY_STRPTIME("Date", '%d-%b-%Y') as date
            FROM read_csv('{csv_file.absolute()}', 
                         header=true, 
                         auto_detect=false,
                         columns={{'Scheme Code': 'VARCHAR', 'Scheme Name': 'VARCHAR',
                                 'ISIN Div Payout/ISIN Growth': 'VARCHAR', 'ISIN Div Reinvestment': 'VARCHAR',
                                 'Net Asset Value': 'VARCHAR', 'Repurchase Price': 'VARCHAR',
                                 'Sale Price': 'VARCHAR', 'Date': 'VARCHAR'}})
            WHERE "Scheme Code" IS NOT NULL 
            AND "Date" IS NOT NULL
            AND "Net Asset Value" IS NOT NULL
            AND "Net Asset Value" != 'N.A.'
            AND "Scheme Code" NOT LIKE '%Open Ended%'
            AND "Scheme Code" NOT LIKE '%Close Ended%'
            AND "Scheme Code" NOT LIKE '%Interval Fund%'
            AND "Scheme Code" NOT LIKE '%Fund of Funds%'
            AND "Scheme Code" NOT LIKE '%Mutual Fund%'
            """
            union_queries.append(query)
        
        # Combine all queries with UNION ALL
        full_query = " UNION ALL ".join(union_queries)
        
        logger.info("📊 Executing combined query on all CSV files...")
        
        # Create the combined table with additional filtering and validation
        conn.execute(f"""
        CREATE OR REPLACE TABLE cleaned_historical_nav AS (
            SELECT *
            FROM ({full_query})
            WHERE nav IS NOT NULL 
            AND date IS NOT NULL
            AND nav >= {Validation.MIN_NAV_VALUE}
            AND nav <= {Validation.MAX_NAV_VALUE}
            ORDER BY scheme_code, date
        )
        """)
        
        # Get statistics
        stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT scheme_code) as unique_schemes,
            MIN(date) as min_date,
            MAX(date) as max_date,
            MIN(nav) as min_nav,
            MAX(nav) as max_nav
        FROM cleaned_historical_nav
        """).fetchone()
        
        logger.info(f"📊 Processing completed successfully:")
        logger.info(f"   Total records: {stats[0]:,}")
        logger.info(f"   Unique schemes: {stats[1]:,}")
        logger.info(f"   Date range: {stats[2]} to {stats[3]}")
        logger.info(f"   NAV range: ₹{stats[4]:.2f} to ₹{stats[5]:,.2f}")
        
        # Export to parquet
        logger.info(f"💾 Exporting to parquet: {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        conn.execute(f"""
        COPY cleaned_historical_nav TO '{output_file.absolute()}' 
        (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
        """)
        
        # Verify the output file
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", output_file, True, file_size_mb)
        
        logger.info("🎉 Successfully created merged historical NAV file using DuckDB!")
        logger.info("➡️  Next step: run 07_create_analytical_nav_daily.py to create analytical view")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create merged historical file: {e}")
        return False
    
    finally:
        # Close DuckDB connection
        conn.close()
        logger.info("🦆 DuckDB connection closed")

def main():
    """Main function to create merged historical NAV file using DuckDB."""
    
    # Check if DuckDB is available
    try:
        import duckdb
        logger.info(f"🦆 DuckDB version: {duckdb.__version__}")
    except ImportError:
        logger.error("❌ DuckDB not installed. Run: uv pip install duckdb")
        return 1
    
    success = create_merged_historical_nav_with_duckdb()
    
    log_script_end(logger, "Historical NAV Cleaner with DuckDB", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)