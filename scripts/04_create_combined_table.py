#!/usr/bin/env python3
"""
Combined NAV Table Creator with DuckDB

Creates a unified NAV table by combining historical data and daily parquet files using DuckDB.
This approach is memory-efficient and handles large datasets without loading into memory.
"""

import duckdb
import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing
from utils.logging_setup import get_combine_table_logger, log_script_start, log_script_end, log_file_operation

# Initialize logger
logger = get_combine_table_logger(__name__)

def create_combined_table_with_duckdb():
    """
    Create combined NAV table using DuckDB for efficient processing.
    Combines existing historical data with daily parquet files.
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_script_start(logger, "Combined NAV Table Creator with DuckDB", 
                    "Creating unified table from historical data and daily parquet files")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Initialize DuckDB connection
    logger.info("🦆 Initializing DuckDB connection...")
    conn = duckdb.connect()
    
    try:
        output_file = Paths.COMBINED_NAV_TABLE
        
        # Step 1: Check for existing historical data
        if output_file.exists():
            logger.info(f"📂 Loading existing historical data from {output_file}...")
            conn.execute(f"""
            CREATE OR REPLACE TABLE historical_nav AS 
            SELECT * FROM read_parquet('{output_file.absolute()}')
            """)
            
            historical_stats = conn.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT scheme_code) as unique_schemes,
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM historical_nav
            """).fetchone()
            
            logger.info(f"✅ Historical data loaded:")
            logger.info(f"   Records: {historical_stats[0]:,}")
            logger.info(f"   Unique schemes: {historical_stats[1]:,}")
            logger.info(f"   Date range: {historical_stats[2]} to {historical_stats[3]}")
        else:
            logger.info("📂 No existing historical data found, creating empty table")
            conn.execute("""
            CREATE OR REPLACE TABLE historical_nav (
                scheme_code VARCHAR,
                scheme_name VARCHAR,
                isin_growth VARCHAR,
                isin_dividend VARCHAR,
                nav DOUBLE,
                repurchase_price DOUBLE,
                sale_price DOUBLE,
                date DATE
            )
            """)
        
        # Step 2: Add daily data if available
        daily_dir = Paths.RAW_NAV_DAILY
        daily_files = sorted(daily_dir.glob("daily_nav_*.parquet"))
        
        if daily_files:
            logger.info(f"📅 Found {len(daily_files)} daily parquet files to add...")
            
            # Create daily table
            daily_queries = []
            for daily_file in daily_files:
                logger.info(f"📄 Adding {daily_file.name}")
                query = f"SELECT * FROM read_parquet('{daily_file.absolute()}')"
                daily_queries.append(query)
            
            if daily_queries:
                daily_union = " UNION ALL ".join(daily_queries)
                
                conn.execute(f"""
                CREATE OR REPLACE TABLE daily_nav AS (
                    {daily_union}
                )
                """)
                
                daily_stats = conn.execute("""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT scheme_code) as unique_schemes,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM daily_nav
                """).fetchone()
                
                logger.info(f"✅ Daily data loaded:")
                logger.info(f"   Records: {daily_stats[0]:,}")
                logger.info(f"   Unique schemes: {daily_stats[1]:,}")
                logger.info(f"   Date range: {daily_stats[2]} to {daily_stats[3]}")
                
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
                logger.info("📝 Using only historical data")
                conn.execute("CREATE OR REPLACE TABLE combined_nav AS SELECT * FROM historical_nav")
        else:
            logger.info("📝 No daily files found, using only historical data")
            conn.execute("CREATE OR REPLACE TABLE combined_nav AS SELECT * FROM historical_nav")
        
        # Step 3: Remove duplicates and sort
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
            MAX(date) as max_date,
            MIN(nav) as min_nav,
            MAX(nav) as max_nav,
            COUNT(CASE WHEN nav IS NULL THEN 1 END) as null_nav_count
        FROM final_nav
        """).fetchone()
        
        logger.info(f"📊 Final dataset statistics:")
        logger.info(f"   Total records: {final_stats[0]:,}")
        logger.info(f"   Unique schemes: {final_stats[1]:,}")
        logger.info(f"   Date range: {final_stats[2]} to {final_stats[3]}")
        logger.info(f"   NAV range: ₹{final_stats[4]:.2f} to ₹{final_stats[5]:,.2f}")
        logger.info(f"   Null NAV values: {final_stats[6]:,}")
        
        # Step 4: Export to parquet
        logger.info(f"💾 Exporting to parquet: {output_file}...")
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        conn.execute(f"""
        COPY final_nav TO '{output_file.absolute()}' 
        (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
        """)
        
        # Verify the output file
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", output_file, True, file_size_mb)
        
        logger.info("🎉 Successfully created combined NAV table using DuckDB!")
        logger.info("➡️  Next step: run 07_create_analytical_nav_daily.py to create analytical view")
        
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
        logger.error("❌ DuckDB not installed. Run: uv pip install duckdb")
        return 1
    
    success = create_combined_table_with_duckdb()
    
    log_script_end(logger, "Combined NAV Table Creator with DuckDB", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)