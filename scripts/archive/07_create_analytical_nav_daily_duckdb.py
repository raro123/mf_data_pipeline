#!/usr/bin/env python3
"""
Analytical NAV Daily Data Creator with DuckDB

Creates analytical view by joining NAV data with scheme metadata using DuckDB.
This approach handles large datasets efficiently without memory constraints.
"""

import duckdb
import sys
from pathlib import Path

# Add project root to Python path
sys.path.append(str(Path(__file__).parent.parent))

# Import centralized configuration
from config.settings import Paths, Processing
from utils.logging_setup import get_analytical_nav_logger, log_script_start, log_script_end, log_file_operation

# Initialize logger
logger = get_analytical_nav_logger(__name__)

def create_analytical_nav_daily_with_duckdb():
    """
    Create analytical NAV daily dataset using DuckDB for efficient processing.
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_script_start(logger, "Analytical NAV Daily Creator with DuckDB", 
                    "Creating analytical dataset by joining NAV data with scheme metadata")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Initialize DuckDB connection
    logger.info("🦆 Initializing DuckDB connection...")
    conn = duckdb.connect()
    
    try:
        # Step 1: Load NAV data
        nav_file = Paths.COMBINED_NAV_TABLE
        if not nav_file.exists():
            logger.error(f"❌ Combined NAV file not found: {nav_file}")
            logger.info("💡 Run 02_clean_historical_nav_duckdb.py first to create historical data")
            return False
        
        logger.info(f"📂 Loading NAV data from {nav_file}...")
        conn.execute(f"""
        CREATE OR REPLACE TABLE nav_data AS 
        SELECT * FROM read_parquet('{nav_file.absolute()}')
        """)
        
        nav_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT scheme_code) as unique_schemes,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM nav_data
        """).fetchone()
        
        logger.info(f"📊 NAV Data Summary:")
        logger.info(f"   Records: {nav_stats[0]:,}")
        logger.info(f"   Unique schemes: {nav_stats[1]:,}")
        logger.info(f"   Date range: {nav_stats[2]} to {nav_stats[3]}")
        
        # Step 2: Load metadata
        metadata_file = Paths.SCHEME_METADATA_CLEAN
        if not metadata_file.exists():
            logger.error(f"❌ Scheme metadata file not found: {metadata_file}")
            logger.info("💡 Run 05_extract_scheme_metadata.py and 06_clean_scheme_metadata.py first")
            return False
        
        logger.info(f"📂 Loading scheme metadata from {metadata_file}...")
        conn.execute(f"""
        CREATE OR REPLACE TABLE metadata AS 
        SELECT * FROM read_parquet('{metadata_file.absolute()}')
        """)
        
        metadata_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT scheme_code) as unique_schemes,
            COUNT(DISTINCT amc_name) as unique_amcs
        FROM metadata
        """).fetchone()
        
        logger.info(f"📊 Metadata Summary:")
        logger.info(f"   Records: {metadata_stats[0]:,}")
        logger.info(f"   Unique schemes: {metadata_stats[1]:,}")
        logger.info(f"   Unique AMCs: {metadata_stats[2]:,}")
        
        # Step 3: Check for enhanced columns
        enhanced_columns = []
        metadata_columns = conn.execute("DESCRIBE metadata").fetchall()
        column_names = [col[0] for col in metadata_columns]
        
        if 'scheme_category_level1' in column_names:
            enhanced_columns.append('scheme_category_level1')
            logger.info("✅ Found enhanced categorization: scheme_category_level1")
        if 'scheme_category_level2' in column_names:
            enhanced_columns.append('scheme_category_level2')
            logger.info("✅ Found enhanced categorization: scheme_category_level2")
        if 'is_direct' in column_names:
            enhanced_columns.append('is_direct')
            logger.info("✅ Found Direct/Regular plan indicator: is_direct")
        if 'is_growth_plan' in column_names:
            enhanced_columns.append('is_growth_plan')
            logger.info("✅ Found Growth/Dividend plan indicator: is_growth_plan")
        
        # Step 4: Create analytical dataset with inner join
        logger.info("🔗 Creating analytical dataset with inner join...")
        
        # Build the select statement dynamically
        base_columns = [
            'n.scheme_code', 'n.isin_growth', 'n.isin_dividend', 'n.nav', 
            'n.repurchase_price', 'n.sale_price', 'n.date',
            'm.scheme_name', 'm.amc_name', 'm.scheme_type', 'm.scheme_category',
            'm.launch_date', 'm.minimum_amount'
        ]
        
        select_columns = base_columns + [f'm.{col}' for col in enhanced_columns]
        select_statement = ', '.join(select_columns)
        
        conn.execute(f"""
        CREATE OR REPLACE TABLE analytical_nav AS
        SELECT 
            {select_statement},
            -- Derived columns
            EXTRACT(YEAR FROM n.date) as year,
            EXTRACT(MONTH FROM n.date) as month,
            EXTRACT(QUARTER FROM n.date) as quarter,
            CASE 
                WHEN EXTRACT(DOW FROM n.date) = 0 THEN 'Sunday'
                WHEN EXTRACT(DOW FROM n.date) = 1 THEN 'Monday'
                WHEN EXTRACT(DOW FROM n.date) = 2 THEN 'Tuesday'
                WHEN EXTRACT(DOW FROM n.date) = 3 THEN 'Wednesday'
                WHEN EXTRACT(DOW FROM n.date) = 4 THEN 'Thursday'
                WHEN EXTRACT(DOW FROM n.date) = 5 THEN 'Friday'
                WHEN EXTRACT(DOW FROM n.date) = 6 THEN 'Saturday'
            END as weekday
        FROM nav_data n
        INNER JOIN metadata m ON n.scheme_code = m.scheme_code
        ORDER BY n.scheme_code, n.date
        """)
        
        # Get join statistics
        join_stats = conn.execute("""
        SELECT 
            COUNT(*) as result_records,
            COUNT(DISTINCT scheme_code) as unique_schemes
        FROM analytical_nav
        """).fetchone()
        
        logger.info(f"✅ Join completed:")
        logger.info(f"   Result records: {join_stats[0]:,}")
        logger.info(f"   Join coverage: {join_stats[0]/nav_stats[0]*100:.1f}% of NAV data")
        logger.info(f"   Unique schemes in result: {join_stats[1]:,}")
        
        # Step 5: Add performance calculations
        logger.info("📊 Adding performance calculations...")
        conn.execute("""
        CREATE OR REPLACE TABLE analytical_nav_with_perf AS
        SELECT *,
            -- NAV change percentage (lag calculation)
            (nav - LAG(nav) OVER (PARTITION BY scheme_code ORDER BY date)) / 
            LAG(nav) OVER (PARTITION BY scheme_code ORDER BY date) * 100 as nav_change_pct
        FROM analytical_nav
        ORDER BY scheme_code, date
        """)
        
        # Get final statistics
        final_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT scheme_code) as unique_schemes,
            COUNT(DISTINCT amc_name) as unique_amcs,
            MIN(date) as min_date,
            MAX(date) as max_date,
            COUNT(DISTINCT scheme_category) as unique_categories
        FROM analytical_nav_with_perf
        """).fetchone()
        
        logger.info(f"📊 Final analytical dataset:")
        logger.info(f"   Total records: {final_stats[0]:,}")
        logger.info(f"   Unique schemes: {final_stats[1]:,}")
        logger.info(f"   Unique AMCs: {final_stats[2]:,}")
        logger.info(f"   Date range: {final_stats[3]} to {final_stats[4]}")
        logger.info(f"   Unique categories: {final_stats[5]:,}")
        
        # Step 6: Export to parquet
        output_file = Paths.ANALYTICAL / "nav_daily_data.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        logger.info(f"💾 Exporting analytical dataset to {output_file}...")
        
        conn.execute(f"""
        COPY analytical_nav_with_perf TO '{output_file.absolute()}' 
        (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
        """)
        
        # Verify the output file
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", output_file, True, file_size_mb)
        
        # Final validation
        logger.info("✅ Validating analytical dataset...")
        validation_stats = conn.execute("""
        SELECT 
            COUNT(CASE WHEN nav IS NULL THEN 1 END) as null_nav_count,
            COUNT(CASE WHEN date IS NULL THEN 1 END) as null_date_count,
            COUNT(CASE WHEN scheme_code IS NULL THEN 1 END) as null_code_count
        FROM analytical_nav_with_perf
        """).fetchone()
        
        if any(validation_stats):
            logger.warning(f"⚠️ Validation issues found:")
            logger.warning(f"   Null NAV values: {validation_stats[0]:,}")
            logger.warning(f"   Null dates: {validation_stats[1]:,}")
            logger.warning(f"   Null scheme codes: {validation_stats[2]:,}")
        else:
            logger.info("✅ All validations passed")
        
        logger.info("🎉 Successfully created analytical NAV daily dataset using DuckDB!")
        logger.info("🔍 Dataset ready for analysis, reporting, and ML applications")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create analytical dataset: {e}")
        return False
    
    finally:
        # Close DuckDB connection
        conn.close()
        logger.info("🦆 DuckDB connection closed")

def main():
    """Main function to create analytical NAV daily dataset using DuckDB."""
    
    # Check if DuckDB is available
    try:
        import duckdb
        logger.info(f"🦆 DuckDB version: {duckdb.__version__}")
    except ImportError:
        logger.error("❌ DuckDB not installed. Run: uv pip install duckdb")
        return 1
    
    success = create_analytical_nav_daily_with_duckdb()
    
    log_script_end(logger, "Analytical NAV Daily Creator with DuckDB", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)