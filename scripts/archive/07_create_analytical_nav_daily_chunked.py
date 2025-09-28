#!/usr/bin/env python3
"""
Analytical NAV Daily Data Creator with Memory-Efficient Chunking

Creates analytical view by processing data in yearly chunks to avoid memory issues.
This approach is much more reliable for large datasets.
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

def create_analytical_nav_chunked():
    """
    Create analytical NAV dataset using memory-efficient yearly chunking.
    
    Returns:
        bool: True if successful, False otherwise
    """
    log_script_start(logger, "Memory-Efficient Analytical NAV Creator", 
                    "Creating analytical dataset using yearly chunking approach")
    
    # Ensure directories exist
    Paths.create_directories()
    
    # Initialize DuckDB connection with memory limits
    logger.info("🦆 Initializing DuckDB with memory limits...")
    conn = duckdb.connect()
    
    try:
        # Set conservative memory limits
        conn.execute("SET memory_limit='3GB'")
        conn.execute("SET threads=2")
        logger.info("🎛️ Set memory limit to 3GB and threads to 2")
        
        # Check input files
        nav_file = Paths.COMBINED_NAV_TABLE
        metadata_file = Paths.SCHEME_METADATA_CLEAN
        
        if not nav_file.exists():
            logger.error(f"❌ NAV file not found: {nav_file}")
            return False
        if not metadata_file.exists():
            logger.error(f"❌ Metadata file not found: {metadata_file}")
            return False
        
        # Load metadata (small file, can fit in memory)
        logger.info(f"📂 Loading metadata from {metadata_file}...")
        conn.execute(f"""
        CREATE TABLE metadata AS 
        SELECT * FROM read_parquet('{metadata_file.absolute()}')
        """)
        
        metadata_stats = conn.execute("""
        SELECT COUNT(*) as records, COUNT(DISTINCT scheme_code) as schemes
        FROM metadata
        """).fetchone()
        logger.info(f"✅ Metadata loaded: {metadata_stats[0]:,} records, {metadata_stats[1]:,} schemes")
        
        # Get year range from NAV data
        logger.info("📅 Determining year range from NAV data...")
        nav_years = conn.execute(f"""
        SELECT 
            MIN(EXTRACT(YEAR FROM date)) as min_year,
            MAX(EXTRACT(YEAR FROM date)) as max_year,
            COUNT(*) as total_records
        FROM read_parquet('{nav_file.absolute()}')
        """).fetchone()
        
        logger.info(f"📊 NAV data spans: {nav_years[0]} to {nav_years[1]} ({nav_years[2]:,} total records)")
        
        # Create output file
        output_file = Paths.ANALYTICAL / "nav_daily_data.parquet"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        # Process each year separately
        total_processed = 0
        years_to_process = list(range(int(nav_years[0]), int(nav_years[1]) + 1))
        
        logger.info(f"🔄 Processing {len(years_to_process)} years in chunks...")
        
        for i, year in enumerate(years_to_process):
            logger.info(f"📅 Processing year {year} ({i+1}/{len(years_to_process)})...")
            
            # Create analytical data for this year
            conn.execute(f"""
            CREATE OR REPLACE TABLE year_analytical AS
            SELECT 
                -- Core NAV columns
                n.scheme_code,
                n.isin_growth,
                n.isin_dividend, 
                n.nav,
                n.repurchase_price,
                n.sale_price,
                n.date,
                -- Metadata columns
                m.scheme_name,
                m.amc_name,
                m.scheme_type,
                m.scheme_category,
                m.launch_date,
                m.minimum_amount,
                -- Enhanced columns (if they exist)
                m.scheme_category_level1,
                m.scheme_category_level2,
                m.is_direct,
                m.is_growth_plan,
                -- Derived time columns
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
            FROM read_parquet('{nav_file.absolute()}') n
            INNER JOIN metadata m ON n.scheme_code = m.scheme_code
            WHERE EXTRACT(YEAR FROM n.date) = {year}
            ORDER BY n.scheme_code, n.date
            """)
            
            # Get year statistics
            year_stats = conn.execute("""
            SELECT 
                COUNT(*) as records,
                COUNT(DISTINCT scheme_code) as schemes,
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM year_analytical
            """).fetchone()
            
            logger.info(f"✅ Year {year}: {year_stats[0]:,} records, {year_stats[1]:,} schemes")
            
            if year_stats[0] > 0:  # Only append if we have data
                # Append to output file (or create if first year)
                if i == 0:
                    # First year - create the file
                    conn.execute(f"""
                    COPY year_analytical TO '{output_file.absolute()}' 
                    (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
                    """)
                    logger.info(f"📝 Created output file with {year} data")
                else:
                    # Subsequent years - append (DuckDB doesn't support direct append to Parquet)
                    # So we'll create a temporary file and combine
                    temp_file = output_file.with_suffix(f'.{year}.parquet')
                    
                    conn.execute(f"""
                    COPY year_analytical TO '{temp_file.absolute()}' 
                    (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
                    """)
                    
                    # Combine with existing data
                    conn.execute(f"""
                    CREATE OR REPLACE TABLE combined_data AS
                    SELECT * FROM read_parquet('{output_file.absolute()}')
                    UNION ALL
                    SELECT * FROM read_parquet('{temp_file.absolute()}')
                    ORDER BY scheme_code, date
                    """)
                    
                    # Write back to main file
                    conn.execute(f"""
                    COPY combined_data TO '{output_file.absolute()}' 
                    (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
                    """)
                    
                    # Clean up temp file
                    temp_file.unlink()
                    logger.info(f"📝 Appended {year} data to output file")
                
                total_processed += year_stats[0]
            
            # Clean up year table to free memory
            conn.execute("DROP TABLE IF EXISTS year_analytical")
            conn.execute("DROP TABLE IF EXISTS combined_data")
        
        # Add performance calculations as a final step
        logger.info("📊 Adding performance calculations...")
        conn.execute(f"""
        CREATE OR REPLACE TABLE final_analytical AS
        SELECT *,
            -- NAV change percentage using LAG
            (nav - LAG(nav) OVER (PARTITION BY scheme_code ORDER BY date)) / 
            LAG(nav) OVER (PARTITION BY scheme_code ORDER BY date) * 100 as nav_change_pct
        FROM read_parquet('{output_file.absolute()}')
        ORDER BY scheme_code, date
        """)
        
        # Write final version
        conn.execute(f"""
        COPY final_analytical TO '{output_file.absolute()}' 
        (FORMAT PARQUET, COMPRESSION '{Processing.PARQUET_COMPRESSION.upper()}')
        """)
        
        # Get final statistics
        final_stats = conn.execute(f"""
        SELECT 
            COUNT(*) as total_records,
            COUNT(DISTINCT scheme_code) as unique_schemes,
            COUNT(DISTINCT amc_name) as unique_amcs,
            MIN(date) as min_date,
            MAX(date) as max_date
        FROM read_parquet('{output_file.absolute()}')
        """).fetchone()
        
        # Verify the output file
        file_size_mb = output_file.stat().st_size / (1024 * 1024)
        log_file_operation(logger, "saved", output_file, True, file_size_mb)
        
        logger.info("📊 Final analytical dataset:")
        logger.info(f"   Total records: {final_stats[0]:,}")
        logger.info(f"   Unique schemes: {final_stats[1]:,}")
        logger.info(f"   Unique AMCs: {final_stats[2]:,}")
        logger.info(f"   Date range: {final_stats[3]} to {final_stats[4]}")
        logger.info(f"   File size: {file_size_mb:.2f} MB")
        
        # Validation
        validation_stats = conn.execute(f"""
        SELECT 
            COUNT(CASE WHEN nav IS NULL THEN 1 END) as null_nav,
            COUNT(CASE WHEN scheme_code IS NULL THEN 1 END) as null_code
        FROM read_parquet('{output_file.absolute()}')
        """).fetchone()
        
        if validation_stats[0] > 0 or validation_stats[1] > 0:
            logger.warning(f"⚠️ Validation issues: {validation_stats[0]} null NAVs, {validation_stats[1]} null codes")
        else:
            logger.info("✅ All validations passed")
        
        logger.info("🎉 Successfully created analytical dataset using chunked processing!")
        return True
        
    except Exception as e:
        logger.error(f"❌ Failed to create analytical dataset: {e}")
        return False
    
    finally:
        conn.close()
        logger.info("🦆 DuckDB connection closed")

def main():
    """Main function to create analytical dataset using chunked processing."""
    
    try:
        import duckdb
        logger.info(f"🦆 DuckDB version: {duckdb.__version__}")
    except ImportError:
        logger.error("❌ DuckDB not installed. Run: uv pip install duckdb")
        return 1
    
    success = create_analytical_nav_chunked()
    
    log_script_end(logger, "Memory-Efficient Analytical NAV Creator", success)
    return 0 if success else 1

if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)