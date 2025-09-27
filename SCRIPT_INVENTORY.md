# Script Inventory and Execution Order

## Current Active Scripts (DuckDB-based Pipeline)

### 1. Data Fetching Scripts
- **`01_fetch_historical_nav.py`** - Fetches historical NAV data from AMFI (initial setup only)
- **`03_fetch_daily_nav.py`** - Fetches current daily NAV data with gap-filling logic

### 2. Data Processing Scripts (DuckDB-based)
- **`02_clean_historical_nav_duckdb.py`** ✅ **ACTIVE** - Processes all historical CSV files into single merged Parquet using DuckDB
- **`04_create_combined_table.py`** ✅ **ACTIVE** - DuckDB-based script to combine historical + daily data
- **`05_extract_scheme_metadata.py`** ✅ **ACTIVE** - Extracts fresh scheme metadata from AMFI
- **`06_clean_scheme_metadata.py`** ✅ **ACTIVE** - Cleans metadata with enhanced categorization (Direct/Regular, Growth/Dividend)

### 3. Analytical Scripts (DuckDB-based)
- **`07_create_analytical_nav_daily_duckdb.py`** ✅ **ACTIVE** - Creates analytical view using DuckDB (handles large datasets)

## Obsolete/Archive Scripts

### Legacy Memory-based Scripts
- **`02_clean_historical_nav.py`** ❌ **OBSOLETE** - Old batch processing approach, replaced by DuckDB version
- **`04_create_combined_table_duckdb.py`** ❌ **OBSOLETE** - Standalone DuckDB script, functionality merged into main script 04
- **`07_create_analytical_nav_daily.py`** ❌ **OBSOLETE** - Memory-based approach, replaced by DuckDB version
- **`08_create_complete_analytical_view.py`** ❌ **OBSOLETE** - Alternative approach, superseded by DuckDB script 07

### Archive/Example Scripts
- **`07_config_example.py`** ❌ **ARCHIVE** - Configuration example, not part of pipeline
- **`archive_clean_nav_data.py`** ❌ **ARCHIVE** - Old cleaning script
- **`archive_fetch_amfi_nav_complex.py`** ❌ **ARCHIVE** - Legacy fetching script

### External/Optional Scripts
- **`ingest_zerodha_mf.py`** ❓ **OPTIONAL** - Zerodha data integration (separate workflow)

## Recommended Execution Order

### Initial Setup (One-time)
1. `01_fetch_historical_nav.py` - Download historical data (if not already done)
2. `02_clean_historical_nav_duckdb.py` - Process all historical CSV files

### Regular Pipeline (Daily/Weekly)
1. `03_fetch_daily_nav.py` - Get latest daily NAV data
2. `05_extract_scheme_metadata.py` - Get fresh metadata (weekly)
3. `06_clean_scheme_metadata.py` - Process metadata with enhancements
4. `04_create_combined_table.py` - Merge historical + daily data (if needed)
5. `07_create_analytical_nav_daily_duckdb.py` - Create analytical dataset

## Current Pipeline Status
✅ **Complete historical data**: 2006-2025 (25.6M records) via DuckDB processing
✅ **Enhanced metadata**: Direct/Regular, Growth/Dividend classification
✅ **Daily updates**: Gap-filling through September 2025
✅ **Memory efficient**: All major scripts use DuckDB for large dataset processing

## Files to Archive
Move these to `scripts/archive/` folder:
- `02_clean_historical_nav.py`
- `04_create_combined_table_duckdb.py`
- `07_create_analytical_nav_daily.py`
- `08_create_complete_analytical_view.py`
- `07_config_example.py`
- `archive_*.py` files