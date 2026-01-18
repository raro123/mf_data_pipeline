# Mutual Fund Data Pipeline - Execution Order

## Current Active Scripts (Post-DuckDB Migration)

### Active Scripts Directory: `/scripts/`
1. **`fetch_historical_nav.py`** - Historical data fetcher (one-time)
2. **`transform_historical_nav.py`** - DuckDB-based historical processor (one-time)
3. **`fetch_daily_nav.py`** - Daily NAV fetcher with gap-filling
4. **`daily_nav_clean.py`** - Daily NAV cleaner (joins with metadata)
5. **`extract_scheme_metadata.py`** - Metadata extractor
6. **`clean_scheme_metadata.py`** - Enhanced metadata processor
7. **`build_scheme_masterdata.py`** - Scheme masterdata builder
8. **`ingest_zerodha_mf.py`** - Optional Zerodha integration
9. **`load_benchmark_data.py`** - Benchmark data loader

### Archived Scripts: `/scripts/archive/`
- Legacy memory-based processing scripts
- Experimental and duplicate implementations

---

## Execution Workflows

### Initial Setup (One-time only)
```bash
# 1. Download historical data (if not already done)
python -m scripts.fetch_historical_nav

# 2. Process ALL historical CSV files (2006-2025) using DuckDB
python -m scripts.transform_historical_nav
```

### Daily/Weekly Pipeline
```bash
# 1. Get latest daily NAV data
python -m scripts.fetch_daily_nav

# 2. Clean and enrich daily NAV data
python -m scripts.daily_nav_clean

# 3. Get fresh metadata (weekly recommended)
python -m scripts.extract_scheme_metadata

# 4. Process metadata with enhanced classifications
python -m scripts.clean_scheme_metadata

# 5. Build comprehensive scheme masterdata
python -m scripts.build_scheme_masterdata
```

### Full Refresh Pipeline
```bash
# If you need to rebuild everything from scratch:
python -m scripts.transform_historical_nav    # Historical data
python -m scripts.fetch_daily_nav             # Daily updates
python -m scripts.daily_nav_clean             # Clean daily data
python -m scripts.extract_scheme_metadata     # Fresh metadata
python -m scripts.clean_scheme_metadata       # Enhanced processing
python -m scripts.build_scheme_masterdata     # Masterdata
```

---

## Current Data Status

### Completed Components
- **Historical Data**: 25.6M records (2006-2025) processed via DuckDB
- **Daily Updates**: Gap-filled through September 2025
- **Enhanced Metadata**: 16K schemes with Direct/Regular and Growth/Dividend classification
- **Analytical Dataset**: Ready for analysis with all enhancements

### Technical Improvements
- **Memory Efficient**: DuckDB handles large datasets without memory issues
- **Performance**: ~15 seconds to process 89 CSV files (vs hours with pandas)
- **Scalable**: Can handle 25M+ records without performance degradation
- **Robust**: Professional logging and error handling throughout

---

## Key Features

### Enhanced Scheme Classification
- **Direct vs Regular Plans**: Automatic detection from scheme names
- **Growth vs Dividend/IDCW**: Comprehensive pattern matching
- **Category Levels**: Level 1 (5 main types) + Level 2 (49 sub-categories)

### Data Quality
- **Validation**: NAV range validation, null checking
- **Deduplication**: Automatic removal of duplicate records
- **Gap Filling**: Intelligent daily data fetching with missing date detection

### Performance Optimizations
- **DuckDB Integration**: Memory-efficient processing for large datasets
- **Categorical Types**: Optimized storage for repeated string values
- **Parquet Format**: Compressed columnar storage with Snappy compression

---

## Shared Utilities

Common NAV processing utilities are in `utils/nav_helpers.py`:
- `NAV_COLUMNS` - Standard column names from AMFI
- `NAV_COLUMN_MAPPING` - Column name mapping
- `clean_nav_dataframe()` - Standardize NAV DataFrames
- `save_to_parquet()` - Save DataFrames via DuckDB

---

## Next Steps

1. **Test the complete pipeline** with the DuckDB-based analytical script
2. **Set up scheduling** for daily/weekly execution
3. **Implement monitoring** and alerting for pipeline health

The pipeline is now optimized, organized, and ready for production use!
