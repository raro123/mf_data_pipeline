# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a data pipeline for ingesting, transforming, and analyzing mutual fund instrument and Net Asset Value (NAV) data from AMFI (Association of Mutual Funds in India).

**Architecture:**
- **Raw Data Storage:** Cloudflare R2 (S3-compatible object storage)
- **Processing Engine:** DuckDB (for memory-efficient large dataset processing)
- **Transformation:** Pandas for data manipulation
- **Data Formats:** CSV (raw), Parquet (processed)
- **Orchestration:** GitHub Actions (daily scheduled runs)

## Environment Setup

### Initial Setup
```bash
# Create virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Initialize project directories
python -c "from config.settings import initialize_project; initialize_project()"
```

### Environment Variables
Copy `.env` file with the following required variables:
- `R2_ACCESS_KEY_ID` - Cloudflare R2 access key
- `R2_SECRET_ACCESS_KEY` - Cloudflare R2 secret key
- `R2_ACCOUNT_ID` - Cloudflare R2 account ID

Optional configuration (see `config/settings.py` for defaults):
- `AMFI_NAV_TIMEOUT`, `AMFI_SCHEME_TIMEOUT` - API timeouts
- `MAX_RETRIES`, `RETRY_DELAY` - Retry configuration
- `LOG_LEVEL` - Logging level (INFO, DEBUG, etc.)

## Pipeline Scripts

### Active Scripts (in execution order)

**One-time Initial Setup:**
```bash
# 1. Fetch historical NAV data from AMFI (2006-present)
python scripts/fetch_historical_nav.py

# 2. Process all historical CSV files into Parquet using DuckDB
python scripts/02_historical_nav_transform.py
```

**Daily/Regular Pipeline:**
```bash
# 1. Fetch latest daily NAV data with gap-filling
python scripts/03_daily_nav_transform.py

# 2. Clean daily NAV data
python scripts/daily_nav_clean.py

# 3. Extract fresh scheme metadata (run weekly)
python scripts/05_extract_scheme_metadata.py

# 4. Process metadata with Direct/Regular and Growth/Dividend classification
python scripts/06_clean_scheme_metadata.py

# 5. Clean and categorize metadata
python scripts/clean_metadata.py
```

**Optional:**
```bash
# Ingest Zerodha mutual fund data (separate workflow)
python scripts/ingest_zerodha_mf.py

# Load benchmark data
python scripts/load_benchmark_data.py
```

### Archived Scripts
Obsolete/legacy scripts are in `scripts/archive/`. These have been replaced by DuckDB-based versions for better memory efficiency.

## Data Flow Architecture

### Directory Structure
```
data/
├── raw/                      # Raw data from APIs
│   ├── nav_historical/      # Historical NAV CSV files
│   ├── nav_daily/           # Daily NAV data
│   └── scheme_metadata/     # Raw scheme metadata
└── processed/               # Processed data in Parquet
    ├── nav_historical/      # Cleaned historical data
    ├── nav_daily/           # Cleaned daily data
    ├── nav_combined/        # Combined historical + daily
    ├── scheme_metadata/     # Cleaned metadata
    └── analytical/          # Analytics-ready datasets
```

### Key Files
- `config/settings.py` - **Central configuration hub** for all paths, URLs, and parameters
- `utils/logging_setup.py` - Centralized logging configuration
- `src/common/storage.py` - R2 storage utilities

## Configuration System

All configuration is centralized in `config/settings.py`:

**Key Classes:**
- `Paths` - All file and directory paths
- `R2` - Cloudflare R2 storage configuration and connection setup
- `API` - AMFI API endpoints, timeouts, retry logic
- `Processing` - Batch sizes, chunk sizes, compression settings
- `Validation` - Data validation rules (NAV ranges, scheme codes, etc.)
- `Logging` - Log file patterns and retention
- `Environment` - Environment-specific settings (dev/prod)

**Always import from config.settings rather than hardcoding values.**

## DuckDB Integration

The pipeline uses DuckDB for memory-efficient processing of large datasets (25M+ records):

```python
from config.settings import R2

# Setup R2 connection
r2 = R2()
con = r2.setup_connection()

# Query data from R2
result = con.sql("""
    SELECT * FROM read_parquet('r2://bucket/path/file.parquet')
""").df()
```

DuckDB handles:
- Historical data processing (89 CSV files → single Parquet)
- Data validation and deduplication
- Analytical view creation
- R2 integration via httpfs extension

## Data Processing Patterns

### Scheme Classification
The pipeline automatically detects:
- **Direct vs Regular Plans** - Pattern matching on scheme names
- **Growth vs Dividend/IDCW** - Comprehensive pattern detection
- **Category Levels:**
  - Level 1: Main types (Equity, Debt, Hybrid, Solution Oriented, Other)
  - Level 2: 49+ sub-categories (Large Cap, Liquid Fund, etc.)

### Data Quality
- NAV range validation (0.01 to 10,000)
- Null value checking
- Automatic deduplication
- Gap-filling for missing dates

## GitHub Actions

Two workflows automate the pipeline:

### Daily NAV Processing
**File:** `.github/workflows/daily-nav-processing.yml`
- **Schedule:** 11:00 PM UTC daily (6:30 AM IST)
- **Runs:** `03_daily_nav_transform.py` → `daily_nav_clean.py`
- **Manual trigger:** Available via workflow_dispatch

### Benchmark Data Loading
**File:** `.github/workflows/load-benchmark-data.yml`
- **Schedule:** Weekly
- **Runs:** `scripts/load_benchmark_data.py`

**Required GitHub Secrets:**
- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_ACCOUNT_ID`

See `GITHUB_ACTIONS_SETUP.md` for detailed configuration.

## Logging

All scripts use centralized logging via `utils/logging_setup.py`:

```python
from utils.logging_setup import setup_logger
from config.settings import Logging

logger = setup_logger(__name__, Logging.FETCH_DAILY_LOG)
logger.info("Processing started")
```

Logs are written to `logs/` directory with patterns defined in `config/settings.py`.

## Common Commands

```bash
# Run the complete daily pipeline
python scripts/03_daily_nav_transform.py && \
python scripts/daily_nav_clean.py && \
python scripts/06_clean_scheme_metadata.py

# Check pipeline status by viewing logs
ls -lh logs/

# View processed data files
ls -lh data/processed/*/

# Test R2 connectivity
python -c "from config.settings import R2; r2 = R2(); con = r2.setup_connection(); print('Connected!')"

# Initialize/verify directory structure
python -c "from config.settings import initialize_project; initialize_project()"
```

## Development Notes

### Adding New Scripts
1. Import from `config.settings` for all configuration
2. Use `utils.logging_setup` for logging
3. Follow the pattern in existing scripts (see `03_daily_nav_transform.py`)
4. Add script to appropriate section in `PIPELINE_EXECUTION_ORDER.md`

### Modifying Configuration
- Edit `config/settings.py` for global changes
- Use environment variables in `.env` for instance-specific overrides
- Configuration precedence: `.env` → `config/settings.py` → defaults

### Data Storage
- **Raw data:** Always CSV from API, stored in `data/raw/`
- **Processed data:** Always Parquet with Snappy compression
- **R2 storage:** Mirror of processed data for production/sharing
- **Local development:** Use `data/` directory, R2 optional

### Performance Considerations
- Use DuckDB for datasets >1M records
- Historical processing: ~15 seconds for 89 CSV files with DuckDB
- Parquet compression reduces storage by ~70% vs CSV
- Categorical dtypes for repeated string values

## Key Data Sources

- **AMFI NAV API:** `https://www.amfiindia.com/spages/NAVAll.txt`
- **AMFI Scheme Metadata:** `https://portal.amfiindia.com/DownloadSchemeData_Po.aspx`
- **Historical NAV (date range):** `https://portal.amfiindia.com/DownloadNAVHistoryReport_Po.aspx`

## Testing

```bash
# Test GitHub Actions setup locally
python scripts/test_github_actions_setup.py

# Verify data quality in notebooks
jupyter notebook notebooks/analysis.ipynb
```

## Documentation Files

- `README.md` - Project overview
- `PIPELINE_EXECUTION_ORDER.md` - Detailed execution workflows and status
- `SCRIPT_INVENTORY.md` - Complete script inventory with active/obsolete status
- `GITHUB_ACTIONS_SETUP.md` - GitHub Actions configuration guide
- `.github/workflows/README.md` - Workflow-specific documentation
