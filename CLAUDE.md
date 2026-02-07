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
python -m scripts.fetch_historical_nav

# 2. Process all historical CSV files into Parquet using DuckDB
python -m scripts.transform_historical_nav
```

**Daily/Regular Pipeline:**
```bash
# 1. Fetch latest daily NAV data with gap-filling
python -m scripts.fetch_daily_nav

# 2. Clean daily NAV data
python -m scripts.daily_nav_clean

# 3. Extract fresh scheme metadata (run weekly)
python -m scripts.extract_scheme_metadata

# 4. Process metadata with Direct/Regular and Growth/Dividend classification
python -m scripts.clean_scheme_metadata

# 5. Build comprehensive scheme masterdata (never loses schemes)
python -m scripts.build_scheme_masterdata
```

**Optional:**
```bash
# Fetch scheme-wise AUM data (on-demand, configurable depth)
python -m scripts.fetch_aum_data              # Default: last 5 years
python -m scripts.fetch_aum_data --years 3    # Specific number of years

# Ingest Zerodha mutual fund data (separate workflow)
python -m scripts.ingest_zerodha_mf

# Load benchmark data
python -m scripts.load_benchmark_data
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
│   ├── scheme_metadata/     # Raw scheme metadata
│   └── aum_schemewise/      # Raw AUM data (optional)
└── processed/               # Processed data in Parquet
    ├── nav_historical/      # Cleaned historical data
    ├── nav_daily/           # Cleaned daily data
    ├── nav_combined/        # Combined historical + daily
    ├── scheme_metadata/     # Cleaned metadata
    ├── aum_schemewise/      # Processed AUM data
    └── analytical/          # Analytics-ready datasets
```

### Key Files
- `config/settings.py` - **Central configuration hub** for all paths, URLs, and parameters (includes R2 storage setup)
- `utils/logging_setup.py` - Centralized logging configuration

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

### Scheme Masterdata
The pipeline maintains a comprehensive masterdata of all schemes ever seen:
- **Never loses schemes** - Inactive schemes are marked but preserved
- **Tracks lifecycle** - Records first_seen_date and last_seen_date
- **Updates attributes** - Scheme details are updated to latest values
- **Active/Inactive flag** - Distinguishes current vs historical schemes

**Key columns:**
- `first_seen_date` - When the scheme first appeared (uses launch_date for initial build)
- `last_seen_date` - Last time the scheme was seen in AMFI data
- `is_active` - True if scheme appears in latest data, False if disappeared
- `attribute_last_updated` - When scheme details were last updated

Use `scheme_masterdata.parquet` for complete scheme history analysis.

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
- **Runs:** `fetch_daily_nav.py` → `daily_nav_clean.py`
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
python -m scripts.fetch_daily_nav && \
python -m scripts.daily_nav_clean && \
python -m scripts.clean_scheme_metadata

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
3. Follow the pattern in existing scripts (see `fetch_daily_nav.py`)
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

### Pandas coding style
 - use chaining in pandas as long as possible
 - use best practice coding for pandas - avoid code smells and non pythonic code
 - avoid use of icons/emojis in code comments
 - use comments in case of complex logic or explaining a section. Avoid over comments
 - prefer inline comments in case of short comments

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
