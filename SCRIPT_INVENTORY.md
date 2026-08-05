# Script Inventory

All executable modules are under `scripts/` and can be run with
`python -m scripts.<module>` from the repository root.

## Core NAV Scripts

| Script | Status | Reads | Writes |
| --- | --- | --- | --- |
| `fetch_historical_nav.py` | Active, manual | AMFI historical NAV endpoint | Chunked CSV files under `data/raw/nav_historical/` |
| `transform_historical_nav.py` | Active, manual | Local historical CSV files | R2 `mutual_funds/raw/nav_historical.parquet` |
| `fetch_daily_nav.py` | Active, scheduled | AMFI and dated raw NAV object names in R2 | Dated raw NAV Parquet in R2 |
| `daily_nav_clean.py` | Deprecated rollback utility, not scheduled | Raw NAV and clean scheme metadata in R2 | Legacy R2 clean output, if explicitly invoked |

## Scheme Metadata Scripts

| Script | Status | Reads | Writes |
| --- | --- | --- | --- |
| `extract_scheme_metadata.py` | Active, scheduled | AMFI scheme download | Dated raw metadata Parquet in R2 |
| `clean_scheme_metadata.py` | Active, local | Latest local timestamped metadata CSV | Clean local CSV and Parquet |
| `build_scheme_masterdata.py` | Active, local | Clean local metadata and prior master data | Updated local master-data CSV and Parquet |
| `demo_masterdata.py` | Demonstration only | Synthetic in-memory data | Console output/demo files as coded |

The scheduled extractor does not feed the local cleaner automatically. The
local metadata flow remains separate from the datalake metadata flow.

## Supporting Data Scripts

| Script | Status | Purpose |
| --- | --- | --- |
| `fetch_aum_data.py` | Active, on demand | Fetch AMFI scheme-wise average AUM for selected financial years and periods |
| `ingest_zerodha_mf.py` | Optional | Fetch the Zerodha mutual-fund instrument dump and upload a dated CSV to R2 |

## Diagnostics

| Script | Status | Purpose |
| --- | --- | --- |
| `test_github_actions_setup.py` | Manual diagnostic | Check environment variables, R2 connectivity, dependencies, and script availability |

Focused unit tests for daily NAV checkpoint and gap behavior live under
`tests/test_fetch_daily_nav.py` and run with:

```bash
python -m unittest discover -s tests -v
```

Despite its name, `test_github_actions_setup.py` is not a unit test and is not
run by a test framework.

## Shared Modules

| Module | Purpose |
| --- | --- |
| `config/settings.py` | Paths, R2 connection setup, AMFI endpoints, retry settings, validation constants, and environment configuration |
| `utils/nav_helpers.py` | AMFI NAV column mapping, cleaning, and DuckDB Parquet writes |
| `utils/logging_setup.py` | File and console logger setup plus common logging helpers |

## Removed Pipeline Generation

Older documentation referred to numbered scripts such as
`03_daily_nav_transform.py`, `04_create_combined_table.py`, and
`07_create_analytical_nav_daily_duckdb.py`. Those files are not in the current
repository and must not be used in runbooks.

The local combined and analytical NAV Parquet files remain as historical
artifacts, but no current script rebuilds them.

## Retired Utilities

`generate_nav_validation_report.py` was retired because it read the legacy
`clean/nav_daily_growth_plan.parquet` object. NAV freshness validation should
query the canonical datalake tables instead.

See `MIGRATION_STATUS.md` for the completed raw NAV migration and remaining
R2 object-retirement steps.
