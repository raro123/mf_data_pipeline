# Repository Guidance

## Project Purpose

This repository collects Indian mutual-fund source data, primarily from AMFI,
and publishes immutable raw snapshots (NAV, scheme metadata, AMC members, AUM,
and TER). Canonical analysis tables such as `mf.nav_daily` and
`mf.nav_daily_open_growth` are built by the datalake. NIFTY benchmark data is
maintained by the separate `nifty_index_ingestion` repository.

The codebase is a collection of executable Python modules rather than an
installed application package.

## Current Architecture

- Cloudflare R2 is the operational store for scheduled workflows.
- Local files under `data/` are used for historical downloads, metadata
  cleaning, master-data maintenance, reports, and analysis.
- DuckDB provides R2 access, Parquet reads/writes, joins, and validation SQL.
- Pandas handles API responses and local transformations.
- GitHub Actions schedules daily NAV, weekly metadata and AMC member
  extraction, quarterly AUM extraction, and twice-monthly TER extraction.

R2 paths are built by `config.settings.R2`:

```text
r2://financial-data-store/mutual_funds/<area>/<file>.parquet
```

## Authoritative Sources

When documentation and code disagree, inspect these in order:

1. The implementation under `scripts/`.
2. `.github/workflows/*.yml` for current automation and schedules.
3. `config/settings.py` for paths, endpoints, and runtime defaults.
4. `PIPELINE_EXECUTION_ORDER.md` and `SCRIPT_INVENTORY.md` for runbooks.

Do not use old numbered script names. Files such as
`03_daily_nav_transform.py`, `04_create_combined_table.py`, and
`07_create_analytical_nav_daily_duckdb.py` are not present.

## Repository Layout

```text
config/settings.py       Central configuration and R2 connection setup
scripts/                 Ingestion, transformation, validation, and diagnostics
utils/nav_helpers.py     Shared AMFI NAV normalization and Parquet writing
utils/logging_setup.py   Shared logger setup and logging helpers
.github/workflows/       Scheduled production jobs
data/                    Local data and reports; ignored by git
notebooks/               Exploratory analysis
```

## Supported Commands

Initialize local directories:

```bash
python -c "from config.settings import initialize_project; initialize_project()"
```

Historical NAV backfill:

```bash
python -m scripts.fetch_historical_nav --start 20060101 --end YYYYMMDD
python -m scripts.transform_historical_nav
```

Daily R2 NAV flow:

```bash
python -m scripts.fetch_daily_nav
```

One-time repair for scheme/date NAV rows published after an existing daily
snapshot:

```bash
python -m scripts.repair_missing_nav --start YYYYMMDD --end YYYYMMDD
python -m scripts.repair_missing_nav --start YYYYMMDD --end YYYYMMDD --write
```

For an empty raw NAV prefix, initialize extraction explicitly:

```bash
python -m scripts.fetch_daily_nav --bootstrap-date YYYYMMDD
```

R2 metadata extraction:

```bash
python -m scripts.extract_scheme_metadata
```

Local metadata maintenance:

```bash
python -m scripts.clean_scheme_metadata
python -m scripts.build_scheme_masterdata
```

Weekly AMC member snapshot:

```bash
python -m scripts.extract_amc_members
```

Monthly TER extraction:

```bash
python -m scripts.fetch_ter_data --scheduled
python -m scripts.fetch_ter_data --month YYYY-MM
```

Optional jobs:

```bash
python -m scripts.fetch_aum_data
python -m scripts.ingest_zerodha_mf
```

Environment diagnostic:

```bash
python -m scripts.test_github_actions_setup
```

## Automated Workflows

| Workflow | Schedule | Main modules |
| --- | --- | --- |
| Daily NAV | `30 6 * * *` | `fetch_daily_nav` |
| Scheme metadata | `30 0 * * 6` | `extract_scheme_metadata` |
| AMC members | `30 1 * * 6` | `extract_amc_members` |
| Scheme-wise AUM | `30 0 10 1,4,7,10 *` | `fetch_aum_data` |
| TER | `30 7 5,20 * *` | `fetch_ter_data --scheduled` |

Schedules are UTC. Their IST times are 12:00 daily, Saturday 06:00 (metadata)
and 07:00 (AMC members), 06:00 on the 10th of each quarter month, and 13:00 on
the 5th and 20th (TER).

NIFTY benchmark ingestion is scheduled in the separate
`nifty_index_ingestion` repository and dispatches successful raw uploads to the
datalake.

## Important Pipeline Boundaries

### Metadata is split across two flows

`extract_scheme_metadata.py` writes dated raw Parquet to R2 under
`mutual_funds/metadata/`. `clean_scheme_metadata.py` reads the latest local CSV
under `data/raw/scheme_metadata/` and writes local processed files.

These flows are not connected. The scheduled metadata job does not:

- run the local cleaner;
- rebuild scheme master data; or
- publish a canonical clean metadata object to R2.

The canonical NAV tables are built by the datalake from raw NAV objects. The
former repository-side clean utility has been removed from this repository.

### Raw NAV extraction is independent of clean outputs

`fetch_daily_nav.py` discovers the latest canonical
`mutual_funds/raw/nav_daily_<YYYYMMDD>.parquet` object and fetches later
weekdays through yesterday. It requires `--bootstrap-date` only when no raw
daily objects exist. A failed date stops the loop so a later object cannot
advance the checkpoint past a gap.

`repair_missing_nav.py` is a manual, missing-only recovery path for late AMFI
reports inside existing daily snapshots. It writes immutable
`nav_repair_missing_*.parquet` objects that the datalake ingests but daily
watermark discovery ignores. It skips and reports NAV restatements.

### Historical transformation is Pandas-based

`transform_historical_nav.py` opens every local CSV and concatenates the
frames in memory. Do not describe it as streaming or DuckDB-native without
changing the implementation.

### Some local Parquet files are legacy snapshots

No current script rebuilds:

- `data/processed/nav_combined/raw_nav_table.parquet`
- `data/processed/analytical/nav_daily_data.parquet`

Do not infer current R2 state from these local files.

### Validation is owned by the datalake

The former local NAV validation report read the retired clean Parquet output and
has been removed. Validate freshness and completeness against the canonical
datalake tables.

## Configuration

Always use `config.settings` rather than introducing hardcoded project paths,
AMFI URLs, retry settings, or R2 credentials.

Required for R2-backed work:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_ACCOUNT_ID`

Credentials are loaded from the environment or local `.env`. Never log or
commit secret values.

Relevant configuration classes:

- `Paths`: local directories and named output files
- `R2`: object path construction and DuckDB R2 secret setup
- `API`: AMFI endpoints, timeouts, and retry settings
- `Processing`: chunking, encoding, and Parquet compression
- `Validation`: range and quality constants
- `Logging`: log names, levels, and retention
- `Environment`: development and production flags

## Data Conventions

Standard NAV columns after `clean_nav_dataframe`:

- `scheme_code` as string
- `isin_growth`
- `isin_dividend`
- `nav` as numeric
- `date` as timestamp

Scheme metadata adds category levels plus `is_direct` and `is_growth_plan`.
Scheme master data adds lifecycle fields including `is_active`,
`first_seen_date`, `last_seen_date`, and `attribute_last_updated`.

Raw API responses should remain reproducible inputs. Processed data should use
Parquet unless an existing workflow explicitly needs CSV for inspection.

## Engineering Practices

- Run modules from the repository root with `python -m scripts.<name>` so
  imports resolve consistently.
- Follow existing Pandas chaining where it remains readable.
- Prefer DuckDB for large scans, joins, and R2 operations.
- Reuse `utils.nav_helpers` and `utils.logging_setup` before adding duplicate
  parsing or logging code.
- Keep R2 path creation in `R2.get_full_path`.
- Preserve scheme codes as strings; numeric conversion can lose semantics.
- Be careful with overwrite behavior in `save_to_parquet`: DuckDB `COPY` writes
  the target object represented by the supplied path.
- Add focused tests when changing parsing, classification, date-gap logic, or
  master-data merge behavior. Existing unit coverage under `tests/` includes
  daily NAV checkpoint and gap logic, missing-only repairs, AUM, AMC members,
  and TER extraction.
- Do not manually edit or commit generated files under `data/` or `logs/`.
  Pipeline and validation commands may update them as part of their normal
  operation; they remain gitignored local artifacts.

## Before Finishing A Change

1. Verify every documented filename and command exists.
2. Run the narrowest relevant local checks without contacting production R2
   unless the task requires it.
3. Check `git diff` for accidental data, notebook, or credential changes.
4. Update `README.md`, `PIPELINE_EXECUTION_ORDER.md`, or
   `SCRIPT_INVENTORY.md` when behavior or orchestration changes.
5. If a workflow changes, also update `GITHUB_ACTIONS_SETUP.md` and
   `.github/workflows/README.md`.
6. Keep `MIGRATION_STATUS.md` current during the legacy R2 object-retirement
   window.
