# GitHub Actions Setup

The repository has three active workflows under `.github/workflows/`. All can
run on schedule or through `workflow_dispatch` from the GitHub Actions tab.

## Active Workflows

| Workflow file | Schedule | Purpose |
| --- | --- | --- |
| `daily-nav-processing.yml` | `0 4 * * *` | Fetch missing daily NAV data and rebuild clean growth-plan NAV in R2 |
| `extract-scheme-metadata.yml` | `0 1 * * 6` | Extract a dated raw AMFI scheme metadata snapshot to R2 |
| `load-benchmark-data.yml` | `30 18 * * *` | Copy the upstream NIFTY Delta dataset to clean Parquet in R2 |

Schedule conversions:

- 04:00 UTC is 09:30 IST on the same day.
- Saturday 01:00 UTC is Saturday 06:30 IST.
- 18:30 UTC is 00:00 IST on the following day.

India does not observe daylight saving time.

## Required GitHub Secrets

Configure these under **Settings > Secrets and variables > Actions**:

| Secret | Purpose |
| --- | --- |
| `R2_ACCESS_KEY_ID` | Cloudflare R2 access key |
| `R2_SECRET_ACCESS_KEY` | Cloudflare R2 secret key |
| `R2_ACCOUNT_ID` | Cloudflare account ID used by DuckDB's R2 secret |

The token must be able to read and write the `financial-data-store` bucket.
The benchmark workflow also needs read access to the upstream
`bronze/nseindex/daily_price_nifty_indices` object path.

Never commit these values. Local credentials belong in `.env`, which is
gitignored.

## Optional GitHub Variables

The workflows use defaults when these repository variables are absent:

| Variable | Default | Used by |
| --- | --- | --- |
| `AMFI_NAV_TIMEOUT` | `30` | Daily NAV |
| `AMFI_SCHEME_TIMEOUT` | `30` | Daily NAV environment and metadata extraction |
| `MAX_RETRIES` | `3` | Daily NAV |
| `RETRY_DELAY` | `5` | Daily NAV |
| `HISTORICAL_FETCH_DAYS` | `90` | Exposed to daily NAV environment, mainly used by historical fetches |
| `CHUNK_SIZE` | `10000` | Exposed to daily NAV environment |
| `LOG_LEVEL` | `INFO` | All workflows |

## R2 Object Layout

The current scripts generate paths under:

```text
r2://financial-data-store/mutual_funds/
```

Relevant objects include:

```text
mutual_funds/
|-- raw/
|   |-- nav_historical.parquet
|   `-- nav_daily_<YYYYMMDD>.parquet
|-- metadata/
|   `-- scheme_metadata_<YYYYMMDD>.parquet
|-- clean/
|   |-- scheme_metadata.parquet
|   |-- nav_daily_growth_plan.parquet
|   `-- mf_benchmark_nifty.parquet
`-- aum/
    `-- aum_schemewise_<YYYYMMDD>.parquet
```

`clean/scheme_metadata.parquet` is consumed by the daily NAV cleaner, but no
current scheduled workflow creates it. It must already exist until metadata
cleaning and publication are integrated into the R2 workflow.

## Workflow Details

### Daily NAV Processing

Runtime: Python 3.9 with dependencies installed by pip.

```bash
python -m scripts.fetch_daily_nav
python -m scripts.daily_nav_clean
```

The fetcher reads its latest checkpoint from canonical raw object names under
`mutual_funds/raw/nav_daily_*.parquet`, not from clean NAV. An empty prefix
requires `--bootstrap-date YYYYMMDD` when run manually. It fetches subsequent
weekdays through yesterday from AMFI and writes dated raw Parquet files.

The cleaner joins raw NAV to R2 scheme metadata and rewrites the clean
growth-plan dataset. This second step is temporarily retained for legacy
compatibility; the datalake builds its canonical NAV tables directly from raw
objects.

The workflow uploads `logs/` as a seven-day artifact on failure. The current
daily scripts mostly print directly to the workflow log, so the artifact may
contain little or no additional detail.

### Weekly Scheme Metadata Extraction

Runtime: Python 3.12 with dependencies installed by uv.

```bash
uv run python -m scripts.extract_scheme_metadata
```

This job only extracts a dated raw metadata Parquet file to R2. It does not run
`clean_scheme_metadata.py`, publish `clean/scheme_metadata.parquet`, or rebuild
local scheme master data.

### Daily Benchmark Loading

Runtime: Python 3.9 with dependencies installed by pip.

```bash
python -m scripts.load_benchmark_data
```

This reads the upstream NIFTY Delta table and overwrites the clean mutual-fund
benchmark Parquet object.

## Manual Verification

After configuring secrets, manually run each workflow before relying on its
schedule:

1. Open the repository's **Actions** tab.
2. Select the workflow.
3. Choose **Run workflow** on the intended branch.
4. Inspect the job log and verify the expected R2 object was updated.

For a local configuration diagnostic:

```bash
python -m scripts.test_github_actions_setup
```

This command checks configuration and connectivity. It may access R2 and AMFI;
it is not a unit-test suite.

## Common Failures

| Symptom | Check |
| --- | --- |
| R2 authentication error | Secret names, token validity, account ID, and bucket permissions |
| DuckDB extension failure | Runner network access and DuckDB extension installation/loading |
| AMFI timeout or malformed response | AMFI availability, timeout variables, and response format |
| Daily clean cannot read metadata | Presence and schema of `mutual_funds/clean/scheme_metadata.parquet` |
| Benchmark `delta_scan` failure | Upstream path, read permission, and DuckDB Delta extension support |
| No useful failure artifact | Read the GitHub Actions step log; not every script uses file logging |

## Operational Gaps

- Metadata extraction, cleaning, publication, and master-data maintenance are
  not yet one automated workflow.
- NAV validation is manual and does not gate the daily workflow.
- Workflows print success/failure messages but do not send external alerts.
- NAV and benchmark jobs use pip/Python 3.9 while metadata uses uv/Python 3.12.
  Standardizing runtimes would reduce maintenance differences.
