# GitHub Actions Setup

The repository has five active workflows under `.github/workflows/`. All can
run on schedule or through `workflow_dispatch` from the GitHub Actions tab.

## Active Workflows

| Workflow file | Schedule | Purpose |
| --- | --- | --- |
| `daily-nav-processing.yml` | `30 6 * * *` | Fetch missing daily NAV data and dispatch raw ingestion to the datalake |
| `extract-scheme-metadata.yml` | `30 0 * * 6` | Extract a dated raw AMFI scheme metadata snapshot to R2 |
| `extract-amc-members.yml` | `30 1 * * 6` | Extract an immutable raw AMFI AMC-member snapshot and dispatch datalake ingestion |
| `fetch-aum-data.yml` | `30 0 10 1,4,7,10 *` | Fetch and publish a dated scheme-wise AUM snapshot to R2 |
| `fetch-ter-data.yml` | `30 7 5,20 * *` | Validate and publish monthly AMFI TER snapshots to R2 |

Schedule conversions:

- 06:30 UTC is 12:00 IST on the same day for daily NAV processing.
- Saturday 00:30 UTC is Saturday 06:00 IST.
- Saturday 01:30 UTC is Saturday 07:00 IST.
- 00:30 UTC on the 10th of a quarter month is 06:00 IST.
- 07:30 UTC on the 5th and 20th is 13:00 IST on the same day.

India does not observe daylight saving time.

## Required GitHub Secrets

Configure these under **Settings > Secrets and variables > Actions**:

| Secret | Purpose |
| --- | --- |
| `R2_ACCESS_KEY_ID` | Cloudflare R2 access key |
| `R2_SECRET_ACCESS_KEY` | Cloudflare R2 secret key |
| `R2_ACCOUNT_ID` | Cloudflare account ID used by DuckDB's R2 secret |
| `DATALAKE_DISPATCH_TOKEN` | GitHub token allowed to dispatch the datalake repository |

The token must be able to read and write the `financial-data-store` bucket.
Never commit these values. Local credentials belong in `.env`, which is
gitignored.

## Optional GitHub Variables

The workflows use defaults when these repository variables are absent:

| Variable | Default | Used by |
| --- | --- | --- |
| `AMFI_NAV_TIMEOUT` | `30` | Daily NAV |
| `AMFI_SCHEME_TIMEOUT` | `30` | Daily NAV environment and metadata extraction |
| `AMFI_MEMBERS_TIMEOUT` | `30` | AMC member extraction |
| `AMFI_TER_TIMEOUT` | `60` | TER extraction |
| `MAX_RETRIES` | `3` | Daily NAV and TER extraction |
| `RETRY_DELAY` | `5` | Daily NAV and TER extraction |
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
|-- amc_members/
|   `-- amc_members_<YYYYMMDDTHHMMSSZ>.parquet
|-- aum/
|   `-- aum_schemewise_<YYYYMMDD>.parquet
`-- ter/
    `-- ter_<YYYYMM>_snapshot_<YYYYMMDD>.parquet
```

The dated raw NAV and metadata objects are the operational inputs. Canonical
enriched NAV tables are published by the datalake rather than by this
repository's scheduled workflows.

## Workflow Details

### Daily NAV Processing

Runtime: Python 3.9 with dependencies installed by pip.

```bash
python -m scripts.fetch_daily_nav
```

The fetcher reads its latest checkpoint from canonical raw object names under
`mutual_funds/raw/nav_daily_*.parquet`, not from clean NAV. An empty prefix
requires `--bootstrap-date YYYYMMDD` when run manually. It fetches subsequent
weekdays through yesterday from AMFI and writes dated raw Parquet files.

The datalake builds its canonical NAV tables directly from the raw objects.

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

### Weekly AMC Member Extraction

Runtime: Python 3.12 with dependencies installed by uv.

```bash
uv run python -m scripts.extract_amc_members
```

This fetches a complete AMFI member snapshot, writes one row per listed member
to a uniquely timestamped R2 Parquet object, and dispatches raw ingestion to
the datalake. It does not create a clean member table.

### Quarterly Scheme-wise AUM Extraction

Runtime: Python 3.12 with dependencies installed by uv.

```bash
uv run python -m scripts.fetch_aum_data
```

This fetches the requested scheme-wise AUM period, writes a dated Parquet
snapshot to R2, and dispatches the successful upload to the datalake.

### Monthly TER Extraction

Runtime: Python 3.12 with dependencies installed by uv.

```bash
uv run python -m scripts.fetch_ter_data --scheduled
```

Scheduled runs on days 1–10 fetch the previous calendar month; other scheduled
runs fetch the current month. Manual dispatch without arguments fetches the
current month. Reviewed backfills use `--start-month YYYY-MM --end-month
YYYY-MM` and start at April 2020. The extractor rejects non-XLSX responses,
unknown or partial source headers, invalid dates, out-of-month dates, and
nonnumeric TER fields before writing. It writes Zstandard Parquet only and
does not dispatch to the datalake in this phase. Workflow runs are serialized
to prevent overlapping writes to the same daily snapshot.

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
| No useful failure artifact | Read the GitHub Actions step log; not every script uses file logging |

## Operational Gaps

- Metadata extraction, cleaning, publication, and master-data maintenance are
  not yet one automated workflow.
- TER historical backfills, modelling, and datalake ingestion are intentionally
  manual/deferred during the first extraction phase.
- NAV validation is manual and does not gate the daily workflow.
- Workflows print success/failure messages but do not send external alerts.
- NAV uses pip/Python 3.9 while metadata and AUM use uv/Python 3.12.
  Standardizing runtimes would reduce maintenance differences.
