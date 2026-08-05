# GitHub Actions Workflows

## `daily-nav-processing.yml`

- Schedule: daily at 04:00 UTC / 09:30 IST
- Runtime: Python 3.9 and pip
- Commands:

```bash
python -m scripts.fetch_daily_nav
```

The job gap-fills raw NAV in R2 using dated raw object names as its checkpoint.
The datalake consumes these raw objects and publishes the canonical NAV tables;
the former repository-side clean step is no longer scheduled.

## `extract-scheme-metadata.yml`

- Schedule: Saturday at 01:00 UTC / 06:30 IST
- Runtime: Python 3.12 and uv
- Command:

```bash
uv run python -m scripts.extract_scheme_metadata
```

The job writes a dated raw AMFI metadata snapshot under the R2
`mutual_funds/metadata/` prefix. It does not clean or publish canonical scheme
metadata.

## `fetch-aum-data.yml`

- Schedule: the 10th of Jan/Apr/Jul/Oct at 02:00 UTC / 07:30 IST
- Runtime: Python 3.12 and uv
- Command:

```bash
uv run python -m scripts.fetch_aum_data
```

The job writes a dated scheme-wise AUM Parquet snapshot under the R2
`mutual_funds/aum/` prefix and dispatches successful uploads to the datalake.

## Shared Configuration

All workflows require these GitHub Actions secrets:

- `R2_ACCESS_KEY_ID`
- `R2_SECRET_ACCESS_KEY`
- `R2_ACCOUNT_ID`

Optional API, retry, and logging settings are supplied through repository
variables. See `GITHUB_ACTIONS_SETUP.md` in the repository root for the full
configuration and troubleshooting guide.

## Operations

Each workflow supports `workflow_dispatch` for manual runs. On failure, inspect
the failed step in the Actions log first. Workflows upload `logs/` as a
seven-day artifact where configured, but scripts that print directly to stdout
may not create a useful log file.

Current automation does not run historical backfills, local metadata cleaning,
scheme master-data rebuilding, AUM ingestion, or NAV validation.
