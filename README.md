# Mutual Fund Data Pipeline

This repository collects Indian mutual-fund source data from AMFI.
It also supports optional Zerodha mutual-fund instrument data.

Scheduled workflows publish immutable raw snapshots to Cloudflare R2.
The separate datalake repository builds canonical analysis tables from those snapshots.

## Data Products

| Product | Source | Destination |
| --- | --- | --- |
| Historical NAV | AMFI NAV history | Local CSV and R2 raw Parquet |
| Daily NAV | AMFI NAV history | R2 daily raw Parquet |
| NAV repairs | AMFI NAV history | R2 missing-only repair Parquet |
| Scheme metadata | AMFI scheme download | R2 raw Parquet |
| Local scheme master data | Local metadata snapshots | Local CSV and Parquet |
| AMC members | AMFI member APIs | R2 raw Parquet |
| Scheme AUM | AMFI AUM API | Local and R2 Parquet |
| TER | AMFI monthly workbook | R2 raw Parquet |
| Market-cap sources | AMFI workbooks | R2 source files and manifests |
| Market-cap rows | Archived AMFI workbooks | R2 values-only Parquet |
| Zerodha instruments | Kite Connect | R2 CSV |

Canonical NAV tables belong to the datalake.
NIFTY benchmark ingestion belongs to the separate `nifty_index_ingestion` repository.

## Architecture

The repository has two distinct paths.

- Scheduled production jobs publish raw data to R2.
- Manual maintenance jobs use local files under `data/`.

The paths are not fully connected.
For example, scheduled metadata extraction does not run local metadata cleaning.

See [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) for components, storage boundaries, and datalake handoffs.

## Current Setup

Install `uv`, then synchronize the Python 3.12 project environment.

```bash
uv sync
uv run python -c "from config.settings import initialize_project; initialize_project()"
```

Create `.env` for commands that access R2.

```text
R2_ACCESS_KEY_ID=...
R2_SECRET_ACCESS_KEY=...
R2_ACCOUNT_ID=...
```

Never commit credential values.

## Common Commands

Run all modules from the repository root.

```bash
uv run python -m scripts.fetch_daily_nav
uv run python -m scripts.extract_scheme_metadata
uv run python -m scripts.extract_amc_members
uv run python -m scripts.fetch_aum_data
uv run python -m scripts.fetch_ter_data --scheduled
uv run python -m scripts.fetch_amfi_mcap_sources
uv run python -m scripts.transform_amfi_mcap_sources
```

See [docs/PIPELINE_ONBOARDING.md](docs/PIPELINE_ONBOARDING.md) before running backfills, repairs, or maintenance flows.

## Tests

The repository contains `unittest` and pytest-style tests.

```bash
uv run pytest
```

The environment diagnostic is separate from unit tests.
It can contact R2 and AMFI.

```bash
uv run python -m scripts.test_github_actions_setup
```

## Repository Layout

```text
config/                 Central configuration and R2 setup
scripts/                Executable ingestion and transformation modules
utils/                  Shared NAV and logging helpers
tests/                  Focused unit tests
.github/workflows/      Scheduled production workflows
data/                   Gitignored local data artifacts
docs/                   Project history and planning notes
notebooks/              Exploratory analysis
```

## Current Boundaries

- Metadata extraction and local metadata maintenance remain separate.
- Historical NAV transformation loads all local CSV files into Pandas memory.
- The repository does not build canonical NAV analysis tables.
- The datalake owns canonical validation and modelling.
- Some local Parquet files are snapshots from retired scripts.

## Documentation

- [Architecture](docs/ARCHITECTURE.md) describes system design and repository boundaries.
- [Pipeline onboarding](docs/PIPELINE_ONBOARDING.md) lists scripts, execution order, workflow schedules, and setup.
- [AGENTS.md](AGENTS.md) defines repository instructions for coding agents.

`docs/project_log.md` records completed work.
`docs/work_queue.md` records planning items.
Neither file defines current system behavior.
