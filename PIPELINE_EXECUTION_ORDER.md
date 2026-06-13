# Pipeline Execution Order

This document describes the current executable pipeline. It separates the R2
production flow from local maintenance tasks because they are not fully joined
yet.

## Prerequisites

1. Install dependencies from `requirements.txt`.
2. Set `R2_ACCESS_KEY_ID`, `R2_SECRET_ACCESS_KEY`, and `R2_ACCOUNT_ID` for any
   R2-backed command.
3. Initialize local directories:

```bash
python -c "from config.settings import initialize_project; initialize_project()"
```

## 1. Historical NAV Backfill

Run this for initial setup or an explicit historical repair.

```bash
python -m scripts.fetch_historical_nav \
  --start 20060101 \
  --end YYYYMMDD

python -m scripts.transform_historical_nav
```

Flow:

```text
AMFI historical endpoint
  -> data/raw/nav_historical/amfi_raw_nav_<start>_<end>.csv
  -> R2 mutual_funds/raw/nav_historical.parquet
```

`fetch_historical_nav` skips existing chunks unless `--force` is supplied.
`transform_historical_nav` currently concatenates all local CSV files with
Pandas, so memory usage grows with the backfill size.

## 2. Daily NAV Production Flow

Run in this order:

```bash
python -m scripts.fetch_daily_nav
python -m scripts.daily_nav_clean
```

Flow:

```text
R2 raw/nav_daily_<YYYYMMDD>.parquet object names
  -> determine latest raw checkpoint
AMFI historical endpoint
  -> R2 raw/nav_daily_<YYYYMMDD>.parquet
R2 raw Parquet + R2 clean/scheme_metadata.parquet
  -> R2 clean/nav_daily_growth_plan.parquet (temporary legacy output)
```

Notes:

- Weekends are skipped; exchange holidays are not precomputed.
- The default cutoff is yesterday, matching the morning schedule that collects
  the prior day's completed AMFI NAV publication.
- Raw checkpoint discovery only accepts canonical
  `nav_daily_<YYYYMMDD>.parquet` names and ignores unrelated objects.
- If no raw daily objects exist, pass `--bootstrap-date YYYYMMDD`. The
  bootstrap date is inclusive.
- `fetch_daily_nav --date YYYYMMDD` fetches a specific date.
- `daily_nav_clean --date YYYYMMDD` still writes to the canonical clean output
  path. Use it only when deliberately replacing that output with the selected
  raw date; the scheduled full rebuild does not pass `--date`.
- The clean step performs an inner join to metadata and keeps rows where
  `is_growth_plan = TRUE`.
- The current code expects a canonical R2
  `mutual_funds/clean/scheme_metadata.parquet` file, but this repository does
  not currently build that file from the scheduled metadata extraction job.
  This dependency belongs only to the temporarily retained legacy clean step,
  not to raw NAV extraction.
- A failed date stops the fetch loop and returns a nonzero process exit code,
  preventing a later raw object from advancing the checkpoint past the gap.

Optional validation after cleaning:

```bash
python -m scripts.generate_nav_validation_report
```

The report compares daily scheme counts with a rolling baseline and writes a
CSV under `data/reports/`. It is not currently scheduled.

## 3. Scheme Metadata

### R2 extraction

```bash
python -m scripts.extract_scheme_metadata
```

This downloads AMFI scheme metadata and writes a dated raw Parquet file to:

```text
r2://financial-data-store/mutual_funds/metadata/scheme_metadata_<YYYYMMDD>.parquet
```

### Local cleaning and master data

Given a timestamped AMFI CSV under `data/raw/scheme_metadata/`:

```bash
python -m scripts.clean_scheme_metadata
python -m scripts.build_scheme_masterdata
```

The cleaner creates:

- `data/processed/scheme_metadata/amfi_scheme_metadata.parquet`
- `data/processed/scheme_metadata/amfi_scheme_metadata.csv`

The master-data builder creates:

- `data/processed/scheme_metadata/scheme_masterdata.parquet`
- `data/processed/scheme_metadata/scheme_masterdata.csv`

The master data preserves missing schemes as inactive and tracks
`first_seen_date`, `last_seen_date`, and `attribute_last_updated`.

The R2 extraction and local cleaning flows are currently separate. A future
pipeline change should choose one canonical handoff and schedule cleaning plus
master-data rebuilding.

## 4. Scheme-wise AUM

Run on demand:

```bash
python -m scripts.fetch_aum_data
python -m scripts.fetch_aum_data --years 3
python -m scripts.fetch_aum_data --fy 1 --period 1
```

The job writes a dated local Parquet file under
`data/processed/aum_schemewise/` and uploads the same data under the R2
`mutual_funds/aum/` prefix.

## 5. Benchmark Data

```bash
python -m scripts.load_benchmark_data
```

This copies an upstream NIFTY Delta table at
`r2://financial-data-store/bronze/nseindex/daily_price_nifty_indices` to
`r2://financial-data-store/mutual_funds/clean/mf_benchmark_nifty.parquet`.

## 6. Zerodha Instruments

```bash
python -m scripts.ingest_zerodha_mf
```

This is a separate optional workflow. It requires Kite Connect credentials and
uploads a dated CSV instrument dump to R2. It is not scheduled by this
repository.

## GitHub Actions Order

The active schedules are independent:

1. Weekly metadata extraction: Saturday at 01:00 UTC / 06:30 IST.
2. Daily NAV processing: every day at 04:00 UTC / 09:30 IST.
3. Daily benchmark loading: every day at 18:30 UTC / 00:00 IST next day.

There is no workflow that orchestrates a full historical rebuild, local
metadata cleaning, master-data building, AUM fetching, or NAV validation.

## Legacy Local Artifacts

The local files `data/processed/nav_combined/raw_nav_table.parquet` and
`data/processed/analytical/nav_daily_data.parquet` were generated by scripts
that are no longer present. Treat them as snapshots, not reproducible current
pipeline outputs.
