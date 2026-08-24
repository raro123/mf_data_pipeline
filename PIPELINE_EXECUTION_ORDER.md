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
```

Flow:

```text
R2 raw/nav_daily_<YYYYMMDD>.parquet object names
  -> determine latest raw checkpoint
AMFI historical endpoint
  -> R2 raw/nav_daily_<YYYYMMDD>.parquet
Datalake ingestion
  -> mf.raw_nav_daily
  -> mf.nav_daily
  -> mf.nav_daily_open_growth
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
- A failed date stops the fetch loop and returns a nonzero process exit code,
  preventing a later raw object from advancing the checkpoint past the gap.

### One-time repair for NAVs reported late

Use this only when canonical daily snapshots exist for the requested weekdays,
but AMFI now contains scheme/date observations that those snapshots omitted.
Always review the dry run before writing:

```bash
python -m scripts.repair_missing_nav \
  --start YYYYMMDD \
  --end YYYYMMDD

python -m scripts.repair_missing_nav \
  --start YYYYMMDD \
  --end YYYYMMDD \
  --write
```

Flow:

```text
Current AMFI history + existing R2 raw NAV keys
  -> missing weekday (scheme_code, date) keys only
  -> R2 raw/nav_repair_missing_<start>_<end>_<UTC-timestamp>.parquet
Manual datalake NAV workflow
  -> mf.raw_nav_daily
  -> rebuilt mf.nav_daily and mf.nav_daily_open_growth
```

The command rejects missing canonical daily snapshots, invalid or out-of-range
dates, and duplicate scheme/date keys. Non-numeric or non-positive NAV rows are
excluded, matching the canonical datalake filter. It reports but does not write
NAV restatements. Repair objects are immutable and their filenames are ignored
by daily watermark discovery.

After a successful write, ingest the new object with:

```bash
gh workflow run mf-daily.yml --repo raro123/datalake
```

Do not schedule this utility or use it for the current publication, daily gap
recovery, full-history initialization, or NAV restatements.

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

## 4. AMC Members

```bash
python -m scripts.extract_amc_members
```

The extractor fetches the AMFI member list, every listed member's detail
record, and the social-media listing. It writes one complete, timestamped raw
snapshot to:

```text
r2://financial-data-store/mutual_funds/amc_members/amc_members_<YYYYMMDDTHHMMSSZ>.parquet
```

Successful scheduled runs dispatch the datalake workflow, which appends unseen
snapshot files to `mf.raw_amc_member_snapshot`. The raw table intentionally
retains source records as JSON; clean-table design is deferred until the raw
contents have been profiled.

## 5. Scheme-wise AUM

Run on demand:

```bash
python -m scripts.fetch_aum_data
python -m scripts.fetch_aum_data --years 3
python -m scripts.fetch_aum_data --fy 1 --period 1
```

The job writes a dated local Parquet file under
`data/processed/aum_schemewise/` and uploads the same data under the R2
`mutual_funds/aum/` prefix.

## 5. Zerodha Instruments

```bash
python -m scripts.ingest_zerodha_mf
```

This is a separate optional workflow. It requires Kite Connect credentials and
uploads a dated CSV instrument dump to R2. It is not scheduled by this
repository.

## 6. Monthly TER Extraction

Run one current-month snapshot manually:

```bash
python -m scripts.fetch_ter_data
```

For a scheduled month selection or a reviewed historical backfill:

```bash
python -m scripts.fetch_ter_data --scheduled
python -m scripts.fetch_ter_data \
  --start-month 2020-04 \
  --end-month YYYY-MM
```

The extractor requests AMFI's all-fund, all-category/type XLSX export. It
recognizes the pre-April-2026 and April-2026-onward header sets, validates the
rows, and writes only validated Zstandard Parquet:

```text
r2://financial-data-store/mutual_funds/ter/ter_<YYYYMM>_snapshot_<YYYYMMDD>.parquet
```

The source workbook is not retained. A same-day rerun skips its exact object;
backfills skip months with any canonical TER snapshot, so interrupted ranges
can resume. A failed month stops the range before later months are fetched or
written. This phase does not dispatch datalake ingestion.

## GitHub Actions Order

The active schedules are independent:

1. Weekly metadata extraction: Saturday at 00:30 UTC / 06:00 IST.
2. Weekly AMC member extraction: Saturday at 01:30 UTC / 07:00 IST.
3. Daily NAV processing: every day at 06:30 UTC / 12:00 IST.
4. Quarterly AUM extraction: the 10th of Jan/Apr/Jul/Oct at 00:30 UTC / 06:00 IST.
5. TER extraction: the 5th and 20th at 07:30 UTC / 13:00 IST.

NIFTY benchmark ingestion is owned by the separate `nifty_index_ingestion`
repository. It publishes to the datalake, which feeds the active tearsheet
benchmark table.

There is no workflow that orchestrates a full historical rebuild, local
metadata cleaning, master-data building, TER historical backfills, AUM
fetching, or NAV validation. TER snapshots are not dispatched to the datalake
in this phase.

## Legacy Local Artifacts

The local files `data/processed/nav_combined/raw_nav_table.parquet` and
`data/processed/analytical/nav_daily_data.parquet` were generated by scripts
that are no longer present. Treat them as snapshots, not reproducible current
pipeline outputs.

## Migration Follow-up

Raw NAV extraction is independent of legacy clean output, and the legacy clean
workflow step has been retired after end-to-end observation through
`mf.nav_daily`. Follow the remaining R2 object-retirement guidance in
`MIGRATION_STATUS.md`.
