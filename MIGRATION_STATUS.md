# Datalake Migration Status

_Status checked: June 13, 2026_

## Current State

The raw NAV extraction dependency on the legacy clean NAV file has been
removed and deployed to `main` in commit `147646a`.

`scripts.fetch_daily_nav` now determines its checkpoint from canonical raw R2
objects:

```text
r2://financial-data-store/mutual_funds/raw/nav_daily_<YYYYMMDD>.parquet
```

It no longer reads:

```text
r2://financial-data-store/mutual_funds/clean/nav_daily_growth_plan.parquet
```

The desired dependency direction is therefore implemented:

```text
AMFI
  -> raw NAV Parquet in R2
  -> mf.raw_nav_daily
  -> mf.nav_daily
  -> mf.nav_daily_open_growth
```

## Live Data Check

As of June 13, 2026:

| Dataset | Latest date or snapshot |
| --- | --- |
| R2 raw daily NAV | June 12, 2026 |
| `mf.raw_nav_daily` | June 12, 2026 |
| `mf.nav_daily` | June 12, 2026 |
| `mf.nav_daily_open_growth` | June 12, 2026 |
| R2 raw scheme metadata | `scheme_metadata_20260613.parquet` |
| `mf.scheme` source snapshot | `scheme_metadata_20260613.parquet` |

The legacy clean NAV also reaches June 12, but it remains a separate
compatibility output rather than a datalake input.

## What Is Complete

- Raw NAV checkpoint discovery uses raw object names, not clean data.
- An empty prefix requires an explicit `--bootstrap-date`.
- The scheduled fetch cutoff is yesterday, avoiding premature requests for
  the current day's NAV.
- A failed date stops the fetch sequence and returns a nonzero process exit
  code.
- Unit tests cover filename parsing, checkpoint selection, weekends,
  bootstrapping, no-op runs, and failed fetches.
- The deployed command successfully reads the live R2 checkpoint without
  contacting AMFI when no dates are missing.

## Rollout Verification Pending

The new checkpoint path has not yet fetched a new weekday object in the
scheduled workflow. June 13 is Saturday and June 12 was already present before
deployment.

The first expected production exercise is the Tuesday, June 16, 2026 morning
run, which should fetch Monday, June 15 NAV. Verify:

1. `nav_daily_20260615.parquet` is created once in the raw R2 prefix.
2. The fetch step reports June 12 as its prior checkpoint.
3. The datalake daily job ingests the new file.
4. `mf.raw_nav_daily`, `mf.nav_daily`, and `mf.nav_daily_open_growth` reach
   June 15.
5. No earlier dates are refetched and no weekday gap is introduced.

## Temporary Legacy Step

The GitHub Actions workflow still runs `scripts.daily_nav_clean` after raw
extraction. It rebuilds:

```text
r2://financial-data-store/mutual_funds/clean/nav_daily_growth_plan.parquet
```

This step is no longer required for raw extraction or for datalake ingestion.
It is temporarily retained for rollout observation and rollback safety.

It still depends on the stale legacy metadata object last modified on
September 28, 2025:

```text
r2://financial-data-store/mutual_funds/clean/scheme_metadata.parquet
```

## Prioritized Next Steps

### 1. Observe the first weekday fetch

Do not remove the legacy clean step until the production checks above pass.

### 2. Confirm consumers use datalake outputs

Search notebooks, dashboards, exports, and downstream repositories for reads
of:

- `mutual_funds/clean/nav_daily_growth_plan.parquet`
- `mutual_funds/clean/scheme_metadata.parquet`

Canonical replacements are:

- `mf.nav_daily`
- `mf.nav_daily_open_growth`
- `mf.scheme`
- `mf.scheme_open_growth`

### 3. Remove legacy cleaning from the daily workflow

After verification, remove `python -m scripts.daily_nav_clean` from
`.github/workflows/daily-nav-processing.yml`. Keep the script deprecated for a
short rollback window before deleting it.

### 4. Move NAV validation into the datalake

Replace validation of `clean/nav_daily_growth_plan.parquet` with checks on
`mf.raw_nav_daily`, `mf.nav_daily`, and expected file/date freshness.

### 5. Align cross-repository ingestion

- Move metadata datalake ingestion later than extraction or trigger it after a
  successful upload.
- Add a shared datalake catalog-write concurrency group.
- Add freshness checks and alerts for raw R2 objects and `mf` tables.

### 6. Retire legacy objects

Only after consumer confirmation and an agreed retention period, archive or
remove the two legacy clean objects. Raw R2 snapshots and datalake tables stay
authoritative.
