# Datalake Migration Status

_Status checked: August 31, 2026_

## Current State

The raw NAV extraction dependency on the legacy clean NAV file was removed and
deployed to `main` in commit `147646a`. The subsequent production observation
period completed successfully, so the scheduled legacy clean step has now been
retired.

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

## Operational Verification

The new raw-to-datalake flow has been observed in production without reported
consumer or freshness issues. The canonical outputs are:

- `mf.raw_nav_daily`
- `mf.nav_daily`
- `mf.nav_daily_open_growth`

The legacy clean NAV and metadata objects remain only as retained R2 artifacts;
they are no longer pipeline inputs or scheduled outputs.

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
- The legacy daily clean step was removed from the GitHub Actions workflow.
- The legacy NAV validation report was retired because it read a frozen clean
  output.
- Repository runbooks now describe the raw-to-datalake flow as authoritative.
- The in-repo rollback utility `scripts/daily_nav_clean.py` has been removed.
  It previously rebuilt the legacy clean NAV object and is no longer available.

## Retained R2 Artifacts

The following objects may still exist in R2. They are not pipeline inputs,
scheduled outputs, or rebuildable from this repository:

```text
r2://financial-data-store/mutual_funds/clean/nav_daily_growth_plan.parquet
r2://financial-data-store/mutual_funds/clean/scheme_metadata.parquet
```

## Remaining Actions

### 1. Confirm consumers use datalake outputs

Search notebooks, dashboards, exports, and downstream repositories for reads
of:

- `mutual_funds/clean/nav_daily_growth_plan.parquet`
- `mutual_funds/clean/scheme_metadata.parquet`

Canonical replacements are:

- `mf.nav_daily`
- `mf.nav_daily_open_growth`
- `mf.scheme`
- `mf.scheme_open_growth`

### 2. Align cross-repository ingestion

- Move metadata datalake ingestion later than extraction or trigger it after a
  successful upload.
- Add a shared datalake catalog-write concurrency group.
- Add freshness checks and alerts for raw R2 objects and `mf` tables.

### 3. Retire legacy objects

After consumer confirmation and the agreed retention period, archive or remove
the two legacy clean objects. Raw R2 snapshots and datalake tables stay
authoritative. Object deletion remains a separate explicit operational action.
