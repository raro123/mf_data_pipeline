# Repository Guidance

## Project Scope

- collects Indian mutual-fund source data, primarily from AMFI.
- Scheduled workflows publish immutable raw snapshots to Cloudflare R2.
- The `datalake` builds and validates canonical analysis tables.
- `nifty_index_ingestion` repository owns NIFTY benchmark data.

## Project References

Use these documents for guidance:

- `README.md` for setup and project entry points.
- `docs/ARCHITECTURE.md` for design and ownership boundaries.
- `docs/PIPELINE_ONBOARDING.md` for scripts and execution order.
- `docs/GITHUB_ACTIONS_SETUP.md` for workflow configuration.

## Configuration and Storage
- Use `config.settings` for shared configuration.
- Use `R2` helpers and existing naming patterns for all R2 paths.
