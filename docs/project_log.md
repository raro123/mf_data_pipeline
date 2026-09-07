# Project Log: Mutual Fund Data Pipeline

**Goal:** Collect reliable Indian mutual-fund source data from AMFI and publish
reproducible raw inputs for downstream analysis. Success means scheduled source
snapshots are complete, immutable, and traceable through the datalake.

---

## ⏳ Pending Decisions

| # | Decision | Raised | Session | Status |
|---|----------|--------|---------|--------|
| 1 | Profile the raw AMC member JSON to select stable fields and define the clean-table shape | 2026-08-24 | S1 | 🟡 Open |
| 2 | Decide whether the upstream member extractor needs an additional count or schema-drift guardrail beyond complete-detail validation | 2026-08-24 | S1 | 🟡 Open |
| 3 | Define the canonical TER modelling and promotion flow after raw ingestion | 2026-08-24 | S2 | 🟡 Open |

---

## Session Log

<!-- Sessions in reverse chronological order (newest first) -->

---

### 📅 Date: 2026-09-07 | Session: S4 — Simplify repository documentation

**What was done:**
We shortened the README and created separate architecture and pipeline onboarding guides.
We removed obsolete and duplicated documents.
We reduced `AGENTS.md` to essential repository guidance and linked `CLAUDE.md` to it.
GitHub Actions setup now forms a concise section in the pipeline onboarding guide.

**Why:**
The previous documentation repeated implementation details and contained completed migration notes.
The new structure gives each document one clear purpose and reduces future maintenance.

**How:**
We checked scripts, workflows, configuration, and document references against the current repository.
We kept architecture boundaries separate from pipeline commands and workflow setup.

**Decisions made:**
- Use `AGENTS.md` as the only agent instruction source.
- Keep architecture details in `docs/ARCHITECTURE.md`.
- Keep scripts, execution order, schedules, and workflow setup in `docs/PIPELINE_ONBOARDING.md`.
- Remove completed migration notes and duplicated workflow documentation.

**Pending decisions:**
- None. Future storage cleanup and `uv` migration remain separate work items.

**Verification:**
Document references and formatting checks passed.

---

### 📅 Date: 2026-09-05 | Session: S3 — Publish values-only market-cap snapshots

**What was done:**
Added the Phase 1 transformer for archived AMFI market-cap workbooks. It verifies
canonical workbook/manifest pairs, preserves source rows without business
cleaning, and publishes each source revision as immutable Zstandard Parquet. The
monthly workflow now runs archive, transform, and datalake dispatch sequentially.

**Why:**
The source archive preserves exact evidence, but downstream ingestion needs a
stable values-only representation. Keeping transformation separate from archive
capture preserves the source boundary while allowing every revision to remain
auditable.

**How:**
The transformer validates manifest identity, workbook size and SHA-256, the exact
11-column worksheet header, formula caches, and the 28-column Parquet contract.
Each revision is isolated, uploaded with provenance metadata, and read back for
schema, row-count, and hash verification. A failed archive or transform prevents
the workflow from dispatching `mf-market-cap-raw-ingestion-completed`.

**Decisions made:**
- Preserve strings, partial rows, note-like rows, duplicates, and source row
  numbers without applying business rules.
- Publish one immutable raw Parquet object per complete source revision.
- Dispatch datalake ingestion only after the complete archive and transform
  sequence succeeds.
- Keep revision selection, business validation, and clean modelling outside the
  producer.

**Pending decisions:**
- None owned by this repository; canonical market-cap modelling remains a
  datalake follow-up.

**Verification:**
The full producer suite passed with 85 tests and 5 pre-existing pytest
return-value warnings. Ruff, workflow YAML parsing, and `git diff --check` also
passed. Commit: `acb871f`. On 2026-09-07, the standalone production transformer
processed all 18 archived revisions, uploaded 18 raw Parquet objects containing
91,691 rows, and reported no failures or skips. Because this was a standalone
transformer run, it did not dispatch datalake ingestion.

---

### 📅 Date: 2026-08-24 | Session: S2 — Dispatch TER snapshots to the datalake

**What was done:**
The scheduled TER workflow now dispatches every successful monthly AMFI expense-ratio snapshot to the datalake for raw ingestion. Same-day no-write reruns remain safe because ingestion is idempotent by source filename. The workflow documentation, setup guide, execution order, script inventory, README, and work queue were updated to reflect the new boundary.

**Why:**
Raw TER snapshots are now available to downstream processing automatically, while immutable source files preserve reproducibility. Keeping canonical TER modelling deferred avoids coupling extraction to an unfinished analytical design.

**How:**
After a successful TER extraction, GitHub Actions sends a repository dispatch event with source repository, workflow, run, attempt, and commit metadata. The datalake owns raw ingestion, and the repository documentation records that canonical modelling remains a follow-up task.

**Decisions made:**
- Automate raw TER snapshot ingestion through the datalake dispatch.
- Keep TER source workbooks unretained and snapshots immutable.
- Defer canonical TER modelling to a later phase.

**Pending decisions:**
- Define the canonical TER modelling and promotion flow → Decision #3.

---

### 📅 Date: 2026-08-24 | Session: S1 — Add upstream raw AMC member ingestion

**What was done:**
The pipeline now uses AMFI listing, detail, and social APIs to produce one raw
JSON row per member and write an immutable weekly snapshot to R2. Requests for
member details are sequential, and validation is all-or-nothing: detail is
mandatory while social data is optional. The workflow dispatches a successful
source event to the datalake.

**Why:**
Keeping complete member-grain source records creates a reproducible history for
future AMC and fund analysis while avoiding premature assumptions about clean
fields. Immutable snapshots preserve changes even when AMFI alters its payloads.

**How:**
The extractor stores listing, detail, and social JSON alongside each member and
does not use ETags or content hashes. The initial source run `32706261117` failed
because `setup-uv` had already created `.venv` and the redundant `uv venv`
exited with code 2; `d7fd522` removed that duplicate step. Source run
`32706345867` then wrote 57 rows and dispatched successful datalake run
`32706425403`.

**Decisions made:**
- Use complete weekly snapshots for the first release.
- Fetch member details sequentially and require complete detail records before publishing.
- Treat social records as optional enrichment.
- Keep raw JSON upstream and defer clean-field selection until profiling.

**Pending decisions:**
- Profile raw JSON for clean fields and decide whether an additional count/schema-drift guardrail is needed → Decisions #1–#2.
