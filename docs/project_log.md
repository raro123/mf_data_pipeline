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

---

## Session Log

<!-- Sessions in reverse chronological order (newest first) -->

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
