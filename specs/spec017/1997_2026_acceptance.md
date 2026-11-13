# FOMC 1997–2026 backfill acceptance

Accepted locally 2026-11-13 on the existing PR #18 branch. All 21 bounded
Dagster batches passed the [contract](1997_2026_contract.md). The
[sanitized evidence](evidence/1997_2026_dagster_backfill.json) records every run,
operation result, source-year reconciliation, restore checkpoint and Slack tick.
Fixture-proven parser and asset changes support the reviewed legacy print layout,
declared Windows-1252 encoding and source-declared PNG/GIF/JPEG charts. All 194
pre-fix selected extraction results/IDs recompute exactly. Schema, dependencies,
capture-time semantics and finite limits are unchanged.

## Acceptance gates

| Gate | Verified result |
| --- | --- |
| Listed-reference coverage | All 390 unique selected primary URLs for 1997–2026 are archived; every final per-year selection reconciles. No unacquired inventory references remain. |
| Originals and history | All 1,081 database/filesystem capture readbacks match. All 172 baseline receipts, capture clocks and append-only rows are preserved. |
| Text | 522 selected representations, 3,200 PDF pages, 4,018 units and 25,537,392 full-text characters reconcile; independent PDF page counts and citations match. |
| Chunks | 9,790 exact source spans, including 8,270 eligible and 1,520 review-flagged chunks; all selected unit characters are covered. |
| Health and publication | Healthy with no issues; coherent inventory/text/chunk pointers and all 29 tables match the last restored checkpoint. |
| Backup/restore | All 32 full-schema and 32 canonical-filesystem checkpoints were actually restored into disposable targets and compared. All 64 retained archive hashes and original restore receipts were reconfirmed. |
| Offline replay | All 21 batch checkpoints replayed original captures/receipt bytes, selected catalogs/chunk rows and latest operation into empty targets, with HTTP and text/chunk transforms disabled. |
| Slack | All 21 requested Dagster runs are SUCCESS, each with a matching SUCCESS sensor tick and acknowledged-delivery log. |
| Final reconciliation | Stable source advisory lock, HTTP disabled, prior immutability, retained archive receipts, automation state and scratch cleanup checked. |

## Execution and annual coverage

The initial plan contained 391 reference selections across 15 two-year batches.
It was refined to 21 batches, using single years for 2009–2020 to leave room
for source-declared SEP addenda/images within unchanged request/document limits.
Final per-year selections total 391, representing
390 distinct URLs. Shared references and HTML/PDF
variants do not imply extra meetings. Current inventory has 33
pages, 371 meeting claims, 470 references
and 561 source associations.

| Year | Listed primary references | Archived | Latest listed meeting date |
| --- | --- | --- | --- |
| 1997 | 8 | 8 | 1997-12-16 |
| 1998 | 8 | 8 | 1998-12-22 |
| 1999 | 8 | 8 | 1999-12-21 |
| 2000 | 8 | 8 | 2000-12-19 |
| 2001 | 9 | 9 | 2001-12-11 |
| 2002 | 8 | 8 | 2002-12-10 |
| 2003 | 8 | 8 | 2003-12-09 |
| 2004 | 8 | 8 | 2004-12-14 |
| 2005 | 8 | 8 | 2005-12-13 |
| 2006 | 8 | 8 | 2006-12-12 |
| 2007 | 10 | 10 | 2007-12-11 |
| 2008 | 16 | 16 | 2008-12-16 |
| 2009 | 16 | 16 | 2009-12-16 |
| 2010 | 16 | 16 | 2010-12-14 |
| 2011 | 16 | 16 | 2011-12-13 |
| 2012 | 16 | 16 | 2012-12-12 |
| 2013 | 16 | 16 | 2013-12-18 |
| 2014 | 16 | 16 | 2014-12-17 |
| 2015 | 16 | 16 | 2015-12-16 |
| 2016 | 16 | 16 | 2016-12-14 |
| 2017 | 16 | 16 | 2017-12-13 |
| 2018 | 16 | 16 | 2018-12-19 |
| 2019 | 16 | 16 | 2019-12-11 |
| 2020 | 16 | 16 | 2020-12-16 |
| 2021 | 16 | 16 | 2021-12-15 |
| 2022 | 16 | 16 | 2022-12-14 |
| 2023 | 16 | 16 | 2023-12-13 |
| 2024 | 16 | 16 | 2024-12-18 |
| 2025 | 16 | 16 | 2025-12-10 |
| 2026 | 12 | 12 | 2026-09-16 |

The 2026 inventory lists minutes through **2026-09-16**,
with the latest explicit release date **2026-10-07**.
October/December minutes have no listed link as of this acceptance date. Those
claims remain explicit; no URL or release timestamp was guessed.

A separate completed 2025–2026 source operation advanced the archive while the
1999–2000 checkpoint was being read. The initial torn-read assertion stopped
verification safely. The operator resumed the same successful Dagster run,
validated the completed operation pointer chain, retained that additional work
and locked subsequent verification snapshots. The final planned 2025–2026
Dagster batch independently passed its own recovery and Slack gates.

The backfill had 10 failed attempts while historical print/encoding and
source-declared charts and addendum wording were being admitted. Published
pointers stayed unchanged; all failed-attempt originals/audits were retained in
separately restored retry checkpoints. Reduced offline fixtures reproduced the
failures before each fix. Successful retries reused verified original bytes;
failed-attempt requests/audit and acknowledged failure notifications are recorded
separately in the evidence.

The evidence separates that additional operation from the 21 requested Dagster
runs. Across all 22 distinct completed source operations, recorded
metrics total **798 data requests plus 44
startup probes and 100,990,326 bytes**. Outcomes are
`{"acquired": 367, "reused": 104}`. No retries, receipts or
requests are silently attributed to another run.

## Archive, recovery and remaining boundaries

The archive contains **1,081 captures**, by kind
`{"asset": 394, "document": 522, "inventory": 165}`. Native text outcomes are
`{"native_extracted": 355, "review_required": 167}`. Exact chunks cover
25,530,400 source-unit characters; 6,992
extraction separators account for the remaining full-text characters.

Remaining source outcomes are `{"not_listed": 49, "review_required": 167, "unavailable": 1}`.
Listed-reference archival coverage is complete for the accepted 1990–2026 scope
as of November 13. Unavailable/not-listed claims and extraction review flags
remain visible. This does **not** certify full historical source availability,
searchable-text completeness or OCR accuracy. No OCR/model service was called;
first-seen time is actual capture time.

Every checkpoint backed up all historical FOMC schema/catalog/operation state
and canonical filesystem originals/derivatives. Selected-catalog replay verifies
its stated selected generations and latest operation; it does not recreate every
old derived generation. The last restore verified 10,207
canonical files. Backups and raw data remain ignored and retained locally;
off-machine storage remains configurable future work. Disposable backfill
databases/roots were removed and operational namespace identities are unchanged.

Both FOMC status sensors remain running. The other eight status sensors and all
six acquisition/import schedules remain stopped. The existing manual local
Dagster job and portable script remain the execution paths. NAAIM stays paused,
FINRA remains at SSV-T05 and embeddings remain independently selected work.

Next FOMC work is review of flagged extraction/page quality and a deliberate
recent-year refresh after the next official release. Re-running completed years
in refresh mode preserves observed corrections and capture-time semantics.

## Engineering verification

All **1,091 regression checks passed**
with 83 optional skips;
all **357 FOMC checks passed**
with isolated PostgreSQL schemas enabled. The operational schema was not used as
a test target. The new cases reproduce pre-fix failures and cover declared
encoding, container ambiguity, explicit addendum association, image URL/format
boundaries, original-byte/profile/chunk storage, reuse and failed-refresh rollback.

The final runtime reproduces all 194 pre-fix extraction results/IDs exactly with
HTTP disabled. Exact source-module hashes pin this proof and match both built
wheel/sdist artifacts. Ruff lint/format, shell syntax and runner-help checks pass;
local data/config/secrets are absent from the artifacts. The local workflow
snapshot is verified. QuantSmith's checkout verifier reports revision drift;
all 61 pinned Git guidance objects were independently verified without modifying
that external checkout. See the current PR's checks/reviews before merging.
