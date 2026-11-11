# FOMC-T03 — bounded native year discovery

Current status: accepted under the [1990-onward T03 closeout](t03_acceptance.md).
The discovery migration is applied locally; the current expected set has 33
pages. The older 87-page result below is historical characterization outside the
now-narrowed operating scope.

Traces FOMC-AC01. `data/discovery.py` reads archived HTML without HTTP, files,
clocks or database access. It does not infer document or text completeness.

`parse_historical_index(payload, source_page_url, manifest)` accepts only the
reviewed official historical-year index, UTF-8 HTML up to 2 MB by default,
with the `#article` heading and decade panels observed on 2026-11-11. Frozen
records retain each labeled year, exact href, resolved official year-page URL
and the original index's SHA-256. Navigation and commented-out markup are not
year evidence. Unknown headings/labels, conflicting URL/label/bucket years,
empty panels, moved year links and gaps from the manifest's first supported
year produce issues. It never constructs a missing URL. Discovery is bounded
by 128 year-link records and the shared HTML structure limits.

`discover_inventory_pages(index_payload, calendar_payload, manifest,
start_year=..., end_year=...)` requires 1–128 explicit supported years and
returns a detached, serializable evidence record. The expected set contains
both source inventories plus every linked historical page in the selected
range. A year covered by both inventories requires both pages. Calendar years
come from source headings, including future labeled years, rather than the
wall clock. Empty or changed year panels and unknown meeting labels are issues.
Selected years absent from both sources are issues; this check does not assume
an annual meeting count. The final expected set must fit the 128-page publication
bound. The calendar is retained in full, even for a historical selection; bounds
select historical year pages, not rows to truncate from that calendar.

The [sanitized source evidence](evidence/year_index_discovery.json) records the
one bounded official index request and an offline comparison with the retained
calendar. There are 85 linked historical years (1936–2020), calendar headings
2021–2027 and 87 expected inventory pages for that explicit selection. This is
a discovery result, not a backfill: the year pages were not fetched. The index
is a local review capture, not an additional production PostgreSQL capture.
At that checkpoint the notation vote and 1936-page/1937 label were unresolved.
Closeout now retains notation votes as reviewed nonmeeting exclusions; the 1936
conflict is outside the selected scope. One 1990-onward metadata snapshot is
committed, with one explicit unavailable 1994 source link.

## Publication and storage

After archiving the expected pages in both stores, an operator can call:

```python
result = bronze.publish_inventory_captures(
    capture_ids, manifest, root=config.root, historical_years=(start_year, end_year)
)
evidence = bronze.read_inventory_discovery(result["run_id"])
```

The method verifies matching committed filesystem/PostgreSQL bytes and manifest
hashes, re-derives discovery from the supplied index/calendar bytes, rejects all
issues and requires an exact expected-page match. Caller-supplied expected URLs
cannot substitute for discovery. Each historical page must independently parse
without issues or an empty meeting inventory. Previous source pages still may
not be dropped. Expanding a published bounded union therefore requires including
its earlier years in the next selection.

The explicit `PostgresBronzeStore.migrate()` now also applies
`fomc-bronze-discovery-postgres/1`, creating `bronze_inventory_discovery` with a
foreign key to the inventory run. It rejects an unowned table or a missing
marked table and upgrades the earlier owned Bronze migrations transactionally.
It does not repurpose the T02 marker. This migration passed isolated-schema
checks and is now applied to the existing local FOMC schema.

Selected-page evidence, its SHA-256, the snapshot, capture links and the current
pointer commit in one transaction. Evidence retains source hashes, explicit year
selection, expected URLs and capture IDs. Readback verifies its hash. A source
link or evidence-insert failure rolls everything back; archival captures remain.
Calling without `historical_years` preserves the earlier issue-gated metadata
union behavior and stores no discovery-coverage claim. Including a historical
index requires explicit bounds. Neither mode certifies complete documents,
addenda, assets, extraction or historical release vintages.

Rollback before applying the new migration is a code revert. After a schema has
used it, retain its table and migration marker together and keep readers that
recognize the marker; older code refuses the newer identity. Do not drop populated
evidence to make old code accept a schema. No production rollback or destructive
migration has been performed.

## Verification

Offline invented HTML covers link and year conflicts, missing/unknown labels,
changed structure, overlap, byte/page/year limits and detached immutable evidence.
Real PostgreSQL tests use isolated schemas to cover missing expected pages,
required roots/bounds, exact evidence/link readback, rollback, owned migration
upgrade, damaged migration state and evidence corruption. Run:

```bash
.venv/bin/python -m pytest -q tests/test_fomc_discovery.py tests/test_fomc_native.py
# With this project's trusted .env loaded; no provider requests:
FOMC_TEST_POSTGRES_CONFIG=config/fomc_meeting_minutes/pipeline.local.yml \
  .venv/bin/python -m pytest -q tests/test_fomc_postgres.py
```

Earlier discovery-increment verification: 866 offline tests passed (25 optional skips); all 133 focused
FOMC checks passed, including 15 real PostgreSQL tests in isolated schemas.
Repository lint/format and source/wheel builds pass. Archive inspection confirms
the HTML fixture, discovery module, contract and sanitized evidence are included,
with ignored runtime inputs absent.

The subsequent [attachment](attachments_contract.md) and
[storage/recovery](storage_recovery_contract.md) increments implement declared
addenda/PNG lineage, content-addressed original-byte migration and durable
attempt recovery. The 1990-onward source exceptions and inventory census are accepted in T03.
T04 complete text/extraction is the pickup; full original backfill, chunks, jobs
and schedules remain later work.
