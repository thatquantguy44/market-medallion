# FOMC-T03 — declared addenda and substantive HTML assets

Traces FOMC-AC01/02/12. `data/dependencies.py` performs bounded, offline discovery
from original HTML, without reading a clock or constructing filenames. Source
review: 2026-11-11. This profile advances archive lineage; text and visual/image
validation remain T04 work.

## Reviewed source rules

The [official historical-materials guide](https://www.federalreserve.gov/monetarypolicy/fomc_historical.htm)
describes the former SEP minutes addendum. The [June 2019 minutes](https://www.federalreserve.gov/monetarypolicy/fomcminutes20190619.htm)
and [June 2017 minutes](https://www.federalreserve.gov/monetarypolicy/fomcminutes20170614.htm)
explicitly describe the SEP as an addendum to those minutes and link it in
`#t4_nav`. A similarly named link without that declaration remains an issue.
A separate press-conference projection is not automatically a minutes addendum.
The policy-normalization addendum embedded in the 2017 minutes stays in the
original HTML; its referenced policy documents do not become minutes addenda.

`discover_html_dependencies` requires one `#article`, at most 2 MB by default
and at most 256 dependency records (configurable up to 1,000). Explicit
“Addendum to these/the minutes” anchors are eligible. Same-document fragment
references remain embedded. Bare “Addendum” and “Minutes addendum” labels remain
unresolved. Ambiguous/missing declared SEP links block coverage.

PNG images inside the substantive article are required content assets; site
navigation/footer images are not. The reviewed URL profile preserves exact case
and permits `/monetarypolicy/files/*.png` and the observed root
`/sepYYYYMMDDfigureN[suffix].png` form. These are validators for source-owned
hrefs, not filename generators. Other hosts, paths or image formats remain
issues. `srcset`, lazy attributes, CSS URL assets, external SVG references,
embedded objects/media and active article content remain issues until reviewed;
they cannot be silently omitted from a successful dependency check. No script
or stylesheet is fetched or executed.

Frozen records retain parent URL, exact target URL, relation (`minutes_addendum`
or `content_asset`), kind (`document` or `asset`), original href/label, payload
hash, exclusions and issues. Full HTML bytes remain canonical. Addenda inherit
the parent document kind; no new meeting or unrelated product is fabricated.

Native inventory parsing additionally recognizes the reviewed uppercase
`/fomc/MINUTES/` legacy path and HTML/PDF anchors inside an explicitly labeled
minutes paragraph. Dates and URL case are preserved. Unscheduled meeting labels
are explicit source labels; cancelled headings and cross-referenced minutes
remain blocking issues. They do not become certified empty inventories.

## Acquisition and PostgreSQL lineage

`AcquisitionRun.acquire(url, "asset")` uses the existing document byte limit and
request budget with the same run-scoped client. It follows only validated exact
URLs and preserves original received bytes and safe receipts in both stores.

`PostgresDependencyStore(bronze).migrate()` explicitly creates separately marked
`bronze_dependency_state`, `bronze_dependency_checks` and `bronze_dependencies`
tables under contract `fomc-bronze-dependencies-postgres/1`. Missing ownership,
partial tables or a conflicting marker refuse. Production has not been migrated.

`commit(parent_capture_id, child_capture_ids, manifest, root=...)` re-derives the
expected direct dependencies from matching, committed filesystem/PostgreSQL
originals. It refuses issues, missing/extra URLs, duplicate captures per URL,
wrong kinds, mismatched manifests/bytes, PNGs without the PNG signature and
PDFs without PDF header/EOF framing. Child HTML must match the reviewed dependency
profile. These checks detect obvious wrong/login/truncated containers; they are
not visual or full PDF-structure validation. Check evidence
and all original-label links commit atomically, without moving an inventory
pointer. Hash-derived check IDs make identical retries idempotent. A new child
capture produces a separate immutable check. `read_check` verifies both evidence
hash and link rows. `meeting_associations` follows parent links to every retained
inventory association, including bundled documents and nested addenda.

Each check covers direct dependencies only. An addendum needs its own check for
its images; no single check certifies a recursively complete archive or text.
Retain populated tables and their marker together on rollback; no destructive
rollback has been performed.

## Bounded conformance

[Sanitized evidence](evidence/attachment_source_sample.json) records 28 retained
local review captures: two minutes, two SEP addenda, their 22 substantive PNGs,
and the 1993/2020 year pages. Each passed byte/hash readback in filesystem and an
isolated temporary PostgreSQL schema. Four direct checks reconcile both parent
addenda and all 22 images. The schema was then removed; production and inventory
pointers remain unchanged. Raw provider files stay ignored.

The 1993 sample now retains eight legacy minutes references without parser
issues. The 2020 sample retains 16 representations and surfaces the March 2
cross-reference and cancelled March 17–18 label. The earlier 1936 date conflict
and current notation vote also remain unresolved. No corpus-wide or text
completeness is claimed. The subsequent
[storage/recovery increment](storage_recovery_contract.md) implements
content-addressed originals and interrupted-write/startup recovery.
