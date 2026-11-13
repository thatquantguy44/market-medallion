# FOMC-T03 — declared addenda and substantive HTML assets

Current status: accepted under [T03 closeout](t03_acceptance.md); the dependency
schema is applied locally and eight direct checks are committed. Earlier sample
counts below remain historical evidence.

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

`discover_html_dependencies` uses one `#article` for modern pages or the reviewed
classic body profile below, at most 2 MB by default
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
partial tables or a conflicting marker refuse. The migration is now applied
to the existing local FOMC schema.

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
issues. The earlier 2020 sample surfaced its March 2 cross-reference and cancelled
March 17–18 label. Closeout now resolves all in-scope source cases, including 14
bundled cross-references and explicit nonmeeting exclusions. The 1936 conflict is
outside scope. Full original-document/text completeness is not claimed. The subsequent
[storage/recovery increment](storage_recovery_contract.md) implements
content-addressed originals and interrupted-write/startup recovery.

## Classic HTML and closeout source regimes

The reviewed `/fomc/MINUTES/YYYY/YYYYMMDDmin.htm` and
`/fomc/minutes/YYYYMMDD.htm` pages use a single body containing the published FOMC
minutes title. Profile `fomc-legacy-body/1` retains the complete original. The two
observed Board-logo/top-arrow GIF paths are decorative exclusions; the final
recognized edge-challenge script is retained in raw bytes and excluded from
content dependencies, even when legacy unclosed markup nests it in a footer.
No script is executed. Other active content/assets remain issues. A base tag's
`target` alone does not rewrite URLs; any `href` base still blocks coverage.

Scoped native inventory supports explicit cross-month dates, sized PDF captions,
nounless unscheduled sessions and slash/hyphen bundled-meeting references. Known
non-minutes products have reviewed family exclusions; unknown panel/attachment
labels raise issues. The standalone 2019 accessibility ZIP was directory-reviewed
and contains the conference transcript/presentation material, not minutes. An
empty minutes href is `unavailable`; raw exclusions and cross-reference claims
are saved in the committed inventory evidence. See the acceptance record for
source coverage, backup/restore and the one known source gap.

## Historical print-container profile — 2007-era pages

Reviewed 2026-11-13 during the 1997–2026 backfill. The official
[June 2008 minutes](https://www.federalreserve.gov/monetarypolicy/fomc20080625.htm)
use `#content > .generalContentText#printThis`, with a minutes heading and
`#subNavLinks` linking the SEP explicitly described as “attached as an addendum”.
The bounded profile accepts one such container on the reviewed
`/monetarypolicy/fomc[minutes]YYYYMMDD[ep].htm` URL shapes. Missing/ambiguous
containers and unsupported embedded resources still fail. The exact site
image URLs for print, masthead, RSS and return-to-top are recorded as decorative
exclusions. Source-declared GIF charts under
`/monetarypolicy/gifjpg/fomcminutesYYYYMMDDChartN[suffix].gif` are additionally
reviewed from the [June 2008 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20080625ep.htm).
The [October 2007 minutes](https://www.federalreserve.gov/monetarypolicy/fomcminutes20071031.htm)
also exercise inline emphasis before the declaration comma and an RSS control.
Its [SEP addendum](https://www.federalreserve.gov/monetarypolicy/fomcminutes20071031ep.htm)
declares the three additional exact `/monetarypolicy/gifjpg/chart1.gif`,
`chart2a.gif` and `chart2b.gif` paths. They retain the same strict GIF framing,
raw-byte and explicit image-review rules; no filenames are generated.
The [January 2008 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20080130ep.htm)
also declares date-prefixed GIFs without `fomcminutes` and one JPEG chart. The
reviewed dated pattern therefore permits that optional prefix and GIF/JPEG
extensions, with JPEG SOI/EOI framing from [ITU T.81 Table B.1](https://www.w3.org/Graphics/JPEG/itu-t81.pdf).
The [April 2008 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20080430ep.htm)
adds underscore-separated names. Admission therefore follows the reviewed
`/monetarypolicy/gifjpg/` content-image folder, ASCII source-owned filenames and
PNG/GIF/JPEG formats, alongside the prior PNG paths. Every URL must actually be
declared inside the reviewed substantive container; none is generated. Other
hosts/folders/formats and query-bearing URLs remain rejected. Source-owned framing checks match the URL
format, GIF87a/GIF89a header, positive logical-screen dimensions and trailer;
truncated/wrong-format assets cannot publish. Full raw chart bytes and lineage
are retained, while extracted image content remains review-required. This is
container framing, without image decoding or OCR. See the [GIF format](https://www.w3.org/Graphics/GIF/spec-gif89a.txt).

These pages declare `iso-8859-1` in an early HTML meta element and contain
Windows-1252 punctuation/names. Per the [HTML encoding labels](https://encoding.spec.whatwg.org/#names-and-labels),
that label and `windows-1252` decode strictly with Windows-1252 on these reviewed
document paths. Conflicting/unknown declarations and undefined bytes fail; no
lossy replacement or inferred fallback is used. Inventories keep UTF-8 decoding
and original response bytes/hashes remain canonical.

Dependency receipts record `fomc-html-print/1`; new print extractions record
`reviewed_print_container_blocks/1` and the canonical `html_encoding`. Existing
UTF-8 article/body profiles remain unchanged. Offline reproduction of all 194
pre-fix selected extractions preserved exact results and IDs. Invented reduced
source-shaped regression cases cover first/last text, names/punctuation, tables,
explicit SEP lineage, exact decorative exclusions, ambiguity and encoding
refusal. An isolated operation test verifies original-byte readback, stored
profile/text/chunks and repeat-backfill reuse.

The [January 2009 minutes](https://www.federalreserve.gov/monetarypolicy/fomcminutes20090128.htm)
use a relative clause with “that” in the explicit attached-addendum declaration.
The declaration rule accepts direct statements and “which”/“that” clauses while
requiring the named SEP to be an addendum to these minutes. A matching navigation
label alone still records an unresolved association. Offline regressions verify
both explicit declarations and refusal after that association is removed.

The [January 2012 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20120125ep.htm)
declares GIF charts in `/monetarypolicy/files/`, including
`SEP20120125_figure1.gif`. That already reviewed content-image folder also admits
source-declared GIF/JPEG files with the same framing/lineage checks. Existing
PNG admission is preserved exactly. Unsupported formats, other hosts/folders,
queries and wrong/truncated image framing still fail; image text stays review-required.

The [March 2014 minutes](https://www.federalreserve.gov/monetarypolicy/fomcminutes20140319.htm)
define “Summary of Economic Projections (SEP)” and later explicitly identify
“SEP” as the attached minutes addendum. Acronym declarations are eligible only
when that definition appears in the same substantive article; undefined acronyms
and navigation labels alone remain unresolved. HTML spacing around the acronym's
parentheses does not change the association. Original labels/hrefs are retained.
