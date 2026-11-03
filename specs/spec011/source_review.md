# Spec011 first source-contract checkpoint

**Reviewed:** 2026-11-03, America/New_York. **Branch:**
`feat/spec011-sec-ftd-parser`. **Scope:** source review, offline SEC parser and
reconciliation of two user-approved local September 2026 archives. This is not
a completed ingestion pipeline or a certified historical provider schema.

## Source/access matrix — S11-T01

| Source | Official route and evidence | Admitted for this slice | Remaining source-dependent gate |
| --- | --- | --- | --- |
| SEC FTD | [Catalog and field table](https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data), [developer guidance](https://www.sec.gov/about/developer-resources), [site policy](https://www.sec.gov/about/privacy-information) | Six-column/trailer profile reconciled against two approved local September2026 archives; invented regression fixtures and local inspection | Earlier October request returned HTTP403. Local samples do not independently authenticate origin, establish broader date/universe coverage or enable acquisition |
| Nasdaq thresholds | [Threshold page](https://www.nasdaqtrader.com/Trader.aspx?id=RegSHOThreshold), [copyright/use notice](https://www.nasdaqtrader.com/Trader.aspx?id=CopyDisclaimMain), [future-hours notice](https://www.nasdaqtrader.com/TraderNews.aspx?id=ETA2026-64) | Documentary review; future fixture-only implementation can proceed | Recurring collection, retained history, transforms and report audience are not admitted by the general notice; physical history/profile conformance remains open |
| NYSE thresholds | [Date/market download page](https://www.nyse.com/regulation/regulation-sho), linked [ICE terms](https://www.ice.com/privacy-security-center/terms-of-use) | Documentary review only | Exact selected-market/date values and payload schema remain open; the general terms exclude automated extraction from their limited download permission, so an applicable allowance is required before live automation |

All three sources are publicly viewable; public viewing alone does not settle
the rights for every intended use. No purchase/account registration, provider
contact, hidden endpoint, browser impersonation or live recurring job is part
of this slice. Local research export/redistribution remains a later admitted
contract; these parser functions do not publish research data.

## Bounded selection and attempted request

- **SEC selection:** October 1–15, 2026, the catalog's October first-half archive.
  Linked URL: `https://www.sec.gov/files/data/other/fails-deliver-data/cnsfails202610a.zip`.
  Candidate member is `cnsfails202610a.txt`; that basename is explicit
  configuration for invented fixtures, **not an observed provider member**.
- **Nasdaq selection:** list dates October 26–30, 2026; provider/market Nasdaq.
  This is a prospective five-date conformance selection, not a coverage claim.
- **NYSE selection:** the same five source dates, NYSE listed-market view first.
  The transport selector values and date interpretation must be verified;
  NYSE American/Arca and other markets are excluded from the first selected scope.
- **Request bounds:** one identified SEC request, 30-second timeout,
  8 MiB maximum download, at most two redirects and no retry. User agent was
  `MarketMedallion/0.1 (+https://github.com/thatquantguy44/market-medallion)`.
  No cookies/member credentials or alternate identity were supplied.
- **Outcome:** HTTP403 at `2026-11-03T07:14:44Z` per the server Date header.
  That header is HTTP evidence, not provider publication or successful capture
  time. No ZIP arrived and no Bronze/Silver warehouse was created. Ignored local
  response headers are under `data/spec011-contract-review/`. No bypass retry
  was attempted. This does not establish a permanent SEC access restriction.

Before a later automated SEC adapter, supply a truthful operator contact and
finite pacing configuration consistent with the then-current fair-access
guidance. Do not treat this one request as recurring-use or distribution evidence.

## Approved local reconciliation — S11-T02

The user supplied `data/sec_ftd/cnsfails202609a.zip` and
`data/sec_ftd/cnsfails202609b.zip` for reconciliation. They replace the unsuccessful
October selection for **this SEC sample check only**. The official catalog links
both September archives. No additional provider request was made. Local
reconciliation ran at `2026-11-03T07:43:30.258082+00:00`; original retrieval and
publication clocks are unknown and must stay null. This inspection time is not
a persisted ingestion/first-seen clock.

| Archive | Archive bytes | Expanded bytes | Fact rows | Observed settlement labels | Quantity trailer | Missing prices | Missing symbols |
| --- | --- | --- | --- | --- | --- | --- | --- |
| `cnsfails202609a.zip` | 1,141,638 | 3,168,735 | 50,332 | 9 dates, Sep1–14 | 1,796,162,548 | 280 | 0 |
| `cnsfails202609b.zip` | 1,512,400 | 4,201,778 | 66,693 | 12 dates, Sep15–30 | 2,745,043,657 | 383 | 1 |

Both archives have exactly one matching `.txt` member, DEFLATE compression,
ASCII text, exact documented header, LF terminators and a final newline. The
longest record is 72 bytes including LF. Two final records are required:
`Trailer record count <integer>` followed by
`Trailer total quantity of shares <integer>`. They match independently counted
facts and integer quantity sums in both files. These sums are integrity checks
across settlement labels, **not** daily/new-fail measures. Both files have zero
duplicate `(settlement_date, cusip)` keys. Every parsed field and source-row
position was compared against an independent split of the local source text.
Leading-zero CUSIP rows number 5,751 and 7,289 respectively; none were coerced.
The single empty symbol stays null; all 663 `.` prices retain the missing reason.

The second file includes **September15**, while the first ends September14.
The original fixed 1–15/16–month-end assumption was incorrect for these files.
The new manifests explicitly pin 1–14 and 15–30. Supported bounds also retain
1–15 and 16–month-end; filename suffixes do not select bounds automatically.
Other months must be characterized before making completeness or universal
boundary claims. Dates remain source labels without moving a row between files.

| Archive | SHA-256 of original ZIP | SHA-256 of original member |
| --- | --- | --- |
| `cnsfails202609a.zip` | `46c12079fe62d84278e9d06f97cfa34eb9ebef5fca709fc856a49e3257ac2f01` | `0faa3ef27c19c31dc64b2b7f85002139fa3e9ed7dbb5898b80f8abdd9344ddd0` |
| `cnsfails202609b.zip` | `a89615d5b7fe1e8dd0fc2872406467ac71e499f0d8216ef9c0afb8cc98bdd99e` | `db3400c8ea67be63633ce47e9527f2ecdc82a4e22b206b1c9203f81852aacd88` |

Original archives remain unchanged and ignored. Aggregate evidence, including
per-date counts and manifest hashes, is in ignored
`data/sec_ftd/reconciliation-202609.json`. No actual security rows enter Git.
No expected settlement-calendar or security-universe inventory was supplied;
observed dates and valid totals do not certify complete population coverage.

## Effective-date and data-meaning decisions

The SEC's modern semi-monthly packaging starts July 2009; this parser excludes
older monthly/quarterly archives even though the all-balances reporting regime
started earlier. Settlement labels stay unchanged, CUSIPs stay strings, counts
stay integers and the documented `.` price is null. The six source labels are
used literally; alternate headers, encoding or member layouts require evidence
and a reviewed profile before acceptance.

Nasdaq's October 30 notice announces changes effective December 6, 2026. The
announced approximately 20:45 ET Sunday–Thursday availability must not be used
as proof of current or historical publication time. The eventual adapter needs
effective-date rules and explicit source-list date validation. This slice adds
no exchange scheduler or calendar assumptions.

## Implemented offline profile — S11-T02/T03 components

Source package: `src/market_data_pipeline/pipelines/sec_fails_to_deliver/`.
Manifest: `manifests/sec-fails-to-deliver.yml`, sample-reconciled profile
`sec-ftd-six-column/2`, explicit `local-file-only`, automation disabled.
Version2 requires both trailers and replaces the provisional version1 profile;
no version1 persisted pipeline data exists. The main manifest remains an October
example, not an assertion that October was reconciled. September manifests are
`manifests/sec-fails-to-deliver-202609a.yml` and `-202609b.yml`.

Validated configuration selects explicit modern semi-monthly bounds and one safe
explicit `.txt` member. Default resource limits are 8 MiB archive, 64 MiB actual
expanded data, 500,000 rows and 4096 bytes per record. Hard caps are 16 MiB,
128 MiB, 2,000,000 rows and 65,536 bytes respectively. These are implementation
budgets, not provider limits. Both local samples fit them; wider history remains
unmeasured.

Pure parsing preserves raw symbols/descriptions, exact price precision, source
row positions and SHA-256 of original archive/member bytes. It validates strict
dates/partition bounds, identifiers, integer counts, duplicate keys and numeric
price/null tokens. ZIP processing reads one regular expected member in memory,
streams bounded records, verifies CRC through the ZIP reader, and rejects unsafe
names, symlinks, extra/duplicate members, unsupported encryption/compression,
inconsistent header flags, corrupted archives and resource-limit violations.

The profile requires ASCII, LF/CRLF and a final record terminator. The supplied
samples verify ASCII/LF; CRLF acceptance has invented-fixture coverage.
Both trailers must be terminal, ordered and exact; missing/mismatched totals,
duplicate trailers and data after trailers fail. Header-only input is unavailable
under this profile, not an asserted
complete zero-fails population. Observed row dates do not establish that every
expected settlement date or eligible security is covered.

Current, executable local command:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver validate \
  --manifest manifests/sec-fails-to-deliver.yml
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver inspect \
  --manifest manifests/sec-fails-to-deliver-202609a.yml \
  --input data/sec_ftd/cnsfails202609a.zip
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver inspect \
  --manifest manifests/sec-fails-to-deliver-202609b.yml \
  --input data/sec_ftd/cnsfails202609b.zip
```

`validate` reads the manifest only. `inspect` reads at most the configured
archive bytes plus one overflow-detection byte and prints hashes/counts;
neither command writes pipeline data or calls a provider. Success explicitly
reports `provider_conformance: UNVERIFIED`: no independent origin authentication
is performed. Successful inspection additionally reports `profile_conformance:
MATCHED`, validated trailer totals and `source_coverage: NOT_CERTIFIED`. Future
`run`, health/replay/export, SQLite,
Parquet and Dagster are not implemented by this checkpoint.

The subsequent T18 configuration foundation adds a strict source-owned
`config.py`, `validate-config` and configured partition inspection. Settings
live under [config/sec_fails_to_deliver](../../config/sec_fails_to_deliver/README.md).
Config validation checks runtime intent only. T18 now also implements independent
source-owned storage schemas, native SQL bindings, exact numeric projections,
Arrow/Parquet codecs and Spark schema descriptions for both source packages.
Actual PostgreSQL/Delta backend compatibility remains unverified.
The user selected PostgreSQL as default with Databricks
disabled until configured. No service or schedule was started.

## Verification and next task

Invented fixtures and adversarial ZIPs are in the source test suite. The initial
tests failed before the package existed. Review reproduced and fixed malformed
YAML-key and mismatched ZIP-header handling. Focused tests, the full offline
suite and Ruff pass; the parser-reconciliation result was **163 passed, 2 skipped**.
The skips are the existing opt-in PostgreSQL tests. The new files pass formatting;
repository-wide formatting reports the pre-existing NAAIM test-file format issue.
The package sdist/wheel build and actual module `validate` command also pass.
No unrelated formatting or runtime/schedule change was made.

Characterization tests first reproduced the unsupported trailers and September15
partition rejection. The revised profile passes 73 focused cases, including
missing/wrong/reordered/nonterminal trailers and explicit September bounds.
Both real `inspect` commands pass, and the independent all-field comparison
passes for all 117,025 observations.

T18's full-suite result is **283 passed, 2 skipped**. New tests round-trip all
eight record schemas through actual SQLite and Parquet, native PostgreSQL binding
codecs, configured decimal bounds and explicit Spark schema descriptions. The
tests also cover raw numeric tokens, null clocks, withdrawals, malformed success
receipts and complete-empty/failed threshold lists. All117,025 approved September
observations pass local numeric/date field preflight; aggregate evidence is in
ignored `data/sec_ftd/storage-types-202609.json`. No warehouse write, new source
request, completeness certification or first-seen assignment occurred. Live
engine parity is still required in T19–T22.

**Next:** S11-T04 immutable Bronze/SQLite/audit and contract/root binding.
The user's subsequent compatibility requirement is recorded in R12/A14/A15:
PostgreSQL warehouse and Databricks Delta delivery/parity are required before
G2 in T19–T22. No backend adapter or engine verification exists yet. Include original rejected-input
retention to finish the remaining S11-T03 item; preserve unknown clocks and
coverage limitations. S11-T02 sample conformance is complete for these two
archives only. Full Spec011/G2, exchange rights/conformance and persistent
publication remain open. NAAIM remains paused.
