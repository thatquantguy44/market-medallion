# Spec011 first source-contract checkpoint

**Reviewed:** 2026-11-03, America/New_York. **Branch:**
`feat/spec011-sec-ftd-parser`. **Scope:** documentary source review and offline
SEC manifest/parser/local-inspection slice. This is not a completed ingestion
pipeline or a certified historical provider schema.

## Source/access matrix — S11-T01

| Source | Official route and evidence | Admitted for this slice | Remaining source-dependent gate |
| --- | --- | --- | --- |
| SEC FTD | [Catalog and field table](https://www.sec.gov/data-research/sec-markets-data/fails-deliver-data), [developer guidance](https://www.sec.gov/about/developer-resources), [site policy](https://www.sec.gov/about/privacy-information) | Documentary six-column contract, invented fixtures and local-file inspection; one bounded schema-characterization request attempted | The linked modern archive returned HTTP403. Actual member/header/encoding/full-date coverage is unverified; no acquisition adapter is enabled |
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
Manifest: `manifests/sec-fails-to-deliver.yml`, documentary profile
`sec-ftd-six-column/1`, explicit `local-file-only`, automation disabled.

Validated configuration selects exactly one modern half-month and one safe
explicit `.txt` member. Default resource limits are 8 MiB archive, 64 MiB actual
expanded data, 500,000 rows and 4096 bytes per record. Hard caps are 16 MiB,
128 MiB, 2,000,000 rows and 65,536 bytes respectively. These are implementation
budgets, not provider limits. Actual successful physical conformance is still
required before treating those choices as sufficient for real coverage.

Pure parsing preserves raw symbols/descriptions, exact price precision, source
row positions and SHA-256 of original archive/member bytes. It validates strict
dates/partition bounds, identifiers, integer counts, duplicate keys and numeric
price/null tokens. ZIP processing reads one regular expected member in memory,
streams bounded records, verifies CRC through the ZIP reader, and rejects unsafe
names, symlinks, extra/duplicate members, unsupported encryption/compression,
inconsistent header flags, corrupted archives and resource-limit violations.

The profile currently requires ASCII, LF/CRLF and a final record terminator;
those physical conventions remain **provisional** until a real sample is
reconciled. Header-only input is unavailable under this profile, not an asserted
complete zero-fails population. Observed row dates do not establish that every
expected settlement date or eligible security is covered.

Current, executable local command:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver validate \
  --manifest manifests/sec-fails-to-deliver.yml
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver inspect \
  --manifest manifests/sec-fails-to-deliver.yml --input /path/to/approved-local.zip
```

`validate` reads the manifest only. `inspect` reads at most the configured
archive bytes plus one overflow-detection byte and prints hashes/counts;
neither command writes pipeline data or calls a provider. Success explicitly
reports `provider_conformance: UNVERIFIED`; inspection additionally reports
`source_coverage: NOT_CERTIFIED`. Future `run`, health/replay/export, SQLite,
Parquet and Dagster are not implemented by this checkpoint.

## Verification and next task

Invented fixtures and adversarial ZIPs are in the source test suite. The initial
tests failed before the package existed. Review reproduced and fixed malformed
YAML-key and mismatched ZIP-header handling. Focused tests, the full offline
suite and Ruff pass; the recorded full result is **151 passed, 2 skipped**.
The skips are the existing opt-in PostgreSQL tests. The new files pass formatting;
repository-wide formatting reports the pre-existing NAAIM test-file format issue.
The package sdist/wheel build and actual module `validate` command also pass.
No unrelated formatting or runtime/schedule change was made.

**Next:** finish the source-conformance portion of S11-T02 using an approved
actual local SEC file or a successful permitted bounded request; reconcile
member/header/encoding/row keys and dates before promoting the profile beyond
documentary-only. Then close the remaining checkpoint and deliver S11-T04's
immutable Bronze/SQLite/audit slice. Nasdaq/NYSE rights and physical conformance
remain separately tracked. NAAIM remains paused.
