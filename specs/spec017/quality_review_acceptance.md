# FOMC-Q02 bounded source-page review acceptance

Accepted locally 2026-11-13 on `feat/fomc-text-quality`.
[Contract QAC05–07/11–12](quality_contract.md) · [Evidence](evidence/quality_review.json)

Q02 implements deterministic case selection, source-bound review records, a
portable pack of verified originals/text/HTML previews, and render-evidence
validation. The existing script supports `quality-review-plan`,
`quality-review-pack`, `quality-review-record` and `quality-review-check`.
Plan/record/check operate on local inputs without a database connection; pack
export verifies the exact pinned source generations in a read-only transaction.
Incomplete reviews return `pending_review` and exit 2; a completed review never
changes extractor status or eligibility.

## Actual bounded review

The Q01 report yielded four nonempty issue × format × extraction-profile strata.
The earliest/latest selector chose **eight cases**, with no uncovered strata or
deferred boundary case. There are **1,079 unselected flagged units**; sampling is
not whole-corpus quality acceptance. All selected render hashes, source hashes,
profile/catalog IDs and actual review timestamps are bound in the evidence.
The reviewer identity is **Codex source-page review**, not human sign-off.

| Selected source | Inspected scope | Disposition |
| --- | --- | --- |
| [January 2012 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20120125ep.htm) | Figure 3.A and adjacent heading/notes | Chart/image content retained |
| [October 2007 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20071031ep.htm) | Chart 2(b) and adjacent notes | Chart/image content retained; empty source alt text stays explicit |
| [February 1990 action minutes](https://www.federalreserve.gov/monetarypolicy/files/fomcmoa19900207.pdf) | Complete PDF page 15 and complete extracted unit | OCR/text-layer completeness candidate: printed page marker absent from native text |
| [June 1999 minutes](https://www.federalreserve.gov/fomc/minutes/19990629.htm) | First replacement-character paragraph and adjacent text | Unresolved source-damaged fractional amount |
| [September 2020 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20200916ep.htm) | Figure 4 and adjacent paragraphs | Chart/image content retained |
| [November 2011 SEP](https://www.federalreserve.gov/monetarypolicy/fomcminutes20111102ep.htm) | Figure 3.D, heading and note | Chart/image content retained; source heading wording preserved |
| [September 2026 minutes](https://www.federalreserve.gov/monetarypolicy/files/fomcminutes20260916.pdf) | Complete cover page and complete native-text unit | Sampled title/date/attribution text agrees; seal/background remain images |
| [May 2007 minutes](https://www.federalreserve.gov/fomc/minutes/20070509.htm) | First replacement-character paragraph and adjacent text | Unresolved source-damaged fractional amount |

Dispositions: **four chart/image**, **one sampled text match**, **one OCR candidate**
and **two unresolved source passages**. No native-parser defect was confirmed in
this bounded review. The scanned page's printed marker is also absent from
independent `pdftotext 26.06.0` output, while sampled body text agrees. Source
images, typo/damaged wording and missing numeric fractions were not rewritten.
Q03 must assess this no-native-fix outcome explicitly rather than invent a patch.

PDF pages were rendered with Poppler `pdftoppm 26.06.0`; HTML content previews
with Chrome `157.0.8103.78` and CDP. Previews strip active elements/remote URLs,
embed exact archived image bytes and enforce a restrictive CSP. Browser HTTP(S)
requests were blocked; **zero external page requests were observed**. HTML
previews preserve reviewed article content with local styling; they do not claim
to reproduce provider CSS/layout. Each screenshot's inspected region is explicit.
Raw originals/renders remain ignored; sanitized evidence contains hashes and
bounded observations. The dedicated temporary browser is closed.

All **29 source-table fingerprints**, **10,207 canonical file hashes** and
publication pointers stayed unchanged during pack qualification. No source
request, OCR/model service, migration, derivative publication, schedule change or
Slack notification occurred. All 167 review-required representations and 1,520
flagged chunks remain as recorded by Q01.

Validation: **1,112 offline regressions passed**, 91 optional skips; **42 focused
checks passed with isolated PostgreSQL**. All four actual script commands passed
against the eight-case pack. Tests cover stable/capped sampling, unknown/stale
records, missing assets, active-content removal, original/mirror integrity,
render tampering, output boundaries and pending-review behavior. Missing-module
and missing-command tests failed before implementation. Lint/format and builds
pass. Source schemas/configuration were not used as test fixtures.

## Portable review workflow

First create a report using [Q01's command](quality_report_acceptance.md#run-with-your-own-configuration).
Use your own ignored configuration/environment and explicit output paths:

```bash
./scripts/run_fomc.sh quality-review-plan \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --report data/fomc_meeting_minutes/quality/my-report.json \
  --output data/fomc_meeting_minutes/quality/my-plan.json
./scripts/run_fomc.sh quality-review-pack \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --report data/fomc_meeting_minutes/quality/my-report.json \
  --plan data/fomc_meeting_minutes/quality/my-plan.json \
  --output data/fomc_meeting_minutes/quality/my-review
```

Render and inspect each named PDF page or archived HTML preview with network
disabled. `pack.json` supplies source/input paths and hashes; store PNG evidence
inside the pack. Create an observation JSON containing `disposition`, `reviewer`,
actual timezone-aware `reviewed_at`, `observations` and a `render` object with its
relative `path`, PNG `sha256`, manifest `input_sha256` and rendering tool/version.
Accepted dispositions are `sample_text_matches`, `chart_or_image`,
`confirmed_native_defect`, `ocr_candidate` and `unresolved`.

Run `quality-review-record` with the same config/report/plan, `--case-id`,
`--observation` and `--output`; it validates the inputs and creates the immutable
review identity. Collect the generated records into an explicit JSON array.
Run `quality-review-check` with the same config/report/plan, `--reviews` pointing
to that array, `--pack` pointing to the pack directory and `--output` for the
check result. It rehashes original/unit/render evidence, rejects stale or duplicate
records and reports remaining cases. Use `--help` for exact supported options.
