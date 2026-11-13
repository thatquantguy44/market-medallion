# FOMC-Q03 remediation assessment and three-slice closeout

Accepted locally 2026-11-13 on `feat/fomc-text-quality`, in
[PR #19](https://github.com/thatquantguy44/market-medallion/pull/19).
[Contract QAC08–12](quality_contract.md) · [Final evidence](evidence/quality_remediation.json)

The three user-selected slices have working implementations and actual validation:
[Q01 offline reporting](quality_report_acceptance.md),
[Q02 bounded source-page review](quality_review_acceptance.md), and Q03's
`quality-remediation-plan` evidence gate. The gate revalidates the actual source/
render pack, binds the complete review set and identifies confirmed native
extraction defects. It refuses incomplete, stale, duplicate, tampered or
inconsistent review evidence. A confirmed defect returns `fix_required` and exit 2;
its failing fixture, new derivatives and publication/recovery acceptance remain
required before such work can be called fixed. No automatic repair or eligibility
override is provided.

## Bounded no-native-fix outcome

All eight selected cases were inspected across all four observed issue × format ×
profile strata. The outcome is **`no_confirmed_native_defect`**. Four cases contain
charts/images, one PDF cover's sampled text agrees, one scanned page has an
OCR/text-layer completeness gap, and two HTML passages retain source-damaged
fractional amounts. The scanned page's printed page marker is missing from both
native engines checked; source-provided chart labels/typos remain verbatim.

An additional original-byte check covers all **26 replacement-character
representations**: all **112 extracted U+FFFD characters** have matching literal
UTF-8 replacement-character counts in their exact archived HTML originals.
This count comparison supports source-origin diagnosis; it does not reconstruct
missing words/numbers or certify semantic accuracy. No numeric fraction was
inferred and no extractor patch was invented to make the flags disappear.

Q03 therefore closes its bounded **no-native-fix path**. Transform/profile edits,
new derivative publication and a new operational restore/replay are **not
required and were not performed**. This is not evidence that a corrective
publication succeeded. The final audit instead reconfirmed that the complete
operational state still exactly matches the previous full restore/replay checkpoint.
All seven existing extractor/acquisition module hashes are unchanged.

## Final archive and engineering qualification

The read-only final audit ran with HTTP/probes, extraction and chunk transforms
disabled, holding the source-operation advisory lock. It verified:

- All **29 table fingerprints** match the accepted backfill and last restored
  schema checkpoint, including historical artifacts, audit rows and pointers.
- All **1,081 capture originals, receipt bytes and clocks** match the retained
  checkpoint and both filesystem/PostgreSQL mirrors.
- All **10,207 canonical files** match the retained, previously restored archive;
  schema/filesystem backup hashes and original restore receipt are unchanged.
- Archive health is **healthy with no issues**. All **167 review-required
  representations and 1,520 flagged chunks** remain explicit; no eligibility
  changed. There are **1,079 unselected flagged units**, beyond the eight-case
  review, plus three unresolved/OCR candidates within that review.
- Only the two configured FOMC status sensors run. All six acquisition/import
  schedules and the other eight sensors stay stopped; NAAIM remains paused.

Final tests: **1,120 offline regressions passed**, 92 optional skips; all
**395 FOMC checks passed with isolated PostgreSQL**, and **51 focused checks**
passed. The operational schema was not a test target. Tests cover reporting,
version/association accounting, empty content, corrupt/stale inputs, read-only
transactions, output boundaries, inert previews, bounded selection, render
validation, complete review sets and no-fix/fix-required behavior. New module/
command tests failed before implementation. Lint/format, shell/runner checks,
wheel/sdist builds and all actual portable workflow commands pass. Both artifacts
contain exact final source bytes and exclude local data/config/secrets.

The pinned agent-skills snapshot verifies. QuantSmith checkout drift remains
reported; all 61 pinned guidance Git objects were verified without repinning or
modifying that external checkout. No dependency/schema change, provider request,
OCR/model integration, schedule activation or Slack test message was introduced.

## Run the assessment with your own review pack

After [Q02's workflow](quality_review_acceptance.md#portable-review-workflow):

```bash
./scripts/run_fomc.sh quality-remediation-plan \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --report data/fomc_meeting_minutes/quality/my-report.json \
  --plan data/fomc_meeting_minutes/quality/my-plan.json \
  --reviews data/fomc_meeting_minutes/quality/my-reviews.json \
  --pack data/fomc_meeting_minutes/quality/my-review \
  --output data/fomc_meeting_minutes/quality/my-remediation.json
```

This command reads local report/plan/review/render artifacts and writes only the
explicit assessment output. It makes no database or provider connection. A
`no_confirmed_native_defect` result describes the chosen reviewed scope; unresolved
cases and unselected units remain in the result. It is not a whole-corpus quality
certificate and does not authorize corrective publication or OCR.

Remaining work is additional source-page review beyond this sample, verified
source reconciliation for damaged HTML, and an explicit OCR/text-layer policy.
Deliberate recent-year refreshes remain available for new releases/corrections.
Optional embeddings remain independently selected; recurring acquisition stays
stopped. The raw source and prior accepted records remain canonical.
