# FOMC-Q01 quality report acceptance

Accepted locally 2026-11-13 on `feat/fomc-text-quality`.
[Contract QAC01–04/11–12](quality_contract.md) · [Evidence](evidence/quality_report.json)

The pipeline-owned pure report builder, verified read-only snapshot adapter and
`quality-report` CLI are implemented. The existing script ran successfully on the
accepted archive and reproduced report ID `4e4d302658dd7040b91a78cdd08adb25ba68181a359441921c0674c426f05001`. Local report
bytes remain ignored under the configured archive's `quality/<report_id>/` path.

The report reconciles **522 selected versions/representations**, **4,018 units**,
**9,790 chunks** and **25,530,400 source-unit characters**. All **167** review
versions and **1,520** flagged chunks remain explicit. There are **1,087**
unit/issue findings: image content affects 141 versions / 1,061 units / 1,253
chunks; replacement characters affect 26 versions / 26 units / 267 chunks.
Grouped memberships may overlap; unique totals are not summed from overlapping
groups. Original release claims remain nullable/date-only, and all meeting and
capture-time associations are retained. Blank/no-unit inputs are characterized
in fixtures even though this archive's selected inputs have none.

Under the source-operation advisory lock, all **29 source-table fingerprints**,
**10,207 canonical file hashes** and inventory/text/chunk pointers were unchanged.
The report's database transaction enforces repeatable-read/read-only access.
HTTP/probe, extraction and chunk transforms were disabled during the archive
proof. Isolated tests cover busy locks, accidental writes, incoherent pointers,
corrupt mirrors, stale identities, shared/reverted versions, cross-year membership,
empty content and output conflicts/symlinks. No flag or eligibility is promoted.

Verification: **1,100 offline regressions passed**, 88 optional skips; **27 focused
checks passed with isolated PostgreSQL**, including CLI and script coverage.
Ruff lint/format, wheel/sdist build and the actual portable script smoke passed.
Initial missing-module and missing-command tests failed before implementation.
The operational schema was never used as a test fixture.

## Run with your own configuration

```bash
./scripts/run_fomc.sh quality-report \
  --config config/fomc_meeting_minutes/pipeline.local.yml \
  --operations config/fomc_meeting_minutes/operations.local.yml \
  --output data/fomc_meeting_minutes/quality/my-report.json
```

Load your own trusted environment separately. The command makes no provider
request and requires no live flag. It prints the report ID/totals and writes the
full deterministic JSON exclusively to the explicit destination. Identical
retries are accepted; conflicting output or canonical-state overlap is refused.
Config/password resolution stays owned by this pipeline.

Q02 original-page review and Q03 confirmed remediation remain separate gates.
These diagnostics do not certify source availability, OCR or text accuracy.
