# Presidential pipeline migration and PRES integration plan

Status: **implementation and local parity merged**; the original sequence below is retained for design history.
Requirements: [Spec016](README.md). Boundary: [data contract](data_contract.md).
Executable work tracker: [todo.md](todo.md). This file is the coordinated plan
for all three repositories; existing root `tasks/` files remain untouched.

## Decisions and sequencing

1. Integrate the donor's tested pure transforms into MarketMedallion's required
   `pipelines/` namespace. A nested standalone project would leave orchestration,
   manifests, revision behavior and package ownership unresolved.
2. Use a versioned immutable bundle as the repository boundary. A direct database
   connection would couple PRES to producer schema and credentials; an arbitrary
   shared JSON path would lose coherent multi-dataset provenance.
3. Preserve v1 dataset input and the current TypeScript engine. Add strict
   adapters for bundle/provenance and supplementary cycle evidence. Do not add
   metadata fields that the existing dataset validator correctly rejects.
4. Ship the smallest end-to-end path first: generated source bytes → approved
   controls → one immutable dataset → explicit PRES load → existing analysis and
   exports. Then expand publication to all five datasets and native paired views.
5. Start local/private, with source fetch separate from build and app reads.
   Readability of an official URL does not settle recurring/redistribution rights.
6. Keep SQLite/Parquet the self-contained completion path. Design source-owned
   storage interfaces without implementing or advertising untested PostgreSQL or
   Delta support. Optional backends have separate parity gates, not inherited
   approval from AAII or SEC.

## Cross-repository implementation surfaces

| Repository | Existing files to adapt / preserve | Planned additions |
| --- | --- | --- |
| Research website | `docs/research/002_PresidentialImpact/pipeline/{src,tests,config}`, its README/QUALITY/SOURCE_DECISIONS/TRANSFER; `MASTER_HANDOFF.md`, methodology and inherited/cycle protocols. | A migration pointer and export/checksum instructions after cutover. Preserve published research/narrative and frozen evidence. |
| MarketMedallion | `shared/files.py`, `shared/yaml.py`, `shared/http.py` are allowed helper dependencies; root CLI/orchestration aggregate only implemented source commands/assets. Existing provider packages are patterns, never schema imports. | `pipelines/presidential_research/`, pipeline-owned config/manifests/contracts/tests, runbook and register entries. |
| QuantMeridian | `src/lib/presidential/{contracts,validation,engine,exports}.ts`, `src/app/presidential-impact/page.tsx`, `src/components/presidential/{ResearchInputs,ResearchEvidence,ResearchFigures,ResearchLedger}.tsx`, `test/presidential-impact.spec.ts`, `docs/guides/markets/pres-{help,whitepaper}.md`. | `src/lib/server/presidential/reader.ts`, `src/lib/presidential/{bundle,comparison,cycle}.ts`, new research routes, focused tests, paired/cycle components, pinned cross-language fixtures. |

QuantMeridian uses Vite/React plus standard Request→Response handlers under
`src/app/api/`; `src/server/registry.ts` discovers route files for dev and built
server. Do not assume a Next.js runtime from directory names. Test the shared
registry path in dev and the built Node server. Choose a free local module-spec
number at implementation time; do not overwrite the unrelated in-progress spec031.
Link that module spec back here rather than creating a second contract authority.

## G1 — Inspection freeze and offline port parity

Tasks T01–T03. Record actual starting HEADs and relevant uncommitted file digests;
use isolated implementation branches without stashing/discarding other work.
Inventory donor imports, fixtures, config roles and referenced protocol versions.
Adapt imports and source resource lookup for installation as part of the target
distribution. Keep all nine donor command capabilities, with matching argument
names initially; improved commands may be added separately.

Exit evidence: original generated-fixture tests pass in the new namespace;
small independently calculated outputs and donor-versus-target outputs agree.
Compare canonical datasets/ledgers and source IDs, allowing only explicitly
documented path/producer metadata changes in sidecars. Pin numeric comparisons
to donor precision: exact stored decimal strings; computed binary-float returns
within `1e-12` absolute tolerance unless exact equality is available. Any changed
sample, assignment, flags or missingness fails parity regardless of tolerance.

## G2 — Reviewed inputs and one local immutable publication

Tasks T04–T06. Add immutable local import with receipts and explicit input
selection; no invocation reaches the network. Provide an archive-import path
that does not depend on the donor checkout being present. Validate both archived
retrieval lineage and new destination admission time. Promotion copies approved
workbook/control evidence as an immutable review version; the source workbook
is not edited by ingestion.

Build and publish one FF baseline dataset with strict v1 schema and replay
sidecar, then atomically select it with a catalog/pointer. Include pending
publication recovery from the first working publication slice, before adding
scheduled execution. Generated fixtures prove the full path without production
credentials or real reviewed history.

Exit evidence: a failed raw hash, unapproved row, incomplete month or failed
pointer replace leaves the previous snapshot usable. Package build can execute
the local fixture path from outside the source checkout. Do not claim historical
frozen-sample reproduction until the corresponding real inputs have been admitted.

## G3 — One dataset loads in PRES with provenance

Tasks T07–T09. Pin shared contract fixtures, implement local catalog/snapshot
reader/routes, then add explicit source selection to ResearchInputs. Opening PRES
continues to show the empty workspace. “Load pipeline snapshot” loads the
selected entry only after full validation; “Check for updates” or refresh is a
separate action. Show profile, construction, range, generated/captured dates,
control review and vintage limitations in user terms.

On a successful selection change, reset analysis config and interpretation as
the current load behavior does. Failed or superseded asynchronous loads retain
the last valid dataset, configuration and author notes. A late response cannot
replace a newer selection. Pin selected generation for the session; pointer
advancement does not silently change the research result.

Exit evidence: synthetic bundle → API → browser → existing engine → existing
CSV/SVG/PNG and v1 replay export works end to end; the source snapshot receipt is
inspectable. Full pipeline-aware v2 exports land at G5. Disabled configuration, 403,
unsupported schema, oversized/corrupt artifact and missing root have explicit
states. Manual import and illustrative workflows still work.

## G4 — Full paired datasets and inherited-condition analysis

Tasks T10–T12. Expand assembly/publication to the five named datasets and all
associated evidence. Add observed-version SQLite and typed Parquet coverage
without changing the established published schema. Frozen profile selects fixed
input digests and sample end dates; refresh profiles are distinctly identified.

Provide a native paired comparison whose primary default is the common-sample
CRSP versus Shiller pair, never full 1936–2023 versus 1936–2022. Show independent
returns, differences, paired sample counts and source labels. Inherited-condition
mode selects the matching two datasets, shows CAPE and FEDFUNDS provenance/timing,
and uses the current engine's availability filter and descriptive summaries.
Preserve full-sample primary results; common year exclusions apply symmetrically
and produce an exclusion ledger. Incompatible windows or missing evidence produce
an unavailable comparison state with a reason.

Exit evidence: fixture joins and numerical outputs reconcile; admitted frozen
history reproduces 88/87/87/68/68 windows and identical paired fundamentals.
The full baseline remains independently selectable. No side-by-side figure
silently selects a longer baseline or drops a missing row from only one series.

## G5 — Native cycle views and replay-complete exports

Tasks T13–T15. Add strict cycle adapter and independently recomputed summaries.
Render party × cycle year (1–4), both return constructions, all-years and
transition-flagged treatments, best/worst years and succession indicators.
Clearly label the year-end assignment and its separation from the primary
political-configuration grouping. The 1936 omission is recorded, not lost.

Complete `presidential-run/2` provenance and export support for standalone,
paired, inherited and cycle views. Keep existing v1 replay handling. Preserve
current numerical run ID logic for existing methods; any new method/config that
changes numerical results gets its own method/view identity. Actual producer
commit is recorded only when available; dirty source gets a tree digest and flag.

Exit evidence: admitted frozen cycle output has 86 paired years, matching flags
and summaries. Replaying an exported run with the producer stopped and the
original directory unavailable reproduces the same rows, groups and numerical
identity. Notes/export timestamps affect only their separate receipts.

## G6 — Replay, operations and ownership cutover

Tasks T16–T21. Add deterministic full replay, health, backup/restore and failure
injection. Only then introduce reviewed bounded HTTP adapters through
`shared/http.py` with pipeline-owned `network` settings and source-owned retry
and raw-archive behavior. Access/transport gates run before any probe. Permit
offline local completeness when fresh-source admission is still unresolved;
report acquisition as unavailable rather than calling a local profile live.

Dagster assets/job own acquisition → normalize/DQ → reviewed-control selection
→ build → publication dependencies. A local rebuild job has no acquisition edge;
a live job requires explicit source/access policy. New schedule definition
`presidential_research_refresh` starts stopped, and its frequency is selected
only after a source cadence/rights decision. Dagster metadata includes snapshot,
source health, profile and coverage, never secrets. Do not start NAAIM.

For actual handoff, import only approved raw captures, review workbook/control
manifest and source evidence into a separate destination root. Preserve hashes
and copy receipts; do not move donor files, whole `.local`, virtual environments,
Git history, secrets, runtime databases or deployment configuration. Rebuild in
the target and run both old/new pipelines against the same selected inputs.

Exit evidence: coherent crash recovery, revisions/reversion, cutoff selection,
replay and backup/restore pass; no-request health detects trailing failure and
coverage gaps. Frozen real-input parity and complete PRES browser acceptance
pass. Record release SHAs and evidence paths in all three handoffs. Only then
replace donor implementation with a documented canonical-package workflow under
a separate reviewed cutover change. Keep research protocols/results and old
exported manifests readable.

Optional production milestone: PostgreSQL adapter parity and any selected Delta
publication, source rights/cadence, designated operator and monitoring, durable
worker, approved retention and hosted access. None is labeled implemented or
production-ready from local SQLite or fixture success.

## Verification commands and evidence discipline

Currently executable donor baseline:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover \
  -s docs/research/002_PresidentialImpact/pipeline/tests -v
```

The current installed-package entrypoint is `python -m market_data_pipeline
presidential`. Run `--help` for the current options. The principal offline
migration is `import-frozen --donor-pipeline <path> --workbook <path> --workspace
<new-path>`; `check`, `health`, `replay`, `backup` and `restore` are separate
commands. The frozen import is bounded to the named checksummed archives and
does not fetch providers. Use an absent destination for import or replay.

After implementation, MarketMedallion focused tests are
`.venv/bin/python -m pytest -q tests/test_presidential_*.py`, followed once by the
full checks in the spec. QuantMeridian current focused baseline:

```bash
npm test -- src/lib/presidential/engine.test.ts src/lib/presidential/exports.test.ts src/components/presidential/ResearchCharts.test.ts
npm run typecheck
npm run build
npx playwright test test/presidential-impact.spec.ts --config=playwright.local.config.ts
```

Extend those tests for the new contracts, reader/routes and research views;
then run the appropriate regression suite and both dev/built-server route checks.
No CI test relies on a live provider. Capture actual commands, exit codes,
fixture versions, merge SHAs, numerical comparisons and screenshots at each gate.
Mark unrun live/backend tests as unrun. Do not reuse old donor research validation
as evidence that new code has passed.

## Failure and rollback design

| Risk / trigger | Required response |
| --- | --- |
| Arithmetic/formatting change during package move | Differential tests block G1; separate semantic change with new method version. |
| Raw/approval evidence absent or hash mismatch | No reproducible publication; keep the previous generation and explain missing dependency. |
| Pointer or warehouse/file commit interrupted | Root-locked recovery journal; staged content never read as committed. |
| Data correction or metadata/review revision | New captured version and snapshot; never rewrite frozen inputs or silently replace selected session. |
| Schema upgrade arrives before consumer upgrade | Explicit unsupported state; manual valid input still usable; keep previous selection. |
| One paired source has shorter/missing history | Common exact sample or unavailable state; no unequal-sample comparison. |
| Provenance digest differs across languages | Golden canonicalization cases block contract adoption; preserve distinct legacy digest. |
| Local endpoint exposes arbitrary paths or leaked errors | Fixed configured root, catalog allowlist, trusted peer/origin checks and sanitized failures. |
| Unrelated in-progress work overlaps | Small explicit patches on isolated branches; no broad reset, clean or automatic stash. |

Rollback disables the new local-consumer flag and returns PRES to manual import.
An operator can atomically select a previously validated immutable snapshot after
verification; record that selection as a new audit action. Stop the new refresh
schedule if enabled. Retain old code releases and donor artifacts until G6;
do not delete Bronze, approval versions or warehouse history to roll back a UI.

## Ownership and open decisions

Source maintainer owns input admission, DQ and capture failures; QuantMeridian
maintainer owns read validation, engine/UI and exports; research author owns
protocol changes and interpretation. Named operators and concrete alert routing
are assigned before scheduled/hosted operation. Health failure is a symptom to
investigate, never permission to substitute another source silently.

Remaining decisions: which actual frozen raw/review artifacts may transfer;
official-source rights/transport review; live profile refresh horizon/cadence;
retention and operator; whether to commission PostgreSQL/Delta and hosted access.
Recommended local defaults are defined above, so these do not block offline
implementation planning. No purchase, publication or source schedule is performed
by this documentation change.
