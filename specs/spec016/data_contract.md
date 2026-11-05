# Presidential research publication and PRES boundary contract

Status: proposed v1; authoritative companion to [Spec016](README.md).

## 1. Contracts and authorities

There are three separately versioned identities:

1. **Dataset:** existing strict `ResearchDataset` schemaVersion **1**, consumed
   by `src/lib/presidential/validation.ts` in QuantMeridian. Keep its exact fields.
2. **Publication bundle:** new `presidential-research-bundle/1`, owned here.
   It selects immutable dataset bytes, replay manifests and supplementary ledgers.
3. **Analysis run:** current `presidential-run/1` remains import/export compatible;
   pipeline-aware exports introduce explicit `presidential-run/2`. No silent
   mutation of the meaning of an existing version or hash.

The TypeScript dataset interface is the existing shape authority. During the
port, check in a strict language-neutral schema plus valid/invalid fixtures under
`contracts/presidential_research/v1/` in MarketMedallion. Pin identical schema and
fixture hashes in QuantMeridian's `contracts/presidential_research/v1/`.
Updates are explicit coordinated changes; no build downloads a floating sibling.

## 2. Immutable acquisition and normalized grains

| Layer / record | Logical grain | Required behavior |
| --- | --- | --- |
| Bronze capture | source ID + capture sequence + payload SHA-256 | Exact bytes, selected source config, safe URL/headers, source/parser/method revision, retrieval/import time, status, byte count and raw pointer. |
| Monthly market fact | source ID + month + construction | Decimal-text input token and normalized return; do not mix FF and Shiller constructions. |
| Policy fact | source ID + observation date | FEDFUNDS in percent units, source date preserved, missing markers explicit in parse audit. |
| Shiller context | source ID + month | Price, annual dividend and CAPE with missingness preserved. |
| Political candidate | source ID + Congress/candidate key | Review-only raw candidate/count text and lineage; no automatic authority upgrade. |
| Reviewed control | control ID + approval version | Dated half-open interval; cited manual source, approval evidence and workbook/config/controls digests. |
| Gold dataset | profile + construction + sample + method + selected fact/review versions | Exact v1 dataset plus its replay sidecar. |
| Publication | snapshot ID | Immutable complete bundle selecting one coherent set of Gold and supplementary artifacts. |

Silver additionally records `version_id`, normalized `content_hash`,
`capture_sequence`, `retrieved_at`, `first_seen_at`, nullable
`provider_published_at`, raw lineage and schema/method versions. A revision is a
new version, including a reversion to an earlier value. Identical consecutive
content creates no new fact version; it still has a new acquisition audit.

Use root-locked monotonic capture ordering to resolve equal timestamps.
Replay preserves original sequence, IDs, first-seen and success/failure outcomes.
Imported old raw archives retain evidenced retrieval time, but destination
admission time is separately recorded; do not invent an old destination first-seen.
When historical ordering cannot be established, block ambiguous as-of claims.
Observed-as-of queries select captured versions by knowledge cutoff before
assembly. Assumed fundamental availability is separate from these knowledge clocks.

SQLite stores exact source decimals as text; Parquet uses explicitly declared
decimal/date/UTC timestamp types after checked bounds. Gold v1 intentionally
uses finite JSON numbers matching the existing algorithm; do not claim exact
decimal return arithmetic or change donor rounding in the parity slice.
Do not cast an unrepresentable source token to a plausible approximate value.

## 3. Controls and quality gates

`controls.csv` columns remain:
`id,startDate,endDate,presidentId,presidentName,presidentParty,houseControl,senateControl,sourceId`.
Allowed labels: `D,R,OTHER,TIED,UNKNOWN`; unknown/tied/other is not split Congress.
Reject invalid dates, overlaps, duplicate IDs, unregistered/ineligible source IDs.
Approval promotion requires APPROVED status, primary locator, evidence note,
reviewer and reviewed-at value, plus workbook/config/control hashes and row-level
evidence. Partial approved controls may validate, but a build must prove its
required dates are covered. Cycle builds additionally require year-end coverage.

Monthly return coverage is unique and contiguous for each requested annual
window, with the preceding December endpoint available. Levels must be finite
and positive. Missing months or Shiller dividends block the affected required
dataset, and therefore promotion of the required bundle. Candidate parse failure
does not erase previously reviewed evidence; explicit input selection determines
whether that candidate source is a required dependency of the selected build.

## 4. PRES dataset v1, unchanged

Top-level allowed fields only:
`schemaVersion,id,title,kind,currency,nominalReal,returnBasis,sources,levels,controls,windows,fundamentals`.

| Field group | Required constraints |
| --- | --- |
| Identity | `schemaVersion:1`, explicit USD/NOMINAL and TOTAL_RETURN or PRICE_RETURN; pipeline-generated kind remains USER_SUPPLIED. |
| `sources` | Unique `{id,title,locator,note?}`; locator string or null, no additional provenance fields. |
| `levels` | Unique strict ISO date, positive finite numeric value, registered sourceId. |
| `controls` | Exact existing camelCase interval fields and allowed labels. |
| `windows` | Unique id, label, CALENDAR_YEAR or PRESIDENTIAL_PERIOD, start/end/decision dates and boolean complete. |
| `fundamentals` | date, nullable availableDate, nullable valuation/policyRatePct, one valuationDefinition and registered sourceId. |

All numbers remain numbers, not numeric strings; missing observations remain
null or audited exclusions as the existing contract requires. Annual default
endpoint tolerance is zero; supplied alternatives remain explicit 0–31 days.
No interpolation. Fundamentals are eligible only if observation and non-null
availability dates are no later than decisionDate. Richer field-level timing,
review and construction metadata belong in sidecars, not unknown v1 fields.

## 5. Bundle filesystem and publication transaction

Proposed root is configured explicitly, e.g. `data/presidential_research/`.

```text
bronze/<source_id>/<capture_id>/payload + receipt.json + config snapshot
silver/                         # pipeline-owned SQLite and typed snapshots
review/                         # immutable workbook/evidence/approval versions
gold/snapshots/<snapshot_id>/
  catalog.json
  datasets/<five-existing-dataset-filenames>
  datasets/<five-existing-manifest-filenames>
  evidence/<comparison, inherited and cycle artifacts>
gold/current.json
audit/                          # run ledger and publication recovery journal
```

The existing dataset manifest naming is `<dataset-basename>.manifest.json`.
Retain `public-series-comparison.json`, `public-series-comparison.csv`,
`inherited-conditions-join-audit.json`, `inherited-conditions-ledger.csv`,
`presidential-cycle-ledger.csv`, `presidential-cycle-summary.json` and the
derived `shiller_total_return_monthly.csv`, with inventoried role and digest.
These basenames match the inspected donor writer functions; filenames are not a
semantic substitute for roles.

Build under a private staging directory on the destination filesystem; validate
all required datasets/sidecars and fsync. Commit the warehouse snapshot journal
as pending, finalize the immutable generation, atomically replace `current.json`,
then mark published. Recovery under the writer lock reconciles each crash
boundary, including a replaced pointer whose final journal mark was interrupted.
Readers pin one generation per load and verify it; they never glob all snapshots
or independently select “latest” for each constituent. Keep the last good pointer
on parse/DQ/approval/publication failure. Concurrent writers serialize.

`current.json` has exactly `schemaVersion:"presidential-research-pointer/1"`,
`snapshotId`, `catalogPath`, and `catalogSha256`. Path resolves strictly beneath
the configured root and identifies one immutable catalog.

## 6. Catalog and identity

Catalog schema `presidential-research-bundle/1` declares:

| Field | Type / meaning |
| --- | --- |
| `snapshotId` | SHA-256 of the canonical semantic inventory below. |
| `profileId`, `decisionPolicy` | Frozen or refresh profile; explicit assignment. |
| `producer` | repository URL, full commit SHA or null, dirty flag, source-tree digest, pipeline method version. Unknown revision remains null. |
| `createdAt` | UTC creation time; excluded from numerical identity. |
| `sources` | IDs, locators, roles, captured evidence hashes, terms/attribution references and admitted output scope. |
| `inputInventory` | Raw/config/review identifiers and SHA-256 digests; private filesystem paths never sent to the browser. |
| `datasets` | Unique selection key, dataset ID/title, construction, sample ID, year bounds/window count, relative file/manifest paths, byte SHA-256, canonical dataset SHA-256 and sizes. |
| `artifacts` | Unique role/key, relative path, schema version, SHA-256, bytes and bound dataset selection keys. |
| `quality` | Passed/failed checks, blocking issues, required versus optional artifacts and coverage. |
| `limitations` | Construction, vintage, assumed-availability, review and replication limitations. |

Canonical JSON for identities is recursively key-sorted UTF-8 with finite
numbers, no extra whitespace and deterministic array ordering. A cross-language
fixture must pin Python/JS canonical output, including floating-point formatting;
do not assume their default serializers match. Preserve the donor's canonical
digest separately if the chosen shared encoder differs.

`snapshotId` hashes profile/decision policy, producer method/source-tree identity,
the selected input version inventory, and ordered dataset/artifact byte digests.
Exclude `createdAt`, itself and its catalog/pointer hashes to avoid recursion.
`catalogSha256` hashes exact finalized catalog bytes. Identical semantic
publication is a no-op or reuses the immutable generation; a conflicting existing
snapshot directory fails integrity checking rather than being overwritten.

The dataset JSON byte hash, canonical dataset hash, raw payload hashes, producer
tree digest, snapshot ID and PRES numerical run ID are distinct fields. A
normalized dataset digest is never called a raw-input hash. Historical donor
sidecars may lack some metadata; import records explicit unknowns and prevents
full-replay certification until required inputs/evidence are present.

## 7. Local application read contract

Proposed independently configured server variables:
`PRES_RESEARCH_LOCAL_ENABLED=1` and absolute `PRES_RESEARCH_ROOT`.
Default off; initially local/private only, using trusted peer-address and origin
checks consistent with QuantMeridian's NAAIM local reader. Do not reuse NAAIM
configuration, paused capture jobs or source permissions.

Proposed GET routes:

- `/api/presidential/research/catalog`: read/validate the committed pointer and
  return sanitized catalog, pinned snapshot ID and read receipt.
- `/api/presidential/research/snapshots/{snapshotId}`: fetch the explicitly
  catalog-selected immutable generation, returning validated datasets, replay
  provenance and normalized supplementary evidence. Reject unknown IDs.

The second route reads the selected immutable generation even if current.json
advances after catalog selection. Catalog/read APIs accept no source URL, local
path, arbitrary date override or simulation flag. Unsupported query parameters
are rejected. File reads must resist traversal/symlink escape and file-change
races; verify the bytes actually read against recorded lengths/digests.
Do not return private paths, reviewer PII beyond admitted display fields, proxy
settings or exception traces. Use no-store responses.

Bounds: one dataset ≤5 MiB, ≤100,000 records per dataset array, catalog ≤1 MiB,
at most five datasets and 32 evidence artifacts, total snapshot response ≤40 MiB.
Stream/read bounded bytes before JSON parsing; validate CSV row/cell limits and
archive expansion limits where workbook/ZIP ingestion is supported. Raise limits
only through a documented contract revision. Never silently truncate data.

Errors use `{error:{code,message},selection:null}` with sanitized messages:
403 LOCAL_ONLY, 400 INVALID_REQUEST, and 503 LOCAL_NOT_CONFIGURED,
SOURCE_UNAVAILABLE, INTEGRITY_FAILURE or UNSUPPORTED_SCHEMA. A blocked source
capture is distinct from an invalid committed snapshot. Expose last-capture
health separately; it must not invalidate an explicitly pinned historical run.

## 8. Supplementary views and exports

Paired comparison joins on exact year, window dates and decision policy; show
the independently calculated returns and delta. Primary shared comparison uses
the common-sample CRSP dataset and Shiller dataset. Inherited view uses the two
68-window frozen datasets, reconciles the join audit and retains field-level
availability assumptions. Return calculations use the existing PRES engine.

Cycle record fields preserve the donor ledger: year, termStartYear,
termCycleYear, inaugurationTransition, presidentChangedWithinWindow,
endOfYearPresidentId/Name/Party, crspMarketProxyReturnDecimal and
shillerSpCompositeApproximationReturnDecimal. A strict typed adapter validates
those fields, boolean representations and bindings to the selected paired
sample. The UI displays all-years and flagged-transition views of the same rows;
flagging alone does not drop rows. Summary count, administration count, mean,
median, sample standard deviation (null for n<2), extrema and best/worst years
reconcile with the donor method, including deterministic tie-breaking.

Proposed `presidential-run/2` adds an optional structured `pipelineProvenance`
receipt: snapshot/catalog IDs/digests, selected dataset/sidecar/input digests,
producer method/code/tree identity, profile/assignment, received-at time and
validation scope. Preserve normalized dataset/config/engine hashes and current
analysis reconciliation. Provenance hashes bind the receipt separately; export
time/author notes do not alter numerical runId. Manual imports can have null
pipeline provenance; never promote an unaccompanied JSON import to a verified
pipeline snapshot. Support replay/export of existing v1 runs explicitly.

Comparison and cycle exports include selected dataset identities, exact sample,
assignment and view state with a distinct view/config hash. Existing CSV formula
escaping and SVG/PNG provenance labels remain required. Exported replay must
reproduce analysis without the original pipeline path or running source service.
