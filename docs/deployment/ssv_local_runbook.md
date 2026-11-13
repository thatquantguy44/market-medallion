# FINRA short-sale-volume — local SQLite and Parquet runbook

Delivered by [SSV-T06–T08](../../specs/spec012/t06_t08_acceptance.md).
This is an offline local source-owned pipeline with strict configuration, immutable
Bronze, audited SQLite Silver/Gold, observed revisions/removals and explicit typed
Parquet publication with a frozen catalog. G1/G2,
real-source conformance and recurring operation remain open. No FINRA Dagster job
or Slack alert sensor exists yet.

## Setup and invented local run

Complete the repository's Python 3.12 setup. From the repository root, copy the
[fixture-default YAML](../../config/finra_short_sale_volume/pipeline.example.yml)
to `config/finra_short_sale_volume/pipeline.local.yml`. Select a fresh output root;
input and receipt paths below are relative to your working directory.

```bash
./scripts/run_finra_short_sale_volume.sh validate-config --config config/finra_short_sale_volume/pipeline.local.yml
./scripts/run_finra_short_sale_volume.sh run --config config/finra_short_sale_volume/pipeline.local.yml \
  --input tests/fixtures/finra_short_sale_volume/invented-valid.json \
  --receipt tests/fixtures/finra_short_sale_volume/invented-valid.receipt.json
./scripts/run_finra_short_sale_volume.sh gold --config config/finra_short_sale_volume/pipeline.local.yml \
  --trade-date 2026-11-09 --facility nyse_trf
./scripts/run_finra_short_sale_volume.sh history --config config/finra_short_sale_volume/pipeline.local.yml \
  --trade-date 2026-11-09 --facility nyse_trf --symbol 000SYN.A
./scripts/run_finra_short_sale_volume.sh coverage --config config/finra_short_sale_volume/pipeline.local.yml \
  --trade-date 2026-11-09 --facility nyse_trf --symbol 000SYN.A
./scripts/run_finra_short_sale_volume.sh audit --config config/finra_short_sale_volume/pipeline.local.yml --limit 20
```

The first fixture produces two Gold rows, exact decimal-text quantities and
per-observation ratios. Output names capture/version lineage, local knowledge
basis and publication status. Another `run` retains a new capture/audit but adds
no versions if quantities/identifiers are unchanged. A new observed correction
or reversion appends a version. Raw source representations remain immutable.
Source dates and symbols are not shifted or crosswalked.

The script works from other directories with this project installed and explicit
config/input paths. Set `FINRA_SSV_PYTHON` to another installed interpreter;
otherwise it uses the repository `.venv`. Equivalent module entrypoint:
`python -m market_data_pipeline.pipelines.finra_short_sale_volume`.
No `.env` is loaded. See [configuration](../../config/finra_short_sale_volume/README.md)
for provider-input admission; fixture grants do not authorize real-data use.

## Cutoffs and complete membership

`gold` accepts `--as-of` as an aware ISO timestamp and optional
`--as-of-sequence` for an equal-time capture boundary. Use actual capture clocks
reported in audit, never trade date or a guessed historical release time. Historical
imports are current-vintage data; local first-seen is actual archive observation.
Unknown retrieval and provider publication stay null. Source quantities are exact
text; only ratios use the fixed 34-digit half-even method.

A complete response starts at offset zero and has all Record-Total rows. A valid
complete empty array/204 can retract prior membership. Partial pages publish no
rows; failed/invalid refreshes preserve the last committed partition. Cross-page
assembly is not implemented. As-of membership selects the last complete capture
at/before the local time/sequence cutoff, including valid empty replacements.

Coverage exposes latest local attempt independently of committed membership:
MISSING means no local attempt; PENDING is an unresolved local STARTED attempt;
FAILED, PARTIAL, COMPLETE and VALID_EMPTY are distinct. These states do not prove
provider release/holiday status. A symbol is NO_REPORTED_ROW only under complete
committed evidence excluding it; otherwise it is UNKNOWN. Never substitute zero
shares or a denominator for absent evidence. Prior Gold may remain usable while
a later failed/partial attempt is reported.

## Retry, failure and storage identity

`run` prints a `capture_id`. Resume an archived capture without new source input:

```bash
./scripts/run_finra_short_sale_volume.sh ingest-capture --config config/finra_short_sale_volume/pipeline.local.yml \
  --capture-id <reported-capture-uuid>
```

Terminal retries verify the original receipt/profile and return its exact result;
they do not add attempts or versions. Nonterminal execution failures retain the
capture and a fixed-code failed attempt. A process interruption leaves STARTED
visible; resume that capture before another. An old failed capture cannot be
promoted after a later terminal capture: ordering uses original local clock plus
sequence, and regression refuses. A postcommit response failure can leave APPLIED
evidence; inspect audit and retry the same capture to resolve it. Partial/rejected
terminal results return CLI exit 2 and remain immutable. Generic execution/config
failure also returns 2 without raw diagnostic or credential material.

`bronze/` is T05's immutable archive. `warehouse/ssv.sqlite` contains source-owned
identity, attempts, terminal captures, versions, capture membership, partition
pointers and the Gold view. Canonical JSON version bodies retain exact quantity
text, with explicit SQL natural-key/time columns. The root `warehouse.json`,
warehouse identity and SQLite metadata must agree. Reads verify actual schema,
foreign keys, membership/version/result checksums and partition scope. Files and
sidecars reject symlinks. Missing/damaged stores or bindings refuse recreation;
initialization damage requires explicit investigation, not deleting identity files.
Do not move/copy a bound warehouse to a new root as a restore procedure.

New versions are bounded at 250,000; raw reservations at T05's 100,000. Audit/history
queries accept limits 1–1,000. Database changes and the partition pointer commit in
one transaction. T10 separately publishes immutable database/file generations;
see the recovery protocol below.
Replay/backup tooling is not delivered here. Existing operational databases,
source samples, services and schedules were not used for local acceptance.

## T09 research Gold

The separate read-only research command uses the same configuration and captured
partitions. For the invented single-facility example above:

```bash
./scripts/run_finra_short_sale_volume.sh research-gold --config config/finra_short_sale_volume/pipeline.local.yml \
  --trade-date 2026-11-09 --scope nyse_trf
```

Repeat `--scope` to request explicit disjoint facilities, or use `--scope listed_trfs`
for all three verified TRFs. Every selected facility must have a complete partition
and each symbol must have a reported row in all of them for an aggregate metric.
Missing partitions return INCOMPLETE_POPULATION and CLI exit2; missing reported
rows withhold population quantities rather than supplying zero. Complete/valid-empty
reports return0. Overlap and excluded scopes fail with exit2. This command creates
no captures, versions or publication changes and does not perform a network probe.

Optional `--as-of` and `--as-of-sequence` apply to all current and prior components.
`--lookback-days` defaults to366 (range1–3660); `--max-symbols` defaults to15000
(range1–250000). The prior date is the nearest earlier complete date for that exact
population at the cutoff, within lookback. Missing membership on the selected
prior date is not skipped to find an older value. The first isolated fixture run
has no prior date, so prior values and changes are null with an explicit reason.

`prior_trade_date` and `calendar_day_gap` are report fields; calendar gaps include
weekends without claiming missing trading sessions. Row fields include current/
prior ratios, ratio changes, missing-row facilities and component version/capture/
source-row lineage. Ratios are ratio-of-summed-quantities using the fixed method.
Current/prior selections share one SQLite snapshot, including while another
writer commits. `evidence_known_at` describes the latest local capture used by
the report, not provider publication. Operational freshness remains separately
inspectable with `coverage` and `audit`.

[T09 acceptance](../../specs/spec012/t09_acceptance.md) records numeric, missingness,
cutoff, concurrency and actual-script evidence. T10 adds the publication below.

## T10 typed publication and recovery

After a complete local ingestion, explicitly publish the requested date/facility
projection. The example includes Silver history within the bounded lookback,
current Gold, scoped research, observed coverage, capture/membership and audit:

```bash
./scripts/run_finra_short_sale_volume.sh publish --config config/finra_short_sale_volume/pipeline.local.yml \
  --trade-date 2026-11-09 --scope nyse_trf
./scripts/run_finra_short_sale_volume.sh published --config config/finra_short_sale_volume/pipeline.local.yml --table gold
./scripts/run_finra_short_sale_volume.sh recover-publication --config config/finra_short_sale_volume/pipeline.local.yml
```

Use `--scope listed_trfs` only when all three current partitions are complete.
`publish` accepts research cutoff/lookback/symbol options. Current incomplete
population refuses; empty/removal and partial-refresh evidence remain distinct.
Resume any STARTED source attempt with `ingest-capture` before publishing.

The operational warehouse can advance after publication. Published consumers use
`parquet/current.json` and the frozen `parquet/catalog.sqlite` facts; they must
not glob `snapshots/` or combine published files with newer operational Gold.
`published` verifies the whole selected generation even with `--table` filtering.
It checks root/source/catalog identity, schema, row/file hashes, typed fields and
all catalog rows. Decimal quantities/ratios use an exact struct (negative, digits,
exponent), with shares/unit-1 metadata; use `data.snapshot.decode_decimal` to
reconstruct exact text, including large values, scale and signed zero.

Publication shares `.ingest.lock` with source writers. It stages private files,
commits frozen facts plus COMMITTED journal metadata, finalizes/fsyncs files,
atomically replaces the sole pointer, then marks PUBLISHED. An ingestion APPLIED
record does not prove publication. If interrupted, run `recover-publication`
before a new publish. Recovery uses the original staged/final files and frozen
facts even if ingestion is newer. It never rebuilds a pending snapshot from
current operational tables. An unchanged current request/state reuses its UUID;
returning to an earlier projection creates a new event. Supplying an old UUID
cannot roll the pointer back.

While the final mark is pending, a verified new pointer can report COMMITTED,
null `published_at` and `recovery_required: true`. Otherwise the prior coherent
PUBLISHED pointer remains readable, with recovery required. Missing/corrupt
pending artifacts block recovery and new publication. A corrupt selected pointer,
file, schema, catalog or binding refuses; do not delete/recreate bindings or edit
the pointer to bypass this. Preserve evidence and investigate. Recovery removes
only verified owned uncommitted staging orphans. No published files are deleted.
A fresh catalog with no committed generation returns NO_PUBLICATION on recovery;
`published` returns exit2 until a generation exists.

Default publication/read budgets are250000 rows and128MiB total logical/Arrow/file
bytes. Flags `--max-rows`/`--max-bytes` are separate from input YAML limits; bounded
overrides are documented in the config guide. Publication history caps at10000
generations. A root-bound catalog cannot be moved to another root as a restore.
This is local POSIX publication; replay, backup tooling, research exchange and
orchestration remain later tasks. [T10 acceptance](../../specs/spec012/t10_acceptance.md)
records the seven injected failure boundaries, numeric parity and CLI evidence.
