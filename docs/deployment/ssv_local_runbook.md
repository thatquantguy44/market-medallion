# FINRA short-sale-volume — local SQLite runbook

Delivered by [SSV-T06–T08](../../specs/spec012/t06_t08_acceptance.md).
This is an offline local source-owned pipeline with strict configuration, immutable
Bronze, audited SQLite Silver/Gold and observed revisions/removals. G1/G2,
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
one transaction; file/Parquet publication and its recovery journal remain T10.
Replay/backup tooling is not delivered here. Existing operational databases,
source samples, services and schedules were not used for local acceptance.
