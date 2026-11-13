# FINRA short-sale-volume local configuration

The [example](pipeline.example.yml) runs invented fixtures only. Copy it to
ignored `pipeline.local.yml` in this folder, then choose a new `root`. Paths
resolve against the YAML's directory. The strict loader rejects unknown/duplicate
keys, invalid bounds, unreviewed manifests and missing operation grants. Loading
or validating configuration performs no network request or store creation.

```bash
cp config/finra_short_sale_volume/pipeline.example.yml config/finra_short_sale_volume/pipeline.local.yml
./scripts/run_finra_short_sale_volume.sh validate-config --config config/finra_short_sale_volume/pipeline.local.yml
```

Fields: `schema_id: finra-ssv-local-config/1`, explicit `root`, source `manifest`,
`operation`, `approved_use` and `limits.max_bytes`/`max_rows`. The only local modes
are `fixture` and `local_import`. The fixture profile is for invented input;
provider data requires its applicable approved-use profile with both local-import
and transform grants. The selected Query API route uses the existing accepted
terms/Public credential metadata; an approved local-file route has its separate
permission evidence. Credentials do not belong in this YAML, request receipts,
SQLite or logs. No authentication/acquisition adapter consumes them in this slice.

Defaults are 20 MiB and 5,000 rows per input. Only complete validated single-page
date/facility responses publish; a partial page remains audit/Bronze evidence.
The fixture receipt accompanies `tests/fixtures/finra_short_sale_volume/invented-valid.json`.
All values there are invented, not provider history. See the
[local runbook](../../docs/deployment/ssv_local_runbook.md) for actual commands,
root identity, retry and missing/pending states. Replay, live acquisition,
research exchange, jobs and alerts remain later tasks. No network probe, service
startup, `.env` loading or schedule activation occurs through these commands.

T09's `research-gold` command uses the same local config. Supply one or more
explicit `--scope` values; the existing manifest bounds the permitted population.
Optional `--as-of`/`--as-of-sequence`, `--lookback-days` and `--max-symbols` bound
research reads. No new YAML fields or warehouse migration is required. See the
[research contract](../../specs/spec012/research_gold_contract.md) for matched
coverage, prior-date and calendar-gap semantics.

T10 uses this same YAML for explicit `publish`, `published` and
`recover-publication`. No additional YAML keys are needed. `publish` accepts the
same date/scope/cutoff/lookback options as `research-gold`, plus an optional
canonical UUID `--generation-id`. Ingestion via `run` does not automatically
publish files. Only publication advances `parquet/current.json`.

All three publication commands accept separate operating budgets `--max-rows`
(default250000; range1–1000000) and `--max-bytes` (default134217728; range1–1073741824).
These bound total projection rows and logical/Arrow/file bytes, independently of
YAML input-page limits. `published --table gold` filters returned tables after
verifying the whole generation. Recovery may require the larger original budgets
when a generation was published with explicit overrides. Local publication history
is capped at10000 generations; there is no automatic deletion/retention in T10.
See [publication/recovery](../../docs/deployment/ssv_local_runbook.md#t10-typed-publication-and-recovery).
