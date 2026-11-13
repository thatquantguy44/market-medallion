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
root identity, retry and missing/pending states. Live acquisition,
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

T11 adds `replay --source-root <original-root> --config <destination-yaml>`.
The config's `root` selects a separate absent/empty destination, while its
manifest/grants and input limits must admit the original input kind and parser
profiles. Start from another copy of the example and choose a new root; existing
store roots are refused. No new YAML keys or credentials are required.

Replay-specific flags: `--max-captures` defaults to 1000 (range1–10000),
`--max-state-rows` to 250000 (range1–1000000), and `--max-total-bytes` to 134217728
(range1–1073741824). The total byte limit covers the logical state plus archived
payload/receipt/outcome/sequence files. YAML `limits` bound each input and original
parser profile. At most 16 original profiles per capture and 100000 original
sequence reservations are accepted. Replay never probes the network. It requires
Bronze plus the verified terminal source warehouse, not a database-free backup.
See [replay operations](../../docs/deployment/ssv_local_runbook.md#t11-offline-replay).

T12 uses separate [acquisition settings](acquisition.example.yml); local YAML
and commands stay compatible. Copy to ignored `acquisition.local.yml`, record
your applicable accepted API terms/reference and Public credential type, and
provide explicit acquire/transform grants. The example is pending and `live:false`;
it deliberately refuses provider execution until configured. Credentials are
**environment-variable references**, defaulting to `FINRA_API_KEY` and
`FINRA_PASSWORD_SECRET`. Export values privately; the library/script never loads
`.env` automatically and never stores token bodies or cookie headers.

```bash
./scripts/run_finra_short_sale_volume.sh acquisition-plan --config config/finra_short_sale_volume/acquisition.local.yml \
  --start-date 2026-11-09 --end-date 2026-11-09 --scope nasdaq_chicago_trf --overlap-days 0
./scripts/run_finra_short_sale_volume.sh acquire --config config/finra_short_sale_volume/acquisition.local.yml \
  --start-date 2026-11-09 --end-date 2026-11-09 --scope nasdaq_chicago_trf --overlap-days 0 --live
```

Planning validates admission/bounds but creates no store, reads no credentials
and makes no probe. Execution requires both config `live:true` and CLI `--live`.
Dates must be within the current New York rolling twelve-month window. Default
backward overlap is 2 calendar days, included in limits; it re-fetches already
complete dates. `--overlap-days 0` disables expansion explicitly. No calendar or
release inference is made. `network` uses the repository's shared settings,
including direct/proxy preference, fallback, NO_PROXY, timeouts and verified CA.
No route change occurs after startup; redirects remain disabled.

Acquisition limits (defaults; finite ranges):

| Setting | Default | Range |
| --- | --- | --- |
| `max_days` | 7 expanded calendar days | 1–31 |
| `max_partitions` | 21 date/facility pairs | 1–93 |
| `max_requests` | 500 charged units | 1–1000 |
| `max_response_bytes` | 3000000 | 1–3000000 |
| `max_total_bytes` | 20971520 | 1–1073741824 |
| `page_rows` | 5000 | 1–5000 |
| `max_pages` | 3 per pass | 1–20 |
| `max_attempts` | 3 per request | 1–5 |
| `max_wait_seconds` | 60 cumulative | 0–300 |
| `max_duration_seconds` | 300 | 1–3600 |

Request preflight reserves two possible startup probes and every page/retry in
both passes plus auth/metadata. Charged request units are an upper bound, not an
assertion that both routes were probed. Bytes include auth/metadata/error bodies;
actual reading reserves a bounded overflow sentinel. A Retry-After outside the
remaining sleep/duration budget stops. Capacity is `page_rows * max_pages` rows
per partition; exceeding any bound leaves explicit incomplete evidence.

Multi-page completion requires two agreeing complete passes. Four raw pages plus
one derived manifest represent the two-page conformance case. Subsequent/
verification pages are evidence-only and cannot promote a changed short response.
Collection parsing/replay must cover the original profile: defaults are 15000 rows
and 18000000 bytes; increase local/replay YAML input limits explicitly when needed.
The existing warehouse version/storage bounds still apply. Publication stays
explicit. [Runbook](../../docs/deployment/ssv_local_runbook.md#t12-bounded-admitted-acquisition)
explains journals, incomplete exit2 and recovery. Jobs and alerts remain absent.
