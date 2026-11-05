# SEC fails-to-deliver configuration

This folder belongs only to `sec-fails-to-deliver`. It is independent of AAII
and NAAIM settings. The implemented loader validates configuration without
connecting to PostgreSQL/Databricks or reading secrets. `inspect` can select an
approved local file entirely from this config. T18's exact numeric projections,
SQL bindings, Arrow schemas and Spark schema descriptions are implemented.
T04 implements SQLite persistence and database Gold. PostgreSQL/Delta adapters
are **not implemented yet**; type tests are not live backend compatibility evidence.

```text
config/sec_fails_to_deliver/
  pipeline.example.yml   # checked-in, editable starting settings
  pipeline.local.yml     # your ignored environment-specific copy
  .env.example           # checked-in password variable name, no credential
  .env                   # your ignored password file
  README.md
```

## Decisions and starting recommendations

| Decision | Example setting | What you need to supply or decide |
| --- | --- | --- |
| Initial operational warehouse | `warehouse: postgres` | User selected PostgreSQL; its adapter remains T19. Explicit `sqlite` selects the delivered T04 local slice; there is no fallback. |
| Pipeline-owned durable root | `../../data/sec_fails_to_deliver/postgres` | Keep this local path or choose a mounted durable directory. Use a distinct root for another backend/warehouse identity. |
| PostgreSQL namespace | database `market_medallion`, schema `sec_ftd`, user `sec_ftd_writer` | These are illustrative names, not provisioned objects. Set your actual host, port, database, login, optional role, schema and SSL mode. |
| Databricks publication | `enabled: false` | Leave disabled until a catalog, schema and writable `/Volumes/<catalog>/<schema>/<volume>/...` staging path are available. Publication later runs in an authenticated Spark session. |
| Quantity projection | precision38, scale0 | `quantity_precision` is editable from1–38; quantities remain whole shares. Exact text is retained regardless of projection. |
| Price projection | precision38, scale12 | `price_precision` and `price_scale` are editable; this example allows26 digits before and12 after the decimal point. Source scale remains in exact text. |
| Selected input partitions | Two approved September2026 archives | Add named manifest/input pairs. Their source manifests retain explicit dates, member name and resource budgets; there is no filename/date inference. |

Decimal38/12 is a starting recommendation, not a statement that every historical
price fits. The implemented codecs preflight actual values against this config
and reject a value needing rounding or overflow. Schema changes alter the
`storage_contract_id`; T04 persistence rejects a conflicting contract on an
existing root. Validation alone still performs no root/database I/O.

Operational choices live in YAML. CUSIPs as strings, source date labels, UTC
knowledge clocks, exact numeric text and rejected lossy conversion are enforced
data rules. The config names those rules explicitly but cannot disable them.
Automatic schema evolution is not supported. Other pipelines have their own
configs rather than inheriting these settings.

The optional `network` section uses the common
[proxy/direct contract](../../docs/development/outbound_http.md): mode,
preference/fallback, proxy environment-variable name, no-proxy hostname list,
timeouts and a config-relative CA bundle. It is validated without resolving
proxy secrets or testing connectivity. Current SEC FTD commands use local files
and never probe; these settings do not implement or enable an SEC HTTP adapter.
Network changes do not alter the storage contract or require a new data root.

## Create local settings

From the repository root:

```bash
cp config/sec_fails_to_deliver/pipeline.example.yml \
  config/sec_fails_to_deliver/pipeline.local.yml
cp config/sec_fails_to_deliver/.env.example config/sec_fails_to_deliver/.env
```

Edit the local YAML and password file. PostgreSQL credentials are resolved only
by the future connection adapter, using the variable named by `password_env`.
The current loader/inspection never reads that variable. There is no global
AAII-config fallback, implicit environment interpolation or automatic `.env`
execution. A later connection command can inherit the trusted local environment:

```bash
set -a
source config/sec_fails_to_deliver/.env
set +a
```

Paths resolve relative to the YAML file's directory, regardless of the shell's
working directory. Absolute paths are allowed. Use `--config` to choose a file;
there is no hidden search/merge precedence. Config-mode inspection uses its
named partition with no `--input` override. Existing explicit `--manifest` plus
`--input` inspection remains supported.

## Implemented commands

These commands work with the checked-in example and need no database password:

```bash
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver validate-config \
  --config config/sec_fails_to_deliver/pipeline.example.yml
.venv/bin/python -m market_data_pipeline.pipelines.sec_fails_to_deliver inspect \
  --config config/sec_fails_to_deliver/pipeline.example.yml \
  --partition september_2026_a
```

Use `--partition september_2026_b` for the second approved file. Substitute your
`pipeline.local.yml` path to select local settings. `validate-config` checks
structure, settings and path resolution; it does not open input/manifest files,
check endpoints, authenticate or certify numeric data. `inspect` then validates
the selected source manifest and archive and writes no data.

Unknown/duplicate YAML keys, literal passwords/tokens, malformed types, invalid
numeric ranges and unsupported identifiers fail before input/service I/O.
SQL namespaces/table prefix currently use simple lowercase identifiers up to63
characters. Enabled Databricks settings must include all three target fields.
T04 adds configured `run` and bounded `gold`; see the
[local runbook](../../docs/deployment/sec_ftd_runbook.md). The example's PostgreSQL
selection deliberately errors before input/root I/O until T19; use an explicit
SQLite local config. T05 adds observed revisions and cutoff queries. PostgreSQL
connections and Delta publication remain in [Spec011 tasks](../../specs/spec011/todo.md).
T06 adds `replay --source-root ... --config <destination.yml> --max-captures 1000`.
The destination selects SQLite, disabled Databricks, matching storage settings
and an empty nonoverlapping root. Configured live partition paths and network
settings are not used during replay; original artifacts/outcomes supply them.
See the [runbook](../../docs/deployment/sec_ftd_runbook.md) for safe reconstruction.

Each partition optionally names `coverage_evidence`, relative to this YAML's
directory. Omission or null retains PARTIAL coverage. Supply reviewed JSON only
when the exact archive hash, member, date inventory and complete-file evidence
have been checked. This enables file-scoped COMPLETE coverage and withdrawals;
it does not certify the source's entire history. The exact JSON contract and
Gold cutoff flags are in the [runbook](../../docs/deployment/sec_ftd_runbook.md).
Evidence references are operator assertions, not automatically verified links.
Keep local proof outside Git, for example under ignored `data/sec_ftd/`.

## Implemented storage types

The source-owned `storage_schemas(config.storage)` API provides fact, coverage,
audit and publication record schemas. Each can normalize values, encode/decode
SQLite and native PostgreSQL bindings, generate explicitly typed Arrow tables,
validate a returned Arrow table and describe a Spark StructType as JSON. Exact
quantity/price source text stays separate from numeric projections. The parser
now retains these original tokens. No Spark installation is needed for this API.

Numeric/date preflight passes all117,025 approved September rows using this
example profile. Ignored aggregate evidence is
`data/sec_ftd/storage-types-202609.json`. It creates no warehouse state or
historical first-seen clocks. See the
[storage contract](../../specs/spec011/data_contract.md#11-implemented-t18-record-schemas-and-serialization)
for field semantics and the remaining live backend verification requirements.

T04 also imports both approved archives into an isolated SQLite verification
root, retaining 117,025 facts with actual November 3 first-seen clocks. The ignored
config is `t04-verification.local.yml`; data lives under
`data/sec_fails_to_deliver/t04-verification`. An unchanged repeat adds zero facts.
Aggregate evidence is `data/sec_ftd/t04-verification.json`. This does not change
the PostgreSQL operational default or enable Databricks.
