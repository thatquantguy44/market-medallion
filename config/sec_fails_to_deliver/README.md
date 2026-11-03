# SEC fails-to-deliver configuration

This folder belongs only to `sec-fails-to-deliver`. It is independent of AAII
and NAAIM settings. The implemented loader validates configuration without
connecting to PostgreSQL/Databricks or reading secrets. `inspect` can select an
approved local file entirely from this config. T18's exact numeric projections,
SQL bindings, Arrow schemas and Spark schema descriptions are implemented.
Warehouse persistence and Delta publication are **not implemented yet**; type
tests are not evidence of live backend compatibility.

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
| Initial operational warehouse | `warehouse: postgres` | PostgreSQL is recommended; `sqlite` is available for the first offline persistence loop when delivered. Neither SEC storage adapter exists yet. |
| Pipeline-owned durable root | `../../data/sec_fails_to_deliver/postgres` | Keep this local path or choose a mounted durable directory. Use a distinct root for another backend/warehouse identity. |
| PostgreSQL namespace | database `market_medallion`, schema `sec_ftd`, user `sec_ftd_writer` | These are illustrative names, not provisioned objects. Set your actual host, port, database, login, optional role, schema and SSL mode. |
| Databricks publication | `enabled: false` | Leave disabled until a catalog, schema and writable `/Volumes/<catalog>/<schema>/<volume>/...` staging path are available. Publication later runs in an authenticated Spark session. |
| Quantity projection | precision38, scale0 | `quantity_precision` is editable from1–38; quantities remain whole shares. Exact text is retained regardless of projection. |
| Price projection | precision38, scale12 | `price_precision` and `price_scale` are editable; this example allows26 digits before and12 after the decimal point. Source scale remains in exact text. |
| Selected input partitions | Two approved September2026 archives | Add named manifest/input pairs. Their source manifests retain explicit dates, member name and resource budgets; there is no filename/date inference. |

Decimal38/12 is a starting recommendation, not a statement that every historical
price fits. The implemented codecs preflight actual values against this config
and reject a value needing rounding or overflow. Schema changes alter the
`storage_contract_id`; future persistence must reject a conflicting contract on
an existing root. That guard is still part of T04, not implemented by validation.

Operational choices live in YAML. CUSIPs as strings, source date labels, UTC
knowledge clocks, exact numeric text and rejected lossy conversion are enforced
data rules. The config names those rules explicitly but cannot disable them.
Automatic schema evolution is not supported. Other pipelines have their own
configs rather than inheriting these settings.

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
`run`, PostgreSQL connections, root-binding enforcement
and Delta publication remain in [Spec011 tasks](../../specs/spec011/todo.md).

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
