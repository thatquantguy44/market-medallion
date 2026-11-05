# Reg SHO threshold-list configuration

This folder configures the source-owned Nasdaq/NYSE threshold pipeline. Copy
`pipeline.example.yml` to ignored `pipeline.local.yml`, choose a durable empty
root, and add only explicitly selected provider/market/date partitions. Relative
paths resolve from the YAML file. The checked-in example keeps acquisition and
the Dagster schedule disabled.

Each partition binds an exact provider, market, list date, physical manifest,
local input and optional official URL. Nasdaq URLs must contain the selected
`nasdaqthYYYYMMDD.txt`; NYSE URLs must use the public `selectedDate=YYYY-MM-DD`
and exact market parameter. A mismatched URL/date/market fails during config
loading. Provider formats are parsed independently even though both currently
normalize to the same membership contract.

Set a declared User-Agent only in the environment:

```bash
export REG_SHO_USER_AGENT='MarketMedallion research contact@example.com'
```

Then deliberately set `acquisition.enabled: true` in the local YAML. Proxy,
direct, fallback, timeouts and CA settings use the common
[outbound HTTP contract](../../docs/development/outbound_http.md). The selected
route is probed once and retained for the whole partition acquisition.

No-request checks and a bounded dry-run plan:

```bash
python -m market_data_pipeline.pipelines.reg_sho_thresholds validate-config \
  --config config/reg_sho_thresholds/pipeline.local.yml
python -m market_data_pipeline.pipelines.reg_sho_thresholds health \
  --config config/reg_sho_thresholds/pipeline.local.yml
python -m market_data_pipeline.pipelines.reg_sho_thresholds plan \
  --config config/reg_sho_thresholds/pipeline.local.yml --provider nasdaq
```

Local or live ingestion and the QuantMeridian exchange:

```bash
python -m market_data_pipeline.pipelines.reg_sho_thresholds run \
  --config config/reg_sho_thresholds/pipeline.local.yml --partition nasdaq_2026_11_03
python -m market_data_pipeline.pipelines.reg_sho_thresholds acquire-run \
  --config config/reg_sho_thresholds/pipeline.local.yml --partition nyse_american_2026_11_03
python -m market_data_pipeline.pipelines.reg_sho_thresholds export-research \
  --config config/reg_sho_thresholds/pipeline.local.yml \
  --destination data/research-exchange/reg-sho-thresholds
```

A valid header-plus-trailer response is `COMPLETE_EMPTY`; an absent/failed
request is `UNAVAILABLE` and cannot withdraw membership or produce an exit.
Complete replacements version corrections and explicit withdrawals. Gold and
the research exchange remain provider/market/native-symbol scoped.

SQLite and PostgreSQL are implemented threshold warehouses. PostgreSQL uses a
separate pipeline-owned schema, environment-only password, root/database
identity binding and no SQLite fallback. Select it with `warehouse: postgres`,
create the configured empty schema, and set the named `postgres.password_env`
variable. T22 supplies a separate threshold-owned Delta publisher; staging and
publication commands, recovery and consumer rules are in the
[runbook](../../docs/deployment/reg_sho_thresholds_runbook.md#delta-snapshot-and-generation-publication).

The optional `databricks` section defaults disabled for older configs that omit
it. When present, supply exactly `enabled`, `catalog`, `schema`, `staging_volume`
and `table_prefix`. Enablement requires safe catalog/schema identifiers and a
matching absolute `/Volumes/<catalog>/<schema>/<volume>/...` path. Local
`stage-delta` freezes terminal history and episodes without Spark or HTTP.
Unity Catalog publication is explicit through
`notebooks/publish_reg_sho_thresholds_delta.py`; it never runs after ingestion
or starts a schedule automatically. Local Spark/Delta parity is verified;
a Databricks workspace run remains separate.


T17's bounded [research acceptance](../../specs/spec011/acceptance.md) includes
ten authentic October26–30 NASDAQ/NYSE files and the operator's verbal-permission
attestation. The sample config is local and disabled again after capture.
For NYSE, the adapter probes its canonical published source page, then reuses
that client for the exact configured Download GET with `Accept: */*`. It records
the probe URL in the capture metadata. All other routing/TLS and configuration
gates remain in force; no recurring job or public display is enabled by this
acceptance.
