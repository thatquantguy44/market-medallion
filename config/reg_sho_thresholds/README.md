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
variable. Databricks Delta remains the Spec011 T22 gate and is not silently
emulated.
