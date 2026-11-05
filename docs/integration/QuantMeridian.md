# QuantMeridian research exchange contracts

MarketMedallion is the upstream source of immutable, attributed research
artifacts. QuantMeridian must read only each artifact's atomic `current.json`
pointer and the selected generation; it must not open MarketMedallion's private
SQLite/PostgreSQL stores, Bronze payloads, runtime YAML, or secrets.

## SEC fails-to-deliver

Create an exchange generation with:

```bash
python -m market_data_pipeline.pipelines.sec_fails_to_deliver export-research \
  --config config/sec_fails_to_deliver/pipeline.local.yml \
  --destination data/research-exchange/sec-ftd
```

`sec-ftd-research/1` selects a coherent committed Parquet snapshot and writes:

- `observations.parquet`: current-vintage SEC FTD Gold facts with exact quantity
  and price tokens, UTC first-seen clocks, version IDs and raw lineage hashes;
- `coverage.parquet`: partition coverage/audit evidence required to interpret
  missing observations;
- `manifest.json`: provider attribution, source snapshot ID/generation clock,
  row counts/checksums, units and explicit interpretation limits; and
- destination `current.json`: the only consumer selector.

Balances are outstanding shares as of a settlement date, not daily new fails.
Age, cause, short-sale origin and abusive/naked-short classification cannot be
inferred. Missing rows are not zero without complete coverage evidence. The
prior-close price is a provider reference field, not a liability or borrow cost.

## Nasdaq and NYSE Reg SHO threshold lists

Create an exchange generation with:

```bash
python -m market_data_pipeline.pipelines.reg_sho_thresholds export-research \
  --config config/reg_sho_thresholds/pipeline.local.yml \
  --destination data/research-exchange/reg-sho-thresholds
```

`reg-sho-thresholds-research/1` writes:

- `membership.parquet`: current observed membership by provider, market, list
  date, native symbol and rule family, with raw source flags and revision IDs;
- `coverage.parquet`: complete, complete-empty, unavailable and failed list
  evidence by provider/market/date;
- `episodes.parquet`: coverage-qualified observed episodes with entry/exit,
  gap and left/right-censoring flags; and
- a checksummed manifest plus atomic `current.json` pointer.

A missing/failed list is unknown, not empty, and cannot produce an exit.
Provider-native symbols are not cross-provider security IDs. Any CUSIP/security
join requires a separate dated, knowledge-aware `security-identity-mapping/1`
artifact; unresolved or ambiguous mappings must preserve the native facts and
must not multiply SEC balances.

## Consumer validation

Before loading either generation, QuantMeridian should:

1. validate the pointer and manifest schema versions;
2. recompute each named file's SHA-256 and row count;
3. reject unknown required fields/schema versions rather than infer them;
4. surface the source generation clock, knowledge basis and coverage labels in
   the report; and
5. retain the artifact/export ID in report provenance.

Both contracts are current-vintage by default. They preserve first-seen clocks
for future observed-as-of work, but an export does not claim the pipeline knew a
historical value before its recorded capture. No repository-to-repository write,
database connection, API service, or automatic UI integration is performed.
