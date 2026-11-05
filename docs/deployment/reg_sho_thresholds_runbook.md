# Nasdaq and NYSE Reg SHO threshold-list runbook

The package owns independent Nasdaq and NYSE physical parsers, one normalized
versioned membership store, coverage-qualified observed episodes and a native
research exchange. It does not join symbols to SEC CUSIPs or infer short
interest, fail age, close-out duration, or a zero from a missing list.

## Safe local sequence

1. Copy the [example config](../../config/reg_sho_thresholds/pipeline.example.yml)
   to ignored `pipeline.local.yml` and select an empty durable root.
2. Run `validate-config`, `health`, and `plan`. None makes a provider request.
3. For an approved local file, use `run --partition ...`. For an admitted exact
   official URL, enable acquisition locally, set `REG_SHO_USER_AGENT`, and use
   `acquire-run --partition ...`.
4. Inspect `gold`; then publish a consumer artifact with `export-research`.

Use `replay --source-root <retained-root> --config <empty-destination.yml>` to
rebuild a separate SQLite or configured PostgreSQL root without provider access. Replay preserves original
capture IDs, sequences, knowledge clocks, exact membership/coverage/audit
versions and Bronze bytes, verifies normalized equality, then atomically exposes
the destination with a checksummed inventory.

Original payloads, provider manifest and receipt are immutable under
`<root>/bronze/<run_id>/`. SQLite or PostgreSQL stores membership versions,
coverage and audit under a root/storage identity. PostgreSQL also binds the
configured database/schema and uses a session advisory writer lock. A complete replacement can correct a row or
withdraw an omitted row; the older version remains selectable at its capture
cutoff. The provider's 14-digit creation token is retained as raw text because
the source does not establish its timezone.

## Coverage outcomes

| Outcome | Meaning |
| --- | --- |
| `COMPLETE` | Header, selected date/market, rows, flags and creation trailer validate. |
| `COMPLETE_EMPTY` | The official envelope validates and contains zero membership rows. |
| `UNAVAILABLE` | No local/provider response was captured; prior membership remains selected. |
| `FAILED` | Bytes were captured but the provider-specific envelope was rejected. |

Observed entries/exits require complete expected dates. Missing/failed dates
split certainty and produce gap/censoring flags. The current export calendar is
explicitly labeled `configured-and-observed-list-dates/1`; it is a bounded
research calendar, not a complete exchange holiday service.

## Scheduling boundary

`reg_sho_threshold_list_job` and `reg_sho_threshold_lists_daily_live` are
source-owned Dagster definitions. The schedule is stopped by default. It skips
unless `REG_SHO_SCHEDULED_LIVE_ACCESS=true`, `REG_SHO_CONFIG`, and
`REG_SHO_PARTITION` are supplied. These settings do not resolve Nasdaq/NYSE
retention or redistribution rights; review them before activation. NAAIM remains
paused and is not affected.

## Known completion gates

This local research slice does not close Spec011 R12. Threshold PostgreSQL is
implemented and verified; actual Delta/Spark or Databricks round trips for both
SEC and threshold artifacts remain required. Keep the schedules stopped until those
gates and the intended source-use scope are accepted.
