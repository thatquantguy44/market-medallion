# Presidential research: local operations

This branch contains a fixture-qualified local producer and consumer. It does not
contain an admitted historical bundle, and no live provider is approved. Keep
workspace data and credentials outside Git. Preserve prior snapshots and backups;
do not replace or delete historical evidence as a routine rollback.

1. Initialize a dedicated workspace with `python -m market_data_pipeline presidential
   init --workspace /absolute/private/path`.
2. Explicitly import each selected, authorized input with `import-local`, the
   source ID, file, SHA-256 and `--input-kind`. Use `SYNTHETIC_FIXTURE` for
   invented inputs and `CAPTURED_SOURCE` only for actual captured bytes. Supply
   `--retrieved-at` only if supported by capture evidence. Select captures for
   `parse`; import the reviewed workbook with `export-approved-controls`.
3. Use `publish` with explicit capture IDs, review ID and profile. `check` and
   `health` inspect the selected local state without fetching. `backup` writes a
   verified archive to a new path; `restore` and `replay` require new empty
   destinations. Point the local PRES reader at the selected workspace only
   after publication validates successfully.
4. The offline `presidential_rebuild_job` takes explicit capture and review IDs
   and performs no fetch. The separate `presidential_live_fetch_job` applies the
   source gate before network setup. `presidential_live_fetch_schedule` defaults
   STOPPED. The actual state of any separately running Dagster instance was not
   changed or inspected. Even when manually enabled, live requests remain blocked
   until source-specific access and transport are reviewed in code.
5. Roll back by choosing an earlier verified backup and restoring it into a new
   workspace; re-point the local reader after checking the restored selection.
   Preserve the failed workspace for diagnosis. No job here changes an existing
   running schedule, deletes snapshots or performs automatic refresh.

Use `python -m market_data_pipeline presidential <command> --help` for exact
required arguments. See [source access review](source_access_review.md) and
[acceptance tracker](todo.md) before treating any real-data study as qualified.
