# FOMC script entrypoint

User-selected continuation after merged T06 (PR #12): run the next bounded 1991
batch through a reusable checkout script that other users can operate with their
own configuration. Active provider scope remains 1990 onward.

- `scripts/run_fomc.sh` delegates unchanged arguments to the existing source-owned
  CLI. It contains no acquisition, retry, parsing, publication or recovery logic.
- Resolve the checkout's `.venv/bin/python` relative to the script; support an
  explicit `FOMC_PYTHON` for a separately installed environment. Preserve the
  caller's working directory, argument boundaries, JSON output and exit status.
- Missing runtime gives actionable setup guidance and exit 2. The script never
  installs packages, reads `.env`, copies local configuration, migrates storage,
  probes a provider or activates a schedule on its own.
- Document Python 3.12/Bash/PostgreSQL setup, user-owned example configuration,
  environment-backed credentials, explicit migration and bounded plan/run/health.
  Every live run still requires explicit years and `--live` under T06 limits.
- Include the script in source distributions; installed wheels retain their
  existing `market-medallion fomc` console entrypoint.

Acceptance: invoke from a different directory with spaces in checkout/config
paths; verify default/overridden interpreter, no-service config validation,
closed live gates, forwarded output/status and helpful missing-runtime failure.
Then use the same script for the 1991 bounded live batch, with before/after
backup/restore and offline readback/replay evidence. Preserve all existing source
bytes/clocks/history and report review gaps. Scheduling and embeddings remain
unselected; NAAIM stays paused.
