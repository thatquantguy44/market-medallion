#!/usr/bin/env bash
# Forward to the pipeline-owned CLI without changing the caller's working directory.
set -euo pipefail

PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${FOMC_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"

if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  printf '%s\n' \
    'FOMC Python runtime unavailable. Complete the setup in docs/deployment/fomc_runbook.md' \
    'or set FOMC_PYTHON to an interpreter with this project installed.' >&2
  exit 2
fi

exec "$PYTHON_BIN" -m market_data_pipeline.pipelines.fomc_meeting_minutes "$@"
