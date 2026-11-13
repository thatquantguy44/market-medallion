#!/usr/bin/env bash
# Forward to the installed pipeline CLI, preserving the caller's working directory.
set -euo pipefail
PROJECT_ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="${FINRA_SSV_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
  printf '%s\n' 'FINRA Python runtime unavailable. Complete repository setup or set FINRA_SSV_PYTHON.' >&2
  exit 2
fi
exec "$PYTHON_BIN" -m market_data_pipeline.pipelines.finra_short_sale_volume "$@"
