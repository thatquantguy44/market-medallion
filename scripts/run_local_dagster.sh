#!/bin/zsh
# LaunchAgent entrypoint: keeps the local Dagster daemon tied to this checkout.
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJECT_ROOT"
set -a
source .env
set +a
export DAGSTER_HOME="$PROJECT_ROOT/.dagster"
mkdir -p "$DAGSTER_HOME"
exec "$PROJECT_ROOT/.venv/bin/dagster" dev \
  -m market_data_pipeline.orchestration.dagster_defs --host 127.0.0.1 --port 3000
