#!/usr/bin/env bash
set -euo pipefail

export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"
export LOG_LEVEL="${LOG_LEVEL:-INFO}"
export OUTPUT_DIR="${OUTPUT_DIR:-/tmp/opsiq-runtime-output}"

python -m opsiq_runtime.app.cli run "$@"

