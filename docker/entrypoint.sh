#!/usr/bin/env sh
set -e

MODE="${1:-api}"
shift || true

if [ "$MODE" = "api" ]; then
  exec uvicorn opsiq_runtime.app.main:app --host 0.0.0.0 --port "${PORT:-8080}"
elif [ "$MODE" = "run" ]; then
  exec python -m opsiq_runtime.app.cli "$@"
else
  echo "Unknown mode: $MODE"
  exit 1
fi

