#!/bin/bash
SHEET_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_DIR="$(cd "$SHEET_DIR/.." && pwd)"
cd "$SHEET_DIR"
export PYTHONPATH="$BASE_DIR:$SHEET_DIR:$PYTHONPATH"
export PYTHONUNBUFFERED=1

if [ -f "$BASE_DIR/.venv/bin/uvicorn" ]; then
    UVICORN_CMD="$BASE_DIR/.venv/bin/uvicorn"
elif command -v uv &>/dev/null; then
    UVICORN_CMD="uv run uvicorn"
else
    UVICORN_CMD="python3 -m uvicorn"
fi

$UVICORN_CMD api:app --host 0.0.0.0 --port 8770 --workers 1 --log-level info >> "$SHEET_DIR/Logs/api.logs" 2>&1
