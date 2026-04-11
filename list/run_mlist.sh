#!/usr/bin/env sh
set -eu

SCRIPT_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

if [ -n "${LIST_PYTHON:-}" ]; then
    PYTHON_BIN="$LIST_PYTHON"
elif [ -x "$SCRIPT_DIR/.venv/bin/python" ]; then
    PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
else
    PYTHON_BIN="python3"
fi

cd "$SCRIPT_DIR"
exec "$PYTHON_BIN" mlist.py
