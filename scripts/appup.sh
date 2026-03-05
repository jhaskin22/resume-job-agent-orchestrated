#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"
mkdir -p "$RUN_DIR"

if [[ -f "$RUN_DIR/backend.pid" ]] && kill -0 "$(cat "$RUN_DIR/backend.pid")" 2>/dev/null; then
  echo "Backend already running (PID $(cat "$RUN_DIR/backend.pid"))."
else
  (cd "$ROOT_DIR" && nohup python3 -m uvicorn app.main:app --host 0.0.0.0 --port 18000 >"$RUN_DIR/backend.log" 2>&1 & echo $! >"$RUN_DIR/backend.pid")
  echo "Backend started on 0.0.0.0:18000"
fi

if [[ -f "$RUN_DIR/frontend.pid" ]] && kill -0 "$(cat "$RUN_DIR/frontend.pid")" 2>/dev/null; then
  echo "Frontend already running (PID $(cat "$RUN_DIR/frontend.pid"))."
else
  (cd "$ROOT_DIR" && nohup python3 -m http.server 8090 --bind 0.0.0.0 --directory frontend >"$RUN_DIR/frontend.log" 2>&1 & echo $! >"$RUN_DIR/frontend.pid")
  echo "Frontend started on 0.0.0.0:8090"
fi

echo "Logs: $RUN_DIR/backend.log and $RUN_DIR/frontend.log"
