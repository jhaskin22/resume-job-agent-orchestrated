#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"

stop_pid_file() {
  local name="$1"
  local pid_file="$2"

  if [[ -f "$pid_file" ]]; then
    local pid
    pid="$(cat "$pid_file")"
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid"
      for _ in $(seq 1 20); do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 0.1
      done
      echo "Stopped $name (PID $pid)."
    else
      echo "$name pid file existed but process was not running."
    fi
    rm -f "$pid_file"
  else
    echo "$name not running."
  fi
}

stop_by_pattern() {
  local name="$1"
  local pattern="$2"
  local pids
  pids="$(pgrep -f "$pattern" || true)"
  if [[ -z "$pids" ]]; then
    return 0
  fi

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    if kill -0 "$pid" 2>/dev/null; then
      kill "$pid" 2>/dev/null || true
      for _ in $(seq 1 20); do
        if ! kill -0 "$pid" 2>/dev/null; then
          break
        fi
        sleep 0.1
      done
      echo "Stopped $name by pattern (PID $pid)."
    fi
  done <<< "$pids"
}

stop_pid_file "backend" "$RUN_DIR/backend.pid"
stop_pid_file "frontend" "$RUN_DIR/frontend.pid"

stop_by_pattern "backend" "python3 -m uvicorn main:app --host 0.0.0.0 --port 18000"
stop_by_pattern "backend" "python3 -m uvicorn app.main:app --host 0.0.0.0 --port 18000"
stop_by_pattern "frontend" "python3 -m http.server 8090 --bind 0.0.0.0 --directory frontend"
