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
      echo "Stopped $name (PID $pid)."
    else
      echo "$name pid file existed but process was not running."
    fi
    rm -f "$pid_file"
  else
    echo "$name not running."
  fi
}

stop_pid_file "backend" "$RUN_DIR/backend.pid"
stop_pid_file "frontend" "$RUN_DIR/frontend.pid"
