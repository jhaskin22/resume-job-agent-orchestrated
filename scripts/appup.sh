#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT_DIR/.run"
mkdir -p "$RUN_DIR"

is_running() {
  local pid="$1"
  if ! kill -0 "$pid" 2>/dev/null; then
    return 1
  fi

  # Treat zombie processes as not running so stale pid files do not block restart.
  if [[ -r "/proc/$pid/stat" ]]; then
    local state
    state="$(awk '{print $3}' "/proc/$pid/stat" 2>/dev/null || true)"
    [[ "$state" != "Z" ]]
    return
  fi

  return 0
}

wait_for_http() {
  local url="$1"
  for _ in $(seq 1 20); do
    if command -v curl >/dev/null 2>&1; then
      if curl -fsS "$url" >/dev/null 2>&1; then
        return 0
      fi
    else
      if python3 -c "import urllib.request; urllib.request.urlopen('$url', timeout=1)" >/dev/null 2>&1; then
        return 0
      fi
    fi
    sleep 0.2
  done
  return 1
}

start_process() {
  local pid_file="$1"
  local log_file="$2"
  shift 2

  (
    cd "$ROOT_DIR"
    nohup "$@" >"$log_file" 2>&1 < /dev/null &
    echo $! >"$pid_file"
  )
}

if [[ -f "$RUN_DIR/backend.pid" ]] && is_running "$(cat "$RUN_DIR/backend.pid")"; then
  echo "Backend already running (PID $(cat "$RUN_DIR/backend.pid"))."
else
  start_process \
    "$RUN_DIR/backend.pid" \
    "$RUN_DIR/backend.log" \
    python3 -m uvicorn main:app --host 0.0.0.0 --port 18000
  if wait_for_http "http://127.0.0.1:18000/api/health"; then
    echo "Backend started on 0.0.0.0:18000"
  else
    echo "Backend failed to start. See $RUN_DIR/backend.log"
    exit 1
  fi
fi

if [[ -f "$RUN_DIR/frontend.pid" ]] && is_running "$(cat "$RUN_DIR/frontend.pid")"; then
  echo "Frontend already running (PID $(cat "$RUN_DIR/frontend.pid"))."
else
  start_process \
    "$RUN_DIR/frontend.pid" \
    "$RUN_DIR/frontend.log" \
    python3 -m http.server 8090 --bind 0.0.0.0 --directory frontend
  if wait_for_http "http://127.0.0.1:8090"; then
    echo "Frontend started on 0.0.0.0:8090"
  else
    echo "Frontend failed to start. See $RUN_DIR/frontend.log"
    exit 1
  fi
fi

echo "Logs: $RUN_DIR/backend.log and $RUN_DIR/frontend.log"
