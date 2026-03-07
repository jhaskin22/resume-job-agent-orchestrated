from __future__ import annotations

import threading
from pathlib import Path

RUN_ID_COUNTER_PATH = Path("var/run_id_counter.txt")
_lock = threading.Lock()


def next_run_id() -> int:
    with _lock:
        current = _read_counter()
        next_value = current + 1
        RUN_ID_COUNTER_PATH.parent.mkdir(parents=True, exist_ok=True)
        RUN_ID_COUNTER_PATH.write_text(str(next_value), encoding="utf-8")
        return next_value


def _read_counter() -> int:
    if not RUN_ID_COUNTER_PATH.exists():
        return 0
    raw = RUN_ID_COUNTER_PATH.read_text(encoding="utf-8").strip()
    if not raw:
        return 0
    try:
        return int(raw)
    except ValueError:
        return 0
