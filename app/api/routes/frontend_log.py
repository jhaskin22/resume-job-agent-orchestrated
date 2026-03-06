from __future__ import annotations

import json
import logging
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request

router = APIRouter()
logger = logging.getLogger(__name__)
FRONTEND_LOG_PATH = Path("var/frontend_failures.log")


@router.post("/frontend/log")
async def log_frontend_failure(request: Request) -> dict[str, str]:
    payload: dict[str, Any]
    try:
        body = await request.json()
        payload = body if isinstance(body, dict) else {"value": body}
    except Exception:
        payload = {"value": "<non-json-payload>"}

    event = {
        "ts": datetime.now(UTC).isoformat(),
        "client": str(request.client.host) if request.client else "",
        "path": str(request.url.path),
        "payload": payload,
    }

    FRONTEND_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with FRONTEND_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=True) + "\n")

    logger.warning("frontend_log kind=%s", str(payload.get("kind", "unknown")))
    return {"status": "ok"}
