from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

_CONFIGURED = False


def configure_pipeline_logging() -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    log_path = Path("var/pipeline.log")
    log_path.parent.mkdir(parents=True, exist_ok=True)

    app_logger = logging.getLogger("app")
    app_logger.setLevel(logging.INFO)

    for handler in app_logger.handlers:
        is_pipeline_file = getattr(handler, "baseFilename", "").endswith("pipeline.log")
        if isinstance(handler, RotatingFileHandler) and is_pipeline_file:
            _CONFIGURED = True
            return

    handler = RotatingFileHandler(
        filename=log_path,
        maxBytes=2_000_000,
        backupCount=3,
        encoding="utf-8",
    )
    handler.setLevel(logging.INFO)
    handler.setFormatter(
        logging.Formatter(
            fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )
    app_logger.addHandler(handler)
    _CONFIGURED = True
