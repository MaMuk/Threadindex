from __future__ import annotations

import logging

from .config import load_config

_LOGGER: logging.Logger | None = None


def get_logger() -> logging.Logger:
    global _LOGGER
    if _LOGGER is not None:
        return _LOGGER

    config = load_config()
    logger = logging.getLogger("threadindex")
    logger.setLevel(logging.INFO)

    handler = logging.FileHandler(config.paths.log_file, encoding="utf-8")
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    _LOGGER = logger
    return logger
