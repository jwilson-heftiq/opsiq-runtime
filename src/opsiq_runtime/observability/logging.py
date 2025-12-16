from __future__ import annotations

import logging
import sys
from typing import Optional

from opsiq_runtime.settings import get_settings


def configure_logging(level: Optional[str] = None) -> None:
    settings = get_settings()
    log_level = level or settings.log_level
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
        stream=sys.stdout,
    )

