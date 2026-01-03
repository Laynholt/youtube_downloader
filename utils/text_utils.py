import re
import logging
from typing import Optional

from utils.paths import log_path

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_FILE_HANDLER: Optional[logging.Handler] = None


def sanitize_text(text: Optional[str]) -> str:
    if not text:
        return ""
    return _ANSI_RE.sub("", str(text)).strip()


def truncate_text(text: Optional[str], limit: int = 220) -> str:
    clean = sanitize_text(text)
    if limit <= 0:
        return clean
    if len(clean) <= limit:
        return clean
    return clean[: max(0, limit - 1)].rstrip() + "…"


def ensure_file_logger(name: str) -> logging.Logger:
    """
    Создаёт логгер с общим файловым хендлером (перезапись файла при первом создании).
    """
    global _FILE_HANDLER
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    if _FILE_HANDLER is None:
        lp = log_path()
        lp.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(lp, encoding="utf-8", mode="w")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        _FILE_HANDLER = fh
    if _FILE_HANDLER not in logger.handlers:
        logger.addHandler(_FILE_HANDLER)
    return logger
