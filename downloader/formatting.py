from __future__ import annotations

import re
import shutil
import time
from typing import Optional, TYPE_CHECKING, Any

from utils.ffmpeg_installer import find_ffmpeg

if TYPE_CHECKING:
    from downloader.ytdlp_client import VideoInfo  # только для type-check, не в runtime

_FMTID_RE = re.compile(r"\.f([0-9A-Za-z_-]+)\.")  # ... .f137.mp4 / ... .f251.webm


def has_ffmpeg() -> bool:
    return find_ffmpeg() is not None


def format_bytes(n: Optional[float]) -> str:
    if n is None:
        return "—"
    units = ["B", "KB", "MB", "GB", "TB"]
    x = float(n)
    for u in units:
        if x < 1024.0:
            return f"{x:.1f} {u}"
        x /= 1024.0
    return f"{x:.1f} PB"


def format_seconds(s: Optional[float]) -> str:
    if s is None:
        return "—"
    s = int(s)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h:d}:{m:02d}:{sec:02d}"
    return f"{m:d}:{sec:02d}"


def infer_part_kind_from_filename(filename: str, info: "VideoInfo") -> Optional[str]:
    """
    Определяем, видео или аудио качается сейчас.
    """
    m = _FMTID_RE.search(filename or "")
    if not m:
        return None
    fmtid = m.group(1)
    return info.format_kind.get(fmtid)


def wait_if_paused_or_cancelled(pause_flag: Any, cancel_flag: Any) -> None:
    """
    Универсальная блокировка: пауза/отмена.
    """
    while pause_flag.is_set():
        if cancel_flag.is_set():
            raise RuntimeError("cancelled")
        time.sleep(0.15)
    if cancel_flag.is_set():
        raise RuntimeError("cancelled")
