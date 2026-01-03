import urllib.request
from pathlib import Path
from typing import Callable, Optional


def download_file(
    url: str,
    dst: Path,
    *,
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
    user_agent: str = "yt-downloader",
    timeout: int = 120,
) -> None:
    """
    Скачивает файл с прогрессом и возможностью отмены.
    progress(msg, ratio[0-1]) | cancel(): bool
    """

    def cancelled() -> bool:
        return bool(cancel and cancel())

    if progress:
        progress("Загрузка...", None)
    if cancelled():
        return

    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as resp, open(dst, "wb") as f:
        total = resp.headers.get("Content-Length")
        total_bytes = int(total) if total and total.isdigit() else None
        read = 0
        chunk = 1024 * 64
        while True:
            buf = resp.read(chunk)
            if not buf:
                break
            f.write(buf)
            read += len(buf)
            if cancelled():
                return
            if progress and total_bytes and total_bytes > 0:
                ratio = min(1.0, read / total_bytes)
                progress(f"Загрузка... {int(ratio * 100)}%", ratio)
        if cancelled():
            return
    if progress:
        progress("Загрузка завершена", 1.0)
