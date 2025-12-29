from __future__ import annotations

import os
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Optional, Tuple

from utils.paths import stuff_dir

FFMPEG_DOWNLOAD_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
FFMPEG_DIRNAME = "ffmpeg"


_ffmpeg_cache: Optional[Path] = None


def _ensure_on_path(bin_dir: Path) -> None:
    """
    Добавляем каталог с ffmpeg в PATH текущего процесса.
    """
    bin_str = str(bin_dir)
    env_path = os.environ.get("PATH") or ""
    parts = env_path.split(os.pathsep)
    if bin_str not in parts:
        os.environ["PATH"] = bin_str + os.pathsep + env_path


def _bundled_candidates() -> list[Path]:
    base = stuff_dir() / FFMPEG_DIRNAME
    bins = base / "bin"
    return [
        bins / "ffmpeg.exe",
        bins / "ffmpeg",
        base / "ffmpeg.exe",
        base / "ffmpeg",
    ]


def find_ffmpeg(refresh: bool = False) -> Optional[Path]:
    """
    Ищет ffmpeg в PATH и в локальной папке stuff/ffmpeg.
    """
    global _ffmpeg_cache
    if not refresh and _ffmpeg_cache and _ffmpeg_cache.exists():
        return _ffmpeg_cache

    path_hit = shutil.which("ffmpeg")
    if path_hit:
        _ffmpeg_cache = Path(path_hit)
        return _ffmpeg_cache

    for candidate in _bundled_candidates():
        if candidate.exists():
            _ffmpeg_cache = candidate
            _ensure_on_path(candidate.parent)
            return _ffmpeg_cache

    return None


def _download(url: str, dst: Path, progress: Optional[Callable[[str], None]] = None) -> None:
    if progress:
        progress("Скачивание архива ffmpeg...")

    with urllib.request.urlopen(url) as resp, open(dst, "wb") as f:
        shutil.copyfileobj(resp, f)


def _move_bin_contents(src_bin: Path, dst_bin: Path) -> Path:
    dst_bin.mkdir(parents=True, exist_ok=True)
    moved: Optional[Path] = None
    for item in src_bin.glob("ff*"):
        target = dst_bin / item.name
        try:
            if target.exists():
                target.unlink()
        except Exception:
            pass
        shutil.move(str(item), target)
        if item.name.lower() in ("ffmpeg.exe", "ffmpeg") and moved is None:
            moved = target
    return moved or dst_bin


def install_ffmpeg(progress: Optional[Callable[[str], None]] = None) -> Tuple[bool, str, Optional[Path]]:
    """
    Скачивает и разворачивает портативный ffmpeg (Windows, gyan.dev).
    Возвращает (ok, message, path_to_ffmpeg).
    """
    if os.name != "nt":
        return False, "Авто-установка реализована только для Windows (nt).", None

    target_root = stuff_dir() / FFMPEG_DIRNAME
    target_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="ffmpeg_dl_"))
    archive_path = tmp_dir / "ffmpeg.zip"

    try:
        _download(FFMPEG_DOWNLOAD_URL, archive_path, progress)
        if progress:
            progress("Распаковка архива...")

        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(tmp_dir)

        extracted_bin = None
        for path in tmp_dir.rglob("bin"):
            if (path / "ffmpeg.exe").exists() or (path / "ffmpeg").exists():
                extracted_bin = path
                break

        if not extracted_bin:
            return False, "Не удалось найти ffmpeg в распакованном архиве.", None

        target_bin = target_root / "bin"
        ffmpeg_path = _move_bin_contents(extracted_bin, target_bin)
        _ensure_on_path(ffmpeg_path.parent)

        global _ffmpeg_cache
        _ffmpeg_cache = ffmpeg_path

        return True, f"FFmpeg установлен: {ffmpeg_path}", ffmpeg_path
    except Exception as e:
        return False, f"Не удалось установить ffmpeg: {e}", None
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
