import logging
import os
import shutil
import tempfile
import zipfile
from pathlib import Path
from typing import Callable, Optional, Tuple

from utils.paths import stuff_dir
from utils.text_utils import sanitize_text, ensure_file_logger
from downloader.http_client import download_file

FFMPEG_DOWNLOAD_URL = "https://www.gyan.dev/ffmpeg/builds/ffmpeg-release-essentials.zip"
FFMPEG_DIRNAME = "ffmpeg"


_ffmpeg_cache: Optional[Path] = None
_custom_path: Optional[Path] = None
_logger = ensure_file_logger("ffmpeg_installer")


class InstallCancelled(Exception):
    pass


def _ensure_on_path(bin_dir: Path) -> None:
    """
    Добавляем каталог с ffmpeg в PATH текущего процесса.
    """
    bin_str = str(bin_dir)
    env_path = os.environ.get("PATH") or ""
    parts = env_path.split(os.pathsep)
    if bin_str not in parts:
        os.environ["PATH"] = bin_str + os.pathsep + env_path


def set_ffmpeg_path(path: Optional[Path]) -> Optional[Path]:
    """
    Явно задаёт путь до ffmpeg и добавляет его в PATH.
    """
    global _custom_path, _ffmpeg_cache
    if path is None:
        _custom_path = None
        return None
    p = Path(path)
    if p.is_dir():
        candidates = list(p.glob("ffmpeg*"))
        if candidates:
            p = candidates[0]
    if not p.exists():
        return None
    _custom_path = p
    _ffmpeg_cache = p
    _ensure_on_path(p.parent)
    _logger.info("Custom FFmpeg path set: %s", p)
    return p


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

    if _custom_path and _custom_path.exists():
        _ffmpeg_cache = _custom_path
        _ensure_on_path(_custom_path.parent)
        return _ffmpeg_cache

    path_hit = shutil.which("ffmpeg")
    if path_hit:
        _ffmpeg_cache = Path(path_hit)
        _logger.info("FFmpeg found in PATH: %s", _ffmpeg_cache)
        return _ffmpeg_cache

    for candidate in _bundled_candidates():
        if candidate.exists():
            _ffmpeg_cache = candidate
            _ensure_on_path(candidate.parent)
            _logger.info("FFmpeg found in bundled path: %s", _ffmpeg_cache)
            return _ffmpeg_cache

    return None


def _download(
    url: str,
    dst: Path,
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> None:
    def cancelled() -> bool:
        return bool(cancel and cancel())

    def on_progress(msg: str, ratio: Optional[float]) -> None:
        if progress:
            progress(msg.replace("Загрузка", "Скачивание архива ffmpeg"), ratio)

    _logger.info("Downloading ffmpeg from %s to %s", url, dst)
    download_file(url, dst, progress=on_progress, cancel=cancelled, user_agent="yt-downloader-ffmpeg-installer")
    if cancelled():
        raise InstallCancelled()
    _logger.info("FFmpeg archive downloaded (%s bytes)", dst.stat().st_size if dst.exists() else 0)


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


def install_ffmpeg(
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
    target_root: Optional[Path] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> Tuple[bool, str, Optional[Path]]:
    """
    Скачивает и разворачивает портативный ffmpeg (Windows, gyan.dev).
    Возвращает (ok, message, path_to_ffmpeg).
    """
    if os.name != "nt":
        return False, "Авто-установка реализована только для Windows (nt).", None

    target_root = target_root or (stuff_dir() / FFMPEG_DIRNAME)
    target_root.mkdir(parents=True, exist_ok=True)
    tmp_dir = Path(tempfile.mkdtemp(prefix="ffmpeg_dl_"))
    archive_path = tmp_dir / "ffmpeg.zip"

    _logger.info("Starting ffmpeg install to %s", target_root)

    try:
        _download(FFMPEG_DOWNLOAD_URL, archive_path, progress, cancel)
        if cancel and cancel():
            raise InstallCancelled()
        if progress:
            progress("Распаковка архива...", None)

        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(tmp_dir)
        if cancel and cancel():
            raise InstallCancelled()

        extracted_bin = None
        for path in tmp_dir.rglob("bin"):
            if cancel and cancel():
                raise InstallCancelled()
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
        set_ffmpeg_path(ffmpeg_path)

        _logger.info("FFmpeg installed at %s", ffmpeg_path)
        return True, f"FFmpeg установлен: {ffmpeg_path}", ffmpeg_path
    except InstallCancelled:
        _logger.info("FFmpeg install cancelled")
        return False, "Установка ffmpeg отменена.", None
    except Exception as e:
        _logger.error("FFmpeg install failed: %s", sanitize_text(e))
        return False, f"Не удалось установить ffmpeg: {e}", None
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
