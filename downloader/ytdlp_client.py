from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Dict, Optional, Set

import yt_dlp

from downloader.formatting import (
    format_bytes,
    format_seconds,
    infer_part_kind_from_filename,
    wait_if_paused_or_cancelled,
)
from utils.ffmpeg_installer import find_ffmpeg
from utils.paths import log_path

UpdateFn = Callable[[str, Dict[str, Any]], None]


@dataclass
class VideoInfo:
    url: str
    title: str = "—"
    thumbnail_url: Optional[str] = None
    webpage_url: Optional[str] = None
    format_kind: Dict[str, str] = field(default_factory=dict)  # format_id -> video/audio/muxed/unknown


@dataclass
class TaskRuntime:
    pause_flag: Any
    cancel_flag: Any
    seen_files: Set[str] = field(default_factory=set)


_logger = logging.getLogger("ytdl")
_cookies_file: Optional[str] = None
_quality_mode: str = "1080p"  # audio | 360p | 480p | 720p | 1080p | max
if not _logger.handlers:
    _logger.setLevel(logging.INFO)
    try:
        lp = log_path()
        lp.parent.mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(lp, encoding="utf-8")
        fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        _logger.addHandler(fh)
    except Exception:
        sh = logging.StreamHandler()
        sh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        _logger.addHandler(sh)


def set_cookies_file(path: Optional[str]) -> None:
    global _cookies_file
    if path and os.path.isfile(path):
        _cookies_file = path
        _logger.info("Using cookies file: %s", path)
    else:
        if path:
            _logger.warning("Cookies file not found: %s (ignoring)", path)
        _cookies_file = None


def set_quality_mode(mode: str) -> None:
    global _quality_mode
    allowed = {"audio", "360p", "480p", "720p", "1080p", "max"}
    normalized = str(mode).lower()
    _quality_mode = normalized if normalized in allowed else "1080p"
    _logger.info("Quality mode: %s", _quality_mode)


def _height_from_mode(mode: str) -> Optional[int]:
    if mode.endswith("p"):
        try:
            return int(mode[:-1])
        except ValueError:
            return None
    return None


def _build_format_string(ffmpeg_available: Optional[bool] = None) -> str:
    mode = _quality_mode
    height = _height_from_mode(mode)
    ffmpeg_available = bool(ffmpeg_available)

    if mode == "audio":
        return "bestaudio/best"

    if ffmpeg_available:
        if mode == "max":
            return "bestvideo+bestaudio/best"
        if height:
            return f"bestvideo[height<={height}]+bestaudio/best[height<={height}]/best[height<={height}]"
        return "bestvideo[height<=1080]+bestaudio/best[height<=1080]/best"

    # без ffmpeg работаем с лучшим единым файлом
    if mode == "max":
        return "best"
    if height:
        return f"best[height<={height}]/best"
    return "best[height<=1080]/best"


def fetch_video_info(url: str) -> VideoInfo:
    _logger.info("Fetch info: %s", url)
    ydl_opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "noplaylist": True,
    }
    if _cookies_file:
        ydl_opts["cookies"] = _cookies_file
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    title = info.get("title") or "—"
    thumb = info.get("thumbnail")
    webpage = info.get("webpage_url") or url
    _logger.info("Fetched title: %s", title)

    fmt_kind: Dict[str, str] = {}
    for f in info.get("formats", []) or []:
        fid = str(f.get("format_id", ""))
        if not fid:
            continue
        vcodec = f.get("vcodec")
        acodec = f.get("acodec")
        if vcodec and vcodec != "none" and (not acodec or acodec == "none"):
            fmt_kind[fid] = "video"
        elif acodec and acodec != "none" and (not vcodec or vcodec == "none"):
            fmt_kind[fid] = "audio"
        elif (vcodec and vcodec != "none") and (acodec and acodec != "none"):
            fmt_kind[fid] = "muxed"
        else:
            fmt_kind[fid] = "unknown"

    return VideoInfo(
        url=url,
        title=title,
        thumbnail_url=thumb,
        webpage_url=webpage,
        format_kind=fmt_kind,
    )


def download_task(
    *,
    task_id: str,
    info: VideoInfo,
    out_dir: str,
    runtime: TaskRuntime,
    update: UpdateFn,
) -> None:
    _logger.info("Download start [%s]: %s", task_id, info.url)
    ffmpeg_path = find_ffmpeg()
    ffmpeg_available = ffmpeg_path is not None
    fmt = _build_format_string(ffmpeg_available)

    outtmpl = os.path.join(out_dir, "%(title).200s [%(id)s].%(ext)s")

    def push(fields: Dict[str, Any]) -> None:
        update(task_id, fields)

    def progress_hook(d: Dict[str, Any]) -> None:
        wait_if_paused_or_cancelled(runtime.pause_flag, runtime.cancel_flag)

        st = d.get("status")
        filename = d.get("filename") or ""
        tmpfilename = d.get("tmpfilename") or ""
        fmt_note = d.get("format_note") or ""
        info_dict = d.get("info_dict") or {}
        height = info_dict.get("height")
        width = info_dict.get("width")
        abr = info_dict.get("abr")
        vcodec = info_dict.get("vcodec")
        acodec = info_dict.get("acodec")

        if filename:
            runtime.seen_files.add(filename)
        if tmpfilename:
            runtime.seen_files.add(tmpfilename)

        if st == "downloading":
            total_b = d.get("total_bytes") or d.get("total_bytes_estimate")
            downloaded_b = d.get("downloaded_bytes")

            pct: Optional[float] = None
            if total_b and downloaded_b is not None:
                pct = max(0.0, min(100.0, (downloaded_b / total_b) * 100.0))

            spd = d.get("speed")
            eta = d.get("eta")

            part_kind = infer_part_kind_from_filename(filename, info) if filename else None
            if not part_kind:
                if vcodec and vcodec != "none":
                    part_kind = "video"
                elif acodec and acodec != "none":
                    part_kind = "audio"
            if part_kind == "video":
                stage = "Скачивание видео:"
            elif part_kind == "audio":
                stage = "Скачивание аудио:"
            else:
                stage = "Скачивание:"

            quality_txt = "-"
            if part_kind == "video":
                if fmt_note:
                    quality_txt = fmt_note
                elif height:
                    quality_txt = f"{height}p"
                elif height and width:
                    quality_txt = f"{width}x{height}"
            elif part_kind == "audio":
                if fmt_note:
                    quality_txt = fmt_note
                elif abr:
                    quality_txt = f"{abr}kbps"

            push({
                "status": stage,
                "quality": quality_txt,
                "progress": float(pct) if pct is not None else 0.0,
                "speed": f"{format_bytes(spd)}/s" if spd else "-",
                "eta": format_seconds(eta),
                "total": format_bytes(total_b) if total_b else "-",
                "pct_text": f"{pct:.1f}%" if pct is not None else "-",
            })

        elif st == "finished":
            push({"status": "Загрузка завершена (часть)", "progress": 100.0})
        elif st == "error":
            push({"status": "Ошибка"})

    def postprocessor_hook(d: Dict[str, Any]) -> None:
        wait_if_paused_or_cancelled(runtime.pause_flag, runtime.cancel_flag)

        pp = str(d.get("postprocessor") or "")
        st = str(d.get("status") or "")
        if st in ("started", "processing"):
            if "merge" in pp.lower() or "merger" in pp.lower() or "ffmpeg" in pp.lower():
                push({"status": "Склейка (ffmpeg)…"})
            else:
                push({"status": f"Пост-обработка: {pp}…"})
        elif st == "finished":
            push({"status": "Пост-обработка завершена"})

    ydl_opts: Dict[str, Any] = {
        "format": fmt,
        "outtmpl": outtmpl,
        "noplaylist": True,
        "quiet": True,
        "no_warnings": True,
        "retries": 10,
        "fragment_retries": 10,
        "progress_hooks": [progress_hook],
        "postprocessor_hooks": [postprocessor_hook],
    }
    if ffmpeg_path:
        ydl_opts["ffmpeg_location"] = str(Path(ffmpeg_path).parent)
    if _cookies_file:
        ydl_opts["cookies"] = _cookies_file

    if not ffmpeg_available and _quality_mode != "audio":
        msg = "ffmpeg не найден: качаю единый файл (без склейки bestvideo+bestaudio)"
        push({"status": msg})
        _logger.warning(msg)

    try:
        push({"status": "Подготовка", "progress": 0.0})
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([info.url])

        if runtime.cancel_flag.is_set():
            push({"status": "Отменено", "progress": 0.0})
            _logger.info("Download cancelled [%s]", task_id)
        else:
            push({"status": "Готово", "progress": 100.0, "speed": "", "eta": "", "total": "", "pct_text": ""})
            _logger.info("Download done [%s]", task_id)

    except Exception as e:
        if "cancelled" in str(e).lower() or runtime.cancel_flag.is_set():
            push({"status": "Отменено", "progress": 0.0})
            _logger.info("Download cancelled after exception [%s]: %s", task_id, e)
        else:
            push({"status": f"Ошибка: {e}"})
            _logger.error("Download failed [%s]: %s", task_id, e)


def probe_url_kind(url: str) -> tuple[str, dict]:
    """
    Возвращает ("playlist"|"video"|"unknown", raw_info_dict)
    """
    _logger.info("Probe url: %s", url)
    ydl_opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,      # плейлист будет "плоским"
        "noplaylist": False,       # разрешаем плейлисты
        "socket_timeout": 20,
    }
    if _cookies_file:
        ydl_opts["cookies"] = _cookies_file
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    t = str(info.get("_type") or "")
    if t == "playlist":
        return "playlist", info

    # Иногда плейлист приходит как "multi_video" (редко), но entries есть
    if info.get("entries"):
        return "playlist", info

    # Иначе считаем, что это видео
    return "video", info


def _iter_entries(entries_obj: Any) -> list[dict]:
    """
    yt-dlp может вернуть entries как list или generator.
    """
    if entries_obj is None:
        return []
    if isinstance(entries_obj, list):
        return entries_obj
    try:
        return list(entries_obj)
    except Exception:
        return []


def _to_webpage_url(entry: dict) -> Optional[str]:
    # При extract_flat=True у YouTube чаще всего есть id/url
    if entry.get("webpage_url"):
        u = str(entry["webpage_url"])
        return u

    u = entry.get("url")
    if isinstance(u, str) and u.startswith("http"):
        return u

    vid = entry.get("id") or u
    if isinstance(vid, str) and vid:
        # Универсально для YouTube
        return f"https://www.youtube.com/watch?v={vid}"
    return None


def expand_playlist(url: str) -> tuple[str, list[VideoInfo]]:
    """
    Возвращает (playlist_title, [VideoInfo...]) для добавления в очередь.
    Каждый элемент - отдельное видео.
    """
    _logger.info("Expand playlist: %s", url)
    ydl_opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
        "noplaylist": False,
        "socket_timeout": 20,
    }
    if _cookies_file:
        ydl_opts["cookies"] = _cookies_file
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    title = str(info.get("title") or "Плейлист")
    entries = _iter_entries(info.get("entries"))

    items: list[VideoInfo] = []
    for e in entries:
        if not isinstance(e, dict):
            continue
        webpage = _to_webpage_url(e)
        if not webpage:
            continue

        items.append(
            VideoInfo(
                url=webpage,
                title=str(e.get("title") or "—"),
                thumbnail_url=e.get("thumbnail"),
                webpage_url=webpage,
                format_kind={},  # заполним позже в fetch_video_info при апдейте строки
            )
        )

    _logger.info("Playlist expanded: %s | %d entries", title, len(items))
    return title, items
