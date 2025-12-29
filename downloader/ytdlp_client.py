from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set

import yt_dlp

from downloader.formatting import (
    format_bytes,
    format_seconds,
    has_ffmpeg,
    infer_part_kind_from_filename,
    wait_if_paused_or_cancelled,
)

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


def fetch_video_info(url: str) -> VideoInfo:
    ydl_opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": False,
        "noplaylist": True,
    }
    with yt_dlp.YoutubeDL(ydl_opts) as ydl:
        info = ydl.extract_info(url, download=False)

    title = info.get("title") or "—"
    thumb = info.get("thumbnail")
    webpage = info.get("webpage_url") or url

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
    if has_ffmpeg():
        fmt = "bestvideo[height=1080]+bestaudio/best[height=1080]/bestvideo+bestaudio/best"
    else:
        fmt = "best[height=1080]/best"

    outtmpl = os.path.join(out_dir, "%(title).200s [%(id)s].%(ext)s")

    def push(fields: Dict[str, Any]) -> None:
        update(task_id, fields)

    def progress_hook(d: Dict[str, Any]) -> None:
        wait_if_paused_or_cancelled(runtime.pause_flag, runtime.cancel_flag)

        st = d.get("status")
        filename = d.get("filename") or ""
        tmpfilename = d.get("tmpfilename") or ""

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
            if part_kind == "video":
                stage = "Скачивание видео:"
            elif part_kind == "audio":
                stage = "Скачивание аудио:"
            else:
                stage = "Скачивание:"

            push({
                "status": stage,
                "progress": float(pct) if pct is not None else 0.0,
                "speed": f"{format_bytes(spd)}/s" if spd else "—",
                "eta": format_seconds(eta),
                "total": format_bytes(total_b) if total_b else "—",
                "pct_text": f"{pct:.1f}%" if pct is not None else "—",
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

    if not has_ffmpeg():
        push({"status": "ffmpeg не найден: качаю единый файл (best)"})

    try:
        push({"status": "Подготовка", "progress": 0.0})
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([info.url])

        if runtime.cancel_flag.is_set():
            push({"status": "Отменено", "progress": 0.0})
        else:
            push({"status": "Готово", "progress": 100.0, "speed": "", "eta": "", "total": "", "pct_text": ""})

    except Exception as e:
        if "cancelled" in str(e).lower() or runtime.cancel_flag.is_set():
            push({"status": "Отменено", "progress": 0.0})
        else:
            push({"status": f"Ошибка: {e}"})


def probe_url_kind(url: str) -> tuple[str, dict]:
    """
    Возвращает ("playlist"|"video"|"unknown", raw_info_dict)
    """
    ydl_opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,      # плейлист будет "плоским"
        "noplaylist": False,       # разрешаем плейлисты
        "socket_timeout": 20,
    }
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
    Каждый элемент — отдельное видео.
    """
    ydl_opts: Dict[str, Any] = {
        "quiet": True,
        "no_warnings": True,
        "skip_download": True,
        "extract_flat": True,
        "noplaylist": False,
        "socket_timeout": 20,
    }
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

    return title, items
