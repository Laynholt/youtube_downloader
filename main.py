from __future__ import annotations

import io
import os
import queue
import re
import shutil
import sys
import threading
import time
import urllib.request
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Optional, Set, Tuple

import tkinter as tk
from tkinter import filedialog, messagebox, ttk



def has_ffmpeg() -> bool:
    return shutil.which("ffmpeg") is not None


# ==============================
# Данные и события
# ==============================

@dataclass
class VideoInfo:
    url: str
    title: str = "—"
    thumbnail_url: Optional[str] = None
    webpage_url: Optional[str] = None
    # format_id -> kind ("video"|"audio"|"muxed"|"unknown")
    format_kind: Dict[str, str] = field(default_factory=dict)


@dataclass
class TaskState:
    task_id: str
    info: VideoInfo
    out_dir: str

    status: str = "Ожидание"
    progress: float = 0.0
    speed: Optional[str] = None
    eta: Optional[str] = None

    # Управление
    pause_flag: threading.Event = field(default_factory=threading.Event)   # True => pause
    cancel_flag: threading.Event = field(default_factory=threading.Event)  # True => hard cancel
    soft_cancelled: bool = False  # после "Отмена" -> режим Возобновить/Удалить

    # Для удаления
    seen_files: Set[str] = field(default_factory=set)
    worker_thread: Optional[threading.Thread] = None


GuiMsg = Tuple[str, str, Dict[str, Any]]


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


# ==============================
# yt-dlp: извлечение инфо и скачивание
# ==============================

def fetch_video_info(url: str) -> VideoInfo:
    import yt_dlp

    ydl_opts: Dict[str, Any] = {
        "force_ipv4": True,
        "socket_timeout": 20,
        "retries": 20,
        "fragment_retries": 20,
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


def download_thumbnail_to_pil(url: str, max_size: Tuple[int, int]) -> "tuple[Any, Any]":
    from PIL import Image, ImageTk

    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = resp.read()

    img = Image.open(io.BytesIO(data))
    img = img.convert("RGB")
    img.thumbnail(max_size)
    tk_img = ImageTk.PhotoImage(img)
    return img, tk_img


_FMTID_RE = re.compile(r"\.f([0-9A-Za-z_-]+)\.")  # ... .f137.mp4, ... .f251.webm, ...


def _infer_part_kind_from_filename(filename: str, info: VideoInfo) -> Optional[str]:
    """
    Пытаемся понять, что сейчас качается: видео или аудио.
    yt-dlp для раздельных потоков делает временные имена вида: <base>.f<format_id>.<ext>
    """
    m = _FMTID_RE.search(filename)
    if not m:
        return None
    fmtid = m.group(1)
    return info.format_kind.get(fmtid)


def _wait_if_paused_or_cancelled(state: TaskState) -> None:
    while state.pause_flag.is_set():
        if state.cancel_flag.is_set():
            raise RuntimeError("cancelled")
        time.sleep(0.15)
    if state.cancel_flag.is_set():
        raise RuntimeError("cancelled")


def start_download_worker(state: TaskState, q: "queue.Queue[GuiMsg]") -> None:
    import yt_dlp

    def push(fields: Dict[str, Any]) -> None:
        q.put(("task_update", state.task_id, fields))

    # Формат:
    # - если есть ffmpeg: стараемся взять 1080p видео + лучший аудио; если 1080 нет -> максимум
    # - если ffmpeg нет: берём единый файл best[height=1080]/best
    if has_ffmpeg():
        fmt = "bestvideo[height=1080]+bestaudio/best[height=1080]/bestvideo+bestaudio/best"
    else:
        fmt = "best[height=1080]/best"

    outtmpl = os.path.join(state.out_dir, "%(title).200s [%(id)s].%(ext)s")

    def progress_hook(d: Dict[str, Any]) -> None:
        _wait_if_paused_or_cancelled(state)

        status = d.get("status")
        filename = d.get("filename") or ""
        tmpfilename = d.get("tmpfilename") or ""

        # сохраняем, что уже создавалось/качалось (для удаления)
        if filename:
            state.seen_files.add(filename)
        if tmpfilename:
            state.seen_files.add(tmpfilename)

        if status == "downloading":
            total = d.get("total_bytes") or d.get("total_bytes_estimate")
            downloaded = d.get("downloaded_bytes")
            pct = 0.0
            if total and downloaded is not None:
                pct = max(0.0, min(100.0, (downloaded / total) * 100.0))

            spd = d.get("speed")
            eta = d.get("eta")

            # определяем: видео или аудио
            part_kind = _infer_part_kind_from_filename(filename, state.info) if filename else None
            if part_kind == "video":
                stage = "Скачивание видео"
            elif part_kind == "audio":
                stage = "Скачивание аудио"
            else:
                stage = "Скачивание"

            push({
                "status": stage,
                "progress": pct,
                "speed": f"{format_bytes(spd)}/s" if spd else "—",
                "eta": format_seconds(eta),
            })

        elif status == "finished":
            # Это "файл скачан" (может быть один из потоков)
            # Сама склейка будет показана через postprocessor_hook.
            push({"status": "Загрузка завершена (часть)", "progress": 100.0})

        elif status == "error":
            push({"status": "Ошибка"})

    def postprocessor_hook(d: Dict[str, Any]) -> None:
        _wait_if_paused_or_cancelled(state)

        pp = d.get("postprocessor") or ""
        st = d.get("status") or ""

        # Примеры: Merger, FFmpegMerger, EmbedThumbnail, etc.
        if st in ("started", "processing"):
            if "merge" in pp.lower() or "merger" in pp.lower() or "ffmpeg" in pp.lower():
                push({"status": "Склейка (ffmpeg)…"})
            else:
                push({"status": f"Пост-обработка: {pp}…"})
        elif st == "finished":
            # не ставим "Готово" тут окончательно — это сделаем после ydl.download
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
        # "verbose": True,
    }

    if not has_ffmpeg():
        push({"status": "ffmpeg не найден: качаю единый файл (best)"})

    try:
        push({"status": "Подготовка", "progress": 0.0})
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            ydl.download([state.info.url])

        if state.cancel_flag.is_set():
            push({"status": "Отменено", "progress": 0.0})
        else:
            push({"status": "Готово", "progress": 100.0, "eta": "—"})
    except Exception as e:
        if str(e).lower().find("cancelled") >= 0 or state.cancel_flag.is_set():
            push({"status": "Отменено", "progress": 0.0})
        else:
            push({"status": f"Ошибка: {e}", "progress": state.progress})


# ==============================
# Удаление файлов задачи
# ==============================

def _expand_delete_candidates(paths: Set[str]) -> Set[str]:
    """
    yt-dlp может создавать .part/.ytdl, а также временные файлы.
    Добавим типичные варианты на удаление.
    """
    out: Set[str] = set()
    for p in paths:
        if not p:
            continue
        out.add(p)

        # если это .part — добавим базовый
        if p.endswith(".part"):
            out.add(p[:-5])
        else:
            out.add(p + ".part")

        out.add(p + ".ytdl")

        # иногда tmpfilename может быть вида "<name>.temp" и т.п.
        # добавим попытку убрать ".temp" (если вдруг)
        if p.endswith(".temp"):
            out.add(p[:-5])
    return out


def delete_task_files(state: TaskState) -> Tuple[int, list[str]]:
    """
    Удаляет все обнаруженные файлы задачи.
    Возвращает (сколько удалено, список ошибок).
    """
    candidates = _expand_delete_candidates(state.seen_files)
    removed = 0
    errors: list[str] = []

    for p in sorted(candidates):
        try:
            if os.path.isfile(p):
                os.remove(p)
                removed += 1
        except Exception as e:
            errors.append(f"{p}: {e}")

    return removed, errors


# ==============================
# GUI
# ==============================

class TaskRow(ttk.Frame):
    def __init__(
        self,
        master: tk.Widget,
        state: TaskState,
        on_pause_toggle: Callable[[str], None],
        on_soft_cancel: Callable[[str], None],
        on_resume_after_cancel: Callable[[str], None],
        on_delete: Callable[[str], None],
    ) -> None:
        super().__init__(master, padding=(8, 6))
        self.state = state
        self._on_pause_toggle = on_pause_toggle
        self._on_soft_cancel = on_soft_cancel
        self._on_resume_after_cancel = on_resume_after_cancel
        self._on_delete = on_delete

        self.thumb_label = ttk.Label(self, text="(нет превью)", width=18, anchor="center")
        self.thumb_label.grid(row=0, column=0, rowspan=3, sticky="nsew", padx=(0, 10))

        self.title_var = tk.StringVar(value=state.info.title)
        self.status_var = tk.StringVar(value=state.status)
        self.meta_var = tk.StringVar(value="Скорость: —   ETA: —")

        self.title_label = ttk.Label(self, textvariable=self.title_var, font=("TkDefaultFont", 10, "bold"))
        self.title_label.grid(row=0, column=1, columnspan=5, sticky="w")

        self.status_label = ttk.Label(self, textvariable=self.status_var)
        self.status_label.grid(row=1, column=1, sticky="w")

        self.meta_label = ttk.Label(self, textvariable=self.meta_var)
        self.meta_label.grid(row=1, column=2, columnspan=4, sticky="w")

        self.progress = ttk.Progressbar(self, orient="horizontal", mode="determinate", length=320, maximum=100.0)
        self.progress.grid(row=2, column=1, columnspan=3, sticky="we", pady=(4, 0))

        # Две кнопки, но они меняют смысл в зависимости от режима
        self.btn1_text = tk.StringVar(value="Пауза")
        self.btn2_text = tk.StringVar(value="Отмена")

        self.btn1 = ttk.Button(self, textvariable=self.btn1_text, command=self._btn1_clicked)
        self.btn1.grid(row=2, column=4, sticky="e", padx=(8, 0))

        self.btn2 = ttk.Button(self, textvariable=self.btn2_text, command=self._btn2_clicked)
        self.btn2.grid(row=2, column=5, sticky="e", padx=(8, 0))

        self.grid_columnconfigure(3, weight=1)

        self._tk_thumb: Optional[Any] = None
        self.mode: str = "normal"  # normal | soft_cancelled | disabled

    def set_thumbnail(self, tk_img: Any) -> None:
        self._tk_thumb = tk_img
        self.thumb_label.configure(image=self._tk_thumb, text="")

    def set_mode(self, mode: str) -> None:
        self.mode = mode
        if mode == "normal":
            # btn1 = Пауза/Продолжить, btn2 = Отмена
            self.btn2_text.set("Отмена")
            self._sync_pause_text()
            self.btn1.configure(state="normal")
            self.btn2.configure(state="normal")
        elif mode == "soft_cancelled":
            # btn1 = Возобновить, btn2 = Удалить
            self.btn1_text.set("Возобновить")
            self.btn2_text.set("Удалить")
            self.btn1.configure(state="normal")
            self.btn2.configure(state="normal")
        elif mode == "disabled":
            self.btn1.configure(state="disabled")
            self.btn2.configure(state="disabled")

    def _sync_pause_text(self) -> None:
        paused = self.state.pause_flag.is_set()
        self.btn1_text.set("Продолжить" if paused else "Пауза")

    def update_fields(self, fields: Dict[str, Any]) -> None:
        if "status" in fields:
            self.status_var.set(str(fields["status"]))
        if "progress" in fields:
            try:
                self.progress["value"] = float(fields["progress"])
            except Exception:
                pass

        speed = fields.get("speed", None)
        eta = fields.get("eta", None)
        if speed is not None or eta is not None:
            speed_txt = speed if speed is not None else "—"
            eta_txt = eta if eta is not None else "—"
            self.meta_var.set(f"Скорость: {speed_txt}   ETA: {eta_txt}")

        # если в нормальном режиме — обновим надпись паузы
        if self.mode == "normal":
            self._sync_pause_text()

    def _btn1_clicked(self) -> None:
        if self.mode == "normal":
            self._on_pause_toggle(self.state.task_id)
            self._sync_pause_text()
        elif self.mode == "soft_cancelled":
            self._on_resume_after_cancel(self.state.task_id)

    def _btn2_clicked(self) -> None:
        if self.mode == "normal":
            self._on_soft_cancel(self.state.task_id)
        elif self.mode == "soft_cancelled":
            self._on_delete(self.state.task_id)


class ScrollableFrame(ttk.Frame):
    def __init__(self, master: tk.Widget) -> None:
        super().__init__(master)
        self.canvas = tk.Canvas(self, highlightthickness=0)
        self.scrollbar = ttk.Scrollbar(self, orient="vertical", command=self.canvas.yview)
        self.inner = ttk.Frame(self.canvas)

        self.inner.bind("<Configure>", lambda e: self.canvas.configure(scrollregion=self.canvas.bbox("all")))
        self.canvas_window = self.canvas.create_window((0, 0), window=self.inner, anchor="nw")
        self.canvas.configure(yscrollcommand=self.scrollbar.set)

        self.canvas.pack(side="left", fill="both", expand=True)
        self.scrollbar.pack(side="right", fill="y")

        self.canvas.bind("<Configure>", self._on_canvas_configure)
        self.canvas.bind_all("<MouseWheel>", self._on_mousewheel)

    def _on_canvas_configure(self, event: tk.Event) -> None:
        self.canvas.itemconfigure(self.canvas_window, width=event.width)

    def _on_mousewheel(self, event: tk.Event) -> None:
        self.canvas.yview_scroll(int(-1 * (event.delta / 120)), "units")


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("YouTube Downloader (yt-dlp)")
        self.geometry("980x560")
        self.minsize(980, 560)

        self.msg_q: "queue.Queue[GuiMsg]" = queue.Queue()
        self.tasks: Dict[str, TaskState] = {}
        self.task_rows: Dict[str, TaskRow] = {}

        self._debounce_job: Optional[str] = None
        self._current_preview_tk: Optional[Any] = None

        self.download_dir = os.path.expanduser("~/Downloads")
        self.default_title = "Вставьте ссылку и нажмите «Получить инфо»"
        self.default_thumb_text = "Превью появится здесь"

        self._build_ui()
        self.after(80, self._poll_queue)

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=12)
        top.pack(fill="x")

        ttk.Label(top, text="Ссылка на видео:").grid(row=0, column=0, sticky="w")
        self.url_var = tk.StringVar()
        self.url_entry = ttk.Entry(top, textvariable=self.url_var, width=88)
        self.url_entry.grid(row=0, column=1, sticky="we", padx=(8, 8))
        self.url_entry.bind("<KeyRelease>", self._on_url_changed)

        self.fetch_btn = ttk.Button(top, text="Получить инфо", command=self._fetch_info_clicked)
        self.fetch_btn.grid(row=0, column=2, sticky="e")

        info = ttk.Frame(self, padding=(12, 0, 12, 12))
        info.pack(fill="x")

        self.preview_label = ttk.Label(info, text=self.default_thumb_text, width=28, anchor="center")
        self.preview_label.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 12))

        self.title_var = tk.StringVar(value=self.default_title)
        self.title_label = ttk.Label(info, textvariable=self.title_var, font=("TkDefaultFont", 11, "bold"))
        self.title_label.grid(row=0, column=1, sticky="w")

        self.folder_var = tk.StringVar(value=self.download_dir)
        folder_row = ttk.Frame(info)
        folder_row.grid(row=1, column=1, sticky="we", pady=(8, 0))
        ttk.Label(folder_row, text="Папка:").pack(side="left")
        ttk.Entry(folder_row, textvariable=self.folder_var).pack(side="left", fill="x", expand=True, padx=(8, 8))
        ttk.Button(folder_row, text="Выбрать…", command=self._choose_folder).pack(side="left")

        action = ttk.Frame(self, padding=(12, 0, 12, 12))
        action.pack(fill="x")
        self.add_btn = ttk.Button(action, text="Скачать", command=self._start_download_clicked)
        self.add_btn.pack(side="left")
        ttk.Label(action, text="(Можно добавлять несколько ссылок — загрузки параллельно)").pack(side="left", padx=(12, 0))

        ttk.Separator(self).pack(fill="x", padx=12, pady=(0, 8))
        ttk.Label(self, text="Очередь загрузок:", padding=(12, 0, 12, 6), font=("TkDefaultFont", 10, "bold")).pack(fill="x")

        self.scroll = ScrollableFrame(self)
        self.scroll.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        top.grid_columnconfigure(1, weight=1)
        info.grid_columnconfigure(1, weight=1)

    # ------------------------------
    # URL → debounce fetch info
    # ------------------------------

    def _on_url_changed(self, _event: tk.Event) -> None:
        if self._debounce_job is not None:
            try:
                self.after_cancel(self._debounce_job)
            except Exception:
                pass
        self._debounce_job = self.after(800, self._auto_fetch_if_possible)

    def _auto_fetch_if_possible(self) -> None:
        self._debounce_job = None
        url = self.url_var.get().strip()
        if not url:
            return
        if "youtu" not in url and "youtube" not in url:
            return
        self._fetch_info_clicked()

    # ------------------------------
    # Fetch info
    # ------------------------------

    def _fetch_info_clicked(self) -> None:
        url = self.url_var.get().strip()
        if not url:
            messagebox.showwarning("Ссылка", "Вставьте ссылку на видео.")
            return

        self.fetch_btn.configure(state="disabled")
        self.title_var.set("Получаю информацию…")
        self.preview_label.configure(text="Загрузка превью…", image="")
        self._current_preview_tk = None

        def worker() -> None:
            try:
                info = fetch_video_info(url)
                self.msg_q.put(("task_update", "__preview__", {"info": info}))
            except Exception as e:
                self.msg_q.put(("task_update", "__preview__", {"error": str(e)}))

        threading.Thread(target=worker, daemon=True).start()

    def _apply_preview_info(self, info: VideoInfo) -> None:
        self.title_var.set(info.title)

        if info.thumbnail_url:
            def thumb_worker(thumb_url: str) -> None:
                try:
                    _, tk_img = download_thumbnail_to_pil(thumb_url, max_size=(260, 146))
                    self.msg_q.put(("task_update", "__preview__", {"thumb_tk": tk_img}))
                except Exception:
                    self.msg_q.put(("task_update", "__preview__", {"thumb_err": True}))

            threading.Thread(target=thumb_worker, args=(info.thumbnail_url,), daemon=True).start()
        else:
            self.preview_label.configure(text="Превью недоступно", image="")
            self._current_preview_tk = None

    # ------------------------------
    # Folder
    # ------------------------------

    def _choose_folder(self) -> None:
        d = filedialog.askdirectory(initialdir=self.folder_var.get() or self.download_dir)
        if d:
            self.folder_var.set(d)

    # ------------------------------
    # Start download
    # ------------------------------

    def _start_download_clicked(self) -> None:
        url = self.url_var.get().strip()
        if not url:
            messagebox.showwarning("Ссылка", "Вставьте ссылку на видео.")
            return

        out_dir = self.folder_var.get().strip() or self.download_dir
        os.makedirs(out_dir, exist_ok=True)

        initial_info = VideoInfo(
            url=url,
            title=self.title_var.get() if self.title_var.get() and self.title_var.get() != self.default_title else "—",
            thumbnail_url=None,
        )

        task_id = uuid.uuid4().hex[:12]
        state = TaskState(task_id=task_id, info=initial_info, out_dir=out_dir)
        self.tasks[task_id] = state

        row = TaskRow(
            self.scroll.inner,
            state=state,
            on_pause_toggle=self._pause_toggle_task,
            on_soft_cancel=self._soft_cancel_task,
            on_resume_after_cancel=self._resume_after_soft_cancel,
            on_delete=self._delete_task,
        )
        row.pack(fill="x", expand=True, pady=6)
        self.task_rows[task_id] = row

        # подтянем инфо (title+thumb+format_kind)
        def info_worker() -> None:
            try:
                info = fetch_video_info(url)
                self.msg_q.put(("task_update", task_id, {"info": info}))
                if info.thumbnail_url:
                    try:
                        _, tk_img = download_thumbnail_to_pil(info.thumbnail_url, max_size=(200, 112))
                        self.msg_q.put(("task_update", task_id, {"thumb_tk": tk_img}))
                    except Exception:
                        pass
            except Exception as e:
                self.msg_q.put(("task_update", task_id, {"status": f"Инфо не получено: {e}"}))

        threading.Thread(target=info_worker, daemon=True).start()

        # старт скачивания
        def dl_worker() -> None:
            start_download_worker(state, self.msg_q)

        t = threading.Thread(target=dl_worker, daemon=True)
        state.worker_thread = t
        t.start()

    # ------------------------------
    # Task controls
    # ------------------------------

    def _pause_toggle_task(self, task_id: str) -> None:
        st = self.tasks.get(task_id)
        row = self.task_rows.get(task_id)
        if not st or not row:
            return
        if st.soft_cancelled:
            return  # в режиме soft-cancel кнопка1 другая

        if st.pause_flag.is_set():
            st.pause_flag.clear()
        else:
            st.pause_flag.set()
            row.update_fields({"status": "Пауза"})

        row.set_mode("normal")

    def _soft_cancel_task(self, task_id: str) -> None:
        """
        "Отмена" по ТЗ: ставит на паузу и меняет кнопки на Возобновить/Удалить.
        """
        st = self.tasks.get(task_id)
        row = self.task_rows.get(task_id)
        if not st or not row:
            return

        st.soft_cancelled = True
        st.pause_flag.set()
        row.set_mode("soft_cancelled")
        row.update_fields({"status": "Пауза (отменено)", "speed": "—", "eta": "—"})

    def _resume_after_soft_cancel(self, task_id: str) -> None:
        st = self.tasks.get(task_id)
        row = self.task_rows.get(task_id)
        if not st or not row:
            return

        st.soft_cancelled = False
        st.pause_flag.clear()
        row.set_mode("normal")
        row.update_fields({"status": "Возобновлено"})

    def _delete_task(self, task_id: str) -> None:
        """
        "Удалить" по ТЗ:
        - остановить загрузку (hard cancel)
        - удалить уже созданные/скачанные файлы
        - убрать задачу из UI
        """
        st = self.tasks.get(task_id)
        row = self.task_rows.get(task_id)
        if not st or not row:
            return

        # UI: временно отключим
        row.set_mode("disabled")
        row.update_fields({"status": "Удаление…", "speed": "—", "eta": "—"})

        # Жёстко отменяем, чтобы поток вышел
        st.cancel_flag.set()
        st.pause_flag.clear()

        def cleanup() -> None:
            # чуть подождём завершения потока (не блокируем GUI)
            th = st.worker_thread
            if th is not None and th.is_alive():
                th.join(timeout=2.0)

            removed, errs = delete_task_files(st)

            # убрать из UI (в main thread)
            def ui_remove() -> None:
                # remove row widget
                r = self.task_rows.pop(task_id, None)
                if r is not None:
                    try:
                        r.destroy()
                    except Exception:
                        pass
                self.tasks.pop(task_id, None)

                # если были ошибки удаления — покажем коротко
                if errs:
                    messagebox.showwarning(
                        "Удаление файлов",
                        f"Удалено файлов: {removed}\n\nНекоторые файлы удалить не удалось (возможно, заняты):\n"
                        + "\n".join(errs[:10])
                        + ("\n…" if len(errs) > 10 else ""),
                    )

            self.after(0, ui_remove)

        threading.Thread(target=cleanup, daemon=True).start()

    # ------------------------------
    # Queue polling
    # ------------------------------

    def _poll_queue(self) -> None:
        try:
            while True:
                msg_type, task_id, fields = self.msg_q.get_nowait()
                if msg_type != "task_update":
                    continue

                if task_id == "__preview__":
                    if "error" in fields:
                        self.title_var.set("Не удалось получить информацию")
                        self.preview_label.configure(text="Ошибка", image="")
                        self._current_preview_tk = None
                        self.fetch_btn.configure(state="normal")
                    if "info" in fields:
                        self.fetch_btn.configure(state="normal")
                        info = fields["info"]
                        if isinstance(info, VideoInfo):
                            self._apply_preview_info(info)
                    if "thumb_tk" in fields:
                        self._current_preview_tk = fields["thumb_tk"]
                        self.preview_label.configure(image=self._current_preview_tk, text="")
                    if "thumb_err" in fields:
                        self.preview_label.configure(text="Превью недоступно", image="")
                        self._current_preview_tk = None
                    continue

                row = self.task_rows.get(task_id)
                st = self.tasks.get(task_id)
                if not row or not st:
                    continue

                if "info" in fields and isinstance(fields["info"], VideoInfo):
                    info: VideoInfo = fields["info"]
                    st.info = info
                    row.title_var.set(info.title)

                if "thumb_tk" in fields:
                    row.set_thumbnail(fields["thumb_tk"])

                row.update_fields(fields)

                # Если задача завершилась — отключим кнопки (как минимум)
                status = fields.get("status")
                if isinstance(status, str) and (status.startswith("Готово") or status.startswith("Ошибка") or status.startswith("Отменено")):
                    # Если пользователь в soft_cancelled — не трогаем (он мог отменить специально)
                    if not st.soft_cancelled:
                        row.set_mode("disabled")

        except queue.Empty:
            pass
        finally:
            self.after(80, self._poll_queue)

def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
