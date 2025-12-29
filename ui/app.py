from __future__ import annotations

import os
import queue
import threading
import tkinter as tk
from tkinter import filedialog, ttk
from typing import Any, Dict, Optional, Tuple

from downloader.ytdlp_client import (
    VideoInfo, TaskRuntime,
    fetch_video_info, download_task,
    probe_url_kind, expand_playlist,
)
from downloader.thumbs import download_thumbnail_to_tk, load_placeholder_to_tk
from downloader.cleanup import delete_task_files

from utils.config import load_config, save_config
from utils.paths import default_download_dir
from ui.dialogs import show_error, show_info, show_warning
from ui.tooltips import add_tooltip
from utils.clipboard import install_layout_independent_clipboard_bindings

from .widgets import ScrollableFrame, TaskRow

GuiMsg = Tuple[str, str, Dict[str, Any]]  # ("task_update", task_id, fields)


class TaskCtx:
    def __init__(
        self,
        *,
        task_id: str,
        info: VideoInfo,
        out_dir: str,
        playlist_id: Optional[str] = None,
    ) -> None:
        self.task_id = task_id
        self.info = info
        self.out_dir = out_dir
        self.playlist_id = playlist_id

        self.pause_flag = threading.Event()
        self.cancel_flag = threading.Event()
        self.soft_cancelled = False
        self.finished_reported = False

        self.runtime = TaskRuntime(pause_flag=self.pause_flag, cancel_flag=self.cancel_flag)
        self.worker: Optional[threading.Thread] = None
        self.row: Optional[TaskRow] = None


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("YouTube Downloader")
        self.geometry("980x560")
        self.minsize(980, 560)

        self.msg_q: "queue.Queue[GuiMsg]" = queue.Queue()
        self.tasks: Dict[str, TaskCtx] = {}
        self._playlist_batches: Dict[str, Dict[str, Any]] = {}
        self.colors = {
            "bg": "#222429",
            "panel": "#262d3b",
            "panel_alt": "#2e3547",
            "text": "#e6e9f0",
            "muted": "#b1b7c7",
            "accent": "#5fa8f5",
            "accent_hover": "#76b6f7",
        }

        cfg = load_config()
        self.download_dir = str(cfg.get("download_dir") or default_download_dir())
        if not self.download_dir:
            self.download_dir = str(default_download_dir())

        self._debounce_job: Optional[str] = None
        self._current_preview_tk: Optional[Any] = None

        self.default_title = "Вставьте ссылку на видео или плейлист."
        self.url_placeholder = "Ссылка на видео или плейлист"

        self._init_theme()
        self._build_ui()
        self._install_hotkeys()
        install_layout_independent_clipboard_bindings(self)

        try:
            self.iconbitmap("assets/icon.ico")
        except Exception:
            pass

        self.after(80, self._poll_queue)

    # ---------------- UI ----------------

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=12, style="Panel.TFrame")
        top.pack(fill="x")

        ttk.Label(top, text="Ссылка на видео:", style="Panel.TLabel").grid(row=0, column=0, sticky="w")
        self.url_var = tk.StringVar()
        self.url_entry = ttk.Entry(top, textvariable=self.url_var, style="Panel.TEntry")
        self.url_entry.grid(row=0, column=0, sticky="we", padx=(0, 0))
        self.url_entry.bind("<KeyRelease>", self._on_url_changed)
        self.url_entry.bind("<FocusIn>", self._url_focus_in)
        self.url_entry.bind("<FocusOut>", self._url_focus_out)
        self._apply_url_placeholder()
        add_tooltip(self.url_entry, "Вставьте ссылку на видео или плейлист, затем нажмите Enter или «Скачать».")

        info = ttk.Frame(self, padding=(12, 0, 12, 12), style="Panel.TFrame")
        info.pack(fill="x")

        # заглушка (держим ссылку, иначе Tk её "съест" GC)
        self._current_preview_tk = load_placeholder_to_tk((260, 146))
        self.preview_label = ttk.Label(info, image=self._current_preview_tk, width=28, anchor="center", style="Panel.TLabel")
        self.preview_label.grid(row=0, column=0, rowspan=2, sticky="nsew", padx=(0, 12))

        self.title_var = tk.StringVar(value=self.default_title)
        ttk.Label(info, textvariable=self.title_var, font=("TkDefaultFont", 11, "bold"), style="PanelBold.TLabel").grid(
            row=0, column=1, sticky="w"
        )

        self.folder_var = tk.StringVar(value=self.download_dir)
        folder_row = ttk.Frame(info, style="Panel.TFrame")
        folder_row.grid(row=1, column=1, sticky="we", pady=(8, 0))
        ttk.Label(folder_row, text="Загрузки:", style="Panel.TLabel").pack(side="left")
        ttk.Entry(folder_row, textvariable=self.folder_var, style="Panel.TEntry").pack(side="left", fill="x", expand=True, padx=(8, 8))
        btn_choose = ttk.Button(folder_row, text="Выбрать…", command=self._choose_folder, style="Accent.TButton")
        btn_choose.pack(side="left")
        add_tooltip(btn_choose, "Выбрать папку, куда сохранять файлы.")

        action = ttk.Frame(self, padding=(12, 0, 12, 12), style="Panel.TFrame")
        action.pack(fill="x")
        btn_download = ttk.Button(action, text="Скачать", command=self._start_download_clicked, style="Accent.TButton")
        btn_download.pack(side="left")
        add_tooltip(btn_download, "Добавить ссылку (или плейлист) в очередь.")
        btn_clear = ttk.Button(action, text="Очистить очередь", command=self._clear_pending_batches, style="Ghost.TButton")
        btn_clear.pack(side="left", padx=(8, 0))
        add_tooltip(btn_clear, "Удалить все ожидающие пачки плейлистов.")
        ttk.Label(action, text="(Можно добавлять несколько ссылок - загрузки параллельно)", style="Muted.TLabel").pack(side="left", padx=(12, 0))

        ttk.Separator(self, style="Dark.TSeparator").pack(fill="x", padx=12, pady=(0, 8))
        header = ttk.Frame(self, padding=(12, 0, 12, 6), style="Bg.TFrame")
        header.pack(fill="x")
        ttk.Label(
            header,
            text="Очередь загрузок",
            padding=(0, 0, 0, 0),
            font=("TkDefaultFont", 12, "bold"),
            style="BgBold.TLabel",
            anchor="center",
            justify="center",
        ).pack(fill="x")

        self.scroll = ScrollableFrame(self, background=self.colors["bg"])
        self.scroll.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.scroll.set_background(self.colors["bg"])

        top.grid_columnconfigure(0, weight=1)
        info.grid_columnconfigure(1, weight=1)

    # -------------- Config --------------

    def _save_download_dir(self, path: str) -> None:
        cfg = load_config()
        cfg["download_dir"] = path
        save_config(cfg)

    def _init_theme(self) -> None:
        colors = self.colors
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except Exception:
            pass

        self.configure(bg=colors["bg"])
        style.configure("Panel.TFrame", background=colors["panel"])
        style.configure("Bg.TFrame", background=colors["bg"])

        style.configure("Panel.TLabel", background=colors["panel"], foreground=colors["text"])
        style.configure("PanelBold.TLabel", background=colors["panel"], foreground=colors["text"])
        style.configure("Muted.TLabel", background=colors["panel"], foreground=colors["muted"])
        style.configure("Bg.TLabel", background=colors["bg"], foreground=colors["text"])
        style.configure("BgBold.TLabel", background=colors["bg"], foreground=colors["text"])

        style.configure(
            "TButton",
            background=colors["panel_alt"],
            foreground=colors["text"],
            borderwidth=1,
            focusthickness=1,
            focuscolor=colors["accent"],
        )
        style.map("TButton", background=[("active", colors["accent_hover"])], foreground=[("active", colors["text"])])
        style.configure(
            "Accent.TButton",
            background=colors["accent"],
            foreground=colors["bg"],
            borderwidth=0,
            focusthickness=1,
            focuscolor=colors["accent_hover"],
            padding=(10, 6),
        )
        style.map("Accent.TButton", background=[("active", colors["accent_hover"])], foreground=[("active", colors["bg"])])
        style.configure(
            "Ghost.TButton",
            background=colors["panel"],
            foreground=colors["text"],
            borderwidth=1,
            focusthickness=1,
            focuscolor=colors["accent"],
            padding=(10, 6),
        )
        style.map("Ghost.TButton", background=[("active", colors["panel_alt"])], foreground=[("active", colors["text"])])

        style.configure(
            "Panel.TEntry",
            fieldbackground=colors["panel_alt"],
            background=colors["panel_alt"],
            foreground=colors["text"],
            insertcolor=colors["text"],
            bordercolor=colors["panel_alt"],
            lightcolor=colors["panel_alt"],
            darkcolor=colors["panel_alt"],
            padding=4,
        )

        style.configure(
            "Dark.TSeparator",
            background=colors["panel"],
            foreground=colors["panel"],
            bordercolor=colors["panel"],
            lightcolor=colors["panel"],
            darkcolor=colors["panel"],
        )

        style.configure(
            "Horizontal.TProgressbar",
            background=colors["accent"],
            troughcolor=colors["panel_alt"],
            lightcolor=colors["accent"],
            darkcolor=colors["accent"],
            bordercolor=colors["panel_alt"],
        )

        style.configure(
            "Dark.Vertical.TScrollbar",
            troughcolor=colors["bg"],
            background=colors["panel_alt"],
            bordercolor=colors["panel_alt"],
            arrowcolor=colors["text"],
            lightcolor=colors["panel_alt"],
            darkcolor=colors["panel_alt"],
        )

        style.configure("Treeview", background=colors["panel"], fieldbackground=colors["panel"], foreground=colors["text"])
        style.map("Treeview", background=[("selected", colors["panel_alt"])])

    def _clear_pending_batches(self) -> None:
        """
        Удаляет все ожидающие пачки плейлистов (оставляя текущие активные загрузки).
        """
        self._playlist_batches.clear()
        show_info("Очередь", "Ожидающие загрузки видео очищены.\nТекущие загрузки незатронуты.", parent=self)

    # -------------- URL helpers ---------

    def _apply_url_placeholder(self) -> None:
        self.url_var.set(self.url_placeholder)
        try:
            self.url_entry.configure(foreground=self.colors["muted"])
        except Exception:
            pass
        return None

    def _url_focus_in(self, _event: tk.Event) -> None:
        if self.url_var.get() == self.url_placeholder:
            self.url_var.set("")
            try:
                self.url_entry.configure(foreground=self.colors["text"])
            except Exception:
                pass
        return None

    def _url_focus_out(self, _event: tk.Event) -> None:
        if not self.url_var.get().strip():
            self._apply_url_placeholder()
        return None

    def _set_url_text(self, text: str) -> None:
        if text:
            self.url_var.set(text)
            try:
                self.url_entry.configure(foreground=self.colors["text"])
            except Exception:
                pass
        else:
            self._apply_url_placeholder()

    # -------------- Hotkeys -------------

    def _install_hotkeys(self) -> None:
        self.bind_all("<Return>", self._on_enter_pressed, add="+")
        self.bind_all("<Control-KeyPress>", self._on_ctrl_keypress, add="+")

    def _is_main_or_url_focus(self) -> bool:
        f = self.focus_get()
        return f is None or f is self or f is self.url_entry

    def _on_enter_pressed(self, _event: tk.Event) -> Optional[str]:
        if not self._is_main_or_url_focus():
            return None
        self._start_download_clicked()
        return "break"

    def _on_ctrl_keypress(self, event: tk.Event) -> Optional[str]:
        if not self._is_main_or_url_focus():
            return None

        keysym = str(getattr(event, "keysym", "")).lower()
        keycode = getattr(event, "keycode", None)
        valid_keys = {"v", "cyrillic_em", "cyrillic_ve"}
        if keycode in (86,):  # common keycode for V/В on Windows
            pass
        elif keysym not in valid_keys:
            return None

        try:
            clip = self.clipboard_get().strip()
        except Exception:
            clip = ""

        if clip:
            self._set_url_text(clip)
            self._on_url_changed(event)
            return "break"
        return None

    # -------------- URL debounce --------

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
        if url == self.url_placeholder:
            return
        if not url:
            return
        if "youtu" not in url and "youtube" not in url:
            return
        self._fetch_info_clicked()

    # -------------- Preview fetch -------

    def _fetch_info_clicked(self) -> None:
        url = self.url_var.get().strip()
        if not url:
            show_warning("Ссылка", "Вставьте ссылку на видео.", parent=self)
            return

        self.title_var.set("Получаю информацию…")
        # оставляем картинку-заглушку, просто убираем текст (если был)
        self.preview_label.configure(text="")

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
            def thumb_worker() -> None:
                try:
                    tk_img = download_thumbnail_to_tk(info.thumbnail_url, max_size=(260, 146))
                    self.msg_q.put(("task_update", "__preview__", {"thumb_tk": tk_img}))
                except Exception:
                    self.msg_q.put(("task_update", "__preview__", {"thumb_err": True}))

            threading.Thread(target=thumb_worker, daemon=True).start()
        else:
            self._current_preview_tk = load_placeholder_to_tk((260, 146))
            self.preview_label.configure(image=self._current_preview_tk, text="")

    # -------------- Folder --------------

    def _choose_folder(self) -> None:
        d = filedialog.askdirectory(initialdir=self.folder_var.get() or self.download_dir)
        if d:
            self.folder_var.set(d)
            self.download_dir = d
            self._save_download_dir(d)

    # -------------- Download start ------
    def _create_task_from_videoinfo(
        self,
        info: VideoInfo,
        out_dir: str,
        *,
        playlist_id: Optional[str] = None,
    ) -> str:
        task_id = os.urandom(6).hex()
        ctx = TaskCtx(task_id=task_id, info=info, out_dir=out_dir, playlist_id=playlist_id)
        self.tasks[task_id] = ctx

        def on_pause() -> None:
            self._pause_toggle(task_id)

        def on_cancel_soft() -> None:
            self._soft_cancel(task_id)

        def on_resume() -> None:
            self._resume(task_id)

        def on_delete() -> None:
            self._delete(task_id)

        def on_close() -> None:
            self._close(task_id)

        row = TaskRow(
            self.scroll.inner,
            title=ctx.info.title,
            on_pause=on_pause,
            on_cancel_soft=on_cancel_soft,
            on_resume=on_resume,
            on_delete=on_delete,
            on_close=on_close,
        )
        row.pack(fill="x", expand=True, pady=6)
        ctx.row = row

        # подтянуть полное info + превью (как и раньше)
        url = info.url

        def info_worker() -> None:
            try:
                full = fetch_video_info(url)
                self.msg_q.put(("task_update", task_id, {"info": full}))
                if full.thumbnail_url:
                    try:
                        tk_img = download_thumbnail_to_tk(full.thumbnail_url, max_size=(200, 112))
                        self.msg_q.put(("task_update", task_id, {"thumb_tk": tk_img}))
                    except Exception:
                        pass
            except Exception as e:
                self.msg_q.put(("task_update", task_id, {"status": f"Инфо не получено: {e}"}))

        threading.Thread(target=info_worker, daemon=True).start()

        # запуск скачивания
        def update(tid: str, fields: Dict[str, Any]) -> None:
            self.msg_q.put(("task_update", tid, fields))

        def dl_worker() -> None:
            download_task(task_id=task_id, info=ctx.info, out_dir=ctx.out_dir, runtime=ctx.runtime, update=update)

        t = threading.Thread(target=dl_worker, daemon=True)
        ctx.worker = t
        t.start()
        return task_id

    def _enqueue_videos_batched(
        self,
        videos: list[VideoInfo],
        *,
        batch_size: int = 5,
        delay_ms: int = 1200,
        out_dir: Optional[str] = None,
    ) -> None:
        """
        Добавляем элементы плейлиста небольшими пачками, следующая пачка идёт только после завершения предыдущей.
        """
        if not videos:
            return

        playlist_id = os.urandom(5).hex()
        self._playlist_batches[playlist_id] = {
            "videos": videos,
            "pos": 0,
            "active": set(),
            "batch_size": batch_size,
            "delay_ms": delay_ms,
            "out_dir": out_dir or self.download_dir,
        }
        self._start_playlist_batch(playlist_id)

    def _start_playlist_batch(self, playlist_id: str) -> None:
        batch = self._playlist_batches.get(playlist_id)
        if not batch:
            return

        start = batch["pos"]
        videos = batch["videos"]
        if start >= len(videos):
            self._playlist_batches.pop(playlist_id, None)
            return

        chunk = videos[start:start + batch.get("batch_size", 5)]
        batch["pos"] = start + len(chunk)

        for vi in chunk:
            if isinstance(vi, VideoInfo):
                tid = self._create_task_from_videoinfo(vi, batch["out_dir"], playlist_id=playlist_id)
                batch["active"].add(tid)

        if not batch["active"]:
            if batch["pos"] >= len(batch["videos"]):
                self._playlist_batches.pop(playlist_id, None)
            else:
                self.after(batch.get("delay_ms", 1200), lambda: self._start_playlist_batch(playlist_id))


    def _start_download_clicked(self) -> None:
        url = self.url_var.get().strip()
        if not url:
            show_warning("Ссылка", "Вставьте ссылку на видео или плейлист.", parent=self)
            return
        if url == self.url_placeholder:
            show_warning("Ссылка", "Вставьте ссылку на видео или плейлист.", parent=self)
            return

        out_dir = (self.folder_var.get().strip() or self.download_dir).strip()
        os.makedirs(out_dir, exist_ok=True)

        self.download_dir = out_dir
        self._save_download_dir(out_dir)

        # Чтобы UI не фризился — распознаём/разворачиваем в фоне
        def worker() -> None:
            try:
                kind, _ = probe_url_kind(url)
                if kind == "playlist":
                    pl_title, videos = expand_playlist(url)
                    if not videos:
                        self.msg_q.put(("task_update", "__ui__", {"ui_error": "Плейлист пустой или не удалось прочитать entries"}))
                        return
                    # отправим в UI пачку для добавления
                    self.msg_q.put(("task_update", "__ui__", {"enqueue_many": videos, "playlist_title": pl_title}))
                else:
                    # обычное видео — создаём один таск
                    initial_title = self.title_var.get()
                    if not initial_title or initial_title == self.default_title:
                        initial_title = "—"
                    vi = VideoInfo(url=url, title=initial_title)
                    self.msg_q.put(("task_update", "__ui__", {"enqueue_one": vi}))
            except Exception as e:
                self.msg_q.put(("task_update", "__ui__", {"ui_error": str(e)}))

        threading.Thread(target=worker, daemon=True).start()


    # -------------- Task actions --------

    def _pause_toggle(self, task_id: str) -> None:
        ctx = self.tasks.get(task_id)
        if not ctx or not ctx.row or ctx.soft_cancelled:
            return

        if ctx.pause_flag.is_set():
            ctx.pause_flag.clear()
            ctx.row.set_mode("normal", paused=False)
        else:
            ctx.pause_flag.set()
            ctx.row.update_fields({"status": "Пауза:"})
            ctx.row.set_mode("normal", paused=True)

    def _soft_cancel(self, task_id: str) -> None:
        ctx = self.tasks.get(task_id)
        if not ctx or not ctx.row:
            return

        ctx.soft_cancelled = True
        ctx.pause_flag.set()
        ctx.row.set_mode("soft_cancelled")
        ctx.row.update_fields({"status": "Пауза (отменено):", "speed": "", "eta": "", "total": "", "pct_text": ""})

    def _resume(self, task_id: str) -> None:
        ctx = self.tasks.get(task_id)
        if not ctx or not ctx.row:
            return

        ctx.soft_cancelled = False
        ctx.pause_flag.clear()
        ctx.row.set_mode("normal", paused=False)
        ctx.row.update_fields({"status": "Возобновлено:"})

    def _delete(self, task_id: str) -> None:
        ctx = self.tasks.get(task_id)
        if not ctx or not ctx.row:
            return

        ctx.row.set_mode("disabled")
        ctx.row.update_fields({"status": "Удаление:", "speed": "", "eta": "", "total": "", "pct_text": ""})

        ctx.cancel_flag.set()
        ctx.pause_flag.clear()

        def cleanup() -> None:
            th = ctx.worker
            if th is not None and th.is_alive():
                th.join(timeout=2.0)

            removed, errs = delete_task_files(ctx.runtime.seen_files)

            def ui_remove() -> None:
                self._close(task_id)
                if errs:
                    show_warning(
                        "Удаление файлов",
                        f"Удалено файлов: {removed}\n\nНе удалось удалить (возможно заняты):\n"
                        + "\n".join(errs[:10])
                        + ("\n…" if len(errs) > 10 else ""),
                        parent=self,
                    )

            self.after(0, ui_remove)

        threading.Thread(target=cleanup, daemon=True).start()

    def _close(self, task_id: str) -> None:
        ctx = self.tasks.pop(task_id, None)
        if not ctx:
            return
        self._on_task_finished(task_id, ctx)
        if ctx.row is not None:
            try:
                ctx.row.destroy()
            except Exception:
                pass

    # -------------- Queue polling -------

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
                    if "info" in fields and isinstance(fields["info"], VideoInfo):
                        self._apply_preview_info(fields["info"])
                    if "thumb_tk" in fields:
                        self._current_preview_tk = fields["thumb_tk"]
                        self.preview_label.configure(image=self._current_preview_tk, text="")
                    if "thumb_err" in fields:
                        self._current_preview_tk = load_placeholder_to_tk((260, 146))
                        self.preview_label.configure(image=self._current_preview_tk, text="")
                    continue
                
                if task_id == "__ui__":
                    if "ui_error" in fields:
                        show_error("Ошибка", str(fields["ui_error"]), parent=self)
                        continue

                    if "enqueue_one" in fields and isinstance(fields["enqueue_one"], VideoInfo):
                        self._create_task_from_videoinfo(fields["enqueue_one"], self.download_dir)
                        continue

                    if "enqueue_many" in fields and isinstance(fields["enqueue_many"], list):
                        videos = fields["enqueue_many"]
                        # Можно показать инфо (не обязательно)
                        # pl_title = str(fields.get("playlist_title") or "Плейлист")
                        self._enqueue_videos_batched(videos, out_dir=self.download_dir)
                        continue


                ctx = self.tasks.get(task_id)
                if not ctx or not ctx.row:
                    continue

                if "info" in fields and isinstance(fields["info"], VideoInfo):
                    ctx.info = fields["info"]
                    # важно: downloader получает ссылку на ctx.info при запуске,
                    # но обновления title/format_kind нам важны для UI и определения video/audio
                    ctx.row.title_var.set(ctx.info.title)

                if "thumb_tk" in fields:
                    ctx.row.set_thumbnail(fields["thumb_tk"])

                ctx.row.update_fields(fields)

                status = str(fields.get("status") or "")
                if status == "Готово":
                    ctx.row.set_mode("done")
                    self._on_task_finished(task_id)
                elif status.startswith("Ошибка"):
                    if not ctx.soft_cancelled:
                        ctx.row.set_mode("disabled")
                    self._on_task_finished(task_id)
                elif status == "Отменено":
                    self._on_task_finished(task_id)

        except queue.Empty:
            pass
        finally:
            self.after(80, self._poll_queue)

    def _on_task_finished(self, task_id: str, ctx: Optional[TaskCtx] = None) -> None:
        ctx = ctx or self.tasks.get(task_id)
        if not ctx or ctx.finished_reported:
            return

        ctx.finished_reported = True

        playlist_id = ctx.playlist_id
        if not playlist_id:
            return

        batch = self._playlist_batches.get(playlist_id)
        if not batch:
            return

        batch["active"].discard(task_id)
        if batch["active"]:
            return

        if batch["pos"] >= len(batch["videos"]):
            self._playlist_batches.pop(playlist_id, None)
            return

        delay_ms = batch.get("delay_ms", 1200)
        self.after(delay_ms, lambda: self._start_playlist_batch(playlist_id))
