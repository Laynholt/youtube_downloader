import os
import re
import sys
import queue
import threading
import tkinter as tk
from pathlib import Path
from tkinter import filedialog, ttk
from typing import Any, Dict, Optional, Tuple, Callable

from downloader.ytdlp_client import (
    VideoInfo, TaskRuntime,
    fetch_video_info, download_task,
    probe_url_kind, expand_playlist, set_cookies_file, set_quality_mode, set_container_mode,
)
from downloader.thumbs import download_thumbnail_to_tk, load_placeholder_to_tk, load_placeholder_error_to_tk
from downloader.cleanup import delete_task_files


from utils.paths import default_download_dir, stuff_dir
from utils.ffmpeg_installer import find_ffmpeg, install_ffmpeg, set_ffmpeg_path
from utils.config import load_config, save_config, get_app_version
from utils.clipboard import install_layout_independent_clipboard_bindings
from utils.updater import fetch_latest_release, compare_versions, install_update_from_url
from utils.text_utils import ensure_file_logger, sanitize_text

from ui.tooltips import add_tooltip
from ui.dialogs import show_error, show_info, show_warning, ask_yes_no
from ui.widgets import ScrollableFrame, TaskRow
from ui.theme import get_default_colors

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


_logger = ensure_file_logger("app")


class UpdateProgressWindow:
    def __init__(
        self,
        master: tk.Tk,
        colors: Dict[str, str],
        *,
        title: str = "Обновление",
        on_cancel: Optional[Callable[[], None]] = None,
    ) -> None:
        self.master = master
        self.colors = colors
        self._on_cancel = on_cancel
        self.win = tk.Toplevel(master)
        self.win.title(title)
        try:
            self.win.iconbitmap("assets/icon.ico")
        except Exception:
            pass
        self.win.configure(bg=self.colors["panel"])
        self.win.transient(master)
        self.win.grab_set()
        self.win.resizable(False, False)
        self.win.protocol("WM_DELETE_WINDOW", self._handle_close)

        frame = ttk.Frame(self.win, padding=14, style="Panel.TFrame")
        frame.pack(fill="both", expand=True)

        self.status_var = tk.StringVar(value="")
        ttk.Label(frame, textvariable=self.status_var, style="Muted.TLabel", wraplength=320, justify="left").pack(anchor="w", pady=(6, 0))

        self.pb = ttk.Progressbar(frame, mode="indeterminate", length=320, maximum=100)
        self.pb.pack(fill="x", pady=(12, 0))
        try:
            self.pb.start(12)
        except Exception:
            pass

        self._link_lbl: Optional[ttk.Label] = None

        frame.update_idletasks()
        w = max(360, frame.winfo_width() + 40)
        h = max(100, frame.winfo_height() + 20)
        sw = self.win.winfo_screenwidth()
        sh = self.win.winfo_screenheight()
        x = max(0, (sw - w) // 2)
        y = max(0, (sh - h) // 2)
        self.win.geometry(f"{w}x{h}+{x}+{y}")

    def set_status(self, text: str) -> None:
        self.status_var.set(text)
        self._maybe_set_link(text)

    def set_progress(self, text: str, ratio: Optional[float]) -> None:
        self.set_status(text)
        if ratio is None:
            try:
                self.pb.configure(mode="indeterminate")
                self.pb.start(12)
            except Exception:
                pass
            return
        try:
            self.pb.stop()
            self.pb.configure(mode="determinate")
            self.pb["value"] = max(0, min(100, int(ratio * 100)))
        except Exception:
            pass

    def close(self) -> None:
        try:
            self.pb.stop()
        except Exception:
            pass
        try:
            self.win.destroy()
        except Exception:
            pass

    def _handle_close(self) -> None:
        try:
            if self._on_cancel:
                self._on_cancel()
        finally:
            self.close()

    def _maybe_set_link(self, text: str) -> None:
        m = re.search(r"https?://\S+", text or "")
        if not m:
            return
        url = m.group(0).rstrip(".,)")
        if self._link_lbl is None:
            self._link_lbl = ttk.Label(self.win, text=url, foreground=self.colors["accent"], style="Panel.TLabel", cursor="hand2", wraplength=340)
            self._link_lbl.pack(fill="x", pady=(6, 0))
            self._link_lbl.bind("<Button-1>", lambda _e: self._open_link(url))
        else:
            self._link_lbl.configure(text=url)

    @staticmethod
    def _open_link(url: str) -> None:
        try:
            import webbrowser
            webbrowser.open(url)
        except Exception:
            pass


class App(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("YouTube Downloader")
        self.geometry("980x590")
        self.minsize(980, 590)

        self.msg_q: "queue.Queue[GuiMsg]" = queue.Queue()
        self.tasks: Dict[str, TaskCtx] = {}
        self._playlist_batches: Dict[str, Dict[str, Any]] = {}
        self.colors = get_default_colors()

        cfg = load_config()
        self.ffmpeg_available = bool(find_ffmpeg())
        self.download_dir = str(cfg.get("download_dir") or default_download_dir())
        if not self.download_dir:
            self.download_dir = str(default_download_dir())
        self.cookies_path = str(cfg.get("cookies_path") or "")
        self.quality_mode = self._pick_quality_mode(cfg)
        self.container_mode = self._pick_container_mode(cfg)
        self.auto_update_enabled = bool(cfg.get("auto_update"))
        self.ffmpeg_path = str(cfg.get("ffmpeg_path") or "")
        if self.ffmpeg_path:
            set_ffmpeg_path(Path(self.ffmpeg_path))
        self.ffmpeg_available = bool(find_ffmpeg())
        set_cookies_file(self.cookies_path or None)
        set_quality_mode(self.quality_mode)
        set_container_mode(self._effective_container_mode())

        self._debounce_job: Optional[str] = None
        self._current_preview_tk: Optional[Any] = None
        self._update_progress_win: Optional[UpdateProgressWindow] = None
        self._update_cancel_evt: Optional[threading.Event] = None
        self._update_thread: Optional[threading.Thread] = None
        self._ffmpeg_progress_win: Optional[UpdateProgressWindow] = None
        self._ffmpeg_cancel_evt: Optional[threading.Event] = None
        self._ffmpeg_install_thread: Optional[threading.Thread] = None
        self._auto_update_check_started = False
        self._closing = False

        self.default_title = "👆 Вставьте ссылку на видео или плейлист выше 👆"
        self.url_placeholder = "Ссылка на видео или плейлист"

        self._init_theme()
        self._build_ui()
        self._install_hotkeys()
        install_layout_independent_clipboard_bindings(self)

        try:
            self.iconbitmap("assets/icon.ico")
        except Exception:
            pass

        self.protocol("WM_DELETE_WINDOW", self._on_close_clicked)
        self.after(80, self._poll_queue)
        self.after(600, self._check_ffmpeg_presence)
        self.after(1400, self._auto_check_updates_if_enabled)

    # ---------------- UI ----------------

    def _build_ui(self) -> None:
        top = ttk.Frame(self, padding=12, style="Panel.TFrame")
        top.pack(fill="x")

        self.url_var = tk.StringVar()
        self.url_entry = ttk.Entry(top, textvariable=self.url_var, style="Url.TEntry")
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

        spacer = ttk.Frame(action, style="Panel.TFrame")
        spacer.pack(side="left", expand=True, fill="x")

        btn_settings = ttk.Button(action, text="Настройки", command=self._open_settings, style="Ghost.TButton")
        btn_settings.pack(side="right")
        add_tooltip(btn_settings, "Открыть настройки (cookies и др.).")

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
        self._update_config(download_dir=path)

    def _save_cookies_path(self, path: str) -> None:
        self.cookies_path = path
        set_cookies_file(path or None)
        self._update_config(cookies_path=path)

    def _save_quality_mode(self, mode: str) -> None:
        normalized = self._normalize_quality_mode(mode) or "max"
        self.quality_mode = normalized
        set_quality_mode(self.quality_mode)
        self._update_config(quality=self.quality_mode)
        return None

    def _save_container_mode(self, mode: str) -> None:
        normalized = self._normalize_container_mode(mode)
        self.container_mode = normalized
        set_container_mode(self._effective_container_mode())
        self._update_config(container=self.container_mode)

    def _save_ffmpeg_path(self, path: str) -> None:
        path = path.strip()
        if path:
            set_ffmpeg_path(Path(path))
            self.ffmpeg_path = path
            self.ffmpeg_available = bool(find_ffmpeg(refresh=True))
        else:
            self.ffmpeg_path = ""
            self.ffmpeg_available = bool(find_ffmpeg(refresh=True))
        self._update_config(ffmpeg_path=self.ffmpeg_path)
        set_container_mode(self._effective_container_mode())

    def _prompt_ffmpeg_choice(self) -> str:
        """
        Возвращает "install" | "pick" | "skip".
        """
        win = tk.Toplevel(self)
        win.title("FFmpeg не найден")
        try:
            win.iconbitmap("assets/icon.ico")
        except Exception:
            pass
        win.configure(bg=self.colors["panel"])
        win.transient(self)
        win.grab_set()
        win.resizable(False, False)

        frame = ttk.Frame(win, padding=14, style="Panel.TFrame")
        frame.pack(fill="both", expand=True)

        ttk.Label(
            frame,
            text="ffmpeg нужен для склейки лучшего видео+аудио и качества выше 1080p.",
            style="Panel.TLabel",
            wraplength=420,
            justify="left",
        ).grid(row=0, column=0, columnspan=3, sticky="w")

        result = {"choice": "skip"}

        def set_choice(val: str) -> None:
            result["choice"] = val
            win.destroy()

        ttk.Button(frame, text="Отмена", style="Ghost.TButton", command=win.destroy).grid(row=1, column=0, pady=(12, 0))
        ttk.Button(frame, text="Указать путь", style="Ghost.TButton", command=lambda: set_choice("pick")).grid(row=1, column=1, padx=(0, 8), pady=(12, 0))
        ttk.Button(frame, text="Установить", style="Accent.TButton", command=lambda: set_choice("install")).grid(row=1, column=2, padx=(0, 8), pady=(12, 0))

        frame.grid_columnconfigure(0, weight=1)
        frame.grid_columnconfigure(1, weight=1)
        frame.grid_columnconfigure(2, weight=1)

        win.update_idletasks()
        x = self.winfo_rootx() + max(0, (self.winfo_width() - win.winfo_width()) // 2)
        y = self.winfo_rooty() + max(0, (self.winfo_height() - win.winfo_height()) // 2)
        win.geometry(f"+{x}+{y}")
        win.wait_window()
        return result["choice"]

    def _choose_existing_ffmpeg(self) -> None:
        path = filedialog.askopenfilename(
            title="Укажите ffmpeg.exe",
            filetypes=[("ffmpeg", "ffmpeg.exe"), ("Все файлы", "*.*")],
            initialdir=os.path.dirname(self.ffmpeg_path or self.download_dir),
        )
        if not path:
            return
        picked = set_ffmpeg_path(Path(path))
        if picked and picked.exists():
            self.ffmpeg_available = True
            self.ffmpeg_path = str(picked)
            self._update_config(ffmpeg_path=self.ffmpeg_path)
            set_container_mode(self._effective_container_mode())
            show_info("FFmpeg", f"FFmpeg найден: {picked}", parent=self)
        else:
            show_error("FFmpeg", "Указанный путь не подходит для ffmpeg.", parent=self)

    def _save_auto_update(self, enabled: bool) -> None:
        self.auto_update_enabled = bool(enabled)
        self._update_config(auto_update=self.auto_update_enabled)
        if self.auto_update_enabled and not self._auto_update_check_started:
            self._auto_check_updates_if_enabled()

    @staticmethod
    def _normalize_quality_mode(mode: Any) -> Optional[str]:
        allowed = {"audio", "360p", "480p", "720p", "1080p", "max"}
        mode_str = str(mode).lower() if mode is not None else ""
        return mode_str if mode_str in allowed else None

    @staticmethod
    def _normalize_container_mode(mode: Any) -> str:
        allowed = {"auto", "mp4", "mkv", "webm"}
        mode_str = str(mode).lower() if mode is not None else ""
        return mode_str if mode_str in allowed else "auto"

    def _pick_quality_mode(self, cfg: Dict[str, Any]) -> str:
        saved = self._normalize_quality_mode(cfg.get("quality"))
        if saved:
            return saved
        return "max"

    def _pick_container_mode(self, cfg: Dict[str, Any]) -> str:
        saved = self._normalize_container_mode(cfg.get("container"))
        return saved if saved else "auto"

    def _effective_container_mode(self) -> str:
        if not self.ffmpeg_available:
            return "auto"
        return self.container_mode or "auto"

    def _update_config(self, **kwargs: Any) -> None:
        cfg = load_config()
        cfg.update(kwargs)
        save_config(cfg)

    def _log_error(self, msg: str, exc: Optional[Exception] = None) -> None:
        text = sanitize_text(msg)
        if exc:
            text = f"{text} | {sanitize_text(exc)}"
        _logger.error(text)

    # -------------- Startup checks -----

    def _check_ffmpeg_presence(self) -> None:
        if getattr(self, "_ffmpeg_check_done", False):
            return
        self._ffmpeg_check_done = True

        if find_ffmpeg():
            self.ffmpeg_available = True
            set_container_mode(self._effective_container_mode())
            return

        choice = self._prompt_ffmpeg_choice()
        if choice == "pick":
            self._choose_existing_ffmpeg()
            return
        if choice != "install":
            msg = "Без ffmpeg загрузки будут в виде одного файла (best), склейка bestvideo+bestaudio недоступна."
            self._log_error(msg)
            show_warning("FFmpeg не установлен", msg, parent=self)
            return

        target_dir = filedialog.askdirectory(
            title="Выберите папку для установки ffmpeg",
            initialdir=str(stuff_dir()),
        )
        if not target_dir:
            show_warning("FFmpeg", "Установка отменена: папка не выбрана.", parent=self)
            return

        try:
            if self._ffmpeg_progress_win:
                self._ffmpeg_progress_win.close()
        except Exception:
            pass

        self._ffmpeg_cancel_evt = threading.Event()
        self._ffmpeg_progress_win = UpdateProgressWindow(
            self,
            self.colors,
            title="Установка FFmpeg",
            on_cancel=self._cancel_ffmpeg_install,
        )
        self._ffmpeg_progress_win.set_status("Подготовка установки...")

        def worker() -> None:
            def progress(msg: str, ratio: Optional[float] = None) -> None:
                self.msg_q.put(("task_update", "__ui__", {"ffmpeg_progress": {"msg": msg, "ratio": ratio}}))

            ok, msg, path = install_ffmpeg(
                target_root=Path(target_dir),
                progress=progress,
                cancel=self._ffmpeg_cancel_evt.is_set if self._ffmpeg_cancel_evt else None,
            )
            canceled = bool(self._ffmpeg_cancel_evt.is_set()) if self._ffmpeg_cancel_evt else False
            self.msg_q.put(("task_update", "__ui__", {"ffmpeg_done": {"ok": ok, "msg": msg, "canceled": canceled, "path": str(path) if path else ""}}))

        self._ffmpeg_install_thread = threading.Thread(target=worker, daemon=True)
        self._ffmpeg_install_thread.start()

    # -------------- Updates -------------

    def _auto_check_updates_if_enabled(self) -> None:
        if self._auto_update_check_started:
            return
        if not self.auto_update_enabled:
            return
        self._auto_update_check_started = True
        self._start_update_check(auto=True)

    def _on_check_updates_clicked(self) -> None:
        self._start_update_check(auto=False)

    def _start_update_check(self, *, auto: bool = False) -> None:
        def worker() -> None:
            try:
                latest = fetch_latest_release()
                latest_ver = latest.get("version") or ""
                download_url = latest.get("download_url") or ""
                page_url = latest.get("page_url") or ""
                current_ver = get_app_version()
                cmp = compare_versions(latest_ver or "0", current_ver or "0")
                is_frozen = bool(getattr(sys, "frozen", False))
                self.msg_q.put(
                    (
                        "task_update",
                        "__ui__",
                        {
                            "update_check": {
                                "latest": latest_ver,
                                "download_url": download_url,
                                "page_url": page_url,
                                "current": current_ver,
                                "cmp": cmp,
                                "frozen": is_frozen,
                                "auto": auto,
                            }
                        },
                    )
                )
            except Exception as e:
                if not auto:
                    self.msg_q.put(("task_update", "__ui__", {"ui_error": f"Не удалось проверить обновления: {e}"}))

        threading.Thread(target=worker, daemon=True).start()

    def _handle_update_check(self, data: Dict[str, Any]) -> None:
        latest = data.get("latest") or ""
        current = data.get("current") or ""
        cmp = int(data.get("cmp") or 0)
        frozen = bool(data.get("frozen"))
        download_url = data.get("download_url") or ""
        page_url = data.get("page_url") or ""
        auto = bool(data.get("auto"))

        if cmp <= 0 or not latest:
            if not auto:
                show_info("Обновления", f"У вас актуальная версия ({current}).", parent=self)
            return

        if not frozen:
            msg = (
                f"Доступна новая версия: {latest} (у вас {current}).\n\n"
                f"Запустите 'git pull' или скачайте с {page_url}"
            )
            show_info("Обновления", msg, parent=self)
            return

        if auto:
            self._start_update_install(download_url)
            return

        consent = ask_yes_no(
            "Обновление доступно",
            f"Доступна новая версия: {latest} (у вас {current}).\n\n"
            "Скачать и установить сейчас?",
            parent=self,
            yes="Установить",
            no="Позже",
        )
        if not consent:
            return

        self._start_update_install(download_url)

    def _start_update_install(self, download_url: str) -> None:
        if not download_url:
            msg = "Не удалось получить ссылку на обновление."
            self._log_error(msg)
            show_error("Обновления", msg, parent=self)
            return

        try:
            if self._update_progress_win:
                self._update_progress_win.close()
        except Exception:
            pass

        self._update_cancel_evt = threading.Event()
        self._update_progress_win = UpdateProgressWindow(self, self.colors, on_cancel=self._cancel_update_download)
        self._update_progress_win.set_status("Подготовка обновления...")

        def worker() -> None:
            def progress(msg: str, ratio: Optional[float] = None) -> None:
                self.msg_q.put(("task_update", "__ui__", {"update_progress": {"msg": msg, "ratio": ratio}}))

            ok, msg = install_update_from_url(
                download_url,
                progress=progress,
                cancel=self._update_cancel_evt.is_set if self._update_cancel_evt else None,
            )
            canceled = bool(self._update_cancel_evt.is_set()) if self._update_cancel_evt else False
            self.msg_q.put(("task_update", "__ui__", {"update_progress_done": {"ok": ok, "msg": msg, "canceled": canceled}}))

        self._update_thread = threading.Thread(target=worker, daemon=True)
        self._update_thread.start()

    def _handle_update_result(self, data: Dict[str, Any]) -> None:
        ok = bool(data.get("ok"))
        canceled = bool(data.get("canceled"))
        msg = str(data.get("msg") or "")
        if self._update_progress_win:
            self._update_progress_win.set_status(msg)

        if canceled:
            if self._update_progress_win:
                self._update_progress_win.close()
                self._update_progress_win = None
            self._update_cancel_evt = None
            self._update_thread = None
            return

        if ok:
            if self._update_progress_win:
                info_msg = (msg + "\n\nПриложение закроется и обновится. Перезапустите его вручную.").strip()
                self._update_progress_win.set_status(info_msg)
            self._update_cancel_evt = None
            self._update_thread = None
            # даём пользователю увидеть уведомление 3 секунды, затем выходим
            self.after(3000, self._exit_for_update)
        else:
            if self._update_progress_win:
                self._update_progress_win.close()
                self._update_progress_win = None
            show_error("Обновление", msg or "Не удалось установить обновление.", parent=self)
            self._log_error(msg or "Не удалось установить обновление.")
            self._update_cancel_evt = None
            self._update_thread = None

    def _handle_ffmpeg_progress(self, data: Dict[str, Any]) -> None:
        msg = str(data.get("msg") or "")
        ratio = data.get("ratio")
        if not self._ffmpeg_progress_win:
            self._ffmpeg_progress_win = UpdateProgressWindow(
                self, self.colors, title="Установка FFmpeg", on_cancel=self._cancel_ffmpeg_install
            )
        try:
            if ratio is None or isinstance(ratio, bool):
                self._ffmpeg_progress_win.set_status(msg)
            else:
                self._ffmpeg_progress_win.set_progress(msg, float(ratio))
        except Exception:
            pass

    def _handle_ffmpeg_done(self, data: Dict[str, Any]) -> None:
        ok = bool(data.get("ok"))
        canceled = bool(data.get("canceled"))
        msg = str(data.get("msg") or "")
        if self._ffmpeg_progress_win:
            self._ffmpeg_progress_win.set_status(msg or ("Установка отменена" if canceled else ""))
            self._ffmpeg_progress_win.close()
            self._ffmpeg_progress_win = None
        self._ffmpeg_cancel_evt = None
        self._ffmpeg_install_thread = None
        if canceled:
            show_info("FFmpeg", msg or "Установка ffmpeg отменена.", parent=self)
            return
        if ok:
            self.ffmpeg_available = True
            set_container_mode(self._effective_container_mode())
            if data.get("path"):
                self._save_ffmpeg_path(str(data.get("path")))
            show_info("FFmpeg", msg or "FFmpeg установлен.", parent=self)
        else:
            show_error("FFmpeg", msg or "Не удалось установить ffmpeg.", parent=self)
            self._log_error(msg or "Не удалось установить ffmpeg.")

    def _exit_for_update(self) -> None:
        try:
            if self._update_progress_win:
                self._update_progress_win.close()
        finally:
            self._update_progress_win = None
            try:
                self.destroy()
            finally:
                # Завершаем процесс гарантированно, чтобы PowerShell смог перезаписать файлы.
                os._exit(0)

    def _cancel_update_download(self) -> None:
        evt = self._update_cancel_evt
        if evt:
            evt.set()
        # Не ждём поток долго, просто дадим ему завершиться и отправить в очередь результат
        if self._update_progress_win:
            self._update_progress_win.set_status("Отмена загрузки...")

    def _cancel_ffmpeg_install(self) -> None:
        evt = self._ffmpeg_cancel_evt
        if evt:
            evt.set()
        if self._ffmpeg_progress_win:
            self._ffmpeg_progress_win.set_status("Отмена загрузки ffmpeg...")

    def _on_close_clicked(self) -> None:
        if self._closing:
            return
        self._closing = True
        if self._update_cancel_evt:
            self._update_cancel_evt.set()
        if self._ffmpeg_cancel_evt:
            self._ffmpeg_cancel_evt.set()
        # останавливаем все задачи и фоновые потоки, чтобы не держали процесс
        for ctx in list(self.tasks.values()):
            try:
                ctx.cancel_flag.set()
                ctx.pause_flag.clear()
                th = ctx.worker
                if th and th.is_alive():
                    th.join(timeout=1.5)
            except Exception:
                pass
        try:
            if self._update_thread and self._update_thread.is_alive():
                self._update_thread.join(timeout=1.5)
        except Exception:
            pass
        try:
            if self._ffmpeg_install_thread and self._ffmpeg_install_thread.is_alive():
                self._ffmpeg_install_thread.join(timeout=1.5)
        except Exception:
            pass

        try:
            self.destroy()
        finally:
            os._exit(0)

    def _open_about(self) -> None:
        win = tk.Toplevel(self)
        win.title("О программе")
        try:
            win.iconbitmap("assets/icon.ico")
        except Exception:
            pass
        win.configure(bg=self.colors["panel"])
        win.transient(self)
        win.grab_set()
        win.resizable(False, False)

        frame = ttk.Frame(win, padding=14, style="Panel.TFrame")
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="YouTube Downloader", style="PanelBold.TLabel", font=("TkDefaultFont", 12, "bold")).grid(row=0, column=0, sticky="w")
        ttk.Label(frame, text=f"Версия: {get_app_version()}", style="Panel.TLabel").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Label(frame, text="Автор: laynholt", style="Panel.TLabel").grid(row=2, column=0, sticky="w")

        btns = ttk.Frame(frame, style="Panel.TFrame")
        btns.grid(row=3, column=0, sticky="we", pady=(12, 0))
        ttk.Button(btns, text="Проверить обновления", style="Ghost.TButton", command=self._on_check_updates_clicked).pack(side="left", padx=(0, 10))
        ttk.Button(btns, text="Закрыть", style="Accent.TButton", command=win.destroy).pack(side="right")

        frame.grid_columnconfigure(0, weight=1)

        win.update_idletasks()
        desired_w = max(320, win.winfo_width() + 60)
        desired_h = win.winfo_height()
        screen_w = win.winfo_screenwidth()
        screen_h = win.winfo_screenheight()
        x = max(0, (screen_w - desired_w) // 2)
        y = max(0, (screen_h - desired_h) // 2)
        win.geometry(f"{desired_w}x{desired_h}+{x}+{y}")

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
        style.configure("Panel.TCheckbutton", background=colors["panel"], foreground=colors["text"])
        style.map("Panel.TCheckbutton", background=[("active", colors["panel_alt"])], foreground=[("active", colors["text"])])

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
            "Url.TEntry",
            fieldbackground=colors["panel"],
            background=colors["panel"],
            foreground=colors["text"],
            insertcolor=colors["text"],
            bordercolor=colors["accent"],
            lightcolor=colors["accent"],
            darkcolor=colors["accent"],
            padding=5,
        )
        style.map(
            "Url.TEntry",
            fieldbackground=[("focus", colors["panel"])],
            background=[("focus", colors["panel"])],
            bordercolor=[("focus", colors["accent_hover"])],
            lightcolor=[("focus", colors["accent_hover"])],
            darkcolor=[("focus", colors["accent_hover"])],
        )
        style.configure(
            "Panel.TCombobox",
            fieldbackground=colors["panel_alt"],
            background=colors["panel_alt"],
            foreground=colors["text"],
            bordercolor=colors["panel_alt"],
            lightcolor=colors["panel_alt"],
            darkcolor=colors["panel_alt"],
            arrowcolor=colors["text"],
            selectbackground=colors["panel_alt"],
            selectforeground=colors["text"],
        )
        style.map(
            "Panel.TCombobox",
            fieldbackground=[("readonly", colors["panel_alt"])],
            foreground=[("readonly", colors["text"])],
        )

        # цвет выпадающего списка combobox (listbox)
        self.option_add("*TCombobox*Listbox.background", colors["panel_alt"])
        self.option_add("*TCombobox*Listbox.foreground", colors["text"])
        self.option_add("*TCombobox*Listbox.selectBackground", colors["accent"])
        self.option_add("*TCombobox*Listbox.selectForeground", colors["bg"])
        self.option_add("*TCombobox*Listbox.borderWidth", 0)

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

    # -------------- Settings ------------

    def _open_settings(self) -> None:
        win = tk.Toplevel(self)
        win.title("Настройки")
        try:
            win.iconbitmap("assets/icon.ico")
        except Exception:
            pass
        win.configure(bg=self.colors["panel"])
        win.transient(self)
        win.grab_set()
        win.resizable(False, False)

        frame = ttk.Frame(win, padding=14, style="Panel.TFrame")
        frame.pack(fill="both", expand=True)

        ttk.Label(frame, text="Cookies файл (yt-dlp):", style="PanelBold.TLabel").grid(row=0, column=0, sticky="w")

        cookies_var = tk.StringVar(value=self.cookies_path)
        entry = ttk.Entry(frame, textvariable=cookies_var, width=52, style="Panel.TEntry")
        entry.grid(row=1, column=0, columnspan=2, sticky="we", pady=(6, 0))
        add_tooltip(
            entry,
            "Файл cookies в формате Netscape. Можно экспортировать из браузера (yt-dlp --cookies-from-browser ... --cookies file.txt).",
        )

        def choose_file() -> None:
            path = filedialog.askopenfilename(
                title="Выберите файл cookies",
                initialdir=os.path.dirname(cookies_var.get() or self.download_dir),
            )
            if path:
                cookies_var.set(path)

        ttk.Button(frame, text="Выбрать…", style="Accent.TButton", command=choose_file).grid(row=1, column=2, padx=(8, 0), pady=(6, 0))

        ttk.Label(frame, text="Путь к FFmpeg:", style="PanelBold.TLabel").grid(row=2, column=0, sticky="w", pady=(12, 0))
        ffmpeg_var = tk.StringVar(value=self.ffmpeg_path)
        ffmpeg_entry = ttk.Entry(frame, textvariable=ffmpeg_var, width=52, style="Panel.TEntry")
        ffmpeg_entry.grid(row=3, column=0, columnspan=2, sticky="we", pady=(6, 0))
        add_tooltip(ffmpeg_entry, "Укажите путь к ffmpeg (папка с ffmpeg.exe или сам файл).")

        def choose_ffmpeg_dir() -> None:
            path = filedialog.askdirectory(
                title="Укажите папку с ffmpeg (bin)",
                initialdir=os.path.dirname(ffmpeg_var.get() or self.download_dir),
            )
            if path:
                ffmpeg_var.set(path)

        ttk.Button(frame, text="Выбрать…", style="Accent.TButton", command=choose_ffmpeg_dir).grid(row=3, column=2, padx=(8, 0), pady=(6, 0))

        ttk.Label(frame, text="Качество загрузки:", style="PanelBold.TLabel").grid(row=4, column=0, sticky="w", pady=(12, 0))
        quality_choices = [
            ("audio", "Только аудио (лучшее)"),
            ("360p", "Видео 360p"),
            ("480p", "Видео 480p"),
            ("720p", "Видео 720p"),
            ("1080p", "Видео 1080p"),
            ("max", "Максимально доступное (лучшее видео + звук)"),
        ]
        label_by_code = {code: label for code, label in quality_choices}
        code_by_label = {label: code for code, label in quality_choices}
        quality_var = tk.StringVar(value=label_by_code.get(self.quality_mode, quality_choices[5][1]))
        quality_cb = ttk.Combobox(
            frame,
            textvariable=quality_var,
            values=[label for _, label in quality_choices],
            state="readonly",
            style="Panel.TCombobox",
            width=40,
        )
        quality_cb.grid(row=5, column=0, columnspan=3, sticky="we", pady=(6, 0))
        add_tooltip(quality_cb, "Выберите лучшее доступное качество в нужной категории: аудио или целевое разрешение.")

        ttk.Label(frame, text="Итоговый контейнер:", style="PanelBold.TLabel").grid(row=6, column=0, sticky="w", pady=(12, 0))
        container_choices = [
            ("auto", "Как в оригинале (без конвертации)"),
            ("mp4", "MP4 (требуется ffmpeg)"),
            ("mkv", "MKV (требуется ffmpeg)"),
            ("webm", "WEBM (требуется ffmpeg)"),
        ]
        container_label_by_code = {code: label for code, label in container_choices}
        container_code_by_label = {label: code for code, label in container_choices}
        container_var = tk.StringVar(value=container_label_by_code.get(self.container_mode, container_choices[0][1]))
        container_state = "readonly" if self.ffmpeg_available else "disabled"
        container_cb = ttk.Combobox(
            frame,
            textvariable=container_var,
            values=[label for _, label in container_choices],
            state=container_state,
            style="Panel.TCombobox",
            width=40,
        )
        container_cb.grid(row=7, column=0, columnspan=3, sticky="we", pady=(6, 0))
        add_tooltip(
            container_cb,
            "Выберите итоговый контейнер для файлов. Опции mp4/mkv/webm доступны после установки ffmpeg. "
            "«Как в оригинале» оставляет контейнер исходного видео.",
        )
        if not self.ffmpeg_available:
            ttk.Label(frame, text="Установите FFmpeg, чтобы выбрать другие контейнеры.", style="Muted.TLabel").grid(
                row=8, column=0, columnspan=3, sticky="w", pady=(4, 0)
            )

        auto_update_var = tk.BooleanVar(value=self.auto_update_enabled)
        auto_update_cb = ttk.Checkbutton(
            frame,
            text="Автопроверка обновлений при запуске",
            variable=auto_update_var,
            style="Panel.TCheckbutton",
        )
        auto_update_cb.grid(row=9, column=0, columnspan=3, sticky="w", pady=(12, 0))
        add_tooltip(
            auto_update_cb,
            "При запуске автоматически проверять наличие новой версии. В сборке .exe обновление установится сразу, "
            "в режиме Python покажется уведомление.",
        )

        def save_and_close() -> None:
            path = cookies_var.get().strip()
            self._save_cookies_path(path)
            selected_code = code_by_label.get(quality_var.get(), "max")
            self._save_quality_mode(selected_code)
            selected_container = container_code_by_label.get(container_var.get(), self.container_mode)
            self._save_container_mode(selected_container)
            self._save_auto_update(auto_update_var.get())
            self._save_ffmpeg_path(ffmpeg_var.get())
            win.destroy()

        btns = ttk.Frame(frame, style="Panel.TFrame")
        btns.grid(row=10, column=0, columnspan=3, sticky="we", pady=(12, 0))
        ttk.Button(btns, text="О программе", style="Ghost.TButton", command=self._open_about).grid(row=0, column=0, sticky="w")
        ttk.Button(btns, text="Отмена", style="Ghost.TButton", command=win.destroy).grid(row=0, column=2, sticky="e")
        ttk.Button(btns, text="Сохранить", style="Accent.TButton", command=save_and_close).grid(row=0, column=3, sticky="e", padx=(8, 0))
        btns.grid_columnconfigure(1, weight=1)

        frame.grid_columnconfigure(0, weight=1)

        win.update_idletasks()
        screen_w = win.winfo_screenwidth()
        screen_h = win.winfo_screenheight()
        win_w = win.winfo_width()
        win_h = win.winfo_height()
        x = max(0, (screen_w - win_w) // 2)
        y = max(0, (screen_h - win_h) // 2)
        win.geometry(f"+{x}+{y}")

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

        def on_retry() -> None:
            self._retry(task_id)

        row = TaskRow(
            self.scroll.inner,
            title=ctx.info.title,
            on_pause=on_pause,
            on_cancel_soft=on_cancel_soft,
            on_resume=on_resume,
            on_delete=on_delete,
            on_close=on_close,
            on_retry=on_retry,
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

        ctx.worker = threading.Thread(target=dl_worker, daemon=True)
        ctx.worker.start()
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

    def _retry(self, task_id: str) -> None:
        ctx = self.tasks.get(task_id)
        if not ctx or not ctx.row:
            return
        if ctx.worker and ctx.worker.is_alive():
            return

        ctx.pause_flag = threading.Event()
        ctx.cancel_flag = threading.Event()
        ctx.runtime = TaskRuntime(pause_flag=ctx.pause_flag, cancel_flag=ctx.cancel_flag)
        ctx.soft_cancelled = False
        ctx.finished_reported = False

        ctx.row.set_mode("normal", paused=False)
        ctx.row.update_fields(
            {
                "status": "Повтор: подготовка",
                "progress": 0.0,
                "speed": "",
                "eta": "",
                "total": "",
                "pct_text": "",
            }
        )

        def update(tid: str, fields: Dict[str, Any]) -> None:
            self.msg_q.put(("task_update", tid, fields))

        def dl_worker() -> None:
            download_task(task_id=task_id, info=ctx.info, out_dir=ctx.out_dir, runtime=ctx.runtime, update=update)

        ctx.worker = threading.Thread(target=dl_worker, daemon=True)
        ctx.worker.start()

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
                        self._current_preview_tk = load_placeholder_error_to_tk((260, 146))
                        self.preview_label.configure(image=self._current_preview_tk, text="")
                        self._log_error(str(fields["error"]))
                    if "info" in fields and isinstance(fields["info"], VideoInfo):
                        self._apply_preview_info(fields["info"])
                    if "thumb_tk" in fields:
                        self._current_preview_tk = fields["thumb_tk"]
                        self.preview_label.configure(image=self._current_preview_tk, text="")
                    if "thumb_err" in fields:
                        self._current_preview_tk = load_placeholder_error_to_tk((260, 146))
                        self.preview_label.configure(image=self._current_preview_tk, text="")
                        self._log_error(str(fields["thumb_err"]))
                    continue
                
                if task_id == "__ui__":
                    if "ui_info" in fields:
                        show_info("Информация", str(fields["ui_info"]), parent=self)
                        continue

                    if "ui_error" in fields:
                        self._log_error(str(fields["ui_error"]))
                        show_error("Ошибка", str(fields["ui_error"]), parent=self)
                        continue

                    if "ui_warning" in fields:
                        _logger.warning(sanitize_text(str(fields["ui_warning"])))
                        show_warning("Предупреждение", str(fields["ui_warning"]), parent=self)
                        continue

                    if "ffmpeg_progress" in fields:
                        self._handle_ffmpeg_progress(fields["ffmpeg_progress"])
                        continue

                    if "ffmpeg_done" in fields:
                        self._handle_ffmpeg_done(fields["ffmpeg_done"])
                        continue

                    if "update_progress" in fields:
                        if self._update_progress_win:
                            prog = fields["update_progress"]
                            if isinstance(prog, dict):
                                msg = str(prog.get("msg") or "")
                                ratio = prog.get("ratio")
                                self._update_progress_win.set_progress(msg, ratio)
                            else:
                                self._update_progress_win.set_status(str(prog))
                        continue

                    if "update_progress_done" in fields:
                        self._handle_update_result(fields["update_progress_done"])
                        continue

                    if "update_check" in fields:
                        self._handle_update_check(fields["update_check"])
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
                if "thumb_err" in fields:
                    ctx.row.set_thumbnail(load_placeholder_error_to_tk((200, 112)))

                ctx.row.update_fields(fields)

                status = str(fields.get("status") or "")
                if status == "Готово":
                    ctx.row.set_mode("done")
                    self._on_task_finished(task_id)
                elif status.startswith("Ошибка"):
                    if not ctx.soft_cancelled:
                        ctx.row.set_mode("error")
                    self._on_task_finished(task_id)
                elif status == "Отменено":
                    self._on_task_finished(task_id)

        except queue.Empty:
            pass
        finally:
            if not getattr(self, "_closing", False):
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
