from __future__ import annotations

import re
import textwrap
import tkinter as tk
from tkinter import ttk
from typing import Optional


_COLORS = {
    "bg": "#222429",
    "panel": "#262d3b",
    "text": "#e6e9f0",
    "muted": "#b1b7c7",
    "accent": "#5fa8f5",
    "warn": "#f0b23d",
    "error": "#f06b60",
}


_STYLE_APPLIED = False


def _sanitize_message(text: str) -> str:
    if not text:
        return ""
    # Убираем ANSI-escape последовательности (цвета и пр.)
    text = re.sub(r"\x1b\[[0-9;]*[A-Za-z]", "", text)
    # Стрип лишних пробелов/переводов строки по краям
    return text.strip()


def _wrap_message(text: str, width: int = 90) -> str:
    if not text:
        return ""
    lines = []
    for block in text.splitlines():
        if not block.strip():
            lines.append("")
            continue
        lines.append(textwrap.fill(block, width=width))
    return "\n".join(lines)


def _ensure_style() -> None:
    global _STYLE_APPLIED
    if _STYLE_APPLIED:
        return
    style = ttk.Style()
    try:
        style.theme_use("clam")
    except Exception:
        pass

    style.configure("Dialog.TFrame", background=_COLORS["panel"])
    style.configure("Dialog.TLabel", background=_COLORS["panel"], foreground=_COLORS["text"])
    style.configure("Dialog.Title.TLabel", background=_COLORS["panel"], foreground=_COLORS["text"], font=("TkDefaultFont", 11, "bold"))
    style.configure(
        "Dialog.TButton",
        background=_COLORS["panel"],
        foreground=_COLORS["text"],
        padding=(10, 6),
        borderwidth=1,
        focusthickness=1,
    )
    style.map("Dialog.TButton", background=[("active", _COLORS["accent"])], foreground=[("active", _COLORS["text"])])

    _STYLE_APPLIED = True


def _show_dialog(kind: str, title: str, message: str, parent: Optional[tk.Widget]) -> None:
    _ensure_style()
    clean_msg = _sanitize_message(message)
    display_msg = _wrap_message(clean_msg, width=90)

    root = parent if parent is not None else (tk._default_root or tk.Tk())
    created_root = False
    if root is None:
        root = tk.Tk()
        root.withdraw()
        created_root = True

    win = tk.Toplevel(root)
    win.title(title)
    win.configure(bg=_COLORS["panel"])
    win.resizable(False, False)
    win.transient(root)
    win.grab_set()
    try:
        win.iconbitmap("assets/icon.ico")
    except Exception:
        pass

    frame = ttk.Frame(win, padding=14, style="Dialog.TFrame")
    frame.pack(fill="both", expand=True)

    title_lbl = ttk.Label(frame, text=title, style="Dialog.Title.TLabel", anchor="w")
    title_lbl.pack(fill="x", pady=(0, 6))

    msg_color = _COLORS["text"]
    if kind == "warning":
        msg_color = _COLORS["warn"]
    elif kind == "error":
        msg_color = _COLORS["error"]

    msg_lbl = ttk.Label(
        frame,
        text=display_msg,
        style="Dialog.TLabel",
        foreground=msg_color,
        justify="left",
        anchor="w",
        wraplength=560,
    )
    msg_lbl.pack(fill="x")

    btn_row = ttk.Frame(frame, style="Dialog.TFrame")
    btn_row.pack(fill="x", pady=(12, 0))

    def copy_text() -> None:
        try:
            win.clipboard_clear()
            win.clipboard_append(clean_msg)
        except Exception:
            pass

    ttk.Button(btn_row, text="Копировать", style="Dialog.TButton", command=copy_text).pack(side="left")
    ttk.Button(btn_row, text="OK", style="Dialog.TButton", command=win.destroy).pack(side="right")

    win.update_idletasks()
    if parent is not None:
        x = parent.winfo_rootx() + max(0, (parent.winfo_width() - win.winfo_width()) // 2)
        y = parent.winfo_rooty() + max(0, (parent.winfo_height() - win.winfo_height()) // 2)
        win.geometry(f"+{x}+{y}")

    win.wait_window()
    if created_root:
        root.destroy()


def show_info(title: str, message: str, parent: Optional[tk.Widget] = None) -> None:
    _show_dialog("info", title, message, parent)


def show_warning(title: str, message: str, parent: Optional[tk.Widget] = None) -> None:
    _show_dialog("warning", title, message, parent)


def show_error(title: str, message: str, parent: Optional[tk.Widget] = None) -> None:
    _show_dialog("error", title, message, parent)


def ask_yes_no(title: str, message: str, parent: Optional[tk.Widget] = None, *, yes: str = "Да", no: str = "Нет") -> bool:
    """
    Показывает диалог с кнопками Да/Нет, возвращает True/False.
    """
    _ensure_style()
    clean_msg = _sanitize_message(message)
    display_msg = _wrap_message(clean_msg, width=90)

    root = parent if parent is not None else (tk._default_root or tk.Tk())
    created_root = False
    if root is None:
        root = tk.Tk()
        root.withdraw()
        created_root = True

    win = tk.Toplevel(root)
    win.title(title)
    win.configure(bg=_COLORS["panel"])
    win.resizable(False, False)
    win.transient(root)
    win.grab_set()
    try:
        win.iconbitmap("assets/icon.ico")
    except Exception:
        pass

    frame = ttk.Frame(win, padding=14, style="Dialog.TFrame")
    frame.pack(fill="both", expand=True)

    ttk.Label(frame, text=title, style="Dialog.Title.TLabel", anchor="w").pack(fill="x", pady=(0, 6))
    ttk.Label(
        frame,
        text=display_msg,
        style="Dialog.TLabel",
        justify="left",
        anchor="w",
        wraplength=560,
    ).pack(fill="x")

    btn_row = ttk.Frame(frame, style="Dialog.TFrame")
    btn_row.pack(fill="x", pady=(12, 0))

    result = tk.BooleanVar(value=False)

    def choose(value: bool) -> None:
        result.set(value)
        win.destroy()

    ttk.Button(btn_row, text=yes, style="Dialog.TButton", command=lambda: choose(True)).pack(side="right", padx=(8, 0))
    ttk.Button(btn_row, text=no, style="Dialog.TButton", command=lambda: choose(False)).pack(side="right")

    win.update_idletasks()
    if parent is not None:
        x = parent.winfo_rootx() + max(0, (parent.winfo_width() - win.winfo_width()) // 2)
        y = parent.winfo_rooty() + max(0, (parent.winfo_height() - win.winfo_height()) // 2)
        win.geometry(f"+{x}+{y}")

    win.wait_variable(result)
    if created_root:
        root.destroy()
    return bool(result.get())
