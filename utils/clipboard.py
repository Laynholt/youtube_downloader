from __future__ import annotations

import tkinter as tk
from typing import Optional, Tuple


# Ctrl+<key> -> ASCII control char (надежно на Windows/Linux/macOS в Tk)
_CTRL_CHAR_TO_ACTION = {
    "\x01": "selectall",  # Ctrl+A
    "\x03": "copy",       # Ctrl+C
    "\x16": "paste",      # Ctrl+V
    "\x18": "cut",        # Ctrl+X
}


def _entry_sel_range(w: tk.Widget) -> Optional[Tuple[int, int]]:
    # Entry/ttk.Entry: selection range через index("sel.first"/"sel.last") может бросать TclError
    try:
        a = int(w.index("sel.first"))  # type: ignore[attr-defined]
        b = int(w.index("sel.last"))   # type: ignore[attr-defined]
        if a != b:
            return a, b
    except Exception:
        return None
    return None


def _text_sel_range(w: tk.Widget) -> Optional[Tuple[str, str]]:
    try:
        a = w.index("sel.first")  # type: ignore[attr-defined]
        b = w.index("sel.last")   # type: ignore[attr-defined]
        if a != b:
            return a, b
    except Exception:
        return None
    return None


def _clipboard_set(root: tk.Tk, s: str) -> None:
    root.clipboard_clear()
    root.clipboard_append(s)


def _clipboard_get(root: tk.Tk) -> str:
    try:
        return root.clipboard_get()
    except Exception:
        return ""


def _copy(root: tk.Tk, w: tk.Widget) -> None:
    cls = w.winfo_class()

    if cls == "Text":
        rng = _text_sel_range(w)
        if not rng:
            return
        a, b = rng
        s = w.get(a, b)  # type: ignore[attr-defined]
        if s:
            _clipboard_set(root, s)
        return

    # Entry / TEntry
    rng2 = _entry_sel_range(w)
    if not rng2:
        return
    a, b = rng2
    s = w.get()[a:b]  # type: ignore[attr-defined]
    if s:
        _clipboard_set(root, s)


def _cut(root: tk.Tk, w: tk.Widget) -> None:
    cls = w.winfo_class()

    if cls == "Text":
        rng = _text_sel_range(w)
        if not rng:
            return
        a, b = rng
        s = w.get(a, b)  # type: ignore[attr-defined]
        if s:
            _clipboard_set(root, s)
            w.delete(a, b)  # type: ignore[attr-defined]
        return

    rng2 = _entry_sel_range(w)
    if not rng2:
        return
    a, b = rng2
    text = w.get()  # type: ignore[attr-defined]
    s = text[a:b]
    if s:
        _clipboard_set(root, s)
        w.delete(a, b)  # type: ignore[attr-defined]


def _paste(root: tk.Tk, w: tk.Widget) -> None:
    s = _clipboard_get(root)
    if not s:
        return

    cls = w.winfo_class()

    if cls == "Text":
        # заменить выделение если есть
        rng = _text_sel_range(w)
        if rng:
            a, b = rng
            w.delete(a, b)  # type: ignore[attr-defined]
        w.insert("insert", s)  # type: ignore[attr-defined]
        w.see("insert")  # type: ignore[attr-defined]
        return

    # Entry/TEntry
    rng2 = _entry_sel_range(w)
    if rng2:
        a, b = rng2
        w.delete(a, b)  # type: ignore[attr-defined]
    w.insert("insert", s)  # type: ignore[attr-defined]
    try:
        w.icursor("insert")  # type: ignore[attr-defined]
    except Exception:
        pass


def _select_all(w: tk.Widget) -> None:
    cls = w.winfo_class()

    if cls == "Text":
        try:
            w.tag_add("sel", "1.0", "end-1c")  # type: ignore[attr-defined]
            w.mark_set("insert", "end-1c")     # type: ignore[attr-defined]
            w.see("insert")                    # type: ignore[attr-defined]
        except Exception:
            pass
        return

    # Entry/TEntry
    try:
        w.selection_range(0, "end")  # type: ignore[attr-defined]
        w.icursor("end")             # type: ignore[attr-defined]
    except Exception:
        pass


def install_layout_independent_clipboard_bindings(root: tk.Tk) -> None:
    """
    Делает Ctrl+A/C/V/X работающими в Entry/ttk.Entry/Text независимо от раскладки.
    """

    def handler(event: tk.Event) -> str:
        action = _CTRL_CHAR_TO_ACTION.get(getattr(event, "char", "") or "")
        if not action:
            return ""

        w = event.widget
        if action == "copy":
            _copy(root, w)
        elif action == "cut":
            _cut(root, w)
        elif action == "paste":
            _paste(root, w)
        elif action == "selectall":
            _select_all(w)

        return "break"

    for cls in ("Entry", "TEntry", "Text"):
        root.bind_class(cls, "<Control-KeyPress>", handler, add=True)
