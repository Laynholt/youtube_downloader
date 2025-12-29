from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Optional


class _Tooltip:
    def __init__(self, widget: tk.Widget, text: str) -> None:
        self.widget = widget
        self.text = text
        self.tipwindow: Optional[tk.Toplevel] = None

        self.widget.bind("<Enter>", self._show)
        self.widget.bind("<Leave>", self._hide)

    def _show(self, _event: tk.Event) -> None:
        if self.tipwindow or not self.text:
            return
        x = self.widget.winfo_rootx() + 12
        y = self.widget.winfo_rooty() + self.widget.winfo_height() + 4
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        frame = ttk.Frame(tw, padding=(8, 4), style="Panel.TFrame")
        frame.pack(fill="both", expand=True)
        label = ttk.Label(frame, text=self.text, style="Panel.TLabel", justify="left")
        label.pack()

    def _hide(self, _event: tk.Event) -> None:
        tw = self.tipwindow
        self.tipwindow = None
        if tw:
            tw.destroy()


def add_tooltip(widget: tk.Widget, text: str) -> None:
    _Tooltip(widget, text)
