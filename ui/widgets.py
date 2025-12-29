from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from typing import Any, Dict, Optional


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


class TaskRow(ttk.Frame):
    def __init__(
        self,
        master: tk.Widget,
        *,
        title: str,
        on_pause,
        on_cancel_soft,
        on_resume,
        on_delete,
        on_close,
    ) -> None:
        super().__init__(master, padding=(8, 6))

        self._on_pause = on_pause
        self._on_cancel_soft = on_cancel_soft
        self._on_resume = on_resume
        self._on_delete = on_delete
        self._on_close = on_close

        self.thumb_label = ttk.Label(self, text="(нет превью)", width=18, anchor="center")
        self.thumb_label.grid(row=0, column=0, rowspan=3, sticky="nsew", padx=(0, 10))

        self.title_var = tk.StringVar(value=title)
        self.status_var = tk.StringVar(value="Ожидание")
        self.meta_var = tk.StringVar(value=self._format_meta("—", "—", "—", "—"))

        ttk.Label(self, textvariable=self.title_var, font=("TkDefaultFont", 10, "bold")).grid(
            row=0, column=1, columnspan=5, sticky="w"
        )
        ttk.Label(self, textvariable=self.status_var).grid(row=1, column=1, sticky="w")
        ttk.Label(self, textvariable=self.meta_var).grid(row=1, column=2, columnspan=4, sticky="w")

        self.progress = ttk.Progressbar(self, orient="horizontal", mode="determinate", length=340, maximum=100.0)
        self.progress.grid(row=2, column=1, columnspan=3, sticky="we", pady=(4, 0))

        self.btn1_text = tk.StringVar(value="Пауза")
        self.btn2_text = tk.StringVar(value="Отмена")

        self.btn1 = ttk.Button(self, textvariable=self.btn1_text, command=self._btn1_clicked)
        self.btn2 = ttk.Button(self, textvariable=self.btn2_text, command=self._btn2_clicked)
        self.btn1.grid(row=2, column=4, sticky="e", padx=(8, 0))
        self.btn2.grid(row=2, column=5, sticky="e", padx=(8, 0))

        self.grid_columnconfigure(3, weight=1)

        self._tk_thumb: Optional[Any] = None
        self.mode: str = "normal"  # normal | soft_cancelled | done | disabled

    @staticmethod
    def _format_meta(speed: str, eta: str, total: str, pct: str) -> str:
        return f"Скорость: {speed}  |  ETA: {eta}  |  Размер: {total}  |  {pct}"

    def set_thumbnail(self, tk_img: Any) -> None:
        self._tk_thumb = tk_img
        self.thumb_label.configure(image=self._tk_thumb, text="")

    def set_mode(self, mode: str, paused: bool = False) -> None:
        self.mode = mode

        if mode == "normal":
            if not self.btn2.winfo_ismapped():
                self.btn2.grid(row=2, column=5, sticky="e", padx=(8, 0))
            self.btn1_text.set("Продолжить" if paused else "Пауза")
            self.btn2_text.set("Отмена")
            self.btn1.configure(state="normal")
            self.btn2.configure(state="normal")

        elif mode == "soft_cancelled":
            if not self.btn2.winfo_ismapped():
                self.btn2.grid(row=2, column=5, sticky="e", padx=(8, 0))
            self.btn1_text.set("Возобновить")
            self.btn2_text.set("Удалить")
            self.btn1.configure(state="normal")
            self.btn2.configure(state="normal")

        elif mode == "done":
            self.btn1_text.set("Закрыть")
            if self.btn2.winfo_ismapped():
                self.btn2.grid_remove()
            self.btn1.configure(state="normal")

        elif mode == "disabled":
            self.btn1.configure(state="disabled")
            self.btn2.configure(state="disabled")

    def update_fields(self, fields: Dict[str, Any]) -> None:
        if "status" in fields:
            self.status_var.set(str(fields["status"]))

        if "progress" in fields:
            try:
                self.progress["value"] = float(fields["progress"])
            except Exception:
                pass

        # По ТЗ: если "Готово" — только этот текст и без мета-инфо
        if self.status_var.get() == "Готово":
            self.meta_var.set("")
            return

        speed = fields.get("speed")
        eta = fields.get("eta")
        total = fields.get("total")
        pct = fields.get("pct_text")

        if speed is not None or eta is not None or total is not None or pct is not None:
            speed_txt = speed if (speed not in (None, "")) else "—"
            eta_txt = eta if (eta not in (None, "")) else "—"
            total_txt = total if (total not in (None, "")) else "—"
            pct_txt = pct if (pct not in (None, "")) else "—"
            self.meta_var.set(self._format_meta(speed_txt, eta_txt, total_txt, pct_txt))

    def _btn1_clicked(self) -> None:
        if self.mode == "normal":
            self._on_pause()
        elif self.mode == "soft_cancelled":
            self._on_resume()
        elif self.mode == "done":
            self._on_close()

    def _btn2_clicked(self) -> None:
        if self.mode == "normal":
            self._on_cancel_soft()
        elif self.mode == "soft_cancelled":
            self._on_delete()
