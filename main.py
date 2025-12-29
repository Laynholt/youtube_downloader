from __future__ import annotations

import importlib.util
import sys
import tkinter as tk

from ui.dialogs import show_error


def _missing_deps() -> list[str]:
    missing: list[str] = []
    if importlib.util.find_spec("yt_dlp") is None:
        missing.append("yt-dlp")
    if importlib.util.find_spec("PIL") is None:
        missing.append("Pillow")
    return missing


_missing = _missing_deps()
if _missing:
    root = tk.Tk()
    root.withdraw()
    show_error(
        "Не хватает зависимостей",
        "Не установлены зависимости: " + ", ".join(_missing) + "\n\n"
        "Установите:\n"
        "  pip install yt-dlp Pillow\n\n"
        "И перезапустите приложение.",
        parent=root,
    )
    root.destroy()
    raise SystemExit(1)

# Импортируем GUI только после проверки, чтобы не падать раньше.
from ui import App


def main() -> None:
    app = App()
    app.mainloop()


if __name__ == "__main__":
    main()
