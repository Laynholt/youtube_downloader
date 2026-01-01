import sys
from pathlib import Path


def project_root() -> Path:
    """
    В сборке PyInstaller кладём данные рядом с exe, а не во временный _MEIPASS.
    """
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parents[1]


def stuff_dir() -> Path:
    return project_root() / "stuff"


def config_path() -> Path:
    return stuff_dir() / "config.ini"


def log_path() -> Path:
    return stuff_dir() / "ytdl.log"


def default_download_dir() -> Path:
    return Path.home() / "Downloads"


def placeholder_path() -> Path:
    return stuff_dir() / "preview_placeholder.png"
