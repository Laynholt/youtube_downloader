from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    """
    project/utils/paths.py -> parents[1] = project/
    """
    return Path(__file__).resolve().parents[1]


def stuff_dir() -> Path:
    return project_root() / "stuff"


def config_path() -> Path:
    return stuff_dir() / "config.ini"


def log_path() -> Path:
    return stuff_dir() / "ytdl.log"


def default_download_dir() -> Path:
    return Path.home() / "Downloads"
