import json
from typing import Any, Dict

APP_VERSION = "1.1.1"

from .paths import config_path, stuff_dir


def load_config() -> Dict[str, Any]:
    p = config_path()
    try:
        with open(p, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def save_config(data: Dict[str, Any]) -> None:
    stuff_dir().mkdir(parents=True, exist_ok=True)
    p = config_path()
    tmp = p.with_suffix(".ini.tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    tmp.replace(p)


def get_app_version() -> str:
    return APP_VERSION
