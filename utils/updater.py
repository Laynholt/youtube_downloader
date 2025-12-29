from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from utils.paths import project_root

ReleaseInfo = Dict[str, str]

GITHUB_LATEST = "https://api.github.com/repos/Laynholt/youtube_downloader/releases/latest"
ASSET_NAME = "youtube_downloader_windows.zip"


def parse_version(v: str) -> Tuple[int, ...]:
    parts = re.split(r"[^\d]+", v)
    nums = []
    for p in parts:
        if p.isdigit():
            nums.append(int(p))
    return tuple(nums)


def compare_versions(a: str, b: str) -> int:
    ta = parse_version(a)
    tb = parse_version(b)
    la = len(ta)
    lb = len(tb)
    for i in range(max(la, lb)):
        va = ta[i] if i < la else 0
        vb = tb[i] if i < lb else 0
        if va > vb:
            return 1
        if va < vb:
            return -1
    return 0


def fetch_latest_release() -> ReleaseInfo:
    req = urllib.request.Request(GITHUB_LATEST, headers={"Accept": "application/vnd.github+json", "User-Agent": "yt-downloader-updater"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8"))

    tag = str(data.get("tag_name") or data.get("name") or "").lstrip("v")
    page_url = str(data.get("html_url") or "")
    download_url = ""
    for asset in data.get("assets") or []:
        if str(asset.get("name") or "") == ASSET_NAME:
            download_url = str(asset.get("browser_download_url") or "")
            break

    return {"version": tag, "page_url": page_url, "download_url": download_url}


def _download_file(url: str, dst: Path, progress: Optional[Callable[[str], None]] = None) -> None:
    if progress:
        progress("Скачивание обновления...")
    req = urllib.request.Request(url, headers={"User-Agent": "yt-downloader-updater"})
    with urllib.request.urlopen(req, timeout=60) as resp, open(dst, "wb") as f:
        shutil.copyfileobj(resp, f)


def _pick_root_dir(extracted_dir: Path) -> Path:
    entries = list(extracted_dir.iterdir())
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return extracted_dir


def _copy_tree(src: Path, dst: Path, skip_existing: Optional[Callable[[Path], bool]] = None) -> None:
    for root, dirs, files in os.walk(src):
        root_path = Path(root)
        rel = root_path.relative_to(src)
        target_root = dst / rel
        target_root.mkdir(parents=True, exist_ok=True)
        for fname in files:
            rel_file = rel / fname
            if skip_existing and skip_existing(rel_file):
                continue
            shutil.copy2(root_path / fname, target_root / fname)


def _should_skip_existing(rel_path: Path) -> bool:
    # Сохраняем пользовательский конфиг/логи
    if rel_path.parts[:2] == ("stuff", "config.ini"):
        return True
    if rel_path.parts[:2] == ("stuff", "ytdl.log"):
        return True
    return False


def install_update_from_url(url: str, target_dir: Optional[Path] = None, progress: Optional[Callable[[str], None]] = None) -> Tuple[bool, str]:
    if not url:
        return False, "Ссылка на обновление не найдена."

    target_dir = target_dir or project_root()
    tmp_dir = Path(tempfile.mkdtemp(prefix="ytupd_"))
    archive_path = tmp_dir / "update.zip"

    try:
        _download_file(url, archive_path, progress)
        if progress:
            progress("Распаковка обновления...")

        extract_dir = tmp_dir / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)

        src_root = _pick_root_dir(extract_dir)
        _copy_tree(src_root, target_dir, skip_existing=_should_skip_existing)

        return True, f"Обновление установлено. Перезапустите приложение. Файлы: {target_dir}"
    except Exception as e:
        return False, f"Не удалось установить обновление: {e}"
    finally:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
