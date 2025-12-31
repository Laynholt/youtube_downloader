import json
import os
import re
import shutil
import subprocess
import sys
import urllib.request
import zipfile
from pathlib import Path
from typing import Callable, Dict, Optional, Tuple

from utils.paths import project_root

ReleaseInfo = Dict[str, str]

GITHUB_LATEST = "https://api.github.com/repos/Laynholt/youtube_downloader/releases/latest"
ASSET_NAME = "youtube_downloader_windows.zip"
UPDATE_ARCHIVE_NAME = "update.zip"
APPLY_SCRIPT_NAME = "apply_update.ps1"


class UpdateCancelled(Exception):
    pass


def _make_local_tmp_dir(target_dir: Path) -> Path:
    """
    Создаём временную папку рядом с exe, чтобы не упираться в права temp.
    """
    for _ in range(30):
        candidate = target_dir / f"_ytupd_{os.urandom(4).hex()}"
        try:
            candidate.mkdir(parents=True, exist_ok=False)
            return candidate
        except FileExistsError:
            continue
    raise RuntimeError("Не удалось создать временную папку для обновления рядом с exe.")


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


def _download_file(
    url: str,
    dst: Path,
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> None:
    def cancelled() -> bool:
        return bool(cancel and cancel())

    if progress:
        progress("Скачивание обновления...", None)
    if cancelled():
        raise UpdateCancelled()
    req = urllib.request.Request(url, headers={"User-Agent": "yt-downloader-updater"})
    with urllib.request.urlopen(req, timeout=120) as resp, open(dst, "wb") as f:
        total = resp.headers.get("Content-Length")
        total_bytes = int(total) if total and total.isdigit() else None
        read = 0
        chunk = 1024 * 64
        while True:
            buf = resp.read(chunk)
            if not buf:
                break
            f.write(buf)
            read += len(buf)
            if cancelled():
                raise UpdateCancelled()
            if progress and total_bytes and total_bytes > 0:
                ratio = min(1.0, read / total_bytes)
                progress(f"Скачивание обновления... {int(ratio * 100)}%", ratio)
        if cancelled():
            raise UpdateCancelled()
    if progress:
        progress("Скачивание завершено", 1.0)


def _pick_root_dir(extracted_dir: Path) -> Path:
    entries = list(extracted_dir.iterdir())
    if len(entries) == 1 and entries[0].is_dir():
        return entries[0]
    return extracted_dir


def _flatten_payload(src_root: Path, target_dir: Path) -> None:
    """
    Переносим содержимое распакованной папки в корень временного каталога.
    """
    for item in src_root.iterdir():
        dest = target_dir / item.name
        if dest.exists():
            if dest.is_dir():
                shutil.rmtree(dest, ignore_errors=True)
            else:
                try:
                    dest.unlink()
                except Exception:
                    pass
        shutil.move(str(item), dest)


def _write_apply_script(tmp_dir: Path) -> Path:
    script_path = tmp_dir / APPLY_SCRIPT_NAME
    ps_code = f"""param(
    [string]$SourceDir,
    [string]$TargetDir,
    [string]$ExeName,
    [int]$ParentPid
)

function Wait-ParentExit($ppid, $timeoutSec) {{
    if ($ppid -le 0) {{ return }}
    try {{
        $proc = Get-Process -Id $ppid -ErrorAction SilentlyContinue
        if ($null -eq $proc) {{ return }}
        $elapsed = 0
        while (-not $proc.HasExited -and $elapsed -lt $timeoutSec) {{
            Start-Sleep -Milliseconds 500
            $elapsed += 0.5
        }}
    }} catch {{}}
    Start-Sleep -Milliseconds 500
}}

Wait-ParentExit -ppid $ParentPid -timeoutSec 60

New-Item -ItemType Directory -Force -Path $TargetDir | Out-Null

Get-ChildItem -LiteralPath $SourceDir | ForEach-Object {{
    if ($_.Name -eq "{UPDATE_ARCHIVE_NAME}" -or $_.Name -eq "{APPLY_SCRIPT_NAME}") {{
        return
    }}
    $dest = Join-Path $TargetDir $_.Name
    if (Test-Path $dest) {{
        try {{
            Remove-Item -LiteralPath $dest -Recurse -Force -ErrorAction Stop
        }} catch {{}}
    }}
    $attempts = 0
    while ($attempts -lt 3) {{
        $attempts += 1
        try {{
            if ($_.PSIsContainer) {{
                Copy-Item -LiteralPath $_.FullName -Destination $dest -Recurse -Force -ErrorAction Stop
            }} else {{
                Copy-Item -LiteralPath $_.FullName -Destination $dest -Force -ErrorAction Stop
            }}
            break
        }} catch {{
            Start-Sleep -Milliseconds 500
        }}
    }}
}}

try {{
    Remove-Item -LiteralPath $SourceDir -Recurse -Force -ErrorAction SilentlyContinue
}} catch {{}}
"""
    script_path.write_text(ps_code, encoding="utf-8")
    return script_path


def _launch_apply_script(script_path: Path, source_dir: Path, target_dir: Path, exe_name: str) -> bool:
    try:
        creation_flags = 0
        startupinfo = None
        if os.name == "nt":
            creation_flags = getattr(subprocess, "CREATE_NO_WINDOW", 0)
            startupinfo = subprocess.STARTUPINFO()
            startupinfo.dwFlags |= getattr(subprocess, "STARTF_USESHOWWINDOW", 0)

        subprocess.Popen(
            [
                "powershell",
                "-NoProfile",
                "-WindowStyle",
                "Hidden",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script_path),
                "-SourceDir",
                str(source_dir),
                "-TargetDir",
                str(target_dir),
                "-ExeName",
                exe_name,
                "-ParentPid",
                str(os.getpid()),
            ],
            creationflags=creation_flags,
            startupinfo=startupinfo,
        )
        return True
    except Exception:
        return False


def install_update_from_url(
    url: str,
    target_dir: Optional[Path] = None,
    progress: Optional[Callable[[str, Optional[float]], None]] = None,
    cancel: Optional[Callable[[], bool]] = None,
) -> Tuple[bool, str]:
    def cancelled() -> bool:
        return bool(cancel and cancel())
    if not url:
        return False, "Ссылка на обновление не найдена."

    if os.name != "nt":
        return False, "Автообновление поддерживается только в Windows сборке."

    if not getattr(sys, "frozen", False):
        return False, "Автообновление доступно только для собранного .exe."

    target_dir = target_dir or project_root()
    tmp_dir = _make_local_tmp_dir(target_dir)
    archive_path = tmp_dir / UPDATE_ARCHIVE_NAME

    try:
        _download_file(url, archive_path, progress, cancel=cancel)
        if cancelled():
            raise UpdateCancelled()
        if progress:
            progress("Распаковка обновления...", None)

        extract_dir = tmp_dir / "extracted"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(archive_path, "r") as zf:
            zf.extractall(extract_dir)
        if cancelled():
            raise UpdateCancelled()

        src_root = _pick_root_dir(extract_dir)
        _flatten_payload(src_root, tmp_dir)
        shutil.rmtree(extract_dir, ignore_errors=True)
        if cancelled():
            raise UpdateCancelled()

        script_path = _write_apply_script(tmp_dir)
        exe_name = Path(sys.executable).name or "main.exe"
        started = _launch_apply_script(script_path, tmp_dir, target_dir, exe_name)
        if not started:
            raise RuntimeError("Не удалось запустить установщик обновления (PowerShell).")

        if progress:
            progress("Файлы скачаны, перезапуск...", 1.0)

        return True, "Обновление загружено."
    except UpdateCancelled:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return False, "Загрузка обновления отменена."
    except Exception as e:
        try:
            shutil.rmtree(tmp_dir, ignore_errors=True)
        except Exception:
            pass
        return False, f"Не удалось подготовить обновление: {e}"
