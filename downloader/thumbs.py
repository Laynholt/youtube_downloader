import io
import urllib.request
from pathlib import Path
from typing import Tuple

from PIL import Image, ImageTk, ImageDraw, ImageFont

from utils.paths import placeholder_path, placeholder_error_path


def ensure_placeholder_image(
    *,
    size: Tuple[int, int] = (320, 180),  # 16:9
    error: bool = False,
) -> Path:
    """
    Убеждаемся, что заглушка существует в stuff/.
    Если нет - рисуем и сохраняем.
    """
    p = placeholder_error_path() if error else placeholder_path()
    p.parent.mkdir(parents=True, exist_ok=True)

    if p.exists():
        return p

    w, h = size
    img = Image.new("RGB", (w, h), (38, 45, 59))  # тёмно-серый фон
    draw = ImageDraw.Draw(img)

    # рамка
    draw.rounded_rectangle((8, 8, w - 8, h - 8), radius=18, outline=(80, 83, 90), width=3)

    # "play" треугольник или крестик
    tri_w = int(w * 0.18)
    tri_h = int(h * 0.22)
    cx, cy = w // 2, h // 2
    if error:
        draw.line((cx - tri_w // 2, cy - tri_h // 2, cx + tri_w // 2, cy + tri_h // 2), fill=(230, 120, 120), width=6)
        draw.line((cx - tri_w // 2, cy + tri_h // 2, cx + tri_w // 2, cy - tri_h // 2), fill=(230, 120, 120), width=6)
    else:
        tri = [
            (cx - tri_w // 2, cy - tri_h // 2),
            (cx - tri_w // 2, cy + tri_h // 2),
            (cx + tri_w // 2, cy),
        ]
        draw.polygon(tri, fill=(230, 230, 230))

    # подпись
    text = "PREVIEW ERROR" if error else "PREVIEW"
    try:
        font = ImageFont.truetype("arial.ttf", size=int(h * 0.13))
    except Exception:
        font = ImageFont.load_default()

    bbox = draw.textbbox((0, 0), text, font=font)
    tw = bbox[2] - bbox[0]
    th = bbox[3] - bbox[1]
    draw.text(((w - tw) // 2, int(h * 0.72)), text, font=font, fill=(200, 200, 200))

    img.save(p, format="PNG")
    return p


def load_placeholder_to_tk(max_size: Tuple[int, int]) -> ImageTk.PhotoImage:
    """
    Загружаем заглушку из stuff/ (создав при отсутствии) и возвращаем PhotoImage.
    """
    p = ensure_placeholder_image()
    img = Image.open(p).convert("RGB")
    img.thumbnail(max_size)
    return ImageTk.PhotoImage(img)


def load_placeholder_error_to_tk(max_size: Tuple[int, int]) -> ImageTk.PhotoImage:
    p = ensure_placeholder_image(error=True)
    img = Image.open(p).convert("RGB")
    img.thumbnail(max_size)
    return ImageTk.PhotoImage(img)


def download_thumbnail_to_tk(thumb_url: str, max_size: Tuple[int, int]) -> ImageTk.PhotoImage:
    """
    Скачиваем превью по URL и возвращаем PhotoImage.
    """
    req = urllib.request.Request(thumb_url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=20) as resp:
        data = resp.read()

    img = Image.open(io.BytesIO(data)).convert("RGB")
    img.thumbnail(max_size)
    return ImageTk.PhotoImage(img)
