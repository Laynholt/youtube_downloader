"""
Shared theme colors for the UI.
"""

from __future__ import annotations

from typing import Dict

# Base palette used across application windows and dialogs.
DEFAULT_COLORS: Dict[str, str] = {
    "bg": "#222429",
    "panel": "#262d3b",
    "panel_alt": "#2e3547",
    "text": "#e6e9f0",
    "muted": "#b1b7c7",
    "accent": "#5fa8f5",
    "accent_hover": "#76b6f7",
    "warn": "#f0b23d",
    "error": "#f06b60",
}


def get_default_colors() -> Dict[str, str]:
    """Return a copy of the base palette so callers can mutate it safely."""
    return dict(DEFAULT_COLORS)
