import os
from typing import List, Set, Tuple


def _expand_delete_candidates(paths: Set[str]) -> Set[str]:
    out: Set[str] = set()
    for p in paths:
        if not p:
            continue
        out.add(p)
        out.add(p + ".part")
        out.add(p + ".ytdl")
        if p.endswith(".part"):
            out.add(p[:-5])
    return out


def delete_task_files(seen_files: Set[str]) -> Tuple[int, List[str]]:
    candidates = _expand_delete_candidates(seen_files)
    removed = 0
    errors: List[str] = []
    for p in sorted(candidates):
        try:
            if os.path.isfile(p):
                os.remove(p)
                removed += 1
        except Exception as e:
            errors.append(f"{p}: {e}")
    return removed, errors
