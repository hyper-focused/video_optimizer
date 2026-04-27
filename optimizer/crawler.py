"""Recursive video file discovery for video_optimizer."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterator

log = logging.getLogger(__name__)


SUPPORTED_EXTENSIONS = frozenset({
    ".mov", ".avi", ".mkv", ".mp4", ".mpeg", ".mpg", ".wmv",
    ".flv", ".webm", ".m4v", ".asf", ".vob", ".ts", ".m2ts", ".mts",
})


# Directory names to skip during recursive walk. These are NAS-server
# system directories (Synology, QNAP) and OS metadata dirs whose contents
# either masquerade as video files (Synology's `.@__thumb` re-uses source
# filenames + extensions for JPEG thumbnails — passes the extension
# filter, ffprobe reports them as 1-frame mjpeg) or are recycled/snapshot
# duplicates we don't want to re-process.
_SKIP_DIRS = frozenset({
    ".@__thumb",          # Synology DSM thumbnail cache
    "@Recycle",           # QNAP recycle bin
    "@Recently-Snapshot", # QNAP snapshot directory
    "#recycle",           # Synology recycle bin
    ".AppleDouble",       # Mac SMB metadata
    "__pycache__", ".git", ".svn",  # tooling hygiene
})


def _is_supported(path: Path) -> bool:
    """Return True if path has a supported video extension."""
    return path.suffix.lower() in SUPPORTED_EXTENSIONS


def _is_skipped_dir(path: Path) -> bool:
    """Return True if a directory name matches the NAS / OS skip-list."""
    return path.name in _SKIP_DIRS


def _is_usable(path: Path) -> bool:
    """Return True if path is a non-empty, stat-able regular file."""
    try:
        st = path.stat()
    except (PermissionError, OSError):
        log.debug("skip unreadable: %s", path)
        return False
    if st.st_size == 0:
        log.debug("skip 0-byte: %s", path)
        return False
    return True


def _escapes_root(path: Path, root_resolved: Path) -> bool:
    """Return True if a symlink resolves outside root_resolved."""
    try:
        target = path.resolve(strict=False)
    except (OSError, RuntimeError):
        return True
    try:
        target.relative_to(root_resolved)
    except ValueError:
        return True
    return False


def _iter_dir_sorted(directory: Path) -> list[Path]:
    """List directory entries sorted by name; empty list on permission error."""
    try:
        return sorted(directory.iterdir(), key=lambda p: p.name)
    except (PermissionError, OSError):
        log.debug("skip unreadable dir: %s", directory)
        return []


def _classify_entry(entry: Path, root_resolved: Path,
                    recursive: bool) -> tuple[str, Path | None]:
    """Decide what to do with a directory entry.

    Returns one of:
      ("skip",  None)   — symlink-escape, unsupported, unreadable, or
                          a subdirectory while not recursing.
      ("recurse", path) — caller should descend into `path`.
      ("yield", path)   — caller should yield `path` as a video file.
    """
    if entry.is_symlink() and _escapes_root(entry, root_resolved):
        log.debug("skip escaping symlink: %s", entry)
        return "skip", None
    if entry.is_dir():
        if _is_skipped_dir(entry):
            log.debug("skip system dir: %s", entry)
            return "skip", None
        return ("recurse", entry) if recursive else ("skip", None)
    if entry.is_file() and _is_supported(entry) and _is_usable(entry):
        return "yield", entry
    return "skip", None


def crawl(root: Path, recursive: bool = True) -> Iterator[Path]:
    """Yield paths of supported video files under root."""
    if root.is_file():
        if _is_supported(root) and _is_usable(root):
            yield root
        return
    if not root.is_dir():
        return

    root_resolved = root.resolve(strict=False)
    stack: list[Path] = [root]
    while stack:
        for entry in _iter_dir_sorted(stack.pop()):
            action, target = _classify_entry(entry, root_resolved, recursive)
            if action == "recurse" and target is not None:
                stack.append(target)
            elif action == "yield" and target is not None:
                yield target
