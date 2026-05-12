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


# NAS-server (Synology / QNAP) system dirs whose contents either
# masquerade as video (`.@__thumb` reuses source filenames for JPEG
# thumbnails — passes the extension filter, ffprobes as 1-frame mjpeg)
# or are recycled/snapshot duplicates.
_SKIP_DIRS = frozenset({
    ".@__thumb",          # Synology DSM thumbnail cache
    "@Recycle",           # QNAP recycle bin (visible)
    ".@Recycle",          # QNAP recycle bin (hidden — newer firmware default)
    "@Recently-Snapshot", # QNAP snapshot directory (visible)
    ".@Recently-Snapshot", # QNAP snapshot directory (hidden)
    "#recycle",           # Synology recycle bin
    ".AppleDouble",       # Mac SMB metadata
    "__pycache__", ".git", ".svn",  # tooling hygiene
})


# Plex / Sonarr / Radarr "local extras": trailers, BTS, featurettes.
# Case-insensitive match (compared against lowered name).
_EXTRAS_DIRS = frozenset({
    "trailer", "trailers",
    "extra", "extras",
    "featurette", "featurettes",
    "behind the scenes", "behindthescenes",
    "behind-the-scenes", "behind_the_scenes",
    "deleted scenes", "deletedscenes",
    "deleted-scenes", "deleted_scenes",
    "deleted scene", "deletedscene",
    "interview", "interviews",
    "short", "shorts",
    "scene", "scenes",
    "other", "others",
    "bonus", "bonuses",
    "bts",
    "sample", "samples",
})

# Filename-stem suffixes (post-`-`) that mark a file as an extra even
# when it lives next to a feature presentation. Plex-flavoured.
_EXTRAS_SUFFIXES = (
    "-trailer", "-behindthescenes", "-bts", "-deleted",
    "-featurette", "-interview", "-scene", "-short",
    "-other", "-bonus", "-extra", "-sample",
)


def _is_supported(path: Path) -> bool:
    """Return True if path has a supported video extension."""
    return path.suffix.lower() in SUPPORTED_EXTENSIONS


def _is_skipped_dir(path: Path) -> bool:
    """Return True if a directory name matches the NAS / OS skip-list."""
    return path.name in _SKIP_DIRS


def _is_extras_dir(path: Path) -> bool:
    """Return True if a directory name matches a known extras convention."""
    return path.name.lower() in _EXTRAS_DIRS


def is_extras_filename(path: Path) -> bool:
    """Return True if a filename stem ends with an extras suffix.

    Public so the plan-gate can defensively catch extras files that
    escaped the directory-level filter (e.g. `Movie-trailer.mp4`
    sitting next to `Movie.mkv`).
    """
    stem = path.stem.lower()
    return any(stem.endswith(suffix) for suffix in _EXTRAS_SUFFIXES)


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
                    recursive: bool, *,
                    skip_extras: bool = True) -> tuple[str, Path | None]:
    """Decide what to do with a directory entry.

    Returns one of:
      ("skip",  None)   — symlink-escape, unsupported, unreadable, an
                          extras dir/file while skip_extras is True, or
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
        if skip_extras and _is_extras_dir(entry):
            log.debug("skip extras dir: %s", entry)
            return "skip", None
        return ("recurse", entry) if recursive else ("skip", None)
    if entry.is_file() and _is_supported(entry) and _is_usable(entry):
        if skip_extras and is_extras_filename(entry):
            log.debug("skip extras filename: %s", entry)
            return "skip", None
        return "yield", entry
    return "skip", None


def crawl(root: Path, recursive: bool = True, *,
          skip_extras: bool = True) -> Iterator[Path]:
    """Yield paths of supported video files under root.

    When skip_extras is True (default), Plex-style local-extras
    directories (Trailers, Behind The Scenes, Featurettes, …) and
    files with extras suffixes (`-trailer`, `-bts`, …) are excluded.
    """
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
            action, target = _classify_entry(
                entry, root_resolved, recursive, skip_extras=skip_extras)
            if action == "recurse" and target is not None:
                stack.append(target)
            elif action == "yield" and target is not None:
                yield target
