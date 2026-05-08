#!/usr/bin/env python3
"""Find AV1 REENCODE outputs whose source files were not cleaned up.

Walk a library path, find every `*.AV1.REENCODE.mkv` file, then check
the same directory for any other video file that looks like the
unencoded source (same release stem, different codec marker). Report
the pairs so the user can decide whether to recycle / unlink.

Read-only: prints findings, takes no destructive action.

Usage:
    ./audit_orphans.py /mnt/nas/media/Movies
    ./audit_orphans.py /mnt/nas/media/Movies --json    # machine-readable
    ./audit_orphans.py /mnt/nas/media/Movies --apply --recycle-to DIR
                                                # actually move them

The --apply flag opts into action; default is read-only.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

VIDEO_EXTS = frozenset({
    ".mkv", ".mp4", ".m4v", ".mov", ".avi", ".wmv", ".ts", ".m2ts", ".webm",
})

# Directory names to skip during the walk — same as crawler._SKIP_DIRS.
SKIP_DIRS = frozenset({
    ".@__thumb",
    "@Recycle", ".@Recycle",
    "@Recently-Snapshot", ".@Recently-Snapshot",
    "#recycle", ".AppleDouble",
    "__pycache__", ".git", ".svn",
})

# Codec / format tokens we recognize as "non-AV1" — i.e. an original.
# Matches both the dotted-name convention (Foo.HEVC.mkv, Foo.x265.mkv) and
# the "+" variant Radarr/Sonarr emits (Foo.HEVC+H.265.mkv).
ORIGINAL_TOKENS = (
    ".HEVC", ".H.265", ".H265", ".x265", ".x.265",
    ".AVC", ".H.264", ".H264", ".x264",
    ".MPEG2", ".MPEG-2",
    ".VC1", ".VC-1",
    ".VP9",
)


def _is_skipped(path: Path) -> bool:
    return path.name in SKIP_DIRS


def _walk_videos(root: Path):
    """Yield every video file under root, skipping NAS / OS system dirs."""
    stack: list[Path] = [root]
    while stack:
        current = stack.pop()
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name)
        except (PermissionError, OSError):
            continue
        for entry in entries:
            try:
                if entry.is_dir():
                    if not _is_skipped(entry):
                        stack.append(entry)
                elif entry.is_file() and entry.suffix.lower() in VIDEO_EXTS:
                    yield entry
            except OSError:
                continue


def _is_av1_reencode(path: Path) -> bool:
    """True if this path is one of our AV1 REENCODE outputs."""
    stem_upper = path.stem.upper()
    return ".AV1.REENCODE" in stem_upper or stem_upper.endswith(".AV1.REENCODE")


def _looks_like_original(path: Path) -> bool:
    """True if this looks like a non-AV1 source (HEVC/h264/etc)."""
    if _is_av1_reencode(path):
        return False
    stem_upper = path.stem.upper()
    return any(token.upper() in stem_upper for token in ORIGINAL_TOKENS)


def _release_stem(name: str) -> str:
    """Strip codec/REENCODE markers so two siblings can be matched.

    `Foo.HEVC.mkv`               → `Foo`
    `Foo.HEVC+H.265.mkv`         → `Foo`
    `Foo.AV1.REENCODE.mkv`       → `Foo`
    `Foo (2024).Remux-2160p.HEVC+H.265.mkv` → `Foo (2024).Remux-2160p`
    """
    upper = name.upper()
    # Strip everything from the first codec marker onward.
    earliest = len(name)
    markers = (".AV1.REENCODE", *ORIGINAL_TOKENS)
    for marker in markers:
        idx = upper.find(marker.upper())
        if idx != -1 and idx < earliest:
            earliest = idx
    return name[:earliest]


def find_orphans(root: Path) -> list[tuple[Path, list[Path]]]:
    """Return [(av1_output, [original_siblings, ...]), ...] for every AV1
    output that has at least one non-AV1 sibling in the same directory.
    """
    by_dir: dict[Path, list[Path]] = {}
    for video in _walk_videos(root):
        by_dir.setdefault(video.parent, []).append(video)

    pairs: list[tuple[Path, list[Path]]] = []
    for directory, files in by_dir.items():
        av1_outputs = [f for f in files if _is_av1_reencode(f)]
        if not av1_outputs:
            continue
        originals = [f for f in files if _looks_like_original(f)]
        if not originals:
            continue
        for av1 in av1_outputs:
            av1_stem = _release_stem(av1.name)
            matched = [
                o for o in originals
                if _release_stem(o.name) == av1_stem
            ]
            if matched:
                pairs.append((av1, matched))
    return pairs


def _bytes_human(n: int) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("path", type=Path,
                    help="Library path to scan (recursive).")
    ap.add_argument("--json", action="store_true",
                    help="Emit JSON instead of text.")
    ap.add_argument("--apply", action="store_true",
                    help="Actually move orphans (requires --recycle-to).")
    ap.add_argument("--recycle-to", type=Path, default=None, metavar="DIR",
                    help="With --apply: directory to move orphans into. "
                         "Source-dir hierarchy is preserved under DIR.")
    args = ap.parse_args()

    if args.apply and not args.recycle_to:
        print("error: --apply requires --recycle-to", file=sys.stderr)
        return 2
    if not args.path.is_dir():
        print(f"error: not a directory: {args.path}", file=sys.stderr)
        return 2

    pairs = find_orphans(args.path)

    if args.json:
        out = []
        for av1, originals in pairs:
            out.append({
                "av1_output": str(av1),
                "originals": [str(o) for o in originals],
                "originals_total_bytes": sum(o.stat().st_size for o in originals),
            })
        print(json.dumps(out, indent=2))
        return 0

    if not pairs:
        print(f"no orphans under {args.path}")
        return 0

    total_bytes = 0
    print(f"==> orphan originals under {args.path}")
    print()
    for av1, originals in pairs:
        print(f"  AV1 output: {av1}")
        for o in originals:
            try:
                size = o.stat().st_size
                total_bytes += size
                print(f"    ORPHAN ({_bytes_human(size)}): {o}")
            except OSError as e:
                print(f"    ORPHAN (size unknown: {e}): {o}")
        print()
    print(f"==> {len(pairs)} pair(s); "
          f"{sum(len(o) for _, o in pairs)} orphan original(s); "
          f"{_bytes_human(total_bytes)} reclaimable")

    if not args.apply:
        print()
        print("read-only run. Re-run with --apply --recycle-to DIR to move.")
        return 0

    # --apply path: move each orphan into the recycle dir, preserving the
    # source-dir hierarchy below the library root.
    print()
    print(f"==> moving orphans into {args.recycle_to}")
    moved = 0
    failed = 0
    for _av1, originals in pairs:
        for o in originals:
            try:
                rel = o.relative_to(args.path)
            except ValueError:
                rel = Path(o.name)
            dst = args.recycle_to / rel
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(o), str(dst))
                print(f"  moved {o} → {dst}")
                moved += 1
            except OSError as e:
                print(f"  FAILED {o}: {e}", file=sys.stderr)
                failed += 1
    print()
    print(f"==> {moved} moved, {failed} failed")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
