"""Command-line entry point for video_optimizer."""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from . import crawler, encoder, naming, probe, report, rules
from .db import DEFAULT_DB_PATH, Database
from .models import ProbeResult, probe_from_dict
from .presets import MIN_PROBE_SIZE_BYTES, PRESETS

_SIZE_SUFFIXES = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3, "t": 1024 ** 4}


def _parse_size(value: str) -> int:
    """Parse `1G`, `500M`, `1024`, `0` etc. into a byte count.

    Used by `--min-size`. Returns a non-negative int. Suffixes are
    case-insensitive and binary (1K = 1024). A bare integer is bytes.
    `0` disables the gate.
    """
    s = value.strip().lower()
    if not s:
        msg = "empty size value"
        raise argparse.ArgumentTypeError(msg)
    suffix = s[-1]
    if suffix in _SIZE_SUFFIXES:
        try:
            n = float(s[:-1])
        except ValueError as e:
            msg = f"invalid size: {value!r}"
            raise argparse.ArgumentTypeError(msg) from e
        return int(n * _SIZE_SUFFIXES[suffix])
    try:
        n_int = int(s)
    except ValueError as e:
        msg = f"invalid size: {value!r} (use bytes or K/M/G/T suffix)"
        raise argparse.ArgumentTypeError(msg) from e
    if n_int < 0:
        msg = f"size must be non-negative: {value!r}"
        raise argparse.ArgumentTypeError(msg)
    return n_int


def _format_size(n: int) -> str:
    """Inverse of _parse_size for human-readable summaries (binary)."""
    if n <= 0:
        return "0"
    for suffix, scale in (("T", 1024 ** 4), ("G", 1024 ** 3),
                          ("M", 1024 ** 2), ("K", 1024)):
        if n >= scale:
            return f"{n / scale:.1f}{suffix}"
    return f"{n}B"


def _add_common_db_arg(p: argparse.ArgumentParser) -> None:
    """Attach the --db argument shared by every subcommand."""
    p.add_argument("--db", type=Path, default=DEFAULT_DB_PATH,
                   help=f"SQLite state file (default: {DEFAULT_DB_PATH})")


def _add_min_size_arg(p: argparse.ArgumentParser) -> None:
    """Attach --min-size, the scan-time probe-eligibility threshold."""
    default_human = _format_size(MIN_PROBE_SIZE_BYTES)
    p.add_argument(
        "--min-size", type=_parse_size, default=MIN_PROBE_SIZE_BYTES,
        metavar="SIZE",
        help=f"Skip files smaller than SIZE at scan time (default: "
             f"{default_human}). Accepts bytes or a K/M/G/T suffix "
             f"(e.g. '500M', '1G'). '0' disables the gate. Skipped files "
             f"are recorded in the skipped_files cache so they don't get "
             f"re-probed; if a file later grows above the threshold, the "
             f"next scan will probe it normally.")


def _add_scan_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `scan` subcommand."""
    s = sub.add_parser("scan", help="Crawl a path and probe every video file.")
    s.add_argument("path", type=Path)
    s.add_argument("--no-recursive", action="store_true",
                   help="Do not descend into subdirectories.")
    s.add_argument("--no-probe-cache", action="store_true",
                   help="Re-probe even if a cached entry matches size+mtime.")
    s.add_argument("--workers", type=int, default=None,
                   help="Parallel ffprobe workers for uncached files. "
                        "Default: min(8, CPU count). Use 1 for sequential. "
                        "ffprobe is I/O-bound, so workers >> NFS server's "
                        "concurrent-read ceiling don't help.")
    _add_min_size_arg(s)
    s.add_argument("--verbose", "-v", action="store_true")
    _add_common_db_arg(s)


def _add_reprobe_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `reprobe` subcommand (alias for scan --no-probe-cache)."""
    r = sub.add_parser(
        "reprobe",
        help="Force re-probe of files under a path; alias for scan --no-probe-cache.",
    )
    r.add_argument("path", type=Path)
    r.add_argument("--no-recursive", action="store_true")
    r.add_argument("--workers", type=int, default=None,
                   help="Parallel ffprobe workers (default: min(8, CPU count)).")
    _add_min_size_arg(r)
    r.add_argument("--verbose", "-v", action="store_true")
    _add_common_db_arg(r)


def _add_plan_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `plan` subcommand."""
    pl = sub.add_parser("plan",
                        help="Run rules over the probe cache and list candidates.")
    pl.add_argument("--rules", default=None,
                    help=f"Comma-separated rule names (default: all). "
                         f"Available: {','.join(rules.RULES.keys())}")
    pl.add_argument("--target", choices=list(encoder.TARGETS.keys()),
                    default="av1+mkv")
    pl.add_argument("--keep-langs", default="en,und",
                    help="Comma-separated languages to retain on apply "
                         "(advisory only here).")
    pl.add_argument("--json", action="store_true",
                    help="Emit JSON candidate list instead of text report.")
    pl.add_argument("--allow-reencoded", action="store_true",
                    help="Re-queue files whose names carry the REENCODE marker "
                         "(prior outputs of this tool). Default behavior is to "
                         "skip them permanently. Use this when intentionally "
                         "re-running an already-encoded file (e.g. trying a "
                         "different CQ).")
    _add_common_db_arg(pl)


def _add_apply_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `apply` subcommand and all its encode/output flags."""
    ap = sub.add_parser("apply", help="Encode pending candidates.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Print planned ffmpeg commands and exit without "
                         "encoding. Use this to preview what would happen "
                         "before committing to a run.")
    _add_apply_workflow_args(ap)
    _add_apply_encoding_args(ap)
    _add_apply_naming_args(ap)
    ap.add_argument("--verbose", "-v", action="store_true")
    _add_common_db_arg(ap)


def _add_apply_workflow_args(ap: argparse.ArgumentParser) -> None:
    """Attach apply-mode flags governing confirmation, output layout, limits."""
    ap.add_argument("--auto", action="store_true",
                    help="Skip per-file confirmation.")
    ap.add_argument("--mode", choices=["side", "replace"], default="side")
    ap.add_argument("--output-root", type=Path,
                    help="Required for --mode side. Mirrored output tree.")
    ap.add_argument("--source-root", type=Path,
                    help="Strip this prefix from source paths when placing outputs "
                         "in --output-root (default: filesystem root).")
    ap.add_argument("--backup", type=Path,
                    help="For --mode replace: copy original here before replacing. "
                         "Doubles disk use during the run; prefer --recycle-to "
                         "for NAS targets that have a recycle-bin directory.")
    ap.add_argument("--recycle-to", type=Path,
                    help="For --mode replace: atomically move (rather than copy or "
                         "delete) originals into this directory. Preserves source "
                         "hierarchy under it. Atomic and instant when source and "
                         "target are on the same filesystem (typical NAS share "
                         "with @Recycle / #recycle). Mutually exclusive with "
                         "--backup.")
    ap.add_argument("--allow-hard-delete", action="store_true",
                    help="Required to combine --mode replace with --auto when "
                         "neither --backup nor --recycle-to is set. Acknowledges "
                         "that originals will be permanently deleted after each "
                         "successful encode (encode-then-unlink, no copy).")
    ap.add_argument("--limit", type=int, default=0,
                    help="Process at most N candidates (0 = no limit).")
    ap.add_argument("--min-height", type=int, default=None,
                    help="Skip candidates whose video height < N. "
                         "Leaves them pending in the db for a later run.")
    ap.add_argument("--max-height", type=int, default=None,
                    help="Skip candidates whose video height > N. "
                         "Leaves them pending in the db for a later run.")


def _add_apply_encoding_args(ap: argparse.ArgumentParser) -> None:
    """Attach encoder/quality/timeout/HDR flags for the apply subcommand."""
    ap.add_argument("--quality", type=int)
    ap.add_argument("--hwaccel",
                    choices=["auto", "qsv", "nvenc", "vaapi", "videotoolbox",
                             "software", "none"],
                    default="auto")
    ap.add_argument("--keep-langs", default="en,und")
    ap.add_argument("--timeout", type=int, default=None,
                    help="Per-file ffmpeg wall-clock cap in seconds. "
                         "0 disables. Default adapts to source duration "
                         "(max(3600, 6 * duration_seconds)).")
    ap.add_argument("--hw-decode", action="store_true",
                    help="Enable zero-copy QSV decode->encode pipeline. "
                         "Off by default; safe to enable when the source "
                         "codecs are modern (H.264/HEVC/AV1).")
    # Compat audio: when a kept track is hi-res lossless (TrueHD, DTS-HD MA,
    # FLAC, multichannel PCM), also emit AAC 5.1 @ 640k and AAC 2.0 @ 320k
    # so devices that can't decode lossless still play sound. On by default.
    ca = ap.add_mutually_exclusive_group()
    ca.add_argument("--compat-audio", action="store_true", default=True,
                    help="(default) Add AAC 5.1 + AAC 2.0 compat tracks "
                         "alongside any hi-res lossless source.")
    ca.add_argument("--no-compat-audio", action="store_false",
                    dest="compat_audio",
                    help="Disable the AAC compat-track shadowing.")


def _add_apply_naming_args(ap: argparse.ArgumentParser) -> None:
    """Attach the filename-rewrite flags (Radarr/Sonarr-friendly options)."""
    ap.add_argument("--name-suffix", default="",
                    help="Free-form string appended to the output stem before "
                         "the extension. Applied last; composes with "
                         "--rewrite-codec / --reencode-tag.")
    ap.add_argument("--rewrite-codec", action="store_true",
                    help="Strip foreign codec tokens (e.g. H.264, HEVC, x265) "
                         "from the output filename and insert the canonical "
                         "target token. Defaults to dotted (Plex-style) names.")
    ap.add_argument("--no-dotted", action="store_true",
                    help="With --rewrite-codec: keep the input's whitespace "
                         "style instead of forcing dots.")
    ap.add_argument("--reencode-tag", action="store_true",
                    help="Append a REENCODE token to the output filename so "
                         "Radarr/Sonarr Custom Formats can match it (e.g. to "
                         "auto-unmonitor re-encoded titles).")
    ap.add_argument("--reencode-tag-value", default="REENCODE",
                    help="Override the token used by --reencode-tag (default: "
                         "REENCODE).")


def _add_status_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `status` subcommand."""
    st = sub.add_parser("status", help="Show recent runs and pending decisions.")
    st.add_argument("--last", type=int, default=10,
                    help="Show this many most-recent runs (default: 10).")
    _add_common_db_arg(st)


def _add_list_encoders_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `list-encoders` introspection subcommand."""
    sub.add_parser(
        "list-encoders",
        help="Show available ffmpeg encoders and the encoder picked per target.",
    )


def _add_replace_list_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `replace-list` subcommand."""
    rl = sub.add_parser(
        "replace-list",
        help="List sources that have hit the av1_qsv encoder watchdog 2+ times "
             "(candidates for finding a different release).",
    )
    _add_common_db_arg(rl)


def _add_doctor_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `doctor` preflight subcommand."""
    dr = sub.add_parser(
        "doctor",
        help="Run preflight checks: ffmpeg/ffprobe, encoders, GPU device, "
             "database. Exits nonzero if anything's wrong; run before your "
             "first encode to surface setup problems early.",
    )
    dr.add_argument("--probe", type=Path, default=None, metavar="PATH",
                    help="Optional video file path; runs a real probe to "
                         "verify ffprobe and the source readability.")
    _add_common_db_arg(dr)


def _add_optimize_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `optimize` one-shot pipeline subcommand."""
    op = sub.add_parser(
        "optimize",
        help="One-shot scan+plan+apply for a library. Auto-runs the UHD "
             "preset on 2160p sources and the HD preset on 1080p-and-below, "
             "with safe defaults. The friendliest entry point for new users.",
        description=(
            "Run scan, plan, and apply against PATH in a single command, "
            "using calibrated presets for each resolution tier (CQ 15 for "
            "UHD, CQ 21 for HD). Pick exactly one of --output (writes new "
            "files to a separate tree, leaves originals untouched) or "
            "--in-place (writes alongside originals and atomically moves "
            "the originals into a recycle directory)."
        ),
    )
    op.add_argument("path", type=Path, help="Library directory to optimize.")
    op.add_argument("--dry-run", action="store_true",
                    help="Print planned ffmpeg commands and exit. Combine "
                         "with --limit 1 to preview a single encode.")

    out = op.add_mutually_exclusive_group(required=True)
    out.add_argument("--output", type=Path, metavar="DIR",
                     help="Side mode: write new files under DIR mirroring "
                          "PATH's structure. Originals are untouched. The "
                          "safest first-time choice.")
    out.add_argument("--in-place", action="store_true",
                     help="Replace mode: write new files alongside originals "
                          "and move the originals into a recycle directory "
                          "(see --recycle-to). Atomic when source and "
                          "recycle dir are on the same filesystem.")

    op.add_argument("--recycle-to", type=Path, default=None, metavar="DIR",
                    help="With --in-place: recycle directory for displaced "
                         "originals. If omitted, an existing @Recycle / "
                         "#recycle / .Trash under PATH is used; otherwise "
                         "<PATH>/.@Recycle is created.")
    op.add_argument("--auto", action="store_true",
                    help="Skip per-file confirmation prompts.")
    op.add_argument("--limit", type=int, default=0, metavar="N",
                    help="Process at most N candidates per tier "
                         "(0 = no limit). Useful with --dry-run to preview "
                         "the first encode that would run.")
    op.add_argument("--quality", type=int, default=None,
                    help="Override the preset CQ (HD default: 21, "
                         "UHD default: 15). Lower = better quality + larger.")
    op.add_argument("--keep-langs", default=None,
                    help="Override languages kept on apply (default: en,und).")
    op.add_argument("--hwaccel",
                    choices=["auto", "qsv", "nvenc", "vaapi",
                             "videotoolbox", "software", "none"],
                    default="auto",
                    help="Encoder hardware backend (default: auto).")
    hwd = op.add_mutually_exclusive_group()
    hwd.add_argument("--hw-decode", action="store_true", default=None,
                     help="Force zero-copy QSV decode->encode on both tiers.")
    hwd.add_argument("--no-hw-decode", action="store_false",
                     dest="hw_decode",
                     help="Force CPU decode->QSV encode on both tiers.")
    op.add_argument("--skip-scan", action="store_true",
                    help="Reuse the existing probe cache; skip the scan "
                         "phase. Fine when PATH hasn't changed since the "
                         "last optimize/scan run.")
    op.add_argument("--workers", type=int, default=8,
                    help="Parallel workers for the scan phase (default: 8).")
    _add_min_size_arg(op)
    op.add_argument("--verbose", "-v", action="store_true")
    _add_common_db_arg(op)


def _add_preset_parsers(sub: "argparse._SubParsersAction") -> None:
    """Register one subcommand per entry in PRESETS, sharing a narrow flag set."""
    for name, cfg in PRESETS.items():
        p = sub.add_parser(
            name,
            help=f"Apply pending decisions with the {cfg['label']} preset.",
        )
        p.add_argument("--dry-run", action="store_true",
                       help="Print planned ffmpeg commands and exit without "
                            "encoding. Use this to preview what would happen "
                            "before committing to a run.")
        # Workflow knobs (mirror _add_apply_workflow_args, narrowed).
        p.add_argument("--auto", action="store_true",
                       help="Skip per-file confirmation.")
        p.add_argument("--mode", choices=["side", "replace"], default="side")
        p.add_argument("--output-root", type=Path,
                       help="Required for --mode side. Mirrored output tree.")
        p.add_argument("--source-root", type=Path,
                       help="Strip this prefix when placing outputs in --output-root.")
        p.add_argument("--backup", type=Path,
                       help="For --mode replace: copy original here before replacing.")
        p.add_argument("--recycle-to", type=Path,
                       help="For --mode replace: atomically move originals into "
                            "this dir (e.g. /mnt/nas/<share>/@Recycle). Preserves "
                            "source hierarchy. Mutually exclusive with --backup.")
        p.add_argument("--allow-hard-delete", action="store_true",
                       help="Required to combine --mode replace with --auto when "
                            "neither --backup nor --recycle-to is set. Originals "
                            "are permanently deleted after each successful encode.")
        p.add_argument("--limit", type=int, default=0,
                       help="Process at most N candidates (0 = no limit).")
        p.add_argument("--min-height", type=int, default=None,
                       help=f"Override preset min-height filter "
                            f"(default: {cfg.get('min_height') or 'none'}).")
        p.add_argument("--max-height", type=int, default=None,
                       help=f"Override preset max-height filter "
                            f"(default: {cfg.get('max_height') or 'none'}).")
        # Selected encoding knobs the user might still want to override.
        p.add_argument("--quality", type=int, default=None,
                       help=f"Override preset quality (default: {cfg['quality']}).")
        p.add_argument("--keep-langs", default=None,
                       help=f"Override languages kept on apply "
                            f"(default: {cfg['keep_langs']}).")
        p.add_argument("--hwaccel",
                       choices=["auto", "qsv", "nvenc", "vaapi",
                                "videotoolbox", "software", "none"],
                       default="auto")
        p.add_argument("--timeout", type=int, default=None,
                       help="Per-file ffmpeg wall-clock cap in seconds. "
                            "0 disables.")
        # Hardware decode default comes from the preset config (`hw_decode`
        # in PRESETS). Default=None at the parser level so cmd_preset can
        # tell user-explicit from preset-default; --hw-decode / --no-hw-decode
        # both override the preset value when passed.
        hwd = p.add_mutually_exclusive_group()
        hwd.add_argument("--hw-decode", action="store_true", default=None,
                         help="Force the zero-copy QSV decode->encode "
                              "pipeline on for this run.")
        hwd.add_argument("--no-hw-decode", action="store_false",
                         dest="hw_decode",
                         help="Force CPU decode -> QSV encode for this run.")
        # Compat audio default-on for presets too.
        ca = p.add_mutually_exclusive_group()
        ca.add_argument("--compat-audio", action="store_true", default=True,
                        help="(default) Add AAC 5.1 + AAC 2.0 compat tracks "
                             "alongside any hi-res lossless source.")
        ca.add_argument("--no-compat-audio", action="store_false",
                        dest="compat_audio",
                        help="Disable the AAC compat-track shadowing.")
        # Naming: preset turns rewrite-codec + reencode-tag on; user can opt
        # out of dotted style or change the marker token.
        p.add_argument("--no-dotted", action="store_true",
                       help="Keep input whitespace style instead of forcing dots.")
        p.add_argument("--name-suffix", default="",
                       help="Free-form trailing append; runs after preset rename.")
        p.add_argument("--reencode-tag-value", default="REENCODE",
                       help="Override the REENCODE marker token (default: REENCODE).")
        p.add_argument("--verbose", "-v", action="store_true")
        _add_common_db_arg(p)


def _build_parser() -> argparse.ArgumentParser:
    """Construct the top-level argparse parser with all subcommands wired up."""
    p = argparse.ArgumentParser(
        prog="video_optimizer",
        description="Probe + rules + re-encode for video libraries.",
    )
    sub = p.add_subparsers(dest="cmd", required=True)
    _add_scan_parser(sub)
    _add_reprobe_parser(sub)
    _add_plan_parser(sub)
    _add_apply_parser(sub)
    _add_status_parser(sub)
    _add_list_encoders_parser(sub)
    _add_replace_list_parser(sub)
    _add_doctor_parser(sub)
    _add_optimize_parser(sub)
    _add_preset_parsers(sub)
    return p


# --------------------------------------------------------------------------- #
# Commands
# --------------------------------------------------------------------------- #


def _probe_one_safe(fp: Path) -> tuple[str, Path, object]:
    """Worker wrapper around probe.probe_file that won't raise.

    Returns ('ok', fp, ProbeResult) on success, ('err', fp, exception)
    otherwise. The caller (main thread) does the SQLite write — workers
    must not touch the db.
    """
    try:
        return ("ok", fp, probe.probe_file(fp))
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            json.JSONDecodeError, ValueError, OSError) as e:
        return ("err", fp, e)


def _scan_walk_phase(args: argparse.Namespace, db: Database, force: bool,
                     ) -> tuple[int, int, int, int, list[Path]]:
    """Tree walk + cache filter + size gate.

    Returns (seen, cached, size_skipped, errors, uncached). The size gate
    is checked first, before the probe cache: a file under the threshold
    is recorded in `skipped_files` and any prior probe row is evicted, so
    a file that shrinks below the threshold (or a threshold raised since
    the last scan) is consistently treated as skipped.

    Files in `skipped_files` that have grown to >= threshold get their
    skip row cleared and fall through to the normal cache-or-probe path.
    """
    min_size = max(0, getattr(args, "min_size", MIN_PROBE_SIZE_BYTES))
    seen = cached = size_skipped = errors = 0
    uncached: list[Path] = []
    for fp in crawler.crawl(args.path, recursive=not args.no_recursive):
        seen += 1
        try:
            st = fp.stat()
        except OSError as e:
            print(f"skip {fp}: {e}", file=sys.stderr)
            errors += 1
            continue
        path_str = str(fp)
        if min_size > 0 and st.st_size < min_size:
            db.record_size_skip(path_str, st.st_size, st.st_mtime)
            size_skipped += 1
            if args.verbose:
                print(f"skip   {fp}  ({st.st_size / 1024 / 1024:.1f} MB "
                      f"< min {_format_size(min_size)})")
            continue
        # File is large enough; if it was previously skipped, lift the
        # skip row so the next branches treat it as a normal candidate.
        db.clear_size_skip(path_str)
        use_cache = not (force or args.no_probe_cache)
        if use_cache and db.get_cached_probe(path_str, st.st_size, st.st_mtime):
            cached += 1
            if args.verbose:
                print(f"cache  {fp}")
            continue
        uncached.append(fp)
    return seen, cached, size_skipped, errors, uncached


def _scan_probe_phase(args: argparse.Namespace, db: Database,
                      uncached: list[Path], workers: int) -> tuple[int, int]:
    """Parallel ffprobe of the uncached set. Returns (probed, errors).

    SQLite writes stay on the main thread (single-writer). Workers
    return results; this loop applies them in completion order.
    """
    if not uncached:
        return 0, 0
    if workers > 1:
        print(f"probing {len(uncached)} new file(s) with {workers} workers...")
    probed = errors = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(_probe_one_safe, fp) for fp in uncached]
        for fut in concurrent.futures.as_completed(futures):
            kind, fp, res = fut.result()
            if kind == "err":
                print(f"probe failed: {fp}: {res}", file=sys.stderr)
                errors += 1
                continue
            db.upsert_probe(res)
            probed += 1
            if args.verbose:
                print(f"probe  {fp}  ({res.video_codec} "
                      f"{res.width}x{res.height} "
                      f"{res.video_bitrate / 1_000_000:.1f} Mbps)")
    return probed, errors


def cmd_scan(args: argparse.Namespace, force: bool = False) -> int:
    """Crawl args.path, probe each video, upsert into the SQLite cache.

    The probe step is parallelised across `--workers` threads because
    ffprobe is I/O-bound (subprocess fork + NFS read). Cache hits stay
    on the main thread (no upside to threading a sqlite point lookup).
    SQLite writes also stay on the main thread — workers return
    ProbeResult, the main loop calls upsert_probe.
    """
    if not args.path.exists():
        print(f"error: path not found: {args.path}", file=sys.stderr)
        return 2
    workers = args.workers if args.workers is not None else min(8, os.cpu_count() or 4)
    workers = max(1, workers)
    with Database(args.db) as db:
        run_id = db.start_run("scan", str(args.path), _args_dict(args))
        seen, cached, size_skipped, walk_errors, uncached = _scan_walk_phase(
            args, db, force)
        probed, probe_errors = _scan_probe_phase(args, db, uncached, workers)
        errors = walk_errors + probe_errors
        summary = {"seen": seen, "probed": probed,
                   "cache_hits": cached, "size_skipped": size_skipped,
                   "errors": errors, "workers": workers}
        db.end_run(run_id, summary)
        skip_note = (f", {size_skipped} skipped (< "
                     f"{_format_size(getattr(args, 'min_size', MIN_PROBE_SIZE_BYTES))})"
                     if size_skipped else "")
        print(f"scan done: {seen} files seen, {probed} probed, "
              f"{cached} cached{skip_note}, {errors} errors")
    return 0


def cmd_reprobe(args: argparse.Namespace) -> int:
    """Force re-probe of every file under args.path, ignoring the cache."""
    return cmd_scan(args, force=True)


_REENCODED_MARKER_RE = re.compile(r"\bREENCODE\b", re.IGNORECASE)


def _is_reencoded_filename(path: str) -> bool:
    """True if `path` looks like one of our prior re-encode outputs.

    Matches the `REENCODE` token inserted by `--reencode-tag` (case
    insensitive, word-boundary). Used to keep the plan gate from queueing
    a file we've already processed — without this, an in-place run that
    chose a non-deletable disposal mode (recycle / backup) would surface
    its own outputs back into a future plan and re-encode them, doubling
    the marker (`...AV1.REENCODE.REENCODE.mkv`) and burning hours.
    """
    return _REENCODED_MARKER_RE.search(Path(path).stem) is not None


def _plan_probe_gate(db: Database, pr,
                     *, allow_reencoded: bool = False) -> str:
    """Pre-rule filter for one probe.

    Returns one of:
      "missing"    — source no longer on disk; cache rows dropped here.
      "stalled"    — two-strikes auto-skip (av1_qsv watchdog twice).
      "dv"         — Dolby Vision source; av1_qsv wedges on DV (Profile 7
                     stalls at frame 0; Profile 8 partway in). Awaiting a
                     DV-aware encode path (RPU strip / BL extraction).
      "reencoded"  — filename carries the REENCODE marker (output of a
                     prior run); skipped permanently unless caller passes
                     allow_reencoded=True (`plan --allow-reencoded`).
      "ok"         — admit to rule evaluation.
    """
    if not Path(pr.path).exists():
        # Decisions FK back to files with default RESTRICT; delete
        # dependent decisions first or the files DELETE fails.
        db.conn.execute("DELETE FROM decisions WHERE path = ?", (pr.path,))
        db.conn.execute("DELETE FROM files WHERE path = ?", (pr.path,))
        return "missing"
    stall_count = db.conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE path = ? "
        "AND status = 'failed' AND error LIKE '%encoder stalled%'",
        (pr.path,),
    ).fetchone()[0]
    if stall_count >= 2:
        return "stalled"
    if pr.dv_profile is not None:
        return "dv"
    if not allow_reencoded and _is_reencoded_filename(pr.path):
        return "reencoded"
    return "ok"


def cmd_plan(args: argparse.Namespace) -> int:
    """Run the rules engine over the probe cache; record pending decisions."""
    enabled = [s.strip() for s in args.rules.split(",")] if args.rules else None
    try:
        engine = rules.RulesEngine(enabled=enabled, target=args.target)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    allow_reencoded = bool(getattr(args, "allow_reencoded", False))
    with Database(args.db) as db:
        run_id = db.start_run("plan", None, _args_dict(args))
        cleared = db.clear_pending_decisions()
        candidates = []
        counts = {"missing": 0, "stalled": 0, "dv": 0, "reencoded": 0}
        # Materialise the probe list so we can mutate the cache (DELETE
        # stale rows) without invalidating the iterator.
        for pr in list(db.iter_probes()):
            verdict = _plan_probe_gate(db, pr, allow_reencoded=allow_reencoded)
            if verdict != "ok":
                counts[verdict] += 1
                continue
            cand = engine.evaluate(pr)
            if cand is None:
                continue
            db.insert_pending_decision(
                path=pr.path,
                rules_fired=[v.rule for v in cand.fired if not _is_advisory(v.rule)],
                target=cand.target,
                projected_savings_mb=cand.total_projected_savings_mb,
            )
            candidates.append(cand)
        pruned = counts["missing"]
        stall_blocked = counts["stalled"]
        dv_blocked = counts["dv"]
        reencoded_blocked = counts["reencoded"]
        if pruned:
            db.conn.commit()
            print(f"pruned {pruned} stale cache rows (source moved or deleted)")
        if stall_blocked:
            print(f"skipped {stall_blocked} files with 2+ stall failures "
                  f"(see `./video_optimizer.py replace-list` for the list)")
        if dv_blocked:
            print(f"skipped {dv_blocked} Dolby Vision sources "
                  f"(av1_qsv stalls on DV; awaiting DV-aware encode path)")
        if reencoded_blocked:
            print(f"skipped {reencoded_blocked} files already tagged "
                  f"REENCODE (prior outputs of this tool; pass "
                  f"--allow-reencoded to re-queue)")

        candidates.sort(key=lambda c: c.total_projected_savings_mb, reverse=True)

        if args.json:
            print(report.format_candidates_json(candidates))
        else:
            print(report.format_candidates_text(candidates))

        summary = {"cleared_pending": cleared,
                   "candidates": len(candidates),
                   "pruned_stale_rows": pruned,
                   "stall_blocked": stall_blocked,
                   "dv_blocked": dv_blocked,
                   "reencoded_blocked": reencoded_blocked}
        db.end_run(run_id, summary)
    return 0


def _prefilter_resolution_gate(db: Database, pending: list[dict],
                               args: argparse.Namespace) -> list[dict]:
    """Drop pending rows outside the preset's height band before --limit slices.

    Without this, `--limit N` can be consumed entirely by DEFER outcomes
    (a queue whose top-N-by-savings are all UHD candidates burns the
    limit on 10 defers and does zero encodes). With it, `--limit N`
    means "N actual encodes within this preset's resolution band."

    The in-loop gate in _apply_one is kept as defense in depth — any
    caller that bypasses cmd_apply still gets correct DEFER behavior.
    """
    min_h = getattr(args, "min_height", None)
    max_h = getattr(args, "max_height", None)
    if min_h is None and max_h is None:
        return pending
    eligible = []
    deferred = 0
    for dec in pending:
        pr = _load_probe_for_decision(db, dec)
        if pr is None:
            # Let _apply_one surface "probe missing" — don't filter here.
            eligible.append(dec)
            continue
        if min_h is not None and pr.height < min_h:
            deferred += 1
            continue
        if max_h is not None and pr.height > max_h:
            deferred += 1
            continue
        eligible.append(dec)
    if deferred:
        print(f"deferred {deferred} candidates outside the resolution band "
              f"(min={min_h}, max={max_h}); they remain pending for another preset.")
    return eligible


def cmd_apply(args: argparse.Namespace) -> int:
    """Encode pending decisions; per-file confirm unless --auto / --dry-run."""
    if args.mode == "side" and not args.output_root:
        print("error: --mode side requires --output-root", file=sys.stderr)
        return 2
    if args.backup and getattr(args, "recycle_to", None):
        print("error: --backup and --recycle-to are mutually exclusive "
              "(both preserve the original; pick one)", file=sys.stderr)
        return 2
    if getattr(args, "recycle_to", None) and args.mode != "replace":
        print("error: --recycle-to only applies to --mode replace "
              "(side mode never deletes originals)", file=sys.stderr)
        return 2
    if not _confirm_hard_delete_if_needed(args):
        return 2

    keep_langs = [s.strip() for s in args.keep_langs.split(",") if s.strip()]

    with Database(args.db) as db:
        run_id = db.start_run("apply", None, _args_dict(args))
        pending = db.list_pending_decisions()
        pending = _prefilter_resolution_gate(db, pending, args)

        if args.limit > 0:
            pending = pending[: args.limit]

        if not pending:
            print("nothing to apply: no pending decisions. run 'plan' first.")
            db.end_run(run_id, {"applied": 0})
            return 0

        counts = {"applied": 0, "skipped": 0, "failed": 0, "deferred": 0}
        bytes_saved = 0
        for i, dec in enumerate(pending, 1):
            status, saved = _apply_one(db, dec, args, keep_langs, i, len(pending))
            # Only count terminal statuses; "dry_run" is a no-op for counters.
            if status in counts:
                counts[status] += 1
            bytes_saved += saved

        summary = {**counts, "approx_bytes_saved": bytes_saved}
        db.end_run(run_id, summary)
        deferred_note = (f", {counts['deferred']} deferred (resolution gate)"
                         if counts["deferred"] else "")
        print(f"\napply done: {counts['applied']} encoded, "
              f"{counts['skipped']} skipped, {counts['failed']} failed"
              f"{deferred_note}; "
              f"~{_format_bytes(bytes_saved)} saved")
    return 0


def _apply_one(db: Database, dec: dict, args: argparse.Namespace,
               keep_langs: list[str], idx: int, total: int) -> tuple[str, int]:
    """Process a single pending decision. Returns (status, bytes_saved)."""
    pr = _load_probe_for_decision(db, dec)
    if pr is None:
        print(f"[{idx}/{total}] {dec['path']}: probe missing, skipping")
        db.mark_decision(dec["id"], "skipped",
                         error="probe missing in cache (rerun scan)")
        return "skipped", 0

    # Defense in depth: catch sources that disappeared between plan and
    # apply (e.g. file moved to recycle by an earlier apply, or unmounted
    # NFS share). cmd_plan already prunes these, but a long-running apply
    # could lose a source mid-run.
    if not Path(pr.path).exists():
        print(f"[{idx}/{total}] {pr.path}: source no longer exists, skipping")
        db.mark_decision(dec["id"], "skipped",
                         error="source no longer exists at apply time")
        return "skipped", 0

    _print_decision_header(dec, pr, idx, total)

    # Resolution gate: defer (leave pending) if outside the requested band.
    # Used by hd-archive to skip UHD candidates and uhd-archive to skip HD.
    min_h = getattr(args, "min_height", None)
    max_h = getattr(args, "max_height", None)
    if min_h is not None and pr.height < min_h:
        print(f"    DEFER: height {pr.height} < min {min_h} "
              f"(left pending for another run)")
        return "deferred", 0
    if max_h is not None and pr.height > max_h:
        print(f"    DEFER: height {pr.height} > max {max_h} "
              f"(left pending for another run)")
        return "deferred", 0

    if pr.is_hdr:
        # av1_qsv main profile carries 10-bit, color metadata is passed
        # through, and -pix_fmt p010le is pinned for 10-bit sources. That's
        # the minimum for correctly-tagged HDR output. Mastering display +
        # MaxCLL SEI are advisory (better tone-mapping on non-reference
        # displays); not yet forwarded — see encoder._color_passthrough_args.
        print("    HDR: passthrough (10-bit + BT.2020/PQ tagging)")

    if not args.auto and not args.dry_run:
        if not _confirm("    encode this file? [y/N/q]: "):
            db.mark_decision(dec["id"], "skipped", error="user declined")
            return "skipped", 0

    target = dec["target"]
    target_container = encoder.TARGETS[target][1]
    output_path = _compute_output_path(pr, args, target)

    try:
        enc_name = encoder.select_encoder(target, args.hwaccel)
    except RuntimeError as e:
        print(f"    FAIL: {e}")
        db.mark_decision(dec["id"], "failed", error=str(e))
        return "failed", 0

    cmd, desc = _build_apply_command(dec, pr, output_path, target_container,
                                     enc_name, keep_langs, args)

    if args.dry_run:
        print(f"    DRY RUN ({desc}) → {output_path}")
        print("    " + " ".join(cmd))
        return "dry_run", 0

    label = f"[{idx}/{total}] {Path(pr.path).name}: "
    return _execute_encode(db, dec, pr, cmd, desc, output_path, args, label)


def _print_decision_header(dec: dict, pr: ProbeResult, idx: int, total: int) -> None:
    """Print the `[idx/total] path / rules / projected savings` block."""
    print(f"\n[{idx}/{total}] {pr.path}")
    print(f"    rules: {dec['rules_fired_json']}  target: {dec['target']}")
    print(f"    projected savings: "
          f"{(dec['projected_savings_mb'] or 0) / 1024:.1f} GB")


def _build_apply_command(dec: dict, pr: ProbeResult, output_path: Path,
                         target_container: str, enc_name: str,
                         keep_langs: list[str],
                         args: argparse.Namespace) -> tuple[list[str], str]:
    """Pick remux vs encode and build the corresponding ffmpeg argv."""
    add_compat = getattr(args, "compat_audio", True)
    if _is_remux_only_decision(dec, pr):
        cmd = encoder.build_remux_command(pr, output_path,
                                          target_container, keep_langs,
                                          add_compat_audio=add_compat)
        return cmd, "remux"
    cmd = encoder.build_encode_command(
        pr, output_path, enc_name, args.quality, keep_langs,
        target_container, hw_decode=getattr(args, "hw_decode", False),
        add_compat_audio=add_compat,
    )
    return cmd, f"encode via {enc_name}"


def _execute_encode(db: Database, dec: dict, pr: ProbeResult,
                    cmd: list[str], desc: str, output_path: Path,
                    args: argparse.Namespace,
                    label: str = "") -> tuple[str, int]:
    """Run ffmpeg, finalise the output path, update the decision row."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    timeout = _resolve_timeout(args.timeout, pr.duration_seconds)
    print(f"    {desc} → {output_path}")
    if args.verbose:
        timeout_label = "disabled" if timeout in (None, 0) else f"{timeout}s"
        print(f"    timeout: {timeout_label}")

    ok, err = encoder.run_ffmpeg(cmd, pr.duration_seconds,
                                 timeout_seconds=timeout,
                                 verbose=args.verbose,
                                 label=label,
                                 source_fps=pr.frame_rate)
    if not ok:
        print(f"    FAIL: {err}")
        # Clean up partial output so re-runs don't trip on it.
        if output_path.exists():
            try:
                output_path.unlink()
            except OSError:
                pass
        db.mark_decision(dec["id"], "failed", error=err[:1000])
        return "failed", 0

    actual_mb = _finalize_output(pr, output_path, args, db, dec)
    return "applied", int(actual_mb * 1024 * 1024)


def cmd_list_encoders(args: argparse.Namespace) -> int:  # noqa: ARG001
    """Print available ffmpeg encoders and the encoder selected per target."""
    available = encoder.get_available_encoders()

    print("Compiled-in video encoders (per `ffmpeg -encoders`)")
    print("====================================================")
    for codec_label, codec_key in (("AV1", "av1"),
                                   ("HEVC", "hevc"),
                                   ("H.264", "h264")):
        present, missing = _split_encoders_by_availability(codec_key, available)
        print(f"{codec_label}:")
        print(f"  built-in:    {', '.join(present) if present else '(none)'}")
        if missing:
            print(f"  not in build: {', '.join(missing)}")

    print("\nRuntime hardware checks")
    print("=======================")
    vaapi = "/dev/dri/renderD128"
    print(f"VAAPI device {vaapi}: "
          f"{'present' if Path(vaapi).exists() else 'absent'}")
    print("Note: hardware encoders (qsv/nvenc/vaapi/videotoolbox) being "
          "built into ffmpeg")
    print("does not guarantee they will run — they additionally need "
          "matching kernel")
    print("modules / drivers / GPU. Failures surface as non-zero exit "
          "codes from apply.")

    print("\nEncoder picked with --hwaccel auto")
    print("==================================")
    for target in encoder.TARGETS:
        try:
            chosen = encoder.select_encoder(target, "auto")
            print(f"  {target:9s} -> {chosen}")
        except RuntimeError as e:
            print(f"  {target:9s} -> (no encoder: {e})")

    return 0


def _split_encoders_by_availability(codec_key: str,
                                    available: set[str]) -> tuple[list[str], list[str]]:
    """Split the codec's known encoders into (available, missing) lists."""
    seen: set[str] = set()
    ordered: list[str] = []
    for encs in encoder.ENCODER_PREFERENCE[codec_key].values():
        for e in encs:
            if e and e not in seen:
                seen.add(e)
                ordered.append(e)
    present = [e for e in ordered if e in available]
    missing = [e for e in ordered if e not in available]
    return present, missing


def _tool_version(name: str) -> str:
    """Best-effort one-line version string from `tool -version`. Empty on failure."""
    try:
        result = subprocess.run([name, "-version"],
                                capture_output=True, text=True, timeout=5)
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return ""
    line = (result.stdout.splitlines() or [""])[0]
    return line.strip()[:80]


def _doctor_check_tools(issues: list[str]) -> dict[str, str | None]:
    print("External tools")
    print("==============")
    tools = encoder.check_external_tools()
    for name, path in tools.items():
        if path is None:
            print(f"  {name}: MISSING — install ffmpeg "
                  f"(provides both ffmpeg and ffprobe)")
            issues.append(f"{name} not on PATH")
            continue
        ver = _tool_version(name) or "(version unavailable)"
        print(f"  {name}: {path}")
        print(f"    {ver}")
    return tools


def _doctor_check_encoders(tools: dict[str, str | None],
                           issues: list[str]) -> None:
    print("\nVideo encoders")
    print("==============")
    if not all(tools.values()):
        print("  (skipped: ffmpeg not on PATH)")
        return
    available = encoder.get_available_encoders()
    if not available:
        print("  no video encoders detected — check the ffmpeg build")
        issues.append("no video encoders available")
        return
    for codec_label, codec_key in (("AV1", "av1"),
                                   ("HEVC", "hevc"),
                                   ("H.264", "h264")):
        present, _missing = _split_encoders_by_availability(codec_key, available)
        mark = "OK" if present else "missing"
        joined = ", ".join(present) or "(none)"
        print(f"  {codec_label:6s} [{mark:7s}]  {joined}")
    print("\n  Encoder picked with --hwaccel auto:")
    for target in encoder.TARGETS:
        try:
            chosen = encoder.select_encoder(target, "auto")
            print(f"    {target:9s} -> {chosen}")
        except RuntimeError as e:
            first_line = str(e).splitlines()[0]
            print(f"    {target:9s} -> NONE ({first_line})")
            issues.append(f"no encoder for target {target}")


def _doctor_check_vaapi() -> None:
    print("\nGPU / VAAPI")
    print("===========")
    vaapi = "/dev/dri/renderD128"
    if Path(vaapi).exists():
        print(f"  {vaapi}: present")
    else:
        print(f"  {vaapi}: absent")
        print("    (VAAPI encoders won't run; QSV/NVENC are independent)")


def _doctor_check_db(db_path: Path, issues: list[str]) -> None:
    print("\nDatabase")
    print("========")
    try:
        with Database(db_path) as db:
            files_n = db.conn.execute(
                "SELECT COUNT(*) FROM files").fetchone()[0]
            pending_n = db.conn.execute(
                "SELECT COUNT(*) FROM decisions WHERE status='pending'"
            ).fetchone()[0]
        print(f"  {db_path}: ok")
        print(f"    {files_n} files cached, {pending_n} pending decisions")
    except sqlite3.Error as e:
        print(f"  {db_path}: ERROR ({e})")
        issues.append("database unreachable")


def _doctor_sample_probe(probe_path: Path, issues: list[str]) -> None:
    print(f"\nSample probe: {probe_path}")
    print("=" * 60)
    if not probe_path.exists():
        print("  path does not exist")
        issues.append(f"probe path missing: {probe_path}")
        return
    try:
        pr = probe.probe_file(probe_path)
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired,
            json.JSONDecodeError, ValueError, OSError) as e:
        print(f"  probe failed: {e}")
        issues.append(f"probe failed for {probe_path}")
        return
    hdr_label = "yes" if pr.is_hdr else "no"
    dv_label = (f"profile {pr.dv_profile}"
                if pr.dv_profile is not None else "no")
    br_mbps = pr.video_bitrate / 1e6
    print(f"  duration:    {pr.duration_seconds:.1f}s")
    print(f"  resolution:  {pr.width}x{pr.height} "
          f"({pr.resolution_class})")
    print(f"  codec:       {pr.video_codec}, "
          f"container={pr.container}")
    print(f"  bit depth:   {pr.bit_depth}, hdr={hdr_label}, "
          f"dolby vision={dv_label}")
    print(f"  bitrate:     {br_mbps:.2f} Mbps")
    print(f"  audio:       {len(pr.audio_tracks)} tracks, "
          f"subs: {len(pr.subtitle_tracks)}")


def cmd_doctor(args: argparse.Namespace) -> int:
    """Run preflight checks; exit 0 if everything's healthy, 1 otherwise.

    Designed to be the first command a new user runs. Each section either
    reports green or names a concrete remediation step. The summary line at
    the end gives a fast yes/no answer.
    """
    issues: list[str] = []
    tools = _doctor_check_tools(issues)
    _doctor_check_encoders(tools, issues)
    _doctor_check_vaapi()
    _doctor_check_db(args.db, issues)
    if args.probe is not None:
        _doctor_sample_probe(args.probe, issues)

    print()
    if issues:
        print(f"FAIL: {len(issues)} issue(s) found")
        for x in issues:
            print(f"  - {x}")
        return 1
    print("OK: all checks passed")
    return 0


_RECYCLE_DIR_NAMES: tuple[str, ...] = ("@Recycle", ".@Recycle", "#recycle", ".Trash")


def _resolve_recycle_dir(path: Path, override: Path | None) -> Path:
    """Return the recycle directory to use for `optimize --in-place`.

    Resolution order: explicit override > existing recycle-named directory
    inside `path` > newly created `path/.@Recycle`. The new dir is created
    on first use; the rename into it must stay on the same filesystem to
    be atomic, which is why we anchor under `path` rather than $HOME.
    """
    if override is not None:
        override.mkdir(parents=True, exist_ok=True)
        return override
    for name in _RECYCLE_DIR_NAMES:
        candidate = path / name
        if candidate.is_dir():
            return candidate
    default = path / ".@Recycle"
    default.mkdir(parents=True, exist_ok=True)
    return default


def _optimize_resolve_paths(
    args: argparse.Namespace,
) -> tuple[bool, Path | None, Path, Path | None] | int:
    """Return (in_place, output_root, source_root, recycle_to) or an exit code."""
    in_place = bool(args.in_place)
    if in_place:
        return (True, None, args.path,
                _resolve_recycle_dir(args.path, args.recycle_to))
    if args.recycle_to is not None:
        print("error: --recycle-to only applies to --in-place", file=sys.stderr)
        return 2
    return (False, args.output, args.path, None)


def _optimize_run_apply(
    args: argparse.Namespace,
    in_place: bool,
    output_root: Path | None,
    source_root: Path,
    recycle_to: Path | None,
) -> int:
    aggregate_rc = 0
    presets_to_run = ("uhd-archive", "hd-archive")
    for step, preset_name in enumerate(presets_to_run, start=3):
        cfg = PRESETS[preset_name]
        print(f"==> [{step}/4] apply: {preset_name} ({cfg['label']})")
        preset_ns = argparse.Namespace(
            cmd=preset_name,
            auto=args.auto,
            mode="replace" if in_place else "side",
            output_root=output_root,
            source_root=source_root,
            backup=None,
            recycle_to=recycle_to,
            allow_hard_delete=False,
            limit=args.limit,
            min_height=None,
            max_height=None,
            quality=args.quality,
            keep_langs=args.keep_langs,
            hwaccel=args.hwaccel,
            timeout=None,
            hw_decode=args.hw_decode,
            compat_audio=True,
            no_dotted=False,
            name_suffix="",
            reencode_tag_value="REENCODE",
            dry_run=args.dry_run,
            verbose=args.verbose,
            db=args.db,
        )
        rc = cmd_preset(preset_ns)
        if rc != 0:
            aggregate_rc = rc
        print()
    return aggregate_rc


def cmd_optimize(args: argparse.Namespace) -> int:
    """One-shot scan+plan+apply pipeline using calibrated presets.

    Sequences UHD then HD so the higher-savings tier ships first; each
    preset's resolution gate (min_height / max_height in PRESETS) keeps
    the two from clobbering each other's queue.
    """
    if not args.path.exists():
        print(f"error: path not found: {args.path}", file=sys.stderr)
        return 2
    if not args.path.is_dir():
        print(f"error: optimize expects a directory: {args.path}", file=sys.stderr)
        return 2

    resolved = _optimize_resolve_paths(args)
    if isinstance(resolved, int):
        return resolved
    in_place, output_root, source_root, recycle_to = resolved

    print(f"==> optimize: {args.path}")
    print(f"    output mode: {'replace (in-place)' if in_place else 'side'}")
    print(f"    recycle to:  {recycle_to}" if in_place
          else f"    output root: {output_root}")
    if args.dry_run:
        print("    DRY RUN (no encodes will run)")
    print()

    if args.skip_scan:
        print("==> [1/4] scan: skipped (--skip-scan)")
    else:
        print(f"==> [1/4] scan: probing {args.path} (cache hits skip ffprobe)...")
        scan_ns = argparse.Namespace(
            cmd="scan", path=args.path, no_recursive=False,
            no_probe_cache=False, workers=args.workers,
            min_size=args.min_size,
            verbose=args.verbose, db=args.db,
        )
        rc = cmd_scan(scan_ns)
        if rc != 0:
            return rc
    print()

    print("==> [2/4] plan: evaluating rules against probe cache...")
    plan_ns = argparse.Namespace(
        cmd="plan", rules=None, target="av1+mkv", json=False,
        keep_langs=args.keep_langs or "en,und",
        allow_reencoded=False,
        db=args.db,
    )
    rc = cmd_plan(plan_ns)
    if rc != 0:
        return rc
    print()

    return _optimize_run_apply(args, in_place, output_root, source_root, recycle_to)


def cmd_preset(args: argparse.Namespace) -> int:
    """Fill in preset values for missing args, then dispatch to cmd_apply."""
    cfg = PRESETS[args.cmd]
    args.target = cfg["target"]
    args.rewrite_codec = bool(cfg["rewrite_codec"])
    args.reencode_tag = bool(cfg["reencode_tag"])
    if args.quality is None:
        args.quality = cfg["quality"]
    if args.keep_langs is None:
        args.keep_langs = cfg["keep_langs"]
    if args.min_height is None and "min_height" in cfg:
        args.min_height = cfg["min_height"]
    if args.max_height is None and "max_height" in cfg:
        args.max_height = cfg["max_height"]
    if args.hw_decode is None:
        args.hw_decode = bool(cfg.get("hw_decode", False))
    if args.verbose:
        bounds = (f"[{args.min_height or '-'}..{args.max_height or '-'}]"
                  if (args.min_height or args.max_height) else "any")
        print(f"preset {args.cmd}: target={args.target}, quality={args.quality}, "
              f"keep_langs={args.keep_langs}, height={bounds}, "
              f"rewrite_codec={args.rewrite_codec}, "
              f"reencode_tag={args.reencode_tag}, hw_decode={args.hw_decode}, "
              f"compat_audio={args.compat_audio}")
    return cmd_apply(args)


def cmd_status(args: argparse.Namespace) -> int:
    """Print recent run history plus the current pending decision queue."""
    with Database(args.db) as db:
        runs = db.recent_runs(limit=args.last)
        pending = db.list_pending_decisions()

    if not runs and not pending:
        print("no history yet. run 'scan PATH' to start.")
        return 0

    print(f"recent runs ({len(runs)}):")
    for r in runs:
        when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["started_at"]))
        dur = (r["ended_at"] - r["started_at"]) if r["ended_at"] else 0
        summary = r["summary_json"] or "(no summary)"
        print(f"  {when}  {r['kind']:7s}  {dur:6.1f}s  {summary}")

    print(f"\npending decisions: {len(pending)}")
    for d in pending[:10]:
        savings = d["projected_savings_mb"] or 0
        print(f"  #{d['id']:>4}  {savings / 1024:6.1f} GB  {d['path']}")
    if len(pending) > 10:
        print(f"  ... and {len(pending) - 10} more")
    return 0


def cmd_replace_list(args: argparse.Namespace) -> int:
    """Print sources that hit the av1_qsv stall watchdog 2+ times.

    These are deterministic encoder failures: the bitstream pattern in the
    source triggers a libvpl AV1 hang that doesn't recover. Re-running
    won't help; the operator needs to grab a different release of the
    same title (or accept losing it from the archive backlog).

    `plan` skips these files automatically — they won't re-queue. The
    list here is purely informational.
    """
    with Database(args.db) as db:
        rows = db.conn.execute(
            "SELECT path, COUNT(*) AS fails, "
            "       MAX(decided_at) AS last_failed_at "
            "FROM decisions "
            "WHERE status = 'failed' AND error LIKE '%encoder stalled%' "
            "GROUP BY path "
            "HAVING COUNT(*) >= 2 "
            "ORDER BY MAX(decided_at) DESC"
        ).fetchall()

    if not rows:
        print("no files have hit the stall watchdog 2+ times.")
        return 0

    print(f"{len(rows)} source(s) with 2+ encoder stalls "
          f"— consider replacing with a different release:\n")
    for r in rows:
        when = time.strftime("%Y-%m-%d", time.localtime(r["last_failed_at"]))
        print(f"  {r['fails']}× stalled  (last: {when})  {r['path']}")
    return 0


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #


def _format_bytes(n: int) -> str:
    """Adaptive byte formatter: bytes / KB / MB / GB / TB."""
    n_abs = abs(n)
    for unit, scale in (("TB", 1024**4), ("GB", 1024**3),
                        ("MB", 1024**2), ("KB", 1024)):
        if n_abs >= scale:
            return f"{n / scale:.1f} {unit}"
    return f"{n} B"


def _args_dict(args: argparse.Namespace) -> dict[str, Any]:
    return {k: (str(v) if isinstance(v, Path) else v)
            for k, v in vars(args).items()}


def _confirm(prompt: str) -> bool:
    """Read a y/N/q answer from stdin; raise SystemExit on q or Ctrl-C."""
    try:
        ans = input(prompt).strip().lower()
    except (EOFError, KeyboardInterrupt) as exc:
        print()
        raise SystemExit(130) from exc
    if ans == "q":
        raise SystemExit(0)
    return ans.startswith("y")


def _confirm_hard_delete_if_needed(args: argparse.Namespace) -> bool:
    """Gate `--mode replace` runs that have no original-preserving option.

    Returns True if the run is allowed to proceed, False if it should abort.
    Side and replace+backup and replace+recycle-to all return True without
    prompting. Replace with neither requires either an explicit
    --allow-hard-delete flag (under --auto) or a typed-yes confirmation
    (interactive). Prints an error/warning to stderr explaining the situation.
    """
    if args.mode != "replace":
        return True
    if args.backup or getattr(args, "recycle_to", None):
        return True
    msg = ("WARNING: --mode replace without --backup or --recycle-to permanently "
           "deletes each source file after its encode succeeds. There is no undo.\n"
           "  prefer --recycle-to <dir> (atomic move into a recycle directory) "
           "or --backup <dir> (copy before delete).")
    if args.auto:
        if getattr(args, "allow_hard_delete", False):
            print(msg, file=sys.stderr)
            print("  (proceeding because --allow-hard-delete was set)",
                  file=sys.stderr)
            return True
        print(msg, file=sys.stderr)
        print("  refusing to run with --auto; pass --allow-hard-delete to "
              "acknowledge, or add --recycle-to / --backup to preserve originals.",
              file=sys.stderr)
        return False
    print(msg, file=sys.stderr)
    return _confirm("  proceed? [y/N]: ")


def _is_advisory(rule_name: str) -> bool:
    rule = rules.RULES.get(rule_name)
    return bool(rule and rule.advisory)


def _load_probe_for_decision(db: Database, dec: dict) -> ProbeResult | None:
    row = db.conn.execute(
        "SELECT probe_json FROM files WHERE path = ?",
        (dec["path"],),
    ).fetchone()
    if row is None:
        return None
    return probe_from_dict(json.loads(row["probe_json"]))


def _is_remux_only_decision(dec: dict, pr: ProbeResult) -> bool:
    """Re-derive the remux-only flag from the persisted rules + current probe."""
    fired = json.loads(dec["rules_fired_json"])
    only_container = (len(fired) == 1 and fired[0] == "container_migration")
    modern = (pr.video_codec or "").lower() in {"h264", "hevc", "av1", "vp9"}
    return only_container and modern


def _build_output_stem(src: Path, args: argparse.Namespace, target: str) -> str:
    """Apply --rewrite-codec, --reencode-tag, --name-suffix to a stem in order."""
    stem = src.stem

    if getattr(args, "rewrite_codec", False):
        target_codec = encoder.TARGETS[target][0]
        stem = naming.rewrite_codec_tokens(
            stem, target_codec,
            dotted=not getattr(args, "no_dotted", False),
        )

    if getattr(args, "reencode_tag", False):
        token = getattr(args, "reencode_tag_value", "REENCODE") or "REENCODE"
        # If --rewrite-codec is on (and --no-dotted isn't), force dotted style
        # for the REENCODE tag too, so the result is consistent.
        force_dotted: bool | None = None
        rewrite_on = getattr(args, "rewrite_codec", False)
        plain_dotted = not getattr(args, "no_dotted", False)
        if rewrite_on and plain_dotted:
            force_dotted = True
        stem = naming.append_token(stem, token, dotted=force_dotted)

    suffix = getattr(args, "name_suffix", "") or ""
    return f"{stem}{suffix}"


def _compute_output_path(pr: ProbeResult, args: argparse.Namespace,
                         target: str) -> Path:
    src = Path(pr.path)
    new_ext = encoder.output_extension(target)
    new_stem = _build_output_stem(src, args, target)
    new_name = f"{new_stem}{new_ext}"

    if args.mode == "replace":
        return src.with_name(new_name)

    # side mode: place under --output-root, preserving relative structure.
    if args.source_root:
        try:
            rel = src.relative_to(args.source_root)
        except ValueError:
            rel = Path(*src.parts[1:]) if src.is_absolute() else src
    else:
        rel = Path(*src.parts[1:]) if src.is_absolute() else src

    return (args.output_root / rel).with_name(new_name)


def _resolve_timeout(user_value: int | None, duration_seconds: float) -> int | None:
    """Adaptive timeout: max(3600, 6 * duration). 0 disables. Explicit value wins."""
    if user_value is not None:
        return user_value if user_value > 0 else 0
    if duration_seconds <= 0:
        return 3600
    return max(3600, int(duration_seconds * 6))


def _recycle_destination(src: Path, recycle_to: Path,
                         source_root: Path | None) -> Path:
    """Compute the recycle-bin destination path for `src`.

    Mirrors the source hierarchy under `recycle_to` using `source_root` as
    the prefix to strip (falls back to filename-only if src isn't under
    source_root). If a file already exists at the computed path, appends
    `_recycled<N>` so prior recycles aren't clobbered.
    """
    if source_root:
        try:
            rel = src.relative_to(source_root)
        except ValueError:
            rel = Path(src.name)
    else:
        rel = Path(src.name)
    dst = recycle_to / rel
    if not dst.exists():
        return dst
    counter = 1
    while True:
        candidate = dst.parent / f"{dst.stem}_recycled{counter}{dst.suffix}"
        if not candidate.exists():
            return candidate
        counter += 1


def _finalize_output(pr: ProbeResult, output_path: Path,
                     args: argparse.Namespace, db: Database,
                     dec: dict) -> float:
    """Compute savings, run backup-or-recycle + remove-original, update db."""
    try:
        out_size = output_path.stat().st_size
    except OSError:
        out_size = 0
    actual_mb = (pr.size - out_size) / (1024 * 1024)

    if args.mode == "replace":
        recycle_to = getattr(args, "recycle_to", None)
        if recycle_to:
            # Atomic move into recycle-bin instead of copy-then-delete. Wins
            # over --backup for NAS targets: no doubled disk use, no I/O
            # cost beyond a directory entry rename when source and target
            # share a filesystem.
            dst = _recycle_destination(Path(pr.path), recycle_to,
                                       getattr(args, "source_root", None))
            dst.parent.mkdir(parents=True, exist_ok=True)
            try:
                shutil.move(str(pr.path), str(dst))
            except OSError as e:
                print(f"    WARN: recycle move failed: {e}; "
                      f"keeping output and original intact")
                db.mark_decision(dec["id"], "completed",
                                 output_path=str(output_path),
                                 actual_savings_mb=actual_mb,
                                 error=f"recycle move failed: {e}")
                return actual_mb
            # Original is now at `dst`; nothing more to delete.
        elif args.backup:
            args.backup.mkdir(parents=True, exist_ok=True)
            backup_path = args.backup / Path(pr.path).name
            counter = 1
            while backup_path.exists():
                backup_path = args.backup / (
                    f"{Path(pr.path).stem}_backup{counter}{Path(pr.path).suffix}"
                )
                counter += 1
            try:
                shutil.copy2(pr.path, backup_path)
            except OSError as e:
                print(f"    WARN: backup failed: {e}; "
                      f"keeping output and original intact")
                db.mark_decision(dec["id"], "completed",
                                 output_path=str(output_path),
                                 actual_savings_mb=actual_mb,
                                 error=f"backup failed: {e}")
                return actual_mb

        # When --recycle-to is set the move above already removed the
        # original; otherwise unlink it now (after the optional backup copy).
        if not recycle_to and Path(pr.path) != output_path:
            try:
                Path(pr.path).unlink()
            except OSError as e:
                db.mark_decision(dec["id"], "completed",
                                 output_path=str(output_path),
                                 actual_savings_mb=actual_mb,
                                 error=f"original not removed: {e}")
                return actual_mb

    db.mark_decision(dec["id"], "completed",
                     output_path=str(output_path),
                     actual_savings_mb=actual_mb)
    return actual_mb


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #


_FFMPEG_DEPENDENT_CMDS: frozenset[str] = frozenset({
    "scan", "reprobe", "apply", "list-encoders", "optimize",
})


def _assert_external_tools_available(cmd: str) -> None:
    """Exit with a clear error if a subcommand needs ffmpeg/ffprobe but they
    aren't on PATH. plan, status, and replace-list work purely against the
    cached probe data and don't need either binary."""
    needs_tools = cmd in _FFMPEG_DEPENDENT_CMDS or cmd in PRESETS
    if not needs_tools:
        return
    tools = encoder.check_external_tools()
    missing = [name for name, path in tools.items() if path is None]
    if missing:
        joined = ", ".join(missing)
        sys.exit(
            f"error: required external tool(s) not on PATH: {joined}\n"
            f"  install ffmpeg (provides both ffmpeg and ffprobe) and retry.\n"
            f"  debian/ubuntu: sudo apt install ffmpeg\n"
            f"  macos (homebrew): brew install ffmpeg\n"
            f"  arch: sudo pacman -S ffmpeg"
        )


def main(argv: list[str] | None = None) -> int:
    """CLI entry point. Dispatches to the chosen subcommand handler."""
    parser = _build_parser()
    args = parser.parse_args(argv)
    _assert_external_tools_available(args.cmd)

    handlers = {
        "scan": cmd_scan,
        "reprobe": cmd_reprobe,
        "plan": cmd_plan,
        "apply": cmd_apply,
        "status": cmd_status,
        "list-encoders": cmd_list_encoders,
        "replace-list": cmd_replace_list,
        "doctor": cmd_doctor,
        "optimize": cmd_optimize,
    }
    # Preset subcommands all dispatch to the same wrapper.
    for preset_name in PRESETS:
        handlers[preset_name] = cmd_preset
    return handlers[args.cmd](args)
