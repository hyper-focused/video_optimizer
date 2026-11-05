"""Command-line entry point for video_optimizer."""

from __future__ import annotations

import argparse
import concurrent.futures
import difflib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from . import crawler, encoder, naming, probe, report, rules
from .db import DEFAULT_DB_PATH, Database
from .decide import Decision, _is_reencoded_filename, decide
from .models import Candidate, ProbeResult, probe_from_dict
from .presets import (
    BITRATE_FLAG_TABLE,
    BLOAT_CHECKPOINTS,
    BLOAT_RATIO_THRESHOLD,
    EST_SECONDS_PER_FILE,
    MIN_PROBE_SIZE_BYTES,
    PRESETS,
    SLOW_CPU_DECODE_CODECS,
    bloat_relax_for_height,
)

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
    s = sub.add_parser("scan")
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
    s.add_argument("--allow-extras", action="store_true",
                   help="Include Plex-style extras (Trailers, Behind The "
                        "Scenes, Featurettes, files with -trailer/-bts/etc. "
                        "suffixes). Default: skip them — a library-scale "
                        "tool shouldn't burn GPU time on add-ons.")
    s.add_argument("--verbose", "-v", action="store_true")
    _add_common_db_arg(s)


def _add_reprobe_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `reprobe` subcommand (alias for scan --no-probe-cache)."""
    r = sub.add_parser("reprobe")
    r.add_argument("path", type=Path)
    r.add_argument("--no-recursive", action="store_true")
    r.add_argument("--workers", type=int, default=None,
                   help="Parallel ffprobe workers (default: min(8, CPU count)).")
    _add_min_size_arg(r)
    r.add_argument("--allow-extras", action="store_true",
                   help="Include Plex-style extras during the re-probe walk.")
    r.add_argument("--verbose", "-v", action="store_true")
    _add_common_db_arg(r)


def _add_status_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `status` subcommand."""
    st = sub.add_parser("status", help="Show recent runs.")
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
    rl = sub.add_parser("replace-list")
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


def _add_pipeline_args(p: argparse.ArgumentParser, *, path_help: str) -> None:
    """Shared CLI surface for `optimize` and the SD/HD/UHD tier subcommands.

    All path-taking pipeline subcommands present the same flags so that
    `./video_optimizer.py UHD /path` and `./video_optimizer.py /path`
    differ only in tier scope. Visible flags cover the common UX
    decisions (where to write, dry-run, confirm, cleanup); advanced
    flags are SUPPRESSed but functional.
    """
    p.add_argument("path", type=Path, help=path_help)
    p.add_argument("--mode", choices=["keep", "side", "replace"],
                   default=None,
                   help="Output mode. 'keep' writes alongside the source "
                        "and leaves originals untouched (default when "
                        "neither --output nor --replace is set). 'side' "
                        "mirrors output into a separate tree (--output). "
                        "'replace' writes alongside originals and moves "
                        "the originals into a recycle directory (--replace).")
    out = p.add_mutually_exclusive_group()
    out.add_argument("--output", type=Path, metavar="DIR",
                     help="Side mode: write new files under DIR mirroring "
                          "PATH's structure. Originals are untouched.")
    out.add_argument("--replace", action="store_true",
                     help="Replace mode: write new files alongside originals "
                          "and move the originals into a recycle directory "
                          "(see --recycle-to).")
    p.add_argument("--recycle-to", type=Path, default=None, metavar="DIR",
                   help="With --replace: recycle directory for displaced "
                        "originals. If omitted, an existing @Recycle / "
                        "#recycle / .Trash under PATH is used; otherwise "
                        "<PATH>/.@Recycle is created.")
    p.add_argument("--limit", type=int, default=0, metavar="N",
                   help="Process at most N candidates (0 = no limit).")
    p.add_argument("--dry-run", action="store_true",
                   help="Print planned ffmpeg commands and exit.")
    p.add_argument("--confirm", action="store_true",
                   help="Prompt per-file before encoding (default is auto-yes).")
    p.add_argument("--cleanup-after", action="store_true",
                   help="After the path command, unlink originals of "
                        "completed encodes. No question unless --confirm "
                        "is also set. --confirm asks Continue? [y/N] and "
                        "leaves the files unless the answer starts with y.")
    p.add_argument("--original-audio", action="store_true",
                   help="Keep every input audio track via stream-copy "
                        "(default strips to --keep-langs and rebuilds a "
                        "3-stream ladder).")
    p.add_argument("--original-subs", action="store_true",
                   help="Keep every input subtitle track via stream-copy "
                        "(default strips to --keep-langs).")
    p.add_argument("--verbose", "-v", action="store_true")
    p.add_argument("--dv-p7-convert", action="store_true",
                   help="For Profile 7 sources: run the dovi_tool "
                        "convert + mkvmerge pipeline before encoding "
                        "instead of the default RPU-strip-only path. "
                        "Requires dovi_tool and mkvmerge on PATH.")
    p.add_argument("--no-auto-relax-cq", action="store_false",
                   dest="auto_relax_cq", default=True,
                   help="Disable the HD/UHD bloat fallback. Default: an "
                        "HD or UHD encode whose mid-encode projection or "
                        "final output is nearly as big as the source is "
                        "retried once at the tier's relaxed CQ.")

    # Hidden / advanced flags below — still functional, just not in --help.
    p.add_argument("--auto", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--workers", type=int, default=8, help=argparse.SUPPRESS)
    p.add_argument("--keep-langs", default=None, help=argparse.SUPPRESS)
    p.add_argument("--hwaccel",
                   choices=["auto", "qsv", "nvenc", "vaapi",
                            "videotoolbox", "software", "none"],
                   default="auto", help=argparse.SUPPRESS)
    hwd = p.add_mutually_exclusive_group()
    hwd.add_argument("--hw-decode", action="store_true", default=None,
                     help=argparse.SUPPRESS)
    hwd.add_argument("--no-hw-decode", action="store_false",
                     dest="hw_decode", help=argparse.SUPPRESS)
    p.add_argument("--quality", type=int, default=None, help=argparse.SUPPRESS)
    p.add_argument("--min-size", type=_parse_size,
                   default=MIN_PROBE_SIZE_BYTES, help=argparse.SUPPRESS)
    p.add_argument("--db", type=Path, default=DEFAULT_DB_PATH,
                   help=argparse.SUPPRESS)
    p.add_argument("--allow-av1", action="store_true", help=argparse.SUPPRESS)
    p.add_argument("--allow-extras", action="store_true",
                   help=argparse.SUPPRESS)
    p.add_argument("--allow-low-bitrate", action="store_true",
                   help=argparse.SUPPRESS)
    p.add_argument("--allow-reencoded", action="store_true",
                   help=argparse.SUPPRESS)
    p.add_argument("--timeout", type=int, default=None,
                   help=argparse.SUPPRESS)
    p.add_argument("--skip-codecs", default="",
                   help=argparse.SUPPRESS)
    p.add_argument("--bare-invocation", action="store_true", default=False,
                   help=argparse.SUPPRESS)


def _add_optimize_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `optimize` one-shot pipeline subcommand (all tiers)."""
    op = sub.add_parser(
        "optimize",
        help="Probe, decide, and encode a library, UHD then HD then SD. "
             "The friendliest entry point for new users.",
        description=(
            "Probe PATH, decide what to encode, and encode it in one job: "
            "UHD, then HD, then SD. Does not write a pending queue. The "
            "default output mode is 'keep': new files land alongside the "
            "source and originals stay untouched (see the `cleanup` "
            "subcommand for removing them). Pass --output DIR for a "
            "mirrored tree, or --replace to recycle originals as the run "
            "proceeds."
        ),
    )
    _add_pipeline_args(op, path_help="Library directory to optimize.")


def _add_preset_parsers(sub: "argparse._SubParsersAction") -> None:
    """Register one subcommand per entry in PRESETS, sharing a narrow flag set."""
    for name, cfg in PRESETS.items():
        p = sub.add_parser(
            name,
            help=f"Probe, decide, and encode with the {cfg['label']} preset.",
            description=(
                f"Probe PATH, decide, and encode using only the {name} "
                f"preset ({cfg.get('label')}). Same flags as `optimize`; "
                f"`optimize` chains UHD, HD, and SD, and this subcommand "
                f"processes only the {name} band. UHD-FILM is used only "
                f"when this preset is named, and only for UHD-tier files."
            ),
        )
        _add_pipeline_args(
            p,
            path_help=f"Library directory to scan for {name}-tier files.",
        )


def _add_cleanup_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `cleanup` subcommand for removing post-encode originals.

    Defaults to a dry-run listing keyed on the most-recent run with at
    least one completed decision. `--apply` actually unlinks the source
    files, gated by a 3-check guard (output exists, non-empty, distinct
    from source) applied per-decision in `cmd_cleanup`.
    """
    cl = sub.add_parser(
        "cleanup",
        help="Remove originals of successfully-encoded files from a prior run.",
    )
    cl.add_argument("--run", type=int, default=None, metavar="N",
                    help="Target a specific run id. If omitted, the most "
                         "recent run with at least one completed decision "
                         "is used.")
    cl.add_argument("--apply", action="store_true",
                    help="Actually remove the originals. Without this flag, "
                         "cleanup prints a dry-run listing and exits.")
    _add_common_db_arg(cl)


def _add_wizard_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `wizard` subcommand (interactive guided run)."""
    wz = sub.add_parser(
        "wizard",
        help="Interactive guided run: prompts for path, output mode, and "
             "tier scope, previews candidates, then encodes. Triggered "
             "automatically when invoked with no args in a TTY.",
    )
    _add_common_db_arg(wz)


def _add_rename_fix_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `rename-fix` maintenance subcommand.

    One-shot rename for already-encoded outputs that were written with
    pre-fix naming (orphaned `+` between stripped codec tokens, and
    HDR10Plus / DV tokens that don't survive the av1_qsv pipeline). The
    walk only touches files whose stem already carries the `REENCODE`
    marker so it can't disturb anything the tool didn't write.
    """
    rf = sub.add_parser(
        "rename-fix",
        help="Rename existing .REENCODE outputs through the current naming "
             "rules. Dry-run by default; pass --apply to actually rename.",
        description=(
            "Walk PATH for files whose filename stem contains the "
            "`REENCODE` marker, run the stem through the current av1 "
            "rewrite rules, and rename to match (along with any same-stem "
            "sidecars). Use this once after upgrading to clean up old "
            "outputs whose names advertise DV / HDR10+ that the encode "
            "path doesn't actually carry."
        ),
    )
    rf.add_argument("path", type=Path,
                    help="Directory to walk (recursive).")
    rf.add_argument("--apply", action="store_true",
                    help="Actually rename. Without this flag, prints a "
                         "dry-run listing of OLD → NEW and exits.")


def _build_parser() -> argparse.ArgumentParser:
    """Construct the top-level argparse parser with all subcommands wired up."""
    p = argparse.ArgumentParser(
        prog="video_optimizer",
        description="Probe + rules + re-encode for video libraries.",
    )
    sub = p.add_subparsers(dest="cmd", required=True,
                           metavar="<subcommand>")
    _add_scan_parser(sub)
    _add_reprobe_parser(sub)
    _add_status_parser(sub)
    _add_list_encoders_parser(sub)
    _add_replace_list_parser(sub)
    _add_doctor_parser(sub)
    _add_optimize_parser(sub)
    _add_preset_parsers(sub)
    _add_cleanup_parser(sub)
    _add_wizard_parser(sub)
    _add_rename_fix_parser(sub)
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
    skip_extras = not bool(getattr(args, "allow_extras", False))
    seen = cached = size_skipped = errors = 0
    uncached: list[Path] = []
    for fp in crawler.crawl(args.path, recursive=not args.no_recursive,
                            skip_extras=skip_extras):
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


def _path_under(candidate: str, root: Path) -> bool:
    """Return True if `candidate` lies under `root` (or equals it).

    Both sides are resolved before comparison so symlinks don't
    cause false negatives.
    """
    try:
        cand = Path(candidate).resolve(strict=False)
    except OSError:
        cand = Path(candidate)
    if cand == root:
        return True
    try:
        cand.relative_to(root)
    except ValueError:
        return False
    return True


def _existing_reencode_sibling(src_path: str) -> Path | None:
    """Return the path of an existing AV1 REENCODE sibling, or None.

    Catches the keep-mode blind spot: when an HEVC/h.264 source has
    already been encoded to AV1 (output sitting next to it as
    `<stem-without-codec-tokens>.AV1.REENCODE.mkv`), a fresh scan that
    re-probes the source still admits it to the plan because the
    source's filename never gained the REENCODE marker. Without this
    sibling check the plan would re-queue the same source on every
    run, ffmpeg's `-y` would overwrite the prior output, and a
    mid-encode kill would leave a partial — exactly what we want to
    prevent.

    Composes the expected output stem using the same naming pipeline
    SD/HD/UHD/optimize use at apply time (rewrite_codec=True,
    reencode_tag=True, dotted style) and looks for that .mkv next to
    the source. Returns the sibling path if found, None otherwise.
    """
    src = Path(src_path)
    if _is_reencoded_filename(src_path):
        # Source itself is already a REENCODE output — handled by
        # _is_reencoded_filename in the gate. Don't double-fire here.
        return None
    target_codec = encoder.TARGETS["av1+mkv"][0]
    stem = naming.rewrite_codec_tokens(src.stem, target_codec, dotted=True)
    stem = naming.append_token(stem, "REENCODE", dotted=True)
    candidate = src.with_name(f"{stem}.mkv")
    if candidate.exists() and candidate != src:
        return candidate
    return None


def _classify_probe(db: Database, pr: ProbeResult,
                    *, allow_reencoded: bool = False,
                    allow_av1: bool = False,
                    allow_extras: bool = False,
                    allow_low_bitrate: bool = False,
                    skip_codecs: frozenset[str] | None = None,
                    ) -> tuple[str, Decision | None]:
    """Filesystem gate plus ``decide``.

    Missing files are dropped from the cache here; ``decide`` does not
    touch the db or the disk. Returns ``("ok", decision)`` for clean or
    encode, or ``(reason, None)`` for a skip bucket including ``"missing"``.
    """
    if not Path(pr.path).exists():
        # Decisions FK back to files with default RESTRICT; delete
        # dependent decisions first or the files DELETE fails.
        db.conn.execute("DELETE FROM decisions WHERE path = ?", (pr.path,))
        db.conn.execute("DELETE FROM files WHERE path = ?", (pr.path,))
        return "missing", None
    stall_count = db.conn.execute(
        "SELECT COUNT(*) FROM decisions WHERE path = ? "
        "AND status = 'failed' AND error LIKE '%encoder stalled%'",
        (pr.path,),
    ).fetchone()[0]
    has_existing_output = _existing_reencode_sibling(pr.path) is not None
    decision = decide(
        pr,
        stall_count=stall_count,
        has_existing_output=has_existing_output,
        allow_reencoded=allow_reencoded,
        allow_av1=allow_av1,
        allow_extras=allow_extras,
        allow_low_bitrate=allow_low_bitrate,
        skip_codecs=skip_codecs,
    )
    if decision.kind == "skip" and decision.reason is not None:
        return decision.reason, None
    return "ok", decision


def _plan_probe_gate(db: Database, pr,
                     *, allow_reencoded: bool = False,
                     allow_av1: bool = False,
                     allow_extras: bool = False,
                     allow_low_bitrate: bool = False,
                     skip_codecs: frozenset[str] | None = None) -> str:
    """Pre-rule filter for one probe. Skip order lives in ``decide``.

    Returns one of:
      "missing"    — source no longer on disk; cache rows dropped here.
      "stalled"    — two-strikes auto-skip (av1_qsv watchdog twice).
      "dv"         — Dolby Vision profile ``dv_strategy`` cannot handle
                     (Profile 5; no HDR10 base). Profiles 7 and 8 are
                     not skipped: ``dv_strategy`` returns ``"p8_strip"``.
      "reencoded"  — filename carries the REENCODE marker (output of a
                     prior run); skipped permanently unless caller passes
                     allow_reencoded=True.
      "existing_output" — a sibling `.AV1.REENCODE.mkv` already exists
                     next to the source (keep-mode prior-run output).
                     Skipped to avoid overwriting it with a fresh encode;
                     allow_reencoded=True overrides (re-uses the same
                     "I want to re-run already-processed files" gate).
      "av1_source" — source codec is AV1; re-encoding is wasteful by
                     default. Caller can pass allow_av1=True to override
                     (`--allow-av1`).
      "skipped_codec" — source codec is in the user-supplied skip set
                     (e.g. ``--no-hevc``). Default is no skip set.
      "low_bitrate" — source bitrate is below the AV1 target for its
                     resolution; re-encoding wouldn't yield meaningful
                     savings. Override with --allow-low-bitrate.
      "extras"     — filename matches a Plex-style extras suffix
                     (`-trailer`, `-bts`, etc.). Defensive: the crawler
                     normally filters these at walk time, but a probe
                     cache populated before extras filtering existed
                     could surface them here. allow_extras=True overrides.
      "ok"         — ``decide`` returned clean or encode. SD content
                     (height < 720) is admitted; the per-tier presets
                     pick which band they want.
    """
    bucket, _decision = _classify_probe(
        db, pr, allow_reencoded=allow_reencoded,
        allow_av1=allow_av1, allow_extras=allow_extras,
        allow_low_bitrate=allow_low_bitrate,
        skip_codecs=skip_codecs)
    return bucket


_PLAN_SKIP_MESSAGES = (
    ("out_of_scope",    "skipped {n} probes outside the requested path "
                        "(other libraries cached from earlier scans)"),
    ("missing",         "pruned {n} stale cache rows (source moved or deleted)"),
    ("stalled",         "skipped {n} files with 2+ stall failures "
                        "(see `./video_optimizer.py replace-list` for the list)"),
    ("dv",              "skipped {n} Dolby Vision sources "
                        "(Profile 5 has no HDR10 fallback; Profile 7 needs "
                        "`dovi_tool` on PATH)"),
    ("reencoded",       "skipped {n} files already tagged REENCODE "
                        "(prior outputs of this tool; pass --allow-reencoded "
                        "to re-queue)"),
    ("existing_output", "skipped {n} sources whose AV1 REENCODE output already "
                        "exists alongside (pass --allow-reencoded to re-queue; "
                        "delete the prior output first if it's partial/bad)"),
    ("av1_source",      "skipped {n} AV1 sources "
                        "(already at the target codec; pass --allow-av1 "
                        "to re-encode anyway)"),
    ("skipped_codec",   "skipped {n} sources via --skip-codecs "
                        "(opted-out codecs; remove from --skip-codecs "
                        "to re-include)"),
    ("low_bitrate",     "skipped {n} sources whose video bitrate is below "
                        "the AV1 target for their resolution "
                        "(pass --allow-low-bitrate to re-encode anyway)"),
    ("extras",          "skipped {n} extras "
                        "(trailers / BTS / featurettes; pass --allow-extras "
                        "to include them)"),
)


def _emit_plan_skip_summary(counts: dict) -> None:
    """Print one summary line per non-zero plan-gate skip bucket."""
    for key, template in _PLAN_SKIP_MESSAGES:
        n = counts.get(key, 0)
        if n:
            print(template.format(n=n))


def _encode_one(db: Database, dec: dict, pr: ProbeResult,
                args: argparse.Namespace, keep_langs: list[str],
                idx: int, total: int) -> tuple[str, int]:
    """Encode one already-loaded decision. No height deferral.

    ``run_job`` calls this directly: the tier filter already ran, and a
    dry-run ``dec`` may have no id (no decision row is written).
    """
    run_id = getattr(args, "_apply_run_id", None)
    if pr.is_hdr:
        # av1_qsv main profile + 10-bit + color metadata passthrough.
        # Mastering-display / MaxCLL not forwarded — see
        # encoder._color_passthrough_args.
        print("    HDR: passthrough (10-bit + BT.2020/PQ tagging)")

    if not args.auto and not args.dry_run:
        if not _confirm("    encode this file? [y/N/q]: "):
            if dec.get("id") is not None:
                db.mark_decision(dec["id"], "skipped", error="user declined",
                                 run_id=run_id, expected_path=pr.path)
            return "skipped", 0

    target = dec["target"]
    target_container = encoder.TARGETS[target][1]
    output_path = _compute_output_path(pr, args, target)

    try:
        enc_name = encoder.select_encoder(target, args.hwaccel)
    except RuntimeError as e:
        print(f"    FAIL: {e}")
        if dec.get("id") is not None:
            db.mark_decision(dec["id"], "failed", error=str(e),
                             run_id=run_id, expected_path=pr.path)
        return "failed", 0

    return _apply_one_after_validation(
        db, dec, pr, args, run_id,
        output_path, target_container, enc_name, keep_langs, idx, total,
    )


def _apply_one_after_validation(db: Database, dec: dict, pr: ProbeResult,
                                args: argparse.Namespace, run_id: int | None,
                                output_path: Path, target_container: str,
                                enc_name: str, keep_langs: list[str],
                                idx: int, total: int) -> tuple[str, int]:
    """Run DV prep (if needed), build the encode argv, dispatch, cleanup.

    DV-prep work_dir lives next to the source (same filesystem for the
    temp stream-copy); try/finally guarantees teardown on encode failure.
    """
    # Dry-run runs *before* _prepare_dv_source so we don't spend hours
    # writing a 50 GB temp file just to print the argv.
    if args.dry_run:
        cmd, desc = _build_apply_command(
            dec, pr, output_path, target_container,
            enc_name, keep_langs, args,
        )
        print(f"    DRY RUN ({desc}) → {output_path}")
        print("    " + " ".join(cmd))
        return "dry_run", 0

    dv_prep_dir: Path | None = None
    source_for_encode: str | None = pr.path
    # encode_probe drives stream layout. For DV strip pre-pass, the
    # stripped file's matroska muxer renumbers kept streams contiguously,
    # so we re-probe the stripped file and reason from those indices.
    encode_probe: ProbeResult = pr
    dv_pre_pass_ran = False
    try:
        # DV strip pre-pass is UHD-only. See NOTES.md#uhd-only-gate.
        if pr.dv_profile is not None and pr.height >= 1440:
            dv_prep_dir, source_for_encode, dv_err = _prepare_dv_source(
                pr, args,
                keep_langs=keep_langs,
                target_container=target_container,
            )
            if source_for_encode is None:
                # dv_err is None → no strategy applies (P5, or P7 without
                # convert tools); mark skipped with a policy code.
                # dv_err set → strategy crashed at runtime; mark failed
                # with the captured error so the user sees the real cause.
                if dv_err is None:
                    db.mark_decision(
                        dec["id"], "skipped",
                        error="dv_no_prep_strategy", run_id=run_id,
                        expected_path=pr.path,
                    )
                    return "skipped", 0
                db.mark_decision(
                    dec["id"], "failed",
                    error=dv_err, run_id=run_id,
                    expected_path=pr.path,
                )
                return "failed", 0
            encode_probe = probe.probe_file(Path(source_for_encode))
            dv_pre_pass_ran = True

        cmd, desc = _build_apply_command(
            dec, encode_probe, output_path, target_container,
            enc_name, keep_langs, args,
            source_override=source_for_encode,
            dv_pre_pass=dv_pre_pass_ran,
        )

        label = f"[{idx}/{total}] {Path(pr.path).name}: "
        status, saved = _execute_encode(
            db, dec, pr, cmd, desc, output_path, args, label,
            encode_probe=encode_probe,
        )
        if status != "bloat_retry":
            return status, saved

        # Bloat fallback: rebuild argv at the tier's relaxed CQ + slower
        # encoder preset, retry once. _execute_encode already deleted the
        # bloated output. Originals are restored after so the next file
        # starts from the preset defaults. See NOTES.md#preset-rationale.
        relax = bloat_relax_for_height(encode_probe.height)
        original_cq = getattr(args, "quality", None)
        original_encoder_preset = getattr(args, "encoder_preset", None)
        args._cq_retried = True  # noqa: SLF001
        args.quality = relax["cq"]
        args.encoder_preset = relax["encoder_preset"]
        try:
            retry_cmd, retry_desc = _build_apply_command(
                dec, encode_probe, output_path, target_container,
                enc_name, keep_langs, args,
                source_override=source_for_encode,
                dv_pre_pass=dv_pre_pass_ran,
            )
            return _execute_encode(
                db, dec, pr, retry_cmd, retry_desc, output_path, args, label,
                encode_probe=encode_probe,
            )
        finally:
            args.quality = original_cq
            args.encoder_preset = original_encoder_preset
            args._cq_retried = False  # noqa: SLF001
    finally:
        if dv_prep_dir is not None:
            shutil.rmtree(dv_prep_dir, ignore_errors=True)


def _prepare_dv_source(
    pr: ProbeResult,
    args: argparse.Namespace,
    *,
    keep_langs: list[str] | None = None,
    target_container: str = "mkv",
) -> tuple[Path | None, str | None, str | None]:
    """Run the appropriate DV pre-stage; return (work_dir, prepared_path, error).

    `keep_langs` + `target_container` flow into the strip's demuxer
    pre-trim so unwanted audio/sub streams drop during the strip rather
    than after. See NOTES.md#demuxer-pre-trim-during-strip.

    Three-tuple disambiguates the failure modes:
      * (None, None, None) — no strategy applies (P5, or P7 without
        convert tools). Caller marks 'skipped'.
      * (None, None, err_msg) — strategy ran but the subprocess crashed.
        Caller marks 'failed' so the real cause surfaces.
      * (work_dir, path, None) — success. Caller `rmtree`s in finally.

    work_dir lives next to the source so the ~50 GB temp stays on the
    same filesystem (no slow NAS link traversal, no /tmp fill).
    """
    strategy = encoder.dv_strategy(
        pr.dv_profile,
        allow_p7_convert=getattr(args, "dv_p7_convert", False),
    )
    if strategy is None:
        return None, None, None

    src = Path(pr.path)
    work_dir = Path(tempfile.mkdtemp(
        prefix=".vo_dv_prep_", dir=str(src.parent),
    ))
    prepared = work_dir / f"{src.stem}.dv-prepped.mkv"

    if strategy == "p8_strip":
        print(f"    DV Profile {pr.dv_profile}: stripping RPU "
              f"(temp file: {prepared.name})")
        cmd = encoder.build_dv_strip_command(
            pr, prepared,
            keep_langs=keep_langs,
            target_container=target_container,
            add_compat_audio=getattr(args, "compat_audio", True),
            original_audio=getattr(args, "original_audio", False),
            original_subs=getattr(args, "original_subs", False),
        )
        ok, err = _run_encode_ffmpeg(cmd, pr, args, label="    DV-strip ")
        if not ok:
            shutil.rmtree(work_dir, ignore_errors=True)
            print(f"    FAIL: DV strip failed: {err[:200]}")
            return None, None, f"dv_strip_failed: {err[:800]}"
        return work_dir, str(prepared), None

    if strategy == "p7_convert":
        ok = _run_dv_p7_pipeline(pr, work_dir, prepared, args)
        if not ok:
            shutil.rmtree(work_dir, ignore_errors=True)
            return None, None, "dv_p7_pipeline_failed"
        return work_dir, str(prepared), None

    # Unknown strategy string — defensive fallback (would indicate a
    # new strategy added to dv_strategy() without updating this dispatch).
    shutil.rmtree(work_dir, ignore_errors=True)
    return None, None, f"dv_unknown_strategy: {strategy}"


def _run_dv_p7_pipeline(pr: ProbeResult, work_dir: Path,
                       prepared: Path,
                       args: argparse.Namespace) -> bool:
    """Run the 4-stage Profile 7 prep: extract → P7→P8 convert → strip → mkvmerge.

    Stages 1+2 are piped (ffmpeg stdout → dovi_tool stdin) so we
    don't write a 50 GB intermediate Annex-B HEVC. Stage 3 strips
    the RPU on the raw HEVC bitstream (output is also raw HEVC, so
    ffmpeg's matroska timestamp issue doesn't apply). Stage 4 uses
    mkvmerge to re-attach audio/subs from the original source —
    mkvmerge handles raw HEVC + B-frame content cleanly where
    ffmpeg's matroska muxer fails with "Timestamps are unset in a
    packet for stream 0".

    Returns True on success, False on any subprocess failure
    (caller cleans up the work_dir).
    """
    print(f"    DV Profile 7: P7→P8 + strip RPU + mkvmerge "
          f"(temp dir: {work_dir.name})")
    p8_hevc = work_dir / "p8.hevc"
    stripped_hevc = work_dir / "stripped.hevc"

    # Stage 1+2: piped extract → convert.
    extract_proc = subprocess.Popen(
        encoder.build_dv_p7_extract_command(pr),
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL if not args.verbose else None,
    )
    try:
        convert_proc = subprocess.run(
            encoder.build_dv_p7_convert_command(p8_hevc),
            stdin=extract_proc.stdout,
            stderr=subprocess.PIPE if not args.verbose else None,
        )
    finally:
        if extract_proc.stdout is not None:
            extract_proc.stdout.close()
        extract_proc.wait()

    if extract_proc.returncode != 0:
        print(f"    FAIL: DV P7 extract (ffmpeg) returned "
              f"{extract_proc.returncode}")
        return False
    if convert_proc.returncode != 0:
        err = (convert_proc.stderr or b"").decode(
            "utf-8", errors="replace")[:400]
        print(f"    FAIL: dovi_tool convert returned "
              f"{convert_proc.returncode}\n      {err}")
        return False
    if not p8_hevc.exists() or p8_hevc.stat().st_size == 0:
        print("    FAIL: dovi_tool produced no output")
        return False

    # Stage 3: strip RPU on raw HEVC → raw HEVC (no container
    # involved, so the matroska timestamp issue doesn't apply).
    ok, err = _run_encode_ffmpeg(
        encoder.build_dv_p7_strip_raw_command(p8_hevc, stripped_hevc),
        pr, args, label="    DV-P7-strip ",
    )
    if not ok:
        print(f"    FAIL: DV P7 strip failed: {err[:200]}")
        return False

    # Free the unstripped intermediate before stage 4 runs — keeps
    # peak temp disk to ~2x source rather than 3x.
    p8_hevc.unlink(missing_ok=True)

    # Stage 4: mkvmerge muxes stripped HEVC + original audio/subs.
    mkvmerge_proc = subprocess.run(
        encoder.build_dv_p7_mkvmerge_command(pr, stripped_hevc, prepared),
        capture_output=True, text=True,
    )
    if mkvmerge_proc.returncode != 0:
        # mkvmerge returns 1 for warnings, 2 for errors. Treat 2 as
        # failure; warnings (1) plus a present output file are fine.
        if mkvmerge_proc.returncode >= 2 or not prepared.exists():
            err = (mkvmerge_proc.stderr or
                   mkvmerge_proc.stdout or "").strip()[:400]
            print(f"    FAIL: mkvmerge returned "
                  f"{mkvmerge_proc.returncode}\n      {err}")
            return False
    return True


def _print_decision_header(dec: dict, pr: ProbeResult, idx: int, total: int) -> None:
    """Print the `[idx/total] path / rules / projected savings` block."""
    print(f"\n[{idx}/{total}] {pr.path}")
    print(f"    rules: {dec['rules_fired_json']}  target: {dec['target']}")
    print(f"    projected savings: "
          f"{(dec['projected_savings_mb'] or 0) / 1024:.1f} GB")


def _should_apply_denoise(pr: ProbeResult) -> bool:
    """Soften h.264 macroblock noise before AV1 sees it.

    Fires for SD (height < 720; heavy compression is the norm) and for
    HD h.264 below the AV1 target bitrate for its bucket (source is
    already showing artifacts). hqdn3d is CPU-only — callers must
    pair with hw_decode=False.
    """
    height = pr.height or 0
    if 0 < height < 720:
        return True
    codec = (pr.video_codec or "").lower()
    if codec != "h264":
        return False
    if height >= 1440 or pr.video_bitrate <= 0:
        return False
    bucket = pr.resolution_class
    entry = BITRATE_FLAG_TABLE.get(bucket)
    if entry is None:
        return False
    target_mbps, _flag_mbps = entry
    actual_mbps = pr.video_bitrate / 1_000_000.0
    return actual_mbps < target_mbps


def _build_apply_command(dec: dict, pr: ProbeResult, output_path: Path,
                         target_container: str, enc_name: str,
                         keep_langs: list[str],
                         args: argparse.Namespace,
                         *,
                         source_override: str | None = None,
                         dv_pre_pass: bool = False,
                         ) -> tuple[list[str], str]:
    """Pick remux vs encode and build the corresponding ffmpeg argv.

    `source_override` swaps the `-i` source path while keeping all
    probe-derived stream layout decisions intact (used by the DV
    strip pre-pass — the stripped temp replaces the source).

    `dv_pre_pass` is presentation-only: controls whether the descriptor
    advertises "+ DV strip pre-pass". Caller sets True iff the strip
    actually ran. See NOTES.md#log-descriptor-format.
    """
    add_compat = getattr(args, "compat_audio", True)
    original_audio = bool(getattr(args, "original_audio", False))
    original_subs = bool(getattr(args, "original_subs", False))
    if _is_remux_only_decision(dec, pr):
        cmd = encoder.build_remux_command(pr, output_path,
                                          target_container, keep_langs,
                                          add_compat_audio=add_compat,
                                          original_audio=original_audio,
                                          original_subs=original_subs,
                                          source_override=source_override)
        return cmd, "remux"
    denoise = _should_apply_denoise(pr)
    hw_decode = bool(getattr(args, "hw_decode", False))
    # Codec-aware upward override of hw_decode. See NOTES.md#codec-aware-override.
    # Guarded against denoise: hqdn3d can't ride a QSV-surface chain.
    # Today denoise never co-occurs with this override (denoise gates fire
    # on H.264/SD only); the guard keeps the contract explicit.
    if not denoise and (pr.video_codec or "").lower() in SLOW_CPU_DECODE_CODECS:
        hw_decode = True
    cmd = encoder.build_encode_command(
        pr, output_path, enc_name, args.quality, keep_langs,
        target_container, hw_decode=hw_decode,
        add_compat_audio=add_compat,
        denoise=denoise,
        original_audio=original_audio,
        original_subs=original_subs,
        source_override=source_override,
        encoder_preset=getattr(args, "encoder_preset", None),
        qsv_overrides=getattr(args, "qsv_overrides", None),
    )
    # Pipeline-shaped descriptor (see NOTES.md#log-descriptor-format).
    src_codec = (pr.video_codec or "?").lower()
    decode_path = "QSV" if hw_decode else "CPU"
    desc = f"decode {src_codec} ({decode_path}) → encode via {enc_name}"
    if denoise:
        desc += " (+ denoise pre-pass)"
    if original_audio:
        desc += " (+ original audio passthrough)"
    if original_subs:
        desc += " (+ original subs passthrough)"
    if dv_pre_pass:
        desc += " (+ DV strip pre-pass)"
    return cmd, desc


def _execute_encode(db: Database, dec: dict, pr: ProbeResult,
                    cmd: list[str], desc: str, output_path: Path,
                    args: argparse.Namespace,
                    label: str = "",
                    encode_probe: ProbeResult | None = None,
                    ) -> tuple[str, int]:
    """Run ffmpeg, finalise the output path, update the decision row.

    `encode_probe` (when set) is the probe of the file ffmpeg actually
    reads — different from `pr` only when DV strip pre-pass produced a
    smaller intermediate (RPU stripped + alternate audio/sub streams
    pre-discarded). The bloat math compares output size against the
    encoder's *input* size (the stripped intermediate), not the
    original source — otherwise a heavy-multi-track source can pass
    the bloat check just because audio stripping shrank the
    denominator, masking the case where the encoder didn't actually
    compress the video. Falls back to `pr` when no DV prep ran.
    """
    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"    {desc} → {output_path}")
    if args.verbose:
        timeout = _resolve_timeout(args.timeout, pr.duration_seconds)
        timeout_label = "disabled" if timeout in (None, 0) else f"{timeout}s"
        print(f"    timeout: {timeout_label}")

    # The bloat math operates on the encoder's input file. When DV
    # strip ran, that's the stripped intermediate; otherwise it's the
    # original source.
    bloat_pr = encode_probe if encode_probe is not None else pr
    bloat_checker = _maybe_make_bloat_checker(bloat_pr, output_path, args)
    ok, err = _run_encode_ffmpeg(
        cmd, pr, args, label=label, bloat_checker=bloat_checker,
    )
    if not ok:
        # A mid-encode bloat trip looks like an ffmpeg failure here
        # because run_ffmpeg returned (False, "bloat_projection ..."),
        # but the caller wants the retry path, not a terminal failure.
        if encoder.BLOAT_PROJECTION_REASON in err:
            # flush=True is load-bearing for log-tailers: stdout is
            # block-buffered when redirected to a file, and the retry
            # fires immediately after — without flushing, the "bloat
            # detected" diagnostic sits in the buffer past the next
            # ffmpeg invocation.
            relax = bloat_relax_for_height(bloat_pr.height)
            print(f"    bloat detected mid-encode: {err}", flush=True)
            print(f"    will retry at CQ {relax['cq']} "
                  f"(encoder preset → {relax['encoder_preset']})",
                  flush=True)
            _unlink_partial_output(output_path)
            return "bloat_retry", 0
        print(f"    FAIL: {err}", flush=True)
        # Clean up partial output so re-runs don't trip on it.
        _unlink_partial_output(output_path)
        db.mark_decision(dec["id"], "failed", error=err[:1000],
                         run_id=getattr(args, "_apply_run_id", None),
                         expected_path=pr.path)
        return "failed", 0

    # Post-encode bloat check (HD/UHD, before _finalize_output so
    # replace mode hasn't recycled the source). Compare against
    # `bloat_pr` (stripped intermediate) when DV prep ran.
    if _should_retry_for_bloat(bloat_pr, output_path, args):
        relax = bloat_relax_for_height(bloat_pr.height)
        out_size = _safe_stat_size(output_path)
        ratio = out_size / bloat_pr.size if bloat_pr.size else 0
        print(f"    bloat detected: output {out_size / 1024 ** 3:.2f} GB "
              f"vs encoder input {bloat_pr.size / 1024 ** 3:.2f} GB "
              f"(ratio {ratio:.2f}); retrying at CQ {relax['cq']} "
              f"(encoder preset → {relax['encoder_preset']})",
              flush=True)
        _unlink_partial_output(output_path)
        return "bloat_retry", 0

    actual_mb = _finalize_output(pr, output_path, args, db, dec)
    return "applied", int(actual_mb * 1024 * 1024)


def _bloat_check_applies(pr: ProbeResult, args: argparse.Namespace) -> bool:
    """Shared precondition for both the mid-encode bloat checker and
    the post-encode bloat retry. Both call this; the post-encode path
    additionally checks the actual output-vs-source size ratio after
    these gates pass.

    Triggers when *all* of these are true:
      - Auto-relax is enabled (default; --no-auto-relax-cq disables).
      - We have not already retried this file (one retry cap).
      - The tier has a relaxed tuning (HD and UHD do; SD opts out — its
        outputs are too small for a doubled encode budget to be worth it).
      - The current CQ is tighter than the tier's relaxed CQ — otherwise
        the retry would re-run with the same (or looser) settings.
      - We know the source size (sanity for the projection math).
    """
    if not getattr(args, "auto_relax_cq", True):
        return False
    if getattr(args, "_cq_retried", False):
        return False
    relax = bloat_relax_for_height(pr.height)
    if relax is None:
        return False
    cur_cq = getattr(args, "quality", None)
    if cur_cq is None or cur_cq >= relax["cq"]:
        return False
    if not pr.size:
        return False
    return True


def _maybe_make_bloat_checker(pr: ProbeResult, output_path: Path,
                              args: argparse.Namespace,
                              ) -> "encoder._BloatChecker | None":
    """Build a mid-encode bloat checker if the gates apply, otherwise
    None. Same precondition as the post-encode `_should_retry_for_bloat`
    check but runs before the encode rather than after, so a doomed
    encode is killed early instead of running to completion."""
    if not _bloat_check_applies(pr, args):
        return None
    return encoder._BloatChecker(  # noqa: SLF001
        source_size=pr.size,
        output_path=output_path,
        threshold=BLOAT_RATIO_THRESHOLD,
        checkpoints=BLOAT_CHECKPOINTS,
    )


def _safe_stat_size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _run_encode_ffmpeg(
    cmd: list[str], pr: ProbeResult, args: argparse.Namespace,
    *, label: str = "",
    bloat_checker: "encoder._BloatChecker | None" = None,
) -> tuple[bool, str]:
    """Standard wrapper around `encoder.run_ffmpeg` for encode-class jobs.

    Centralises the boilerplate every encode-side ffmpeg invocation
    needs: timeout derived from `args.timeout` + source duration,
    `source_fps=pr.frame_rate` (so the progress display can fall back
    to the frame counter when the muxer's `out_time_ms` lags), and
    `verbose` plumbed from `args`. Without this wrapper it's easy to
    forget `source_fps` at a new callsite — the DV-strip path missed
    it for a while and produced bogus % / ETA on every DV source
    until that was caught.

    Use for encode and any encode-shaped helper (DV strip, P7 strip,
    main av1_qsv encode). Don't use for the small ffprobe-style
    invocations in `validate_output` etc. — those have their own
    short-timeout shape.
    """
    timeout = _resolve_timeout(args.timeout, pr.duration_seconds)
    return encoder.run_ffmpeg(
        cmd, pr.duration_seconds,
        timeout_seconds=timeout,
        verbose=args.verbose,
        label=label,
        source_fps=pr.frame_rate,
        bloat_checker=bloat_checker,
    )


def _unlink_partial_output(path: Path) -> None:
    """Remove a partial encode output, ignoring "missing" / "no permission" /
    "filesystem went away on the NAS" errors. Used wherever we throw away
    an in-progress output: bloat-retry deletes it before re-encoding;
    encode-failure cleanup deletes it before marking the row failed. None
    of those callers can do anything useful if unlink itself fails — and
    the next run's plan-gate will catch a stale orphan via the
    `existing_output` check anyway. Unconditional unlink + ignore
    OSError covers both "file missing" (FileNotFoundError) and
    "couldn't unlink" without a separate exists() pre-check."""
    try:
        path.unlink()
    except OSError:
        pass


def _should_retry_for_bloat(pr: ProbeResult, output_path: Path,
                            args: argparse.Namespace) -> bool:
    """Decide whether to throw the encode away and retry at relaxed CQ.

    Shared `_bloat_check_applies` gates plus the actual output-vs-source
    ratio check. The mid-encode `_BloatChecker` covers the same gates
    but takes a projection-based decision; this is the post-encode
    belt-and-braces check on the finished file size.
    """
    if not _bloat_check_applies(pr, args):
        return False
    return _safe_stat_size(output_path) >= pr.size * BLOAT_RATIO_THRESHOLD


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
          "codes from an encode.")

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


def _doctor_check_dv_tools() -> None:
    """Report dovi_tool / mkvmerge availability for the DV strip path.

    Both are needed for Profile 7 sources; only dovi_tool is needed-
    less (Profile 8.x uses ffmpeg's built-in dovi_rpu bsf). We don't
    flag missing DV tools as errors — the plan-gate falls back to
    skipping P7 sources cleanly when they're absent — just informational.
    """
    print("\nDolby Vision tools")
    print("==================")
    if encoder.has_dovi_tool():
        print("  dovi_tool: present (P7 → P8 conversion available)")
    else:
        print("  dovi_tool: absent")
        print("    (P7 sources will be skipped at plan time; P8 still works)")
    if encoder.has_mkvmerge():
        print("  mkvmerge:  present (P7 re-mux available)")
    else:
        print("  mkvmerge:  absent")
        print("    (install mkvtoolnix-cli to enable P7 prep)")


def _doctor_check_db(db_path: Path, issues: list[str]) -> None:
    print("\nDatabase")
    print("========")
    try:
        with Database(db_path) as db:
            files_n = db.conn.execute(
                "SELECT COUNT(*) FROM files").fetchone()[0]
        print(f"  {db_path}: ok")
        print(f"    {files_n} files cached")
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
    _doctor_check_dv_tools()
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
    """Return the recycle directory to use for `optimize --replace`.

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
) -> tuple[str, Path | None, Path, Path | None] | int:
    """Return (mode, output_root, source_root, recycle_to) or an exit code.

    mode is one of "keep", "side", "replace". Resolution order:
    explicit --mode wins; else --replace → replace; else --output → side;
    else default to keep.
    """
    if args.mode is not None:
        mode = args.mode
    elif args.replace:
        mode = "replace"
    elif args.output is not None:
        mode = "side"
    else:
        mode = "keep"

    if mode == "replace":
        # Single-file source: resolve recycle/source-root against the
        # parent directory so @Recycle lives next to siblings, not
        # inside an empty path-of-the-file context.
        anchor = args.path if args.path.is_dir() else args.path.parent
        return (mode, None, anchor,
                _resolve_recycle_dir(anchor, args.recycle_to))
    if mode == "side":
        if args.output is None:
            print("error: --mode side requires --output DIR", file=sys.stderr)
            return 2
        if args.recycle_to is not None:
            print("error: --recycle-to only applies to --mode replace",
                  file=sys.stderr)
            return 2
        return (mode, args.output, args.path, None)
    # keep
    if args.recycle_to is not None:
        print("error: --recycle-to only applies to --mode replace",
              file=sys.stderr)
        return 2
    if args.output is not None:
        print("error: --mode keep is incompatible with --output "
              "(keep writes alongside the source)", file=sys.stderr)
        return 2
    return (mode, None, args.path, None)


@dataclass
class RunConfig:
    """Encode settings for one preset, built once at the job boundary.

    Attribute names match the apply Namespace the encode helpers already
    read, so Dolby Vision, bloat-retry, and ``_finalize_output`` stay
    put. ``run_job`` does not build an apply Namespace.
    """

    auto: bool
    mode: str
    output_root: Path | None
    source_root: Path | None
    backup: Path | None
    recycle_to: Path | None
    allow_hard_delete: bool
    quality: int
    keep_langs: str
    hwaccel: str
    timeout: int | None
    hw_decode: bool
    compat_audio: bool
    original_audio: bool
    original_subs: bool
    no_dotted: bool
    name_suffix: str
    reencode_tag_value: str
    dry_run: bool
    verbose: bool
    target: str
    rewrite_codec: bool
    reencode_tag: bool
    encoder_preset: str | None
    qsv_overrides: dict[str, Any]
    auto_relax_cq: bool = True
    dv_p7_convert: bool = False
    _apply_run_id: int | None = None
    _cq_retried: bool = False


def _build_run_config(args: argparse.Namespace, preset_name: str,
                      mode: str, output_root: Path | None,
                      source_root: Path | None,
                      recycle_to: Path | None,
                      run_id: int) -> RunConfig:
    """Fill one preset's CQ and flags from PRESETS plus the CLI args."""
    cfg = PRESETS[preset_name]
    if args.quality is not None:
        quality = args.quality
    else:
        quality = cast(int, cfg["quality"])
    if args.keep_langs is not None:
        keep_langs = args.keep_langs
    else:
        keep_langs = cast(str, cfg["keep_langs"])
    if args.hw_decode is not None:
        hw_decode = bool(args.hw_decode)
    else:
        hw_decode = bool(cfg.get("hw_decode", False))
    raw_preset = cfg.get("encoder_preset")
    encoder_preset = raw_preset if isinstance(raw_preset, str) else None
    raw_qsv = cfg.get("qsv_overrides", {})
    qsv_overrides = raw_qsv if isinstance(raw_qsv, dict) else {}
    return RunConfig(
        auto=args.auto,
        mode=mode,
        output_root=output_root,
        source_root=source_root,
        backup=None,
        recycle_to=recycle_to,
        allow_hard_delete=False,
        quality=quality,
        keep_langs=keep_langs,
        hwaccel=args.hwaccel,
        timeout=getattr(args, "timeout", None),
        hw_decode=hw_decode,
        compat_audio=True,
        original_audio=bool(getattr(args, "original_audio", False)),
        original_subs=bool(getattr(args, "original_subs", False)),
        no_dotted=False,
        name_suffix="",
        reencode_tag_value="REENCODE",
        dry_run=bool(args.dry_run),
        verbose=bool(args.verbose),
        target=cast(str, cfg["target"]),
        rewrite_codec=bool(cfg["rewrite_codec"]),
        reencode_tag=bool(cfg["reencode_tag"]),
        encoder_preset=encoder_preset,
        qsv_overrides=qsv_overrides,
        auto_relax_cq=bool(getattr(args, "auto_relax_cq", True)),
        dv_p7_convert=bool(getattr(args, "dv_p7_convert", False)),
        _apply_run_id=run_id,
    )


def _preset_tiers(preset_name: str) -> frozenset[str]:
    """Tiers this preset encodes. Never infers UHD-FILM from the file.

    UHD-FILM is opt-in: only when the user named that preset, and only
    for files whose tier is UHD. Every other preset encodes its own tier.
    """
    if preset_name == "UHD-FILM":
        return frozenset({"UHD"})
    return frozenset({preset_name})


def _reject_bad_job_path(args: argparse.Namespace, label: str) -> int:
    """Return 2 if args.path is missing or not a file/directory."""
    if not args.path.exists():
        print(f"error: path not found: {args.path}", file=sys.stderr)
        return 2
    if not (args.path.is_dir() or args.path.is_file()):
        print(f"error: {label} expects a directory or a single video file: "
              f"{args.path}", file=sys.stderr)
        return 2
    return 0


def _job_scan_args(args: argparse.Namespace) -> argparse.Namespace:
    """Args the existing scan helpers read. Not a second probe pool."""
    return argparse.Namespace(
        path=args.path,
        no_recursive=False,
        no_probe_cache=False,
        min_size=getattr(args, "min_size", MIN_PROBE_SIZE_BYTES),
        allow_extras=getattr(args, "allow_extras", False),
        verbose=getattr(args, "verbose", False),
    )


def _scan_worker_count(args: argparse.Namespace) -> int:
    workers = getattr(args, "workers", None)
    if workers is None:
        workers = min(8, os.cpu_count() or 4)
    return max(1, workers)


def _scan_for_job(db: Database, args: argparse.Namespace) -> None:
    """Cache-aware probe of args.path. Does not write a runs row."""
    print(f"==> scan: probing {args.path} (cache hits skip ffprobe)...")
    scan_args = _job_scan_args(args)
    workers = _scan_worker_count(args)
    seen, cached, size_skipped, walk_errors, uncached = _scan_walk_phase(
        scan_args, db, False)
    probed, probe_errors = _scan_probe_phase(scan_args, db, uncached, workers)
    errors = walk_errors + probe_errors
    min_size = getattr(scan_args, "min_size", MIN_PROBE_SIZE_BYTES)
    skip_note = ""
    if size_skipped:
        skip_note = f", {size_skipped} skipped (< {_format_size(min_size)})"
    print(f"scan done: {seen} files seen, {probed} probed, "
          f"{cached} cached{skip_note}, {errors} errors")
    print()


def _decide_flags(args: argparse.Namespace) -> dict[str, Any]:
    skip_raw = getattr(args, "skip_codecs", "") or ""
    skip_codecs = frozenset(
        s.strip().lower() for s in skip_raw.split(",") if s.strip()
    )
    return {
        "allow_reencoded": bool(getattr(args, "allow_reencoded", False)),
        "allow_av1": bool(getattr(args, "allow_av1", False)),
        "allow_extras": bool(getattr(args, "allow_extras", False)),
        "allow_low_bitrate": bool(getattr(args, "allow_low_bitrate", False)),
        "skip_codecs": skip_codecs,
    }


def _scope_path(path: Path) -> Path:
    try:
        return path.resolve(strict=False)
    except OSError:
        return path


def _decisions_for_job(db: Database, args: argparse.Namespace,
                       ) -> list[Decision]:
    """Classify cached probes under args.path. Skips get no decision row."""
    scope = _scope_path(args.path)
    flags = _decide_flags(args)
    counts = {key: 0 for key, _template in _PLAN_SKIP_MESSAGES}
    encodes: list[Decision] = []
    for pr in list(db.iter_probes()):
        if not _path_under(pr.path, scope):
            counts["out_of_scope"] += 1
            continue
        bucket, decision = _classify_probe(db, pr, **flags)
        if bucket != "ok":
            counts[bucket] += 1
            continue
        if decision is not None and decision.kind == "encode":
            encodes.append(decision)
    if counts["missing"]:
        db.conn.commit()
    _emit_plan_skip_summary(counts)
    return encodes


def _sorted_encodes(decisions: list[Decision]) -> list[Decision]:
    """Biggest projected savings first. Stable for equal savings."""
    def savings(d: Decision) -> float:
        if d.candidate is None:
            return 0.0
        return d.candidate.total_projected_savings_mb
    return sorted(decisions, key=savings, reverse=True)


def _print_job_candidates(decisions: list[Decision]) -> None:
    cands: list[Candidate] = []
    for decision in decisions:
        if decision.candidate is not None:
            cands.append(decision.candidate)
    print(report.format_candidates_text(cands))
    print()


def _fired_rules_json(cand: Candidate) -> str:
    names = [v.rule for v in cand.fired if not _is_advisory(v.rule)]
    return json.dumps(names)


def _load_decision_row(db: Database, decision_id: int) -> dict:
    row = db.conn.execute(
        "SELECT * FROM decisions WHERE id = ?", (decision_id,),
    ).fetchone()
    return dict(row)


def _encode_job_file(db: Database, decision: Decision, cfg: RunConfig,
                     idx: int, total: int) -> tuple[str, int]:
    """Insert a running row (unless dry-run) and encode one candidate."""
    cand = decision.candidate
    if cand is None:
        return "skipped", 0
    pr = cand.probe
    rules_json = _fired_rules_json(cand)
    if cfg.dry_run:
        dec = {
            "id": None,
            "path": pr.path,
            "rules_fired_json": rules_json,
            "target": cand.target,
            "projected_savings_mb": cand.total_projected_savings_mb,
        }
    else:
        dec_id = db.insert_running_decision(
            pr.path, rules_json, cand.target,
            cand.total_projected_savings_mb,
            cast(int, cfg._apply_run_id),
        )
        dec = _load_decision_row(db, dec_id)
    _print_decision_header(dec, pr, idx, total)
    keep_langs = [s.strip() for s in cfg.keep_langs.split(",") if s.strip()]
    return _encode_one(db, dec, pr, cfg, keep_langs, idx, total)


def _print_preset_verbose(name: str, cfg: RunConfig) -> None:
    print(f"preset {name}: target={cfg.target}, quality={cfg.quality}, "
          f"keep_langs={cfg.keep_langs}, "
          f"rewrite_codec={cfg.rewrite_codec}, "
          f"reencode_tag={cfg.reencode_tag}, hw_decode={cfg.hw_decode}, "
          f"compat_audio={cfg.compat_audio}")


def _encode_preset(db: Database, args: argparse.Namespace,
                   ordered: list[Decision], preset_name: str,
                   mode: str, output_root: Path | None,
                   source_root: Path | None, recycle_to: Path | None,
                   run_id: int, limit: int) -> tuple[dict[str, int], int]:
    """Encode one preset's band, biggest savings first, at most limit."""
    preset = PRESETS[preset_name]
    print(f"==> encode: {preset_name} ({preset['label']})")
    cfg = _build_run_config(
        args, preset_name, mode, output_root, source_root, recycle_to, run_id,
    )
    if args.verbose:
        _print_preset_verbose(preset_name, cfg)
    band = [d for d in ordered if d.tier in _preset_tiers(preset_name)]
    if limit > 0:
        band = band[:limit]
    counts = {"applied": 0, "skipped": 0, "failed": 0, "dry_run": 0}
    saved = 0
    if not band:
        print(f"nothing to encode in {preset_name}.")
        print()
        return counts, saved
    for i, decision in enumerate(band, 1):
        status, n = _encode_job_file(db, decision, cfg, i, len(band))
        if status in counts:
            counts[status] += 1
        saved += n
    print()
    return counts, saved


def _encode_all_presets(db: Database, args: argparse.Namespace,
                        ordered: list[Decision],
                        presets_to_run: tuple[str, ...],
                        mode: str, output_root: Path | None,
                        source_root: Path | None, recycle_to: Path | None,
                        run_id: int) -> dict[str, int]:
    """Encode presets in order. ``--limit`` applies inside each preset."""
    totals = {"applied": 0, "skipped": 0, "failed": 0, "dry_run": 0}
    bytes_saved = 0
    limit = getattr(args, "limit", 0) or 0
    for preset_name in presets_to_run:
        counts, saved = _encode_preset(
            db, args, ordered, preset_name, mode, output_root,
            source_root, recycle_to, run_id, limit,
        )
        for key, value in counts.items():
            totals[key] += value
        bytes_saved += saved
    summary = {k: v for k, v in totals.items() if k != "dry_run"}
    summary["approx_bytes_saved"] = bytes_saved
    touched = sum(totals.values())
    if touched and not getattr(args, "no_report", False):
        _emit_run_report(db, run_id)
    print(f"\njob done: {totals['applied']} encoded, "
          f"{totals['skipped']} skipped, {totals['failed']} failed; "
          f"~{_format_bytes(bytes_saved)} saved")
    return summary


def run_job(args: argparse.Namespace,
            presets_to_run: tuple[str, ...],
            *, label: str) -> int:
    """Probe one path, decide, and encode. One runs row for the whole job.

    ``optimize`` passes ``("UHD", "HD", "SD")``. A preset passes
    ``(name,)``. Files whose tier is not in this job are left out — no
    pending row. ``--dry-run`` prints ffmpeg commands and writes no
    decision rows. ``--limit N`` is N files per preset.
    """
    _apply_bare_invocation_defaults(args)
    if getattr(args, "confirm", False):
        args.auto = False
    rc = _reject_bad_job_path(args, label)
    if rc != 0:
        return rc
    resolved = _optimize_resolve_paths(args)
    if isinstance(resolved, int):
        return resolved
    mode, output_root, source_root, recycle_to = resolved
    _print_pipeline_banner(args, label, mode, output_root, recycle_to)
    with Database(args.db) as db:
        run_id = db.start_run(label, str(args.path), _args_dict(args))
        _scan_for_job(db, args)
        encodes = _decisions_for_job(db, args)
        ordered = _sorted_encodes(encodes)
        _print_job_candidates(ordered)
        summary = _encode_all_presets(
            db, args, ordered, presets_to_run, mode, output_root,
            source_root, recycle_to, run_id,
        )
        db.end_run(run_id, summary)
    if getattr(args, "cleanup_after", False):
        _invoke_cleanup_after(args)
    return 0


def _run_path_pipeline(args: argparse.Namespace,
                       presets_to_run: tuple[str, ...],
                       *, label: str) -> int:
    """Wizard encode step. Path commands call ``run_job`` directly."""
    return run_job(args, presets_to_run, label=label)


def _apply_bare_invocation_defaults(args: argparse.Namespace) -> None:
    """Flip pipeline defaults so every path-taking subcommand is point-and-shoot.

    Applied unconditionally to every path-taking pipeline invocation
    (optimize, SD, HD, UHD, plus the bare-path rewrite). The user
    picked a path-taking subcommand → they want auto-yes encoding
    against the source they pointed at. --confirm opts back into
    per-file prompts; --output/--replace/--mode opts out of keep.

    The bare-invocation sentinel (`--bare-invocation`) additionally
    flips verbose on; that's the one place where we infer the user
    is brand-new and probably wants to see more output.
    """
    if not args.auto:
        args.auto = True
    if args.mode is None and not args.replace and args.output is None:
        args.mode = "keep"
    if getattr(args, "bare_invocation", False) and not args.verbose:
        args.verbose = True


def _print_pipeline_banner(
    args: argparse.Namespace,
    label: str,
    mode: str,
    output_root: Path | None,
    recycle_to: Path | None,
) -> None:
    """Print the `==> <label>:` header for a path-taking pipeline run."""
    print(f"==> {label}: {args.path}")
    if mode == "keep":
        print("    output mode: keep (alongside source; originals untouched)")
    elif mode == "replace":
        print("    output mode: replace (originals moved to recycle)")
        print(f"    recycle to:  {recycle_to}")
    else:
        print("    output mode: side (mirrored output tree)")
        print(f"    output root: {output_root}")
    if args.dry_run:
        print("    DRY RUN (no encodes will run)")
    print()


def cmd_optimize(args: argparse.Namespace) -> int:
    """Probe, decide, and encode UHD, then HD, then SD."""
    return run_job(args, ("UHD", "HD", "SD"), label="optimize")


def _invoke_cleanup_after(args: argparse.Namespace) -> None:
    """Chain `cmd_cleanup --apply` after a successful optimize run.

    Called when `--cleanup-after` is set and the apply phase returned 0.
    Resolves the target run id (most-recent run with completions) so
    the user-facing confirmation prompt can name it. Under `--auto`
    (which the bare invocation flips on) we skip the prompt — the user
    opted into chained cleanup already. Otherwise we ask once; on
    anything other than `y` we print the equivalent `cleanup` command
    so the user can run it deliberately later.
    """
    with Database(args.db) as db:
        target_run = db.latest_run_with_completions()
    if target_run is None:
        print("--cleanup-after: no completed encodes to clean up.")
        return

    # Count completions just so the prompt is informative.
    with Database(args.db) as db:
        decisions = [
            d for d in db.decisions_for_run(target_run)
            if d.get("status") == "completed"
        ]
    n = len(decisions)

    if not args.auto:
        try:
            ans = input(
                f"Run --cleanup-after will permanently delete {n} originals "
                f"from run #{target_run}. Continue? [y/N]: "
            ).strip().lower()
        except (EOFError, KeyboardInterrupt):
            print()
            ans = ""
        if not ans.startswith("y"):
            print(f"skipped cleanup; run "
                  f"'./video_optimizer.py cleanup --run {target_run} --apply' "
                  f"later if you change your mind")
            return

    cleanup_ns = argparse.Namespace(
        cmd="cleanup",
        run=None,
        apply=True,
        db=args.db,
    )
    cmd_cleanup(cleanup_ns)


def cmd_preset(args: argparse.Namespace) -> int:
    """Probe, decide, and encode one preset's tier.

    UHD-FILM encodes only files whose tier is UHD, and only because
    the user named that preset.
    """
    return run_job(args, (args.cmd,), label=args.cmd)


def cmd_status(args: argparse.Namespace) -> int:
    """Print recent run history."""
    with Database(args.db) as db:
        runs = db.recent_runs(limit=args.last)

    if not runs:
        print("no history yet.")
        return 0

    print(f"recent runs ({len(runs)}):")
    for r in runs:
        when = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(r["started_at"]))
        dur = (r["ended_at"] - r["started_at"]) if r["ended_at"] else 0
        summary = r["summary_json"] or "(no summary)"
        print(f"  {when}  {r['kind']:7s}  {dur:6.1f}s  {summary}")
    return 0


def cmd_replace_list(args: argparse.Namespace) -> int:
    """Print sources that hit the av1_qsv stall watchdog 2+ times.

    These are deterministic encoder failures: the bitstream pattern in the
    source triggers a libvpl AV1 hang that doesn't recover. Re-running
    won't help; the operator needs to grab a different release of the
    same title (or accept losing it from the archive backlog).

    ``decide`` skips these files — they are not encoded again. The
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


_REPORT_DIR = Path.home() / ".video_optimizer" / "reports"


def _emit_run_report(db: Database, run_id: int) -> None:
    """Print + persist the post-apply report keyed on `run_id`.

    Stdout always happens; persistence is best-effort and falls back to a
    one-line stderr warning if `~/.video_optimizer/reports/` isn't writable
    (read-only home, OSError on mkdir, etc.).
    """
    decisions = db.decisions_for_run(run_id)
    runs_row = db.get_run(run_id) or {"id": run_id}
    if not decisions:
        return
    stdout_text, persist_text = report.format_run_report(decisions, runs_row)
    print()
    print(stdout_text)

    try:
        _REPORT_DIR.mkdir(parents=True, exist_ok=True)
        report_path = _REPORT_DIR / f"run-{run_id}.txt"
        report_path.write_text(persist_text)
    except OSError as e:
        print(f"warning: could not persist run report: {e}", file=sys.stderr)


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

    if args.mode == "keep":
        # keep mode: write next to the source; originals stay put. The
        # collision-safety guarantee comes from --rewrite-codec +
        # --reencode-tag producing e.g. foo.AV1.REENCODE.mkv from foo.mkv.
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


def _finalize_replace_disposal(pr: ProbeResult, output_path: Path,
                               args: argparse.Namespace, db: Database,
                               dec: dict, actual_mb: float,
                               run_id: int | None) -> str | None:
    """Run the replace-mode disposal (recycle, backup, unlink original).

    Returns None on success or a status string ("recycled"/"backed-up"/etc.)
    when the disposal completed without further work; returns the string
    "done" when the caller should mark the decision and return without
    additional processing because the helper already wrote a partial-error
    completion row.
    """
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
                             error=f"recycle move failed: {e}",
                             run_id=run_id, expected_path=pr.path)
            return "done"
        # Original is now at `dst`; nothing more to delete.
        return None
    if args.backup:
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
                             error=f"backup failed: {e}",
                             run_id=run_id, expected_path=pr.path)
            return "done"
    # When --recycle-to is set the move above already removed the
    # original; otherwise unlink it now (after the optional backup copy).
    if Path(pr.path) != output_path:
        try:
            Path(pr.path).unlink()
        except OSError as e:
            db.mark_decision(dec["id"], "completed",
                             output_path=str(output_path),
                             actual_savings_mb=actual_mb,
                             error=f"original not removed: {e}",
                             run_id=run_id, expected_path=pr.path)
            return "done"
    return None


def _finalize_output(pr: ProbeResult, output_path: Path,
                     args: argparse.Namespace, db: Database,
                     dec: dict) -> float:
    """Compute savings, run backup-or-recycle + remove-original, update db.

    Post-encode validation guard: before any disposal step (recycle /
    backup / unlink) and before marking the row 'completed', ffprobe
    the output and confirm the encode actually produced a valid file
    matching the source's duration. A partial encode (ffmpeg exited
    cleanly but only wrote N seconds of an N+M-second source) gets
    marked 'failed' here — the original stays untouched and the
    cleanup step will never claim the source is safe to remove.
    """
    run_id = getattr(args, "_apply_run_id", None)
    valid, err = encoder.validate_output(pr, output_path)
    if not valid:
        print(f"    FAIL: output validation: {err}")
        db.mark_decision(dec["id"], "failed",
                         output_path=str(output_path),
                         error=f"validation: {err}",
                         run_id=run_id, expected_path=pr.path)
        return 0.0
    out_size = _safe_stat_size(output_path)
    actual_mb = (pr.size - out_size) / (1024 * 1024)

    if args.mode == "keep":
        # keep mode never touches the original — the whole point is that
        # the user (or a follow-up cleanup step) decides when to delete
        # them. Skip every disposal branch and record the success.
        db.mark_decision(dec["id"], "completed",
                         output_path=str(output_path),
                         actual_savings_mb=actual_mb,
                         run_id=run_id, expected_path=pr.path)
        return actual_mb

    if args.mode == "replace":
        outcome = _finalize_replace_disposal(pr, output_path, args, db,
                                             dec, actual_mb, run_id)
        if outcome == "done":
            return actual_mb

    db.mark_decision(dec["id"], "completed",
                     output_path=str(output_path),
                     actual_savings_mb=actual_mb,
                     run_id=run_id, expected_path=pr.path)
    return actual_mb


# --------------------------------------------------------------------------- #
# Entrypoint
# --------------------------------------------------------------------------- #


_FFMPEG_DEPENDENT_CMDS: frozenset[str] = frozenset({
    "scan", "reprobe", "list-encoders", "optimize",
})


# Single source of truth for argv preprocessing (see main()). Includes
# forward-declared subcommands (cleanup, wizard) so the dispatch logic
# doesn't need re-editing when their full handlers land.
KNOWN_SUBCOMMANDS: frozenset[str] = frozenset({
    "scan", "reprobe", "status",
    "list-encoders", "replace-list", "doctor",
    "optimize", "cleanup", "wizard", "rename-fix",
}) | frozenset(PRESETS.keys())


def _classify_cleanup_decision(dec: dict) -> tuple[str, int, str | None]:
    """Apply the cleanup 3-check guard to one completed decision.

    Returns ``(source_path, size_bytes, reason_or_None)``. A non-None
    reason means the decision is **not** cleanable and should be
    reported as SKIP; size_bytes is meaningful only when reason is None.

    The 3-check guard:
      (a) decisions.output_path exists on disk,
      (b) output is non-empty (stat().st_size > 0),
      (c) output_path != source_path (paranoid; keep mode makes them
          siblings, not the same).
    Plus a sanity check that the source still exists — a no-op cleanup
    on a vanished source is silent noise we'd rather log explicitly.
    """
    source_path = dec.get("path") or ""
    output_path = dec.get("output_path") or ""

    if not output_path:
        return source_path, 0, "no output_path recorded"
    # Guard (c) first — paranoid same-path check.
    if output_path == source_path:
        return source_path, 0, "output_path == source_path"
    out = Path(output_path)
    # Guard (a) — output must exist.
    if not out.exists():
        return source_path, 0, f"output missing: {output_path}"
    # Guard (b) — non-empty.
    try:
        out_size = out.stat().st_size
    except OSError as e:
        return source_path, 0, f"stat failed: {e}"
    if out_size <= 0:
        return source_path, 0, "output zero-byte"

    src = Path(source_path)
    if not src.exists():
        return source_path, 0, "source already removed"
    try:
        src_size = src.stat().st_size
    except OSError as e:
        return source_path, 0, f"source stat failed: {e}"
    return source_path, src_size, None


def _cleanup_apply_unlinks(cleanable: list[tuple[str, int]]) -> tuple[int, int]:
    """Unlink each cleanable source. Returns (removed_count, freed_bytes)."""
    removed = 0
    freed_bytes = 0
    for source_path, sz in cleanable:
        try:
            Path(source_path).unlink()
        except OSError as e:
            print(f"warning: could not remove {source_path}: {e}",
                  file=sys.stderr)
            continue
        removed += 1
        freed_bytes += sz
    return removed, freed_bytes


def cmd_cleanup(args: argparse.Namespace) -> int:
    """Remove originals of successfully-encoded files from a prior run.

    Default mode is dry-run: print which originals *would* be removed,
    sized. `--apply` actually `unlink()`s the source files in Python
    (not via shell `rm`) after the 3-check guard in
    `_classify_cleanup_decision`. Skipped sources are reported as SKIP
    and never unlinked. Frames the work in a `start_run`/`end_run`
    pair so `runs` captures the audit trail.
    """
    with Database(args.db) as db:
        run_id = args.run
        if run_id is None:
            run_id = db.latest_run_with_completions()
        if run_id is None:
            print("no completed encodes in run None "
                  "(or no run found); nothing to clean up")
            return 0

        cleanup_run_id = db.start_run(
            "cleanup", None,
            {"target_run": run_id, "apply": bool(args.apply)},
        )
        decisions = [
            d for d in db.decisions_for_run(run_id)
            if d.get("status") == "completed"
        ]
        if not decisions:
            print(f"no completed encodes in run {run_id} "
                  f"(or no run found); nothing to clean up")
            db.end_run(cleanup_run_id,
                       {"target_run": run_id, "removed": 0,
                        "freed_bytes": 0, "skipped": 0})
            return 0

        cleanable: list[tuple[str, int]] = []
        skipped: list[tuple[str, str]] = []
        for dec in decisions:
            source_path, sz, reason = _classify_cleanup_decision(dec)
            if reason is None:
                cleanable.append((source_path, sz))
            else:
                skipped.append((source_path, reason))

        for source_path, reason in skipped:
            print(f"SKIP {reason}  {source_path}")

        if not args.apply:
            total_bytes = sum(sz for _, sz in cleanable)
            for source_path, sz in cleanable:
                print(f"would remove  {_format_bytes(sz)}  {source_path}")
            print(
                f"summary: {len(cleanable)} cleanable, "
                f"{_format_bytes(total_bytes)} total. "
                f"Re-run with --apply to actually remove."
            )
            db.end_run(cleanup_run_id, {
                "target_run": run_id,
                "cleanable": len(cleanable),
                "skipped": len(skipped),
                "freed_bytes": 0,
                "dry_run": True,
            })
            return 0

        removed, freed_bytes = _cleanup_apply_unlinks(cleanable)
        print(f"removed {removed} originals, "
              f"freed {_format_bytes(freed_bytes)}")
        db.end_run(cleanup_run_id, {
            "target_run": run_id,
            "removed": removed,
            "freed_bytes": freed_bytes,
            "skipped": len(skipped),
        })
        return 0


class _WizardAbort(Exception):
    """Raised by `_prompt` on Ctrl-C / EOF to unwind cmd_wizard cleanly.

    The outer try/except in `cmd_wizard` translates this into a single-line
    "aborted" message + sys.exit(130), so the user never sees a traceback
    just for closing the terminal mid-prompt.
    """


def _prompt(prompt: str, default: str | None = None,
            choices: list[str] | None = None) -> str:
    """input() wrapper used by the wizard. Tests patch builtins.input.

    `default`: returned verbatim if the user just presses Enter.
    `choices`: lowercased; the user's answer (lowercased) must match one
    of them. On no-match, re-prompt with a "please choose: <list>" hint
    (no retry cap — the user keeps typing until they pick one).

    KeyboardInterrupt / EOFError are caught and re-raised as `_WizardAbort`
    so the outer wizard loop can convert them into a clean exit-130.
    """
    if choices is not None:
        lowered = [c.lower() for c in choices]
    else:
        lowered = None
    while True:
        try:
            answer = input(prompt)
        except (KeyboardInterrupt, EOFError) as exc:
            raise _WizardAbort from exc
        answer = answer.strip()
        if not answer and default is not None:
            return default
        if lowered is not None:
            if answer.lower() in lowered:
                return answer.lower()
            print(f"  please choose: {', '.join(choices or [])}")
            continue
        return answer


def _wizard_estimate_seconds(decisions: list[dict],
                             db: Database) -> tuple[int, int, int, int]:
    """Return (uhd, hd, sd, total_estimated_seconds) for `decisions`.

    Tier is decided by probe height against the SD/HD/UHD preset bounds
    (UHD ≥ 1440, HD 720..1439, SD < 720). Each tier's count multiplies
    its `EST_SECONDS_PER_FILE` entry into the total estimate.
    """
    uhd_count = hd_count = sd_count = total_seconds = 0
    uhd_min = PRESETS["UHD"].get("min_height", 1440) or 1440
    hd_min = PRESETS["HD"].get("min_height", 720) or 720
    for dec in decisions:
        pr = _load_probe_for_decision(db, dec)
        height = pr.height if pr is not None else 0
        if height >= uhd_min:
            uhd_count += 1
            total_seconds += EST_SECONDS_PER_FILE.get("UHD", 0)
        elif height >= hd_min:
            hd_count += 1
            total_seconds += EST_SECONDS_PER_FILE.get("HD", 0)
        else:
            sd_count += 1
            total_seconds += EST_SECONDS_PER_FILE.get("SD", 0)
    return uhd_count, hd_count, sd_count, total_seconds


def _format_hours(seconds: int) -> str:
    """Render a wall-clock estimate as ~Nh / ~Nm. Used by the wizard summary."""
    if seconds <= 0:
        return "~0m"
    if seconds < 3600:
        return f"~{max(1, seconds // 60)}m"
    hours = seconds / 3600.0
    if hours < 10:
        return f"~{hours:.1f}h"
    return f"~{int(round(hours))}h"


def _wizard_pick_path(args: argparse.Namespace) -> Path | None:  # noqa: ARG001
    """Prompt for the library path. Returns None if user gave an empty answer
    (treated as a clean exit), or a validated Path. Re-prompts up to 3 times
    on invalid paths before giving up via `_WizardAbort`."""
    for _ in range(3):
        raw = _prompt("Path to the directory you want to optimize: ",
                      default="")
        if not raw:
            return None
        candidate = Path(raw).expanduser()
        if candidate.is_dir():
            return candidate
        print(f"  not a directory: {candidate}")
    print("  too many invalid paths; aborting", file=sys.stderr)
    raise _WizardAbort


def _wizard_pick_mode(
    args: argparse.Namespace,  # noqa: ARG001
    library: Path,
) -> tuple[str, Path | None, Path | None]:
    """Prompt for the output mode. Returns (mode, output_root, recycle_to)."""
    print()
    print("Where should the encoded files go?")
    print("  [1] Next to the originals (originals untouched)              [default]")
    print("  [2] Mirror into a separate output directory")
    print("  [3] Replace originals (move them to a recycle directory)")
    choice = _prompt("Choice [1]: ", default="1", choices=["1", "2", "3"])
    if choice == "1":
        return ("keep", None, None)
    if choice == "2":
        raw = _prompt("  Output directory: ", default="")
        if not raw:
            print("  no output directory given; aborting", file=sys.stderr)
            raise _WizardAbort
        return ("side", Path(raw).expanduser(), None)
    # choice == "3"
    raw = _prompt("  Recycle directory (blank = auto-detect under library): ",
                  default="")
    recycle_to = Path(raw).expanduser() if raw else None
    return ("replace", None, recycle_to or _resolve_recycle_dir(library, None))


def _wizard_pick_tier() -> tuple[str, ...]:
    """Prompt for tier scope. Returns the preset tuple to feed the pipeline.

    The four options mirror the path-taking subcommand surface: "all"
    chains UHD → HD → SD (same as `optimize`); the single-tier choices
    apply just that preset (same as `UHD` / `HD` / `SD`).
    """
    print()
    print("Which resolution tier(s) should be re-encoded?")
    print("  [a] All tiers (UHD + HD + SD)                              [default]")
    print("  [u] UHD only (≥ 1440p)")
    print("  [f] UHD-FILM only (≥ 1440p, CQ 21 for grainy older film)")
    print("  [h] HD only (720–1439p)")
    print("  [s] SD only (< 720p)")
    choice = _prompt("Choice [a]: ", default="a",
                     choices=["a", "u", "f", "h", "s"])
    if choice == "u":
        return ("UHD",)
    if choice == "f":
        return ("UHD-FILM",)
    if choice == "h":
        return ("HD",)
    if choice == "s":
        return ("SD",)
    return ("UHD", "HD", "SD")


def _wizard_pick_skip_codecs() -> str:
    """Prompt for codecs to exclude from the plan. Returns the comma-separated value.

    Default: nothing skipped — the bitrate-floor gate already filters
    sources too low-bitrate to be worth re-encoding, so opting out by
    codec is a power-user choice (e.g. "I trust HEVC, leave it alone").
    """
    print()
    print("Are there any source codecs you want to leave alone?")
    print("  [n] No — re-encode every non-AV1 source                       [default]")
    print("  [hevc] Skip HEVC sources")
    print("  [h264] Skip h.264 sources")
    print("  [other] Enter a custom comma-separated list "
          "(e.g. `hevc,vp9`)")
    choice = _prompt("Choice [n]: ", default="n",
                     choices=["n", "hevc", "h264", "other"])
    if choice == "n":
        return ""
    if choice in ("hevc", "h264"):
        return choice
    raw = _prompt("  Codecs to skip (comma-separated, ffprobe names "
                  "e.g. `hevc,vp9,mpeg2video`): ", default="")
    return raw.strip()


def _wizard_apply_namespace(
    args: argparse.Namespace,
    library: Path,
    mode: str,
    output_root: Path | None,
    recycle_to: Path | None,
    limit: int,
    skip_codecs: str = "",
) -> argparse.Namespace:
    """Build the Namespace cmd_optimize hands to its preset router.

    Mirrors `_optimize_run_apply`'s shape (which is the proven recipe for
    chaining the UHD + HD presets through one queue) but lets the wizard
    inject its own `limit` and `mode`-derived paths.
    """
    return argparse.Namespace(
        path=library,
        auto=True,
        mode=mode,
        output=output_root,
        replace=(mode == "replace"),
        recycle_to=recycle_to,
        limit=limit,
        dry_run=False,
        confirm=False,
        cleanup_after=False,
        verbose=True,
        workers=8,
        keep_langs=None,
        hwaccel="auto",
        hw_decode=None,
        quality=None,
        min_size=MIN_PROBE_SIZE_BYTES,
        db=args.db,
        bare_invocation=False,
        skip_codecs=skip_codecs,
        allow_low_bitrate=False,
    )


def _wizard_doctor_preflight(args: argparse.Namespace) -> bool:
    """Run cmd_doctor; return True iff the wizard should proceed."""
    print("==> doctor: preflight checks")
    doctor_ns = argparse.Namespace(probe=None, db=args.db)
    rc = cmd_doctor(doctor_ns)
    if rc == 0:
        return True
    ans = _prompt("doctor reported issues. continue anyway? [y/N]: ",
                  default="n", choices=["y", "n"])
    return ans == "y"


def _wizard_preview_args(args: argparse.Namespace, library: Path,
                         skip_codecs: str) -> argparse.Namespace:
    """Args for the wizard's in-memory preview. Same gates as the encode."""
    return argparse.Namespace(
        path=library,
        min_size=MIN_PROBE_SIZE_BYTES,
        allow_extras=False,
        allow_av1=False,
        allow_low_bitrate=False,
        verbose=False,
        workers=None,
        skip_codecs=skip_codecs,
        db=args.db,
    )


def _rows_for_limit_prompt(decisions: list[Decision]) -> list[dict]:
    """Shape encode decisions for the limit prompt. Not written to the db."""
    rows: list[dict] = []
    for decision in decisions:
        cand = decision.candidate
        if cand is None:
            continue
        rows.append({
            "path": cand.probe.path,
            "projected_savings_mb": cand.total_projected_savings_mb,
            "target": cand.target,
        })
    return rows


def _wizard_preview(args: argparse.Namespace, library: Path,
                    skip_codecs: str) -> list[dict]:
    """Probe and decide for the summary. Writes no decision rows.

    ``run_job`` probes the same tree again. Cache hits skip ffprobe.
    Files the user does not encode are not stored.
    """
    preview_args = _wizard_preview_args(args, library, skip_codecs)
    with Database(args.db) as db:
        _scan_for_job(db, preview_args)
        encodes = _sorted_encodes(_decisions_for_job(db, preview_args))
    _print_job_candidates(encodes)
    return _rows_for_limit_prompt(encodes)


def _wizard_choose_limit(args: argparse.Namespace, library: Path,
                         skip_codecs: str) -> int | None:
    """Show candidates and ask for a limit. None means do not encode."""
    rows = _wizard_preview(args, library, skip_codecs)
    if not rows:
        print()
        print("no candidates found. nothing to do.")
        return None
    with Database(args.db) as db:
        return _wizard_pick_limit(rows, db)


def _wizard_pick_limit(candidates: list[dict], db: Database) -> int | None:
    """Print the candidate summary and ask how many files to encode.

    ``candidates`` are in-memory rows (path, projected savings). They
    are not pending decisions. Returns the limit (0 == all), or None
    if the user chose to quit.
    """
    uhd, hd, sd, est_seconds = _wizard_estimate_seconds(candidates, db)
    uhd_hours = _format_hours(uhd * EST_SECONDS_PER_FILE["UHD"])
    hd_hours = _format_hours(hd * EST_SECONDS_PER_FILE["HD"])
    sd_hours = _format_hours(sd * EST_SECONDS_PER_FILE["SD"])
    print()
    print(f"Found {len(candidates)} candidate(s): "
          f"{uhd} UHD ({uhd_hours}), {hd} HD ({hd_hours}), "
          f"{sd} SD ({sd_hours})")
    print(f"Estimated total time: {_format_hours(est_seconds)} on "
          "Intel Battlemage iGPU; your hardware may vary.")
    print()
    print("Encode all of them, or just the first N?")
    print("  [a] All of them                                              "
          "[default]")
    print("  [n] First N (you'll be asked how many)")
    print("  [q] Quit without encoding")
    choice = _prompt("Choice [a]: ", default="a", choices=["a", "n", "q"])
    if choice == "q":
        return None
    if choice == "a":
        return 0
    while True:
        raw = _prompt("How many? ", default="")
        try:
            limit = int(raw)
        except ValueError:
            print("  please enter an integer")
            continue
        if limit <= 0:
            print("  please enter a positive integer")
            continue
        return limit


def _wizard_run_cleanup_prompt(args: argparse.Namespace) -> None:
    """After apply, offer to remove originals if ≥1 file was encoded."""
    with Database(args.db) as db:
        recent = db.recent_runs(limit=1)
    encoded = 0
    saved_bytes = 0
    if recent and recent[0].get("summary_json"):
        try:
            summary = json.loads(recent[0]["summary_json"])
        except (TypeError, ValueError):
            summary = {}
        encoded = int(summary.get("applied", 0) or 0)
        saved_bytes = int(summary.get("approx_bytes_saved", 0) or 0)
    if encoded < 1:
        return
    print()
    print(f"Run complete. {encoded} original(s) can be removed "
          f"(saved {_format_bytes(saved_bytes)} total).")
    ans = _prompt("Remove the originals now? [y/N]: ",
                  default="n", choices=["y", "n"])
    if ans == "y":
        cmd_cleanup(argparse.Namespace(run=None, apply=True, db=args.db))
    else:
        print("keep 'em. Run "
              "'./video_optimizer.py cleanup --run M --apply' "
              "later when you're ready.")


_REENCODE_STEM_MARKER = re.compile(r"(?<![A-Za-z0-9])REENCODE(?![A-Za-z0-9])",
                                   re.IGNORECASE)


def _has_reencode_marker(stem: str) -> bool:
    """True if the stem carries the tool's `REENCODE` token as a standalone word."""
    return bool(_REENCODE_STEM_MARKER.search(stem))


def _plan_rename_group(video: Path) -> tuple[str, list[Path]] | None:
    """For a video file already tagged REENCODE, compute its canonical stem.

    Returns (new_stem, siblings) where siblings is every file in the same
    directory whose stem == video.stem (the .mkv plus any .nfo / .srt /
    etc.). Returns None if the stem is already canonical.
    """
    new_stem = naming.rewrite_codec_tokens(video.stem, "av1", dotted=True)
    if new_stem == video.stem:
        return None
    siblings = [p for p in video.parent.iterdir()
                if p.is_file() and p.stem == video.stem]
    return new_stem, siblings


def _collect_rename_plans(
    root: Path,
) -> tuple[list[tuple[Path, Path, list[Path]]], list[tuple[Path, Path]]]:
    """Walk `root` and return (planned, skipped_collisions).

    planned holds (old_video_path, new_video_path, siblings); siblings
    is every sibling sharing the old stem so sidecars travel with the
    video. skipped_collisions holds (old, new) pairs where the target
    name already exists and the rename would clobber.
    """
    skip_dirs = crawler._SKIP_DIRS  # noqa: SLF001 — maintenance-only command
    planned: list[tuple[Path, Path, list[Path]]] = []
    collisions: list[tuple[Path, Path]] = []
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in skip_dirs]
        d = Path(dirpath)
        for fn in filenames:
            fp = d / fn
            if fp.suffix.lower() not in crawler.SUPPORTED_EXTENSIONS:
                continue
            if not _has_reencode_marker(fp.stem):
                continue
            plan = _plan_rename_group(fp)
            if plan is None:
                continue
            new_stem, siblings = plan
            new_video = fp.with_name(f"{new_stem}{fp.suffix}")
            if new_video.exists() and new_video != fp:
                collisions.append((fp, new_video))
                continue
            planned.append((fp, new_video, siblings))
    return planned, collisions


def _print_rename_plan(
    root: Path,
    planned: list[tuple[Path, Path, list[Path]]],
    collisions: list[tuple[Path, Path]],
) -> None:
    print(f"{len(planned)} file(s) to rename under {root}:\n")
    for old_video, new_video, siblings in planned:
        print(f"  {old_video.name}")
        print(f"  → {new_video.name}")
        extras = [s for s in siblings
                  if s.suffix.lower() != old_video.suffix.lower()]
        for s in extras:
            print(f"    + sidecar: {s.name} → {new_video.stem}{s.suffix}")
        print()
    if collisions:
        print(f"{len(collisions)} file(s) skipped — target already exists:")
        for old, new in collisions:
            print(f"  {old.name}\n    ↛ {new.name} (exists)")
        print()


def _apply_rename_plan(
    planned: list[tuple[Path, Path, list[Path]]],
) -> tuple[int, list[tuple[Path, str]]]:
    renamed = 0
    failed: list[tuple[Path, str]] = []
    for _old_video, new_video, siblings in planned:
        new_stem = new_video.stem
        for s in siblings:
            target = s.with_name(f"{new_stem}{s.suffix}")
            if target.exists() and target != s:
                failed.append((s, f"target exists: {target.name}"))
                continue
            try:
                s.rename(target)
                renamed += 1
            except OSError as exc:
                failed.append((s, str(exc)))
    return renamed, failed


def cmd_rename_fix(args: argparse.Namespace) -> int:
    """Rename existing .REENCODE outputs through the current av1 naming rules.

    Walks `args.path` for video files whose stem carries the `REENCODE`
    token (the marker every preset-driven encode appends), runs each
    stem through `naming.rewrite_codec_tokens(stem, 'av1')`, and renames
    when the result differs. Same-stem sidecars (.nfo, .srt, etc.) move
    with the video so Radarr/Sonarr metadata stays paired. Dry-run by
    default; `--apply` performs the rename via `Path.rename`. Skips any
    rename whose target already exists.
    """
    root = args.path
    if not root.is_dir():
        print(f"path is not a directory: {root}", file=sys.stderr)
        return 2

    planned, collisions = _collect_rename_plans(root)

    if not planned and not collisions:
        print(f"no .REENCODE files in {root} need renaming")
        return 0

    _print_rename_plan(root, planned, collisions)

    if not args.apply:
        print("dry run; pass --apply to perform the renames")
        return 0

    renamed, failed = _apply_rename_plan(planned)
    print(f"renamed {renamed} file(s)")
    if failed:
        print(f"{len(failed)} failure(s):")
        for path, why in failed:
            print(f"  {path.name}: {why}")
        return 1
    return 0


def cmd_wizard(args: argparse.Namespace) -> int:
    """Interactive prompt-based workflow.

    Composes ``cmd_doctor``, an in-memory probe and decide preview, then
    ``run_job`` (via ``_run_path_pipeline``), and ``cmd_cleanup``. The
    preview writes no decision rows. Encoding stores only files the
    chosen presets actually encode.
    """
    try:
        if not _wizard_doctor_preflight(args):
            return 0
        print()

        library = _wizard_pick_path(args)
        if library is None:
            return 0

        mode, output_root, recycle_to = _wizard_pick_mode(args, library)

        presets_to_run = _wizard_pick_tier()

        skip_codecs = _wizard_pick_skip_codecs()

        limit = _wizard_choose_limit(args, library, skip_codecs)
        if limit is None:
            return 0

        print()
        if _prompt("Proceed? [Y/n]: ",
                   default="y", choices=["y", "n"]) != "y":
            print("aborted by user; nothing encoded.")
            return 0

        apply_ns = _wizard_apply_namespace(
            args, library, mode, output_root, recycle_to, limit,
            skip_codecs=skip_codecs,
        )
        label = "optimize" if len(presets_to_run) > 1 else presets_to_run[0]
        rc = _run_path_pipeline(apply_ns, presets_to_run, label=label)
        _wizard_run_cleanup_prompt(args)
    except (KeyboardInterrupt, EOFError, _WizardAbort):
        print("\naborted", file=sys.stderr)
        sys.exit(130)
    return rc


def _preprocess_argv(argv: list[str]) -> list[str]:
    """Rewrite a bare `<path>` invocation to `optimize <path> --bare-invocation`.

    Predicate, evaluated in order (first match wins):
      - argv has zero positional args + stdin/stdout TTY → rewrite to wizard.
      - argv has zero positional args (no TTY) → no rewrite (top-level help).
      - argv[1] is -h / --help         → no rewrite.
      - argv[1] starts with `-`        → no rewrite (let argparse handle).
      - argv[1] is a known subcommand  → no rewrite.
      - otherwise                      → rewrite to optimize-with-sentinel.
    """
    if len(argv) <= 1:
        # Bare invocation with no args. If we're attached to a real terminal
        # on both ends, drop into the interactive wizard; otherwise fall
        # through to argparse's "subcommand required" error / top-level help
        # so cron / piped contexts stay non-interactive.
        if sys.stdin.isatty() and sys.stdout.isatty():
            return [argv[0], "wizard"]
        return argv
    first = argv[1]
    if first in {"-h", "--help"}:
        return argv
    if first.startswith("-"):
        return argv
    if first in KNOWN_SUBCOMMANDS:
        return argv
    # Typo-of-subcommand check: close-match + no-such-path → "did you
    # mean" instead of the misleading argparse error from the
    # bare-invocation rewrite path.
    if not Path(first).exists():
        suggestions = difflib.get_close_matches(
            first, KNOWN_SUBCOMMANDS, n=1, cutoff=0.7,
        )
        if suggestions:
            print(
                f"video_optimizer: '{first}' is not a known subcommand. "
                f"Did you mean '{suggestions[0]}'?",
                file=sys.stderr,
            )
            sys.exit(2)
    # Preserve argv[0] (prog name) so the caller's argv[1:] slice still works.
    return [argv[0], "optimize", first, *argv[2:], "--bare-invocation"]


def _assert_external_tools_available(cmd: str) -> None:
    """Exit with a clear error if a subcommand needs ffmpeg/ffprobe but they
    aren't on PATH. status and replace-list work purely against the
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
    # Force line-buffering so flush=True progress prints don't jump
    # ahead of the un-flushed plan listing under redirect.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except (AttributeError, OSError):
        pass
    if argv is None:
        argv = sys.argv
    argv = _preprocess_argv(argv)
    parser = _build_parser()
    args = parser.parse_args(argv[1:])
    _assert_external_tools_available(args.cmd)

    handlers = {
        "scan": cmd_scan,
        "reprobe": cmd_reprobe,
        "status": cmd_status,
        "list-encoders": cmd_list_encoders,
        "replace-list": cmd_replace_list,
        "doctor": cmd_doctor,
        "optimize": cmd_optimize,
        "cleanup": cmd_cleanup,
        "wizard": cmd_wizard,
        "rename-fix": cmd_rename_fix,
    }
    # Preset subcommands all dispatch to the same wrapper.
    for preset_name in PRESETS:
        handlers[preset_name] = cmd_preset
    return handlers[args.cmd](args)
