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
import tempfile
import time
from pathlib import Path
from typing import Any

from . import crawler, encoder, naming, probe, report, rules
from .db import DEFAULT_DB_PATH, Database
from .models import ProbeResult, probe_from_dict
from .presets import (
    BITRATE_FLAG_TABLE,
    EST_SECONDS_PER_FILE,
    MIN_PROBE_SIZE_BYTES,
    PRESETS,
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
    s.add_argument("--allow-extras", action="store_true",
                   help="Include Plex-style extras (Trailers, Behind The "
                        "Scenes, Featurettes, files with -trailer/-bts/etc. "
                        "suffixes). Default: skip them — a library-scale "
                        "tool shouldn't burn GPU time on add-ons.")
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
    r.add_argument("--allow-extras", action="store_true",
                   help="Include Plex-style extras during the re-probe walk.")
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
    pl.add_argument("--allow-av1", action="store_true",
                    help="Re-queue AV1 sources. Default behavior is to skip "
                         "AV1 entirely (it's already at the target codec; "
                         "re-encoding is wasteful and quality-lossy).")
    pl.add_argument("--allow-extras", action="store_true",
                    help="Re-queue files matching Plex extras suffixes "
                         "(`-trailer`, `-bts`, `-deleted`, …). Default "
                         "skips them; the crawler also filters extras "
                         "directories at walk time.")
    pl.add_argument("--allow-hd-hevc", action="store_true",
                    help="Include HEVC at HD (720..1439) as a re-encode "
                         "candidate. Default leaves HD HEVC alone — it's "
                         "already efficient enough that the AV1 savings "
                         "rarely justify GPU time. Enable for testing or "
                         "library-wide AV1 consolidation.")
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
    # Suppress the post-run report. Hidden in --help; this is for users with
    # piped output / cron contexts who genuinely don't want the summary or
    # the persisted ~/.video_optimizer/reports/run-N.txt file.
    ap.add_argument("--no-report", action="store_true",
                    help=argparse.SUPPRESS)
    _add_common_db_arg(ap)


def _add_apply_workflow_args(ap: argparse.ArgumentParser) -> None:
    """Attach apply-mode flags governing confirmation, output layout, limits."""
    ap.add_argument("--auto", action="store_true",
                    help="Skip per-file confirmation.")
    ap.add_argument("--mode", choices=["beside", "side", "replace"],
                    default="side")
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
    ap.add_argument("--original-audio", action="store_true",
                    help="Bypass the 3-stream audio ladder; map every "
                         "input audio track via stream-copy. Ignores "
                         "--keep-langs and --compat-audio (subtitles "
                         "still respect --keep-langs). Use when you "
                         "want every track preserved bit-perfectly.")
    ap.add_argument("--original-subs", action="store_true",
                    help="Bypass the --keep-langs filter for subtitles; "
                         "map every input subtitle track via stream-copy. "
                         "MKV target preserves all formats; MP4 still "
                         "drops image subs (PGS/VOBSUB) and converts "
                         "text to mov_text (the container's own limit, "
                         "not the flag's).")


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


def _add_pipeline_args(p: argparse.ArgumentParser, *, path_help: str) -> None:
    """Shared CLI surface for `optimize` and the SD/HD/UHD tier subcommands.

    All path-taking pipeline subcommands present the same flags so that
    `./video_optimizer.py UHD /path` and `./video_optimizer.py /path`
    differ only in tier scope. Visible flags cover the common UX
    decisions (where to write, dry-run, confirm, cleanup); advanced
    flags are SUPPRESSed but functional.
    """
    p.add_argument("path", type=Path, help=path_help)
    p.add_argument("--mode", choices=["beside", "side", "replace"],
                   default=None,
                   help="Output mode. 'beside' writes alongside the source "
                        "and leaves originals untouched (default when "
                        "neither --output nor --in-place is set). 'side' "
                        "mirrors output into a separate tree (--output). "
                        "'replace' writes alongside originals and moves "
                        "the originals into a recycle directory (--in-place).")
    out = p.add_mutually_exclusive_group()
    out.add_argument("--output", type=Path, metavar="DIR",
                     help="Side mode: write new files under DIR mirroring "
                          "PATH's structure. Originals are untouched.")
    out.add_argument("--in-place", action="store_true",
                     help="Replace mode: write new files alongside originals "
                          "and move the originals into a recycle directory "
                          "(see --recycle-to).")
    p.add_argument("--recycle-to", type=Path, default=None, metavar="DIR",
                   help="With --in-place: recycle directory for displaced "
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
                   help="After a successful run, prompt to remove the "
                        "originals of completed encodes.")
    p.add_argument("--original-audio", action="store_true",
                   help="Keep every input audio track via stream-copy "
                        "(default strips to --keep-langs and rebuilds a "
                        "3-stream ladder).")
    p.add_argument("--original-subs", action="store_true",
                   help="Keep every input subtitle track via stream-copy "
                        "(default strips to --keep-langs).")
    p.add_argument("--verbose", "-v", action="store_true")

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
    p.add_argument("--allow-hd-hevc", action="store_true",
                   help=argparse.SUPPRESS)
    p.add_argument("--bare-invocation", action="store_true", default=False,
                   help=argparse.SUPPRESS)


def _add_optimize_parser(sub: "argparse._SubParsersAction") -> None:
    """Register the `optimize` one-shot pipeline subcommand (all tiers)."""
    op = sub.add_parser(
        "optimize",
        help="One-shot scan+plan+apply for a library, all three tiers "
             "(UHD + HD + SD). The friendliest entry point for new users.",
        description=(
            "Run scan, plan, and apply against PATH in a single command, "
            "chaining the UHD, HD, and SD presets so every supported "
            "resolution band is covered. The default output mode is "
            "'beside': new files land alongside the source and originals "
            "stay untouched (see the `cleanup` subcommand for removing "
            "them). Pass --output DIR for a mirrored tree, or --in-place "
            "to recycle originals as the run proceeds."
        ),
    )
    _add_pipeline_args(op, path_help="Library directory to optimize.")


def _add_preset_parsers(sub: "argparse._SubParsersAction") -> None:
    """Register one subcommand per entry in PRESETS, sharing a narrow flag set."""
    for name, cfg in PRESETS.items():
        p = sub.add_parser(
            name,
            help=f"Tier-only pipeline (scan+plan+apply) with the "
                 f"{cfg['label']} preset.",
            description=(
                f"Run scan, plan, and apply against PATH using only the "
                f"{name} preset (resolution-band filtered to "
                f"{cfg.get('label')}). Same flags as `optimize`; the "
                f"difference is that `optimize` chains all three tiers "
                f"and this subcommand processes only the {name} band."
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
             "tier scope, then runs the full pipeline. Triggered "
             "automatically when invoked with no args in a TTY.",
    )
    _add_common_db_arg(wz)


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
    _add_cleanup_parser(sub)
    _add_wizard_parser(sub)
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


def _path_under(candidate: str, root: Path) -> bool:
    """Return True if `candidate` lies under `root` (or equals it).

    Used by the plan-time path-scope filter to keep cmd_plan from
    surfacing candidates that aren't under the user's requested path.
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

    Catches the beside-mode blind spot: when an HEVC/h.264 source has
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


def _plan_probe_gate(db: Database, pr,
                     *, allow_reencoded: bool = False,
                     allow_av1: bool = False,
                     allow_extras: bool = False) -> str:
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
      "existing_output" — a sibling `.AV1.REENCODE.mkv` already exists
                     next to the source (beside-mode prior-run output).
                     Skipped to avoid overwriting it with a fresh encode;
                     allow_reencoded=True overrides (re-uses the same
                     "I want to re-run already-processed files" gate).
      "av1_source" — source codec is AV1; re-encoding is wasteful by
                     default. Caller can pass allow_av1=True to override
                     (`plan --allow-av1`).
      "extras"     — filename matches a Plex-style extras suffix
                     (`-trailer`, `-bts`, etc.). Defensive: the crawler
                     normally filters these at walk time, but a probe
                     cache populated before extras filtering existed
                     could surface them here. allow_extras=True overrides.
      "ok"         — admit to rule evaluation. SD content (height < 720)
                     is admitted; the per-tier presets pick which band
                     they want.
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
    if pr.dv_profile is not None and encoder.dv_strategy(pr.dv_profile) is None:
        # Profile 5 (custom DV colorspace, no HDR10 base) and Profile 7
        # without dovi_tool installed both fall here. Profile 8.x and
        # Profile 7 with dovi_tool available are admitted; the apply
        # layer routes them through the strip / convert prep pipeline.
        return "dv"
    if not allow_reencoded and _is_reencoded_filename(pr.path):
        return "reencoded"
    if not allow_reencoded and _existing_reencode_sibling(pr.path) is not None:
        return "existing_output"
    if not allow_av1 and (pr.video_codec or "").lower() == "av1":
        return "av1_source"
    if not allow_extras and crawler.is_extras_filename(Path(pr.path)):
        return "extras"
    return "ok"


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


def _resolve_enabled_rules(args: argparse.Namespace) -> list[str] | None:
    """Compose the rule-name list for cmd_plan based on flags.

    None → RulesEngine uses its default-enabled set (every non-opt-in
    rule). Otherwise returns an explicit list — either the user's
    `--rules` override, or the default set extended with whichever
    opt-in rules have been gated on (`--allow-hd-hevc` for now).
    """
    if args.rules:
        return [s.strip() for s in args.rules.split(",")]
    opt_in_active: list[str] = []
    if getattr(args, "allow_hd_hevc", False):
        opt_in_active.append("hd_hevc_opt_in")
    if not opt_in_active:
        return None
    enabled = [n for n, r in rules.RULES.items() if not r.opt_in]
    enabled.extend(opt_in_active)
    return enabled


def cmd_plan(args: argparse.Namespace) -> int:
    """Run the rules engine over the probe cache; record pending decisions."""
    enabled = _resolve_enabled_rules(args)
    try:
        engine = rules.RulesEngine(enabled=enabled, target=args.target)
    except ValueError as e:
        print(f"error: {e}", file=sys.stderr)
        return 2

    allow_reencoded = bool(getattr(args, "allow_reencoded", False))
    allow_av1 = bool(getattr(args, "allow_av1", False))
    allow_extras = bool(getattr(args, "allow_extras", False))
    # Path scope: when a path is supplied (path-pipeline subcommands like
    # SD/HD/UHD/optimize/bare invocation), only probes under that path
    # are eligible for candidate creation. Without this, the plan would
    # happily create candidates from older cache entries outside the
    # user's requested directory and apply would encode them. The
    # standalone `plan` subcommand has no path and operates on the
    # whole cache (existing behavior preserved).
    scope_path = getattr(args, "path", None)
    if scope_path is not None:
        try:
            scope_resolved = Path(scope_path).resolve(strict=False)
        except OSError:
            scope_resolved = Path(scope_path)
    else:
        scope_resolved = None
    with Database(args.db) as db:
        run_id = db.start_run("plan", None, _args_dict(args))
        cleared = db.clear_pending_decisions()
        candidates = []
        counts = {"missing": 0, "stalled": 0, "dv": 0, "reencoded": 0,
                  "av1_source": 0, "extras": 0, "existing_output": 0,
                  "out_of_scope": 0}
        # Materialise the probe list so we can mutate the cache (DELETE
        # stale rows) without invalidating the iterator.
        for pr in list(db.iter_probes()):
            if scope_resolved is not None and not _path_under(pr.path,
                                                              scope_resolved):
                counts["out_of_scope"] += 1
                continue
            verdict = _plan_probe_gate(
                db, pr, allow_reencoded=allow_reencoded,
                allow_av1=allow_av1, allow_extras=allow_extras)
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
                run_id=run_id,
            )
            candidates.append(cand)
        if counts["missing"]:
            db.conn.commit()
        _emit_plan_skip_summary(counts)

        candidates.sort(key=lambda c: c.total_projected_savings_mb, reverse=True)

        if args.json:
            print(report.format_candidates_json(candidates))
        else:
            print(report.format_candidates_text(candidates))

        summary = {"cleared_pending": cleared,
                   "candidates": len(candidates),
                   "pruned_stale_rows": counts["missing"],
                   "stall_blocked": counts["stalled"],
                   "dv_blocked": counts["dv"],
                   "reencoded_blocked": counts["reencoded"],
                   "av1_blocked": counts["av1_source"],
                   "extras_blocked": counts["extras"],
                   "existing_output_blocked": counts["existing_output"],
                   "out_of_scope_blocked": counts["out_of_scope"]}
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


def _validate_apply_args(args: argparse.Namespace) -> int:
    """Pre-flight checks for cmd_apply. Returns 0 on ok, nonzero exit code."""
    if args.mode == "side" and not args.output_root:
        print("error: --mode side requires --output-root", file=sys.stderr)
        return 2
    if args.mode == "beside" and getattr(args, "output_root", None):
        print("error: --mode beside is incompatible with --output-root "
              "(beside writes alongside the source)", file=sys.stderr)
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
    return 0


def cmd_apply(args: argparse.Namespace) -> int:
    """Encode pending decisions; per-file confirm unless --auto / --dry-run."""
    rc = _validate_apply_args(args)
    if rc != 0:
        return rc

    keep_langs = [s.strip() for s in args.keep_langs.split(",") if s.strip()]

    with Database(args.db) as db:
        run_id = db.start_run("apply", None, _args_dict(args))
        # Stashed on the namespace so downstream helpers (_apply_one,
        # _execute_encode, _finalize_output) can stamp every mark_decision
        # with the apply run id without growing every signature. The
        # post-run report (`_emit_run_report`) keys on this run id.
        args._apply_run_id = run_id  # noqa: SLF001
        pending = db.list_pending_decisions()
        pending = _prefilter_resolution_gate(db, pending, args)

        if args.limit > 0:
            pending = pending[: args.limit]

        if not pending:
            print("nothing to apply: no pending decisions. run 'plan' first.")
            db.end_run(run_id, {"applied": 0})
            return 0

        counts = {"applied": 0, "skipped": 0, "failed": 0,
                  "deferred": 0, "dry_run": 0}
        bytes_saved = 0
        for i, dec in enumerate(pending, 1):
            status, saved = _apply_one(db, dec, args, keep_langs, i, len(pending))
            # Count every terminal/observable status so the report knows
            # whether ≥1 decision was processed (dry_run included).
            if status in counts:
                counts[status] += 1
            bytes_saved += saved

        summary = {k: v for k, v in counts.items() if k != "dry_run"}
        summary["approx_bytes_saved"] = bytes_saved
        db.end_run(run_id, summary)
        deferred_note = (f", {counts['deferred']} deferred (resolution gate)"
                         if counts["deferred"] else "")
        print(f"\napply done: {counts['applied']} encoded, "
              f"{counts['skipped']} skipped, {counts['failed']} failed"
              f"{deferred_note}; "
              f"~{_format_bytes(bytes_saved)} saved")

        touched = (counts["applied"] + counts["skipped"] + counts["failed"]
                   + counts["dry_run"])
        if touched and not getattr(args, "no_report", False):
            _emit_run_report(db, run_id)
    return 0


def _apply_one(db: Database, dec: dict, args: argparse.Namespace,
               keep_langs: list[str], idx: int, total: int) -> tuple[str, int]:
    """Process a single pending decision. Returns (status, bytes_saved)."""
    run_id = getattr(args, "_apply_run_id", None)
    pr = _load_probe_for_decision(db, dec)
    if pr is None:
        print(f"[{idx}/{total}] {dec['path']}: probe missing, skipping")
        db.mark_decision(dec["id"], "skipped",
                         error="probe missing in cache (rerun scan)",
                         run_id=run_id, expected_path=dec["path"])
        return "skipped", 0

    # Defense in depth: catch sources that disappeared between plan and
    # apply (e.g. file moved to recycle by an earlier apply, or unmounted
    # NFS share). cmd_plan already prunes these, but a long-running apply
    # could lose a source mid-run.
    if not Path(pr.path).exists():
        print(f"[{idx}/{total}] {pr.path}: source no longer exists, skipping")
        db.mark_decision(dec["id"], "skipped",
                         error="source no longer exists at apply time",
                         run_id=run_id, expected_path=pr.path)
        return "skipped", 0

    _print_decision_header(dec, pr, idx, total)

    # Resolution gate: defer (leave pending) if outside the requested band.
    # Used by HD/SD to skip UHD candidates and UHD to skip HD/SD, etc.
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

    Split from `_apply_one` to keep the validation/gate front-half
    focused and readable. The DV-prep work_dir lives next to the source
    so the temp stream-copy stays on the same filesystem; try/finally
    guarantees teardown even on encode failure.
    """
    dv_prep_dir: Path | None = None
    source_for_encode: str | None = pr.path
    try:
        if pr.dv_profile is not None:
            dv_prep_dir, source_for_encode = _prepare_dv_source(pr, args)
            if source_for_encode is None:
                # Strategy was None (P5 / P7-without-dovi_tool). Plan
                # gate should have caught this; defensive belt-and-braces.
                db.mark_decision(dec["id"], "skipped",
                                 error="dv_no_prep_strategy", run_id=run_id,
                                 expected_path=pr.path)
                return "skipped", 0

        cmd, desc = _build_apply_command(
            dec, pr, output_path, target_container,
            enc_name, keep_langs, args,
            source_override=source_for_encode,
        )

        if args.dry_run:
            print(f"    DRY RUN ({desc}) → {output_path}")
            print("    " + " ".join(cmd))
            db.stamp_decision_run(dec["id"], run_id,
                                  expected_path=pr.path)
            return "dry_run", 0

        label = f"[{idx}/{total}] {Path(pr.path).name}: "
        return _execute_encode(db, dec, pr, cmd, desc, output_path, args, label)
    finally:
        if dv_prep_dir is not None:
            shutil.rmtree(dv_prep_dir, ignore_errors=True)


def _prepare_dv_source(pr: ProbeResult,
                       args: argparse.Namespace) -> tuple[Path | None, str | None]:
    """Run the appropriate DV pre-stage; return (work_dir, prepared_path).

    Profile 8.x: stream-copy with `dovi_rpu=strip` bsf. Single ffmpeg
    subprocess, ~5-10 minutes on a 50 GB UHD remux (I/O-bound). Output
    is a clean HDR10 MKV that the QSV pipeline accepts.

    Profile 7: not yet implemented in code — returns (None, None) so
    the caller skips the file. The plan-gate is supposed to catch this
    case (admit only when `dovi_tool` is on PATH); the apply-side
    handler is the next step. Documented in TODO.md.

    Returns:
      (work_dir, prepared_source_path) on success — caller `rmtree`s
        work_dir in finally to clean up the temp stream-copy.
      (None, None) on no-op or unimplemented strategy — caller treats
        as "skip this source."

    The work_dir lives next to the source on its own filesystem so the
    ~50 GB temp file write doesn't traverse a slow NAS link or fill /tmp.
    """
    strategy = encoder.dv_strategy(pr.dv_profile)
    if strategy is None:
        return None, None

    src = Path(pr.path)
    work_dir = Path(tempfile.mkdtemp(
        prefix=".vo_dv_prep_", dir=str(src.parent),
    ))
    prepared = work_dir / f"{src.stem}.dv-prepped.mkv"

    if strategy == "p8_strip":
        print(f"    DV Profile {pr.dv_profile}: stripping RPU "
              f"(temp file: {prepared.name})")
        cmd = encoder.build_dv_strip_command(pr, prepared)
        timeout = _resolve_timeout(args.timeout, pr.duration_seconds)
        ok, err = encoder.run_ffmpeg(
            cmd, pr.duration_seconds,
            timeout_seconds=timeout, verbose=args.verbose,
            label="    DV-strip ",
        )
        if not ok:
            shutil.rmtree(work_dir, ignore_errors=True)
            print(f"    FAIL: DV strip failed: {err[:200]}")
            return None, None
        return work_dir, str(prepared)

    if strategy == "p7_convert":
        ok = _run_dv_p7_pipeline(pr, work_dir, prepared, args)
        if not ok:
            shutil.rmtree(work_dir, ignore_errors=True)
            return None, None
        return work_dir, str(prepared)

    # Unknown strategy string — defensive fallback.
    shutil.rmtree(work_dir, ignore_errors=True)
    return None, None


def _run_dv_p7_pipeline(pr: ProbeResult, work_dir: Path,
                       prepared: Path,
                       args: argparse.Namespace) -> bool:
    """Run the 3-stage Profile 7 prep: extract HEVC → P7→P8 convert → re-mux+strip.

    Stage 1+2 are piped (ffmpeg stdout → dovi_tool stdin) so we don't
    write a 50 GB intermediate Annex-B HEVC. Stage 2 writes the
    converted P8.1 HEVC to a temp file because stage 3 needs it as a
    file input alongside the original source. Stage 3 re-attaches
    audio/subs from the original and applies `dovi_rpu=strip=true` in
    one pass — the output is plain HDR10 MKV ready for av1_qsv.

    Returns True on success, False on any subprocess failure (caller
    cleans up the work_dir).
    """
    print(f"    DV Profile 7: converting to P8 + stripping RPU "
          f"(temp dir: {work_dir.name})")
    p8_hevc = work_dir / "p8.hevc"
    extract_cmd = encoder.build_dv_p7_extract_command(pr)
    convert_cmd = encoder.build_dv_p7_convert_command(p8_hevc)

    # Stage 1+2: piped extract → convert. Both stdouts captured so
    # ffmpeg's progress logs can be flushed if it errors before the
    # pipe closes; on success we just discard them.
    extract_proc = subprocess.Popen(
        extract_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL if not args.verbose else None,
    )
    try:
        convert_proc = subprocess.run(
            convert_cmd,
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

    # Stage 3: re-mux + strip via the existing run_ffmpeg helper so
    # the timeout / progress / stall watchdog all apply.
    remux_cmd = encoder.build_dv_p7_remux_strip_command(pr, p8_hevc, prepared)
    timeout = _resolve_timeout(args.timeout, pr.duration_seconds)
    ok, err = encoder.run_ffmpeg(
        remux_cmd, pr.duration_seconds,
        timeout_seconds=timeout, verbose=args.verbose,
        label="    DV-P7-remux ",
    )
    if not ok:
        print(f"    FAIL: DV P7 remux+strip failed: {err[:200]}")
        return False
    return True


def _print_decision_header(dec: dict, pr: ProbeResult, idx: int, total: int) -> None:
    """Print the `[idx/total] path / rules / projected savings` block."""
    print(f"\n[{idx}/{total}] {pr.path}")
    print(f"    rules: {dec['rules_fired_json']}  target: {dec['target']}")
    print(f"    projected savings: "
          f"{(dec['projected_savings_mb'] or 0) / 1024:.1f} GB")


def _should_apply_denoise(pr: ProbeResult) -> bool:
    """Return True if this source benefits from a software denoise pre-pass.

    Triggers in two cases that share the same root cause — sources where
    AV1's bit budget is at risk of being spent reproducing h.264
    macroblock noise rather than real picture detail:

      1. SD content (height < 720). SD almost always rides on heavy
         compression and benefits universally from light cleanup.
      2. h.264 in the HD band whose source bitrate is below the AV1
         target bitrate for its resolution bucket. Above the AV1 target,
         the source has bitrate headroom and a clean re-encode is fine.
         Below it, the source is already showing artifacts and we want
         to soften them before AV1 sees them.

    hqdn3d is CPU-only, so callers that pass denoise=True must also
    disable hw_decode (the QSV zero-copy pipeline can't host a software
    filter mid-stream). Library-scale assumption: edge-case slowdown on
    the rare low-bitrate file is preferable to a worse-quality output.
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
                         ) -> tuple[list[str], str]:
    """Pick remux vs encode and build the corresponding ffmpeg argv.

    `source_override` swaps the `-i` source path while keeping all
    probe-derived stream layout decisions intact. Used by the DV
    strip pipeline (the prepared HDR10 stream-copy replaces the
    original DV source for the encode stage; audio/subtitle indices
    and metadata still come from the probe of the original).
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
    # No explicit hw_decode override: every code path that triggers
    # denoise lands in the HD preset (height < 1440), which already
    # defaults hw_decode=False. The UHD preset never sees a denoise
    # candidate because its resolution gate is min_height=1440.
    cmd = encoder.build_encode_command(
        pr, output_path, enc_name, args.quality, keep_langs,
        target_container, hw_decode=getattr(args, "hw_decode", False),
        add_compat_audio=add_compat,
        denoise=denoise,
        original_audio=original_audio,
        original_subs=original_subs,
        source_override=source_override,
    )
    desc = f"encode via {enc_name}"
    if denoise:
        desc += " (+ denoise pre-pass)"
    if original_audio:
        desc += " (+ original audio passthrough)"
    if original_subs:
        desc += " (+ original subs passthrough)"
    if source_override is not None:
        desc += " (+ DV strip pre-pass)"
    return cmd, desc


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
        db.mark_decision(dec["id"], "failed", error=err[:1000],
                         run_id=getattr(args, "_apply_run_id", None),
                         expected_path=pr.path)
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
) -> tuple[str, Path | None, Path, Path | None] | int:
    """Return (mode, output_root, source_root, recycle_to) or an exit code.

    mode is one of "beside", "side", "replace". Resolution order:
    explicit --mode wins; else --in-place → replace; else --output → side;
    else default to beside.
    """
    if args.mode is not None:
        mode = args.mode
    elif args.in_place:
        mode = "replace"
    elif args.output is not None:
        mode = "side"
    else:
        mode = "beside"

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
    # beside
    if args.recycle_to is not None:
        print("error: --recycle-to only applies to --mode replace",
              file=sys.stderr)
        return 2
    if args.output is not None:
        print("error: --mode beside is incompatible with --output "
              "(beside writes alongside the source)", file=sys.stderr)
        return 2
    return (mode, None, args.path, None)


def _build_apply_namespace(args: argparse.Namespace, preset_name: str,
                           mode: str, output_root: Path | None,
                           source_root: Path,
                           recycle_to: Path | None) -> argparse.Namespace:
    """Construct the full apply Namespace for one preset run.

    Path-taking pipeline subcommands carry only the optimize-style flag
    surface; the preset apply step needs additional fields (backup,
    name_suffix, etc.) to reach cmd_apply. Hardcode the ones that aren't
    user-facing on the optimize/SD/HD/UHD parsers.
    """
    return argparse.Namespace(
        cmd=preset_name,
        auto=args.auto,
        mode=mode,
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
        original_audio=getattr(args, "original_audio", False),
        original_subs=getattr(args, "original_subs", False),
        no_dotted=False,
        name_suffix="",
        reencode_tag_value="REENCODE",
        dry_run=args.dry_run,
        verbose=args.verbose,
        db=args.db,
    )


def _apply_with_preset_config(args: argparse.Namespace) -> int:
    """Fill preset config into args; dispatch to cmd_apply.

    Internal helper used by `_run_path_pipeline`. Keeps the
    apply-specific Namespace carrying everything cmd_apply expects
    (target, rewrite_codec, reencode_tag, plus the preset's CQ /
    keep_langs / height band / hw_decode defaults).
    """
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


def _run_path_pipeline(args: argparse.Namespace,
                       presets_to_run: tuple[str, ...],
                       *, label: str) -> int:
    """Shared scan + plan + apply pipeline for path-taking subcommands.

    Used by `cmd_optimize` (all three tiers) and `cmd_preset` (one tier
    per SD/HD/UHD invocation). The apply phase loops once per preset in
    `presets_to_run`; each preset's own min_height/max_height filter
    keeps its band isolated.
    """
    _apply_bare_invocation_defaults(args)
    if getattr(args, "confirm", False):
        args.auto = False

    if not args.path.exists():
        print(f"error: path not found: {args.path}", file=sys.stderr)
        return 2
    if not (args.path.is_dir() or args.path.is_file()):
        print(f"error: {label} expects a directory or a single video file: "
              f"{args.path}", file=sys.stderr)
        return 2

    resolved = _optimize_resolve_paths(args)
    if isinstance(resolved, int):
        return resolved
    mode, output_root, source_root, recycle_to = resolved

    _print_pipeline_banner(args, label, mode, output_root, recycle_to)

    total = 2 + len(presets_to_run)
    print(f"==> [1/{total}] scan: probing {args.path} (cache hits skip ffprobe)...")
    scan_ns = argparse.Namespace(
        cmd="scan", path=args.path, no_recursive=False,
        no_probe_cache=False, workers=args.workers,
        min_size=args.min_size,
        allow_extras=getattr(args, "allow_extras", False),
        verbose=args.verbose, db=args.db,
    )
    rc = cmd_scan(scan_ns)
    if rc != 0:
        return rc
    print()

    print(f"==> [2/{total}] plan: evaluating rules against probe cache...")
    plan_ns = argparse.Namespace(
        cmd="plan", path=args.path, rules=None,
        target="av1+mkv", json=False,
        keep_langs=args.keep_langs or "en,und",
        allow_reencoded=False,
        allow_av1=getattr(args, "allow_av1", False),
        allow_extras=getattr(args, "allow_extras", False),
        allow_hd_hevc=getattr(args, "allow_hd_hevc", False),
        db=args.db,
    )
    rc = cmd_plan(plan_ns)
    if rc != 0:
        return rc
    print()

    aggregate_rc = 0
    for step, preset_name in enumerate(presets_to_run, start=3):
        cfg = PRESETS[preset_name]
        print(f"==> [{step}/{total}] apply: {preset_name} ({cfg['label']})")
        preset_ns = _build_apply_namespace(
            args, preset_name, mode, output_root, source_root, recycle_to,
        )
        rc = _apply_with_preset_config(preset_ns)
        if rc != 0:
            aggregate_rc = rc
        print()

    if aggregate_rc == 0 and getattr(args, "cleanup_after", False):
        _invoke_cleanup_after(args)
    return aggregate_rc


def _apply_bare_invocation_defaults(args: argparse.Namespace) -> None:
    """Flip pipeline defaults so every path-taking subcommand is point-and-shoot.

    Applied unconditionally to every path-taking pipeline invocation
    (optimize, SD, HD, UHD, plus the bare-path rewrite). The user
    picked a path-taking subcommand → they want auto-yes encoding
    against the source they pointed at. --confirm opts back into
    per-file prompts; --output/--in-place/--mode opts out of beside.

    The bare-invocation sentinel (`--bare-invocation`) additionally
    flips verbose on; that's the one place where we infer the user
    is brand-new and probably wants to see more output.
    """
    if not args.auto:
        args.auto = True
    if args.mode is None and not args.in_place and args.output is None:
        args.mode = "beside"
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
    if mode == "beside":
        print("    output mode: beside (alongside source; originals untouched)")
    elif mode == "replace":
        print("    output mode: replace (in-place)")
        print(f"    recycle to:  {recycle_to}")
    else:
        print("    output mode: side (mirrored output tree)")
        print(f"    output root: {output_root}")
    if args.dry_run:
        print("    DRY RUN (no encodes will run)")
    print()


def cmd_optimize(args: argparse.Namespace) -> int:
    """One-shot scan+plan+apply across all three tiers (UHD + HD + SD)."""
    return _run_path_pipeline(args, ("UHD", "HD", "SD"), label="optimize")


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
    """Tier-only path-taking pipeline (SD / HD / UHD subcommands).

    Same flow as `cmd_optimize` but applies a single preset, so only
    files within that tier's height band get encoded.
    """
    return _run_path_pipeline(args, (args.cmd,), label=args.cmd)


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

    if args.mode == "beside":
        # beside mode: write next to the source; originals stay put. The
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
    try:
        out_size = output_path.stat().st_size
    except OSError:
        out_size = 0
    actual_mb = (pr.size - out_size) / (1024 * 1024)

    if args.mode == "beside":
        # beside mode never touches the original — the whole point is that
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
    "scan", "reprobe", "apply", "list-encoders", "optimize",
})


# Single source of truth for argv preprocessing (see main()). Includes
# forward-declared subcommands (cleanup, wizard) so the dispatch logic
# doesn't need re-editing when their full handlers land.
KNOWN_SUBCOMMANDS: frozenset[str] = frozenset({
    "scan", "reprobe", "plan", "apply", "status",
    "list-encoders", "replace-list", "doctor",
    "optimize", "cleanup", "wizard",
}) | frozenset(PRESETS.keys())


def _classify_cleanup_decision(dec: dict) -> tuple[str, int, str | None]:
    """Apply the cleanup 3-check guard to one completed decision.

    Returns ``(source_path, size_bytes, reason_or_None)``. A non-None
    reason means the decision is **not** cleanable and should be
    reported as SKIP; size_bytes is meaningful only when reason is None.

    The 3-check guard:
      (a) decisions.output_path exists on disk,
      (b) output is non-empty (stat().st_size > 0),
      (c) output_path != source_path (paranoid; beside mode makes them
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
        return ("beside", None, None)
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
    print("  [h] HD only (720–1439p)")
    print("  [s] SD only (< 720p)")
    choice = _prompt("Choice [a]: ", default="a",
                     choices=["a", "u", "h", "s"])
    if choice == "u":
        return ("UHD",)
    if choice == "h":
        return ("HD",)
    if choice == "s":
        return ("SD",)
    return ("UHD", "HD", "SD")


def _wizard_apply_namespace(
    args: argparse.Namespace,
    library: Path,
    mode: str,
    output_root: Path | None,
    recycle_to: Path | None,
    limit: int,
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
        in_place=(mode == "replace"),
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


def _wizard_run_scan_plan(args: argparse.Namespace, library: Path) -> int:
    """Run scan + plan against `library`. Returns 0 on success, nonzero else."""
    print()
    print(f"==> scan: probing {library}")
    scan_ns = argparse.Namespace(
        cmd="scan", path=library, no_recursive=False,
        no_probe_cache=False, workers=None,
        min_size=MIN_PROBE_SIZE_BYTES,
        verbose=False, db=args.db,
    )
    if cmd_scan(scan_ns) != 0:
        print("scan failed; aborting", file=sys.stderr)
        return 1
    print()
    print("==> plan: evaluating rules")
    plan_ns = argparse.Namespace(
        cmd="plan", path=library, rules=None,
        target="av1+mkv", json=False,
        keep_langs="en,und", allow_reencoded=False,
        allow_av1=False, allow_extras=False, allow_hd_hevc=False,
        db=args.db,
    )
    if cmd_plan(plan_ns) != 0:
        print("plan failed; aborting", file=sys.stderr)
        return 1
    return 0


def _wizard_pick_limit(pending: list[dict], db: Database) -> int | None:
    """Print the plan summary and ask how many files to encode.

    Returns the limit (0 == all), or None if the user chose to quit.
    """
    uhd, hd, sd, est_seconds = _wizard_estimate_seconds(pending, db)
    uhd_hours = _format_hours(uhd * EST_SECONDS_PER_FILE["UHD"])
    hd_hours = _format_hours(hd * EST_SECONDS_PER_FILE["HD"])
    sd_hours = _format_hours(sd * EST_SECONDS_PER_FILE["SD"])
    print()
    print(f"Found {len(pending)} candidate(s): "
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


def cmd_wizard(args: argparse.Namespace) -> int:
    """Interactive prompt-based workflow.

    Composes `cmd_doctor`, `cmd_scan`, `cmd_plan`, `cmd_optimize` (which
    itself routes through `cmd_preset` → `cmd_apply`), and `cmd_cleanup`.
    No new business logic — this is purely a Q&A surface for users who
    don't want to read --help. See plan §6 / "Wizard prompt sequence".
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

        rc = _wizard_run_scan_plan(args, library)
        if rc != 0:
            return rc

        with Database(args.db) as db:
            pending = db.list_pending_decisions()
            if not pending:
                print()
                print("no candidates found. nothing to do.")
                return 0
            limit = _wizard_pick_limit(pending, db)
        if limit is None:
            return 0

        print()
        if _prompt("Proceed? [Y/n]: ",
                   default="y", choices=["y", "n"]) != "y":
            print("aborted by user; nothing encoded.")
            return 0

        apply_ns = _wizard_apply_namespace(
            args, library, mode, output_root, recycle_to, limit,
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
    # Preserve argv[0] (prog name) so the caller's argv[1:] slice still works.
    return [argv[0], "optimize", first, *argv[2:], "--bare-invocation"]


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
    if argv is None:
        argv = sys.argv
    argv = _preprocess_argv(argv)
    parser = _build_parser()
    args = parser.parse_args(argv[1:])
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
        "cleanup": cmd_cleanup,
        "wizard": cmd_wizard,
    }
    # Preset subcommands all dispatch to the same wrapper.
    for preset_name in PRESETS:
        handlers[preset_name] = cmd_preset
    return handlers[args.cmd](args)
