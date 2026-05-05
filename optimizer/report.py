"""Candidate list rendering: human-readable text and JSON."""

from __future__ import annotations

import json
import os
from dataclasses import asdict

from .models import Candidate

_HEADER_COLS = ("idx", "path", "res", "codec", "bitrate",
                "target", "rules", "savings")
_WIDTHS = (3, 38, 7, 11, 10, 11, 31, 10)


def format_candidates_text(candidates: list[Candidate]) -> str:
    """Render a multi-line human-readable report."""
    total_mb = sum(c.total_projected_savings_mb for c in candidates)
    total_gb = total_mb / 1024.0
    lines: list[str] = [
        f"{len(candidates)} candidates found ({total_gb:.1f} GB projected savings)",
        "",
    ]
    if not candidates:
        return "\n".join(lines)

    lines.append(_format_row(_HEADER_COLS))
    lines.append(_format_row(tuple("-" * w for w in _WIDTHS)))
    for i, cand in enumerate(candidates, start=1):
        lines.append(_format_row(_row_cells(i, cand)))

    detail = _format_details(candidates)
    if detail:
        lines.append("")
        lines.extend(detail)
    return "\n".join(lines)


def format_candidates_json(candidates: list[Candidate]) -> str:
    """Compact-but-readable JSON: top-level object with summary + items."""
    items = [_candidate_to_item(c) for c in candidates]
    payload = {
        "candidate_count": len(candidates),
        "total_projected_savings_mb": round(
            sum(c.total_projected_savings_mb for c in candidates), 2
        ),
        "items": items,
    }
    return json.dumps(payload, indent=2, default=_json_default)


# --------------------------------------------------------------------------- #
# Internals
# --------------------------------------------------------------------------- #


def _format_row(cells: tuple[str, ...]) -> str:
    parts = []
    for cell, width in zip(cells, _WIDTHS, strict=True):
        s = str(cell)
        if len(s) > width:
            s = s[: max(0, width - 1)] + "."
        parts.append(s.ljust(width))
    return "  ".join(parts).rstrip()


def _row_cells(idx: int, cand: Candidate) -> tuple[str, ...]:
    p = cand.probe
    tail = _path_tail(p.path)
    bitrate = f"{p.video_bitrate / 1_000_000:.1f} Mbps" if p.video_bitrate else "?"
    rules = ", ".join(cand.rule_names) or "-"
    savings = _format_size_mb(cand.total_projected_savings_mb)
    return (
        str(idx),
        tail,
        p.resolution_class,
        p.video_codec or "?",
        bitrate,
        cand.target,
        rules,
        savings,
    )


def _path_tail(path: str) -> str:
    parts = path.replace("\\", "/").strip("/").split("/")
    return os.sep.join(parts[-2:]) if len(parts) >= 2 else path


def _format_size_mb(mb: float) -> str:
    if mb >= 1024:
        return f"{mb / 1024:.1f} GB"
    return f"{mb:.0f} MB"


def _format_details(candidates: list[Candidate]) -> list[str]:
    out: list[str] = []
    for i, cand in enumerate(candidates, start=1):
        p = cand.probe
        out.append(f"[{i}] {p.path}")
        for v in cand.fired:
            sav = (f", ~{_format_size_mb(v.projected_savings_mb)}"
                   if v.projected_savings_mb else "")
            reason = v.reason or ""
            out.append(f"    - {v.rule} ({v.severity}{sav}): {reason}".rstrip())
        flags = [f"audio={len(p.audio_tracks)}", f"subs={len(p.subtitle_tracks)}"]
        if cand.is_hdr:
            flags.append("HDR")
        if cand.remux_only:
            flags.append("remux-only")
        out.append("    " + ", ".join(flags))
    return out


def _candidate_to_item(cand: Candidate) -> dict:
    p = cand.probe
    return {
        "path": p.path,
        "size_mb": round(p.size / (1024 * 1024), 2),
        "duration_seconds": round(p.duration_seconds, 2),
        "resolution": p.resolution_class,
        "video_codec": p.video_codec,
        "video_bitrate_mbps": round(p.video_bitrate / 1_000_000, 2)
        if p.video_bitrate else 0.0,
        "target": cand.target,
        "remux_only": cand.remux_only,
        "is_hdr": cand.is_hdr,
        "fired_rules": [
            {
                "rule": v.rule,
                "reason": v.reason,
                "severity": v.severity,
                "projected_savings_mb": v.projected_savings_mb,
            }
            for v in cand.fired
        ],
        "audio_tracks": [
            {"language": a.language, "codec": a.codec, "channels": a.channels}
            for a in p.audio_tracks
        ],
        "subtitle_tracks": [
            {"language": s.language, "codec": s.codec}
            for s in p.subtitle_tracks
        ],
    }


def _json_default(o):
    try:
        return asdict(o)
    except TypeError:
        return str(o)


# --------------------------------------------------------------------------- #
# Per-run post-apply report (Task #5)
#
# Two sibling renderings of the same `decisions` rows:
#
#   stdout_text — multi-section, human-readable, includes header + footer.
#   persist_text — one tab-delimited line per file, no header/footer.
#
# Persist columns (tab-separated, in this order):
#   OK   <savings_mb>    <output_path>   <source_path>
#   FAIL <error_token>   <source_path>
#   SKIP <reason>        <source_path>
#   DRY  <target>        <source_path>
#
# Stable column-1 prefix lets `grep ^OK report.txt | awk -F'\t' '{print $4}'`
# pull the originals out without writing a parser.
# --------------------------------------------------------------------------- #


# Map a verbose error string to a short, machine-greppable token. The first
# matching needle wins; the order is rough frequency — most common ffmpeg
# failures first.
_ERROR_TOKEN_PATTERNS: tuple[tuple[str, str], ...] = (
    ("encoder stalled", "encoder_stalled"),
    ("timeout", "timeout"),
    ("timed out", "timeout"),
    ("recycle move failed", "recycle_failed"),
    ("backup failed", "backup_failed"),
    ("original not removed", "original_not_removed"),
    ("probe missing", "probe_missing"),
    ("source no longer exists", "source_missing"),
    ("user declined", "user_declined"),
    ("dolby vision", "dolby_vision"),
    ("no encoder available", "no_encoder"),
)


def _error_token(error: str | None) -> str:
    """Reduce a verbose error string to a short, stable token."""
    if not error:
        return "ffmpeg_failed"
    low = error.lower()
    for needle, token in _ERROR_TOKEN_PATTERNS:
        if needle in low:
            return token
    return "ffmpeg_failed"


def _classify(dec: dict) -> str:
    """Return one of: 'ok', 'fail', 'skip', 'dry', 'other'."""
    status = dec.get("status") or ""
    if status == "completed" and dec.get("output_path"):
        return "ok"
    if status == "failed":
        return "fail"
    if status == "skipped":
        return "skip"
    if status == "pending":
        # In the report path, pending rows that carry our run_id are the
        # observed-but-not-encoded ones from --dry-run.
        return "dry"
    return "other"


def _format_savings_mb(mb: float | None) -> str:
    """Stable integer-MB rendering for the persist line and stdout list."""
    if mb is None:
        return "0"
    return f"{int(round(mb))}"


def _format_savings_total(total_mb: float) -> str:
    """Friendly total — GB if ≥ 1024 MB, else MB."""
    if total_mb >= 1024:
        return f"{total_mb / 1024:.1f} GB"
    return f"{total_mb:.0f} MB"


def _bucket_decisions(decisions: list[dict]) -> dict[str, list[dict]]:
    """Group decisions into the four report buckets used by both renderings."""
    buckets: dict[str, list[dict]] = {
        "encoded": [], "failed": [], "skipped": [], "dry": [],
    }
    name_for = {"ok": "encoded", "fail": "failed",
                "skip": "skipped", "dry": "dry"}
    for d in decisions:
        b = _classify(d)
        if b in name_for:
            buckets[name_for[b]].append(d)
    return buckets


def _stdout_section(title: str, rows: list[dict],
                    line_fn) -> list[str]:
    """Render one named bucket as `["", title, "  line", ...]` or [] if empty."""
    if not rows:
        return []
    out = ["", title]
    out.extend(line_fn(d) for d in rows)
    return out


def _stdout_lines(buckets: dict[str, list[dict]],
                  run_id: int) -> str:
    """Compose the human-readable stdout report."""
    encoded, failed = buckets["encoded"], buckets["failed"]
    skipped, dry = buckets["skipped"], buckets["dry"]

    total_savings_mb = sum((d.get("actual_savings_mb") or 0.0) for d in encoded)
    header = (f"Run #{run_id} complete: {len(encoded)} encoded, "
              f"{len(failed)} failed, {len(skipped)} skipped")
    if dry:
        header += f", {len(dry)} dry-run"
    header += f" (saved {_format_savings_total(total_savings_mb)})"

    parts: list[str] = [header]
    parts.extend(_stdout_section("Encoded:", encoded, lambda d: (
        f"  OK   {_format_savings_mb(d.get('actual_savings_mb'))} MB  "
        f"{d.get('output_path') or '?'}  (from {d['path']})")))
    parts.extend(_stdout_section("Failed:", failed, lambda d:
        f"  FAIL {_error_token(d.get('error'))}  {d['path']}"))
    parts.extend(_stdout_section("Skipped:", skipped, lambda d:
        f"  SKIP {_error_token(d.get('error'))}  {d['path']}"))
    parts.extend(_stdout_section("Dry-run (no changes made):", dry, lambda d:
        f"  DRY  {d['path']}"))

    parts.append("")
    parts.append(f"Full report: ~/.video_optimizer/reports/run-{run_id}.txt")
    if encoded:
        parts.append(f'To delete the {len(encoded)} originals listed under '
                     f'"Encoded":')
        parts.append(f"  ./video_optimizer.py cleanup --run {run_id} --apply")
    return "\n".join(parts)


def _persist_lines(buckets: dict[str, list[dict]]) -> str:
    """Compose the tab-separated machine-greppable persist text."""
    pp: list[str] = []
    for d in buckets["encoded"]:
        pp.append("\t".join((
            "OK", _format_savings_mb(d.get("actual_savings_mb")),
            d.get("output_path") or "", d["path"])))
    for d in buckets["failed"]:
        pp.append("\t".join(("FAIL", _error_token(d.get("error")), d["path"])))
    for d in buckets["skipped"]:
        pp.append("\t".join(("SKIP", _error_token(d.get("error")), d["path"])))
    for d in buckets["dry"]:
        pp.append("\t".join(("DRY", "dry-run", d["path"])))
    return "\n".join(pp) + ("\n" if pp else "")


def format_run_report(decisions: list[dict],
                      runs_row: dict) -> tuple[str, str]:
    """Render the post-apply per-run report.

    Returns (stdout_text, persist_text). See module-level comment for the
    persist format. The stdout text is human-readable: header tally,
    per-status sections (omitted when empty), and a `cleanup` hint if any
    row succeeded.
    """
    run_id = runs_row.get("id", 0)
    buckets = _bucket_decisions(decisions)
    return _stdout_lines(buckets, run_id), _persist_lines(buckets)
