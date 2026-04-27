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
