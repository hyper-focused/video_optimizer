"""Data models for video_optimizer.

All structures are plain dataclasses. JSON ser/de helpers live here so that
db.py can persist a ProbeResult as a single TEXT blob without each call site
re-implementing it.
"""

from __future__ import annotations

import datetime as _dt
import json
from dataclasses import asdict, dataclass, field
from typing import Any

from .presets import EST_OUTPUT_MB_PER_HOUR

# --------------------------------------------------------------------------- #
# Stream models
# --------------------------------------------------------------------------- #


@dataclass
class AudioTrack:
    """Single audio stream extracted from ffprobe output."""
    index: int
    codec: str
    language: str | None
    channels: int
    channel_layout: str | None
    bitrate: int | None
    title: str | None
    default: bool


@dataclass
class SubtitleTrack:
    """Single subtitle stream extracted from ffprobe output."""
    index: int
    codec: str
    language: str | None
    forced: bool
    default: bool
    title: str | None


# --------------------------------------------------------------------------- #
# Probe result
# --------------------------------------------------------------------------- #


@dataclass
class ProbeResult:
    """Everything we extract from one ffprobe call: container, video, audio, subs."""
    path: str
    size: int
    mtime: float

    duration_seconds: float
    container: str            # canonical container key: mp4, mkv, avi, ...
    format_name: str          # raw ffprobe format_name (comma-joined)

    video_codec: str
    width: int
    height: int
    frame_rate: float
    pixel_format: str
    bit_depth: int            # 8 / 10 / 12 — parsed from pix_fmt
    video_bitrate: int        # bps; estimate if stream-level absent

    color_primaries: str | None
    color_transfer: str | None
    color_space: str | None
    is_hdr: bool

    audio_tracks: list[AudioTrack] = field(default_factory=list)
    subtitle_tracks: list[SubtitleTrack] = field(default_factory=list)

    creation_time: _dt.datetime | None = None

    # DV profile from DOVI config side data; None if not DV.
    # See NOTES.md#dolby-vision-pipeline. Default keeps older cache JSON
    # round-tripping.
    dv_profile: int | None = None

    # ---- convenience -------------------------------------------------------

    @property
    def resolution_class(self) -> str:
        """Coarse resolution bucket used by rules and reports."""
        h = self.height
        if h <= 0:
            return "unknown"
        if h <= 480:
            return "480p"
        if h <= 720:
            return "720p"
        if h <= 1080:
            return "1080p"
        if h <= 1440:
            return "1440p"
        return "2160p"


# --------------------------------------------------------------------------- #
# Rules / decisions
# --------------------------------------------------------------------------- #


@dataclass
class RuleVerdict:
    """One rule's verdict on a probed file (fired/not, reason, savings estimate)."""
    rule: str                       # rule name (e.g. "over_bitrate")
    fired: bool
    reason: str = ""
    severity: str = "low"           # low | medium | high
    projected_savings_mb: float | None = None
    notes: dict[str, Any] = field(default_factory=dict)


@dataclass
class Candidate:
    """A probed file the rules engine recommends for re-encoding (or remux)."""
    probe: ProbeResult
    fired: list[RuleVerdict]
    target: str                     # e.g. "av1+mkv", "hevc+mp4", "h264+mp4"
    remux_only: bool                # container-only fast path
    is_hdr: bool                    # mirrored from probe for filtering ease

    @property
    def total_projected_savings_mb(self) -> float:
        """Best per-rule savings, capped at a realistic output size.

        Per-rule projections are competing estimates of the same encode,
        not orthogonal contributions — so max(), not sum(). Then ceiling
        by a tier-aware estimate of what the AV1 output will actually
        cost: `source - duration × EST_OUTPUT_MB_PER_HOUR[bucket]`. The
        over_bitrate rule otherwise projects "if video dropped to
        target_mbps", which ignores audio + the fact that archive-grade
        CQ encodes run above the rate-flag target. Final fallback: 95%
        of source size.
        """
        if not self.fired:
            return 0.0
        best = max((v.projected_savings_mb or 0.0) for v in self.fired)
        size_mb = (self.probe.size or 0) / (1024 * 1024)
        # Tier-aware realistic ceiling. Lookup miss (unknown bucket) or
        # zero-duration source falls through to the 95% size cap below.
        rate = EST_OUTPUT_MB_PER_HOUR.get(self.probe.resolution_class)
        if rate is not None and self.probe.duration_seconds > 0:
            expected_output_mb = (self.probe.duration_seconds / 3600.0) * rate
            realistic_savings = max(size_mb - expected_output_mb, 0.0)
            best = min(best, realistic_savings)
        return min(best, size_mb * 0.95)

    @property
    def rule_names(self) -> list[str]:
        """Names of every fired rule, in the order they fired."""
        return [v.rule for v in self.fired]


# --------------------------------------------------------------------------- #
# JSON helpers
# --------------------------------------------------------------------------- #


def _default(o: Any) -> Any:
    if isinstance(o, _dt.datetime):
        return o.isoformat()
    raise TypeError(f"Cannot serialize {type(o).__name__}")


def to_json(obj: Any) -> str:
    """Serialize a dataclass (or list of dataclasses) to a compact JSON string."""
    if isinstance(obj, list):
        return json.dumps([asdict(o) for o in obj], default=_default)
    return json.dumps(asdict(obj), default=_default)


def probe_from_dict(d: dict[str, Any]) -> ProbeResult:
    """Inverse of asdict(probe). Reconstructs nested dataclasses + datetime."""
    audio = [AudioTrack(**a) for a in d.get("audio_tracks", [])]
    subs = [SubtitleTrack(**s) for s in d.get("subtitle_tracks", [])]

    ct = d.get("creation_time")
    creation_time: _dt.datetime | None = None
    if ct:
        try:
            creation_time = _dt.datetime.fromisoformat(ct)
        except ValueError:
            creation_time = None

    fields = {k: v for k, v in d.items()
              if k not in ("audio_tracks", "subtitle_tracks", "creation_time")}
    return ProbeResult(
        audio_tracks=audio,
        subtitle_tracks=subs,
        creation_time=creation_time,
        **fields,
    )


def probe_from_json(s: str) -> ProbeResult:
    """Parse a JSON string previously produced by `to_json(probe)`."""
    return probe_from_dict(json.loads(s))
