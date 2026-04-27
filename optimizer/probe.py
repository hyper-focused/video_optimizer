"""ffprobe wrapper.

Runs ffprobe once per file and reduces the JSON output to a ProbeResult.
"""

from __future__ import annotations

import datetime as _dt
import json
import re
import subprocess
from pathlib import Path

from .models import AudioTrack, ProbeResult, SubtitleTrack

PROBE_TIMEOUT = 30  # seconds per file


# Map known file extensions to a canonical container key. The ffprobe
# format_name is unhelpfully comma-joined (e.g. "mov,mp4,m4a,3gp,3g2,mj2"), so
# the extension is more reliable for routing decisions.
_CONTAINER_BY_EXT = {
    ".mp4": "mp4",  ".m4v": "mp4",
    ".mov": "mov",
    ".mkv": "mkv",  ".webm": "webm",
    ".avi": "avi",
    ".wmv": "wmv",  ".asf": "asf",
    ".flv": "flv",
    ".mpg": "mpeg", ".mpeg": "mpeg", ".vob": "vob",
    ".ts":  "mpegts", ".m2ts": "mpegts", ".mts": "mpegts",
}


_PIX_BIT_DEPTH = re.compile(r"p(\d+)(le|be)?$")


def _bit_depth_from_pix_fmt(pix_fmt: str) -> int:
    """Parse pixel format strings like 'yuv420p10le' -> 10. Default to 8."""
    if not pix_fmt:
        return 8
    m = _PIX_BIT_DEPTH.search(pix_fmt)
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            pass
    return 8


def _is_hdr(color_transfer: str | None,
            color_primaries: str | None,
            color_space: str | None,
            bit_depth: int) -> bool:
    """Heuristic HDR detection.

    HDR10 -> transfer 'smpte2084'. HLG -> transfer 'arib-std-b67'.
    Some sources only carry primaries/colorspace; treat 10-bit BT.2020 as HDR
    too. False positives possible but acceptable for an advisory rule.
    """
    if color_transfer in ("smpte2084", "arib-std-b67"):
        return True
    if bit_depth >= 10 and (color_primaries == "bt2020"
                            or color_space in ("bt2020nc", "bt2020c")):
        return True
    return False


def _parse_creation_time(tags: dict | None) -> _dt.datetime | None:
    if not tags:
        return None
    for key in ("creation_time", "date", "CreationDate"):
        raw = tags.get(key)
        if not raw:
            continue
        try:
            if "T" in raw:
                return _dt.datetime.fromisoformat(raw.replace("Z", "+00:00"))
            # Tag-style "%Y-%m-%d %H:%M:%S" rarely carries tz info; ffmpeg
            # writes UTC for container creation_time, so assume that.
            naive = _dt.datetime.strptime(raw, "%Y-%m-%d %H:%M:%S")  # noqa: DTZ007
            return naive.replace(tzinfo=_dt.timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


def _parse_frame_rate(rate_str: str | None) -> float:
    if not rate_str:
        return 0.0
    try:
        if "/" in rate_str:
            num, den = rate_str.split("/", 1)
            den_f = float(den)
            return float(num) / den_f if den_f else 0.0
        return float(rate_str)
    except (ValueError, ZeroDivisionError):
        return 0.0


def _container_key(path: Path, format_name: str) -> str:
    ext_key = _CONTAINER_BY_EXT.get(path.suffix.lower())
    if ext_key:
        return ext_key
    # Fall back to first segment of format_name.
    return format_name.split(",")[0] if format_name else path.suffix.lstrip(".").lower()


def _int_or_none(v) -> int | None:
    try:
        return int(v) if v is not None else None
    except (ValueError, TypeError):
        return None


def _video_bitrate_estimate(stream_br: int | None,
                            format_br: int | None,
                            size: int,
                            duration: float,
                            audio_brs: list[int | None]) -> int:
    """Best-effort video bitrate in bps.

    Priority: stream bit_rate -> format bit_rate minus known audio -> size/dur.
    """
    if stream_br and stream_br > 0:
        return stream_br
    if format_br and format_br > 0:
        # Subtract known audio bitrates; remainder is video + overhead.
        audio_total = sum(b for b in audio_brs if b)
        est = format_br - audio_total
        if est > 0:
            return est
        return format_br
    if size > 0 and duration > 0:
        return int((size * 8) / duration)
    return 0


def _run_ffprobe(path: Path) -> dict:
    """Invoke ffprobe in JSON mode and return the parsed payload."""
    cmd = [
        "ffprobe",
        "-v", "error",
        "-print_format", "json",
        "-show_format",
        "-show_streams",
        str(path),
    ]
    proc = subprocess.run(
        cmd, capture_output=True, text=True, timeout=PROBE_TIMEOUT, check=True,
    )
    return json.loads(proc.stdout)


def _build_audio_tracks(streams: list[dict]) -> list[AudioTrack]:
    """Translate ffprobe stream entries of type=audio into AudioTrack records."""
    tracks: list[AudioTrack] = []
    for s in streams:
        if s.get("codec_type") != "audio":
            continue
        tags = s.get("tags") or {}
        disposition = s.get("disposition") or {}
        tracks.append(AudioTrack(
            index=s.get("index", -1),
            codec=s.get("codec_name", ""),
            language=tags.get("language"),
            channels=int(s.get("channels") or 0),
            channel_layout=s.get("channel_layout"),
            bitrate=_int_or_none(s.get("bit_rate")),
            title=tags.get("title"),
            default=bool(disposition.get("default")),
        ))
    return tracks


def _build_subtitle_tracks(streams: list[dict]) -> list[SubtitleTrack]:
    """Translate ffprobe stream entries of type=subtitle into SubtitleTrack records."""
    tracks: list[SubtitleTrack] = []
    for s in streams:
        if s.get("codec_type") != "subtitle":
            continue
        tags = s.get("tags") or {}
        disposition = s.get("disposition") or {}
        tracks.append(SubtitleTrack(
            index=s.get("index", -1),
            codec=s.get("codec_name", ""),
            language=tags.get("language"),
            forced=bool(disposition.get("forced")),
            default=bool(disposition.get("default")),
            title=tags.get("title"),
        ))
    return tracks


def probe_file(path: Path) -> ProbeResult:
    """Run ffprobe and return a ProbeResult.

    Raises CalledProcessError or json.JSONDecodeError on hard failure.
    For unreadable / un-probeable files, the caller should catch and skip.
    """
    data = _run_ffprobe(path)
    fmt = data.get("format", {}) or {}
    streams = data.get("streams", []) or []

    # The primary video stream drives codec/resolution/HDR detection. Files
    # with multiple video streams (alternate angles) are uncommon and out of
    # scope for v1; we just take the first.
    video_stream = next((s for s in streams if s.get("codec_type") == "video"), None)
    if video_stream is None:
        raise ValueError(f"No video stream in {path}")

    duration = float(fmt.get("duration") or 0.0)
    size = int(fmt.get("size") or path.stat().st_size)
    format_name = fmt.get("format_name", "") or ""
    pix_fmt = video_stream.get("pix_fmt", "") or ""
    bit_depth = _bit_depth_from_pix_fmt(pix_fmt)
    color_transfer = video_stream.get("color_transfer")
    color_primaries = video_stream.get("color_primaries")
    color_space = video_stream.get("color_space")

    audio_tracks = _build_audio_tracks(streams)
    subtitle_tracks = _build_subtitle_tracks(streams)

    video_br = _video_bitrate_estimate(
        stream_br=_int_or_none(video_stream.get("bit_rate")),
        format_br=_int_or_none(fmt.get("bit_rate")),
        size=size,
        duration=duration,
        audio_brs=[a.bitrate for a in audio_tracks],
    )

    return ProbeResult(
        path=str(path),
        size=size,
        mtime=path.stat().st_mtime,
        duration_seconds=duration,
        container=_container_key(path, format_name),
        format_name=format_name,
        video_codec=video_stream.get("codec_name", ""),
        width=int(video_stream.get("width") or 0),
        height=int(video_stream.get("height") or 0),
        frame_rate=_parse_frame_rate(video_stream.get("r_frame_rate")),
        pixel_format=pix_fmt,
        bit_depth=bit_depth,
        video_bitrate=video_br,
        color_primaries=color_primaries,
        color_transfer=color_transfer,
        color_space=color_space,
        is_hdr=_is_hdr(color_transfer, color_primaries, color_space, bit_depth),
        audio_tracks=audio_tracks,
        subtitle_tracks=subtitle_tracks,
        creation_time=_parse_creation_time(fmt.get("tags")),
    )
