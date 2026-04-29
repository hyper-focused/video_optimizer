"""Shared test fixtures: synthetic ProbeResult builders.

Tests use these to construct just enough of a probe to exercise a
specific code path without dragging in ffprobe or a real file.
"""

from __future__ import annotations

from optimizer.models import AudioTrack, ProbeResult, SubtitleTrack


def make_probe(
    *,
    audio: list[AudioTrack] | None = None,
    subs: list[SubtitleTrack] | None = None,
    height: int = 1080,
    width: int | None = None,
    bit_depth: int = 8,
    codec: str = "hevc",
    duration: float = 3600.0,
    is_hdr: bool = False,
    color_primaries: str | None = None,
    color_transfer: str | None = None,
    color_space: str | None = None,
    video_bitrate: int = 20_000_000,
    dv_profile: int | None = None,
) -> ProbeResult:
    """Construct a synthetic ProbeResult.

    Defaults are neutral; override only what each test cares about.
    Width is auto-derived from height (1080→1920, 2160→3840) unless
    provided explicitly.
    """
    if width is None:
        width = 3840 if height >= 1440 else 1920
    return ProbeResult(
        path="/tmp/x.mkv",
        size=10_000_000_000,
        mtime=0.0,
        duration_seconds=duration,
        container="matroska",
        format_name="matroska,webm",
        video_codec=codec,
        width=width,
        height=height,
        frame_rate=23.976,
        pixel_format="yuv420p10le" if bit_depth >= 10 else "yuv420p",
        bit_depth=bit_depth,
        video_bitrate=video_bitrate,
        color_primaries=color_primaries,
        color_transfer=color_transfer,
        color_space=color_space,
        is_hdr=is_hdr,
        audio_tracks=audio or [],
        subtitle_tracks=subs or [],
        dv_profile=dv_profile,
    )


def aud(
    idx: int,
    codec: str,
    lang: str | None,
    channels: int,
    *,
    title: str | None = None,
    default: bool = False,
    bitrate: int | None = None,
) -> AudioTrack:
    """Concise AudioTrack constructor for test scenarios."""
    layout = "stereo" if channels == 2 else (
        "5.1" if channels == 6 else "7.1" if channels == 8 else f"{channels}.0"
    )
    return AudioTrack(
        index=idx,
        codec=codec,
        language=lang,
        channels=channels,
        channel_layout=layout,
        bitrate=bitrate,
        title=title,
        default=default,
    )


def sub(
    idx: int,
    codec: str,
    lang: str | None,
    *,
    title: str | None = None,
    default: bool = False,
    forced: bool = False,
) -> SubtitleTrack:
    """Concise SubtitleTrack constructor."""
    return SubtitleTrack(
        index=idx,
        codec=codec,
        language=lang,
        forced=forced,
        default=default,
        title=title,
    )
