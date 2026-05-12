"""ffmpeg command builder + runner for video_optimizer."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from .models import AudioTrack, ProbeResult, SubtitleTrack
from .presets import AV1_QSV_BASE, AV1_QSV_DEFAULT_ENCODER_PRESET, AV1_QSV_TIER

# --------------------------------------------------------------------------- #
# Targets + encoder preference
# --------------------------------------------------------------------------- #


TARGETS: dict[str, tuple[str, str]] = {
    "av1+mkv":  ("av1",  "mkv"),
    "hevc+mp4": ("hevc", "mp4"),
    "h264+mp4": ("h264", "mp4"),
}


ENCODER_PREFERENCE: dict[str, dict[str, list[str]]] = {
    "h264": {
        "auto": [
            "h264_videotoolbox", "h264_qsv", "h264_nvenc",
            "h264_vaapi", "libx264",
        ],
        "qsv":          ["h264_qsv"],
        "nvenc":        ["h264_nvenc"],
        "vaapi":        ["h264_vaapi"],
        "videotoolbox": ["h264_videotoolbox"],
        "software":     ["libx264"],
        "none":         ["libx264"],
    },
    "hevc": {
        "auto": [
            "hevc_videotoolbox", "hevc_qsv", "hevc_nvenc",
            "hevc_vaapi", "libx265",
        ],
        "qsv":          ["hevc_qsv"],
        "nvenc":        ["hevc_nvenc"],
        "vaapi":        ["hevc_vaapi"],
        "videotoolbox": ["hevc_videotoolbox"],
        "software":     ["libx265"],
        "none":         ["libx265"],
    },
    "av1": {
        "auto": [
            "av1_qsv", "av1_nvenc", "av1_vaapi",
            "libsvtav1", "libaom-av1",
        ],
        "qsv":          ["av1_qsv"],
        "nvenc":        ["av1_nvenc"],
        "vaapi":        ["av1_vaapi"],
        "videotoolbox": [],
        "software":     ["libsvtav1", "libaom-av1"],
        "none":         ["libsvtav1", "libaom-av1"],
    },
}


_VAAPI_DEVICE = "/dev/dri/renderD128"

# Image-based subtitle codecs cannot live inside MP4. Anything not in this
# set is treated as text and either copied (mkv) or converted to mov_text (mp4).
_IMAGE_SUB_CODECS = {"hdmv_pgs_subtitle", "dvd_subtitle", "dvb_subtitle", "xsub"}


# --------------------------------------------------------------------------- #
# Encoder discovery + selection
# --------------------------------------------------------------------------- #


_ENCODER_CACHE: set[str] | None = None


REQUIRED_TOOLS: tuple[str, ...] = ("ffmpeg", "ffprobe")


def check_external_tools() -> dict[str, str | None]:
    """Return {tool: absolute_path_or_None} for the ffmpeg/ffprobe binaries.

    A None value means the tool isn't on PATH. Used by the CLI for early
    preflight so users hit a clear error rather than a confusing
    FileNotFoundError deep inside scan or apply.
    """
    return {tool: shutil.which(tool) for tool in REQUIRED_TOOLS}


def get_available_encoders() -> set[str]:
    """Return cached set of ffmpeg encoder names parsed from `ffmpeg -encoders`."""
    global _ENCODER_CACHE
    if _ENCODER_CACHE is not None:
        return _ENCODER_CACHE
    encoders: set[str] = set()
    try:
        result = subprocess.run(
            ["ffmpeg", "-hide_banner", "-encoders"],
            capture_output=True, text=True, timeout=15,
        )
        for line in result.stdout.splitlines():
            parts = line.strip().split()
            if len(parts) >= 2 and parts[0].startswith("V"):
                encoders.add(parts[1])
    except (subprocess.TimeoutExpired, FileNotFoundError):
        pass
    _ENCODER_CACHE = encoders
    return encoders


def select_encoder(target: str, hwaccel: str = "auto") -> str:
    """Return best-available encoder for (target, hwaccel) or raise RuntimeError."""
    if target not in TARGETS:
        raise RuntimeError(f"Unknown target {target!r}; valid: {sorted(TARGETS)}")
    codec, _ = TARGETS[target]
    available = get_available_encoders()
    candidates = list(ENCODER_PREFERENCE.get(codec, {}).get(hwaccel, []))

    # On Linux with no VAAPI device, drop VAAPI candidates.
    if not os.path.exists(_VAAPI_DEVICE):
        candidates = [c for c in candidates if not c.endswith("_vaapi")]

    for enc in candidates:
        if enc in available:
            return enc

    for enc in ENCODER_PREFERENCE[codec]["software"]:
        if enc in available:
            return enc

    raise RuntimeError(
        f"No usable encoder for target={target} hwaccel={hwaccel}.\n"
        f"  tried: {candidates}\n"
        f"  available on this system: {sorted(available)}\n"
        f"  run `./video_optimizer.py doctor` to diagnose, or "
        f"`./video_optimizer.py list-encoders` for the full encoder list."
    )


def output_extension(target: str) -> str:
    """Return file extension (with dot) for a TARGETS key."""
    if target not in TARGETS:
        raise RuntimeError(f"Unknown target {target!r}")
    return "." + TARGETS[target][1]


# --------------------------------------------------------------------------- #
# Dolby Vision: per-profile prep strategy
# --------------------------------------------------------------------------- #


# Module-level caches for external-tool availability. Populated on first
# call; tests can reset by re-binding the relevant `*_cache` variable
# back to the sentinel.
_TOOL_UNCACHED = object()
_dovi_tool_cache: object = _TOOL_UNCACHED
_mkvmerge_cache: object = _TOOL_UNCACHED


def has_dovi_tool() -> bool:
    """Return True if `dovi_tool` is on PATH.

    Required only for Profile 7 sources (P7 → P8 conversion). The
    P8.x strip path uses ffmpeg's built-in `dovi_rpu` bsf and doesn't
    need dovi_tool. Cached at module scope.
    """
    global _dovi_tool_cache
    if _dovi_tool_cache is _TOOL_UNCACHED:
        _dovi_tool_cache = shutil.which("dovi_tool")
    return _dovi_tool_cache is not None


def has_mkvmerge() -> bool:
    """Return True if `mkvmerge` (mkvtoolnix-cli) is on PATH.

    Required for Profile 7 sources alongside dovi_tool — the P7 prep
    pipeline needs to mux the converted+stripped raw HEVC bitstream
    back into MKV with the original audio/subs. ffmpeg's matroska
    muxer chokes on raw HEVC + B-frame content with "Timestamps are
    unset in a packet" because the raw bitstream has no container
    PTS/DTS. mkvmerge handles raw HEVC + DV cleanly, by design.
    """
    global _mkvmerge_cache
    if _mkvmerge_cache is _TOOL_UNCACHED:
        _mkvmerge_cache = shutil.which("mkvmerge")
    return _mkvmerge_cache is not None


def dv_strategy(dv_profile: int | None, *,
                allow_p7_convert: bool = False) -> str | None:
    """Pick the prep strategy for a given DV profile, or None to skip.

    Returns one of:
      "p8_strip"   — single-pass `dovi_rpu=strip` via ffmpeg bsf, then
                     encode the HDR10 base layer to AV1. The default
                     for both Profile 8.x (clean HDR10 base) and
                     Profile 7 (gambles on the BL being HDR10-decodable
                     after RPU strip).
      "p7_convert" — multi-stage: extract HEVC, `dovi_tool convert
                     --discard` to flatten P7 → P8, re-mux + strip RPU,
                     then encode. Requires dovi_tool *and* mkvmerge on
                     PATH. Opt-in via `--dv-p7-convert` because the
                     pipeline is fragile (long temp files, NAS write
                     latency, mkvmerge muxer quirks) and the strip-only
                     path is a viable first attempt for most P7 sources.
      None         — skip permanently. Profile 5 (custom DV-only colour
                     space, no clean HDR10 fallback) is the only profile
                     that always falls here; P7 with `allow_p7_convert`
                     but the tools missing also lands here so the user
                     gets an explicit failure rather than a silent
                     downgrade.
    """
    if dv_profile is None:
        return None  # not DV; caller shouldn't have asked
    if dv_profile == 5:
        return None  # P5 has no clean HDR10 base layer
    if dv_profile == 7:
        if allow_p7_convert:
            # User opted into the dovi_tool pipeline. Both tools must
            # be present — fail closed rather than silently fall back.
            if has_dovi_tool() and has_mkvmerge():
                return "p7_convert"
            return None
        # Default: try the simple ffmpeg bsf strip. Many P7 sources
        # have an HDR10-compatible base layer that decodes cleanly
        # once the RPU SEIs are gone; if a specific source wedges,
        # the user can opt back into p7_convert.
        return "p8_strip"
    if dv_profile == 8:
        return "p8_strip"
    # Profile 4 / 9 / 10 (rare or AV1-native) — be conservative, skip
    return None


def build_dv_strip_command(
    probe: ProbeResult, prepared_path: Path,
    *, keep_langs: list[str] | None = None,
    target_container: str = "mkv",
    add_compat_audio: bool = True,
    original_audio: bool = False,
    original_subs: bool = False,
) -> list[str]:
    """Return ffmpeg argv that produces `prepared_path` from `probe.path`.

    Applies `dovi_rpu=strip` on `v:0` (primary video track only — bare
    `v` crashes on attached JPEG cover art) and pre-discards audio/sub
    streams the encode won't keep, saving the NAS write+read of those
    streams. See NOTES.md#bsf-scoping-pitfall and #demuxer-pre-trim-during-strip.

    Use for Profile 8.x; Profile 7 with `--dv-p7-convert` runs the
    `dovi_tool extract → mkvmerge remux` path instead.

    `keep_langs=None` falls back to copy-every-stream for callers that
    want a faithful clone minus the RPU.
    """
    cmd: list[str] = ["ffmpeg", "-hide_banner", "-nostdin", "-y"]
    if keep_langs is not None:
        # Same demuxer-side discards the encode uses. Output mux
        # renumbers streams contiguously from 0; callers that need
        # accurate stream layout for the next stage should re-probe
        # the prepared file.
        cmd += _input_discard_args(
            probe, keep_langs, target_container,
            add_compat_audio=add_compat_audio,
            original_audio=original_audio,
            original_subs=original_subs,
        )
    cmd += [
        "-i", probe.path,
        "-map", "0",
        "-c", "copy",
        # `strip=true` (not bare `strip`) is required — the bsf parses
        # bare-form as "strip is the value of an implicit option".
        "-bsf:v:0", "dovi_rpu=strip=true",
        "-progress", "pipe:1", "-nostats",
        str(prepared_path),
    ]
    return cmd


def build_dv_p7_extract_command(probe: ProbeResult) -> list[str]:
    """Return ffmpeg argv that emits a raw HEVC Annex-B bitstream on stdout.

    First stage of the Profile 7 conversion pipeline: extract the video
    stream as a clean Annex-B HEVC bitstream that `dovi_tool convert`
    can consume on stdin. Audio + subtitles are dropped here and re-
    muxed back from the original source after conversion.
    """
    return [
        "ffmpeg", "-hide_banner", "-nostdin", "-y",
        "-i", probe.path,
        "-map", "0:v:0",
        "-c", "copy",
        "-bsf:v", "hevc_mp4toannexb",
        "-f", "hevc", "-",
    ]


def build_dv_p7_convert_command(p8_hevc_path: Path) -> list[str]:
    """Return dovi_tool argv: read HEVC on stdin, write P8 HEVC to file.

    `--discard` drops the enhancement layer, flattening Profile 7
    (BL+EL+RPU) to Profile 8.1 (BL+RPU). The resulting HEVC bitstream
    still carries DV RPU; the strip stage runs after this on the
    re-muxed MKV to produce clean HDR10.

    Note: dovi_tool 2.x replaced the older `-m 2` mode flag with the
    explicit `--discard`. The 1.x mode-flag form will fail with
    "unexpected argument '-m'" on the newer CLI.
    """
    return [
        "dovi_tool", "convert", "--discard",
        "-i", "-",
        "-o", str(p8_hevc_path),
    ]


def validate_output(probe: ProbeResult,
                    output_path: Path) -> tuple[bool, str]:
    """ffprobe the encoded output; return (ok, error_message).

    Sanity checks every encode passes through before we declare it
    "completed" and let downstream operations (cleanup, etc.) trust it:

    - Output is a parseable container (ffprobe doesn't error).
    - At least one video stream is present.
    - Output's reported duration is within ±5% of the source's
      duration (catches partial / aborted encodes that exit cleanly
      but write only N seconds of an N+M-second source).
    - Output is smaller than the source (lossy AV1 re-encode of any
      reasonable source should always net out smaller; output ≥ source
      means something went wrong — typically a CQ that's too low for
      grainy 4K content where AV1 over-allocates bits to preserve
      film grain detail). Princess Bride observed live: 47 GB UHD
      HEVC source → 48.8 GB AV1 at CQ 15. Without this guard the
      "completed" label propagates to cleanup, which then unlinks
      the source the user actually wanted to keep.

    A non-zero ffprobe exit, missing video stream, out-of-band
    duration, or output ≥ source trips a "failed" verdict — the
    source stays untouched and cleanup won't unlink it. The output
    is preserved on disk for the user to inspect or delete manually.

    Caller (`_finalize_output` in cli.py) propagates the failure:
    decision is marked status='failed' instead of 'completed', so
    the cleanup 3-check guard never sees a stale "I was encoded
    successfully" claim that didn't actually finish.
    """
    try:
        result = subprocess.run(
            ["ffprobe", "-v", "error", "-print_format", "json",
             "-show_streams", "-show_entries", "format=duration",
             str(output_path)],
            capture_output=True, text=True, timeout=60,
        )
    except (subprocess.SubprocessError, OSError) as e:
        return False, f"ffprobe failed to run: {e}"
    if result.returncode != 0:
        return False, (f"ffprobe exit {result.returncode}: "
                       f"{result.stderr.strip()[:200]}")
    try:
        info = json.loads(result.stdout or "{}")
    except ValueError as e:
        return False, f"ffprobe output not JSON: {e}"
    streams = info.get("streams") or []
    video_streams = [s for s in streams if s.get("codec_type") == "video"]
    if not video_streams:
        return False, "no video stream in output"
    fmt_dur_str = (info.get("format") or {}).get("duration")
    try:
        out_dur = float(fmt_dur_str) if fmt_dur_str else 0.0
    except (ValueError, TypeError):
        out_dur = 0.0
    src_dur = float(probe.duration_seconds or 0.0)
    if src_dur <= 0:
        # Source had no duration metadata — can't validate. Trust the
        # encode (rare; mostly synthetic/streaming content the rules
        # engine wouldn't admit anyway).
        return True, ""
    # ±5% tolerance: catches partial encodes (which are typically off
    # by minutes-to-hours, far more than 5%) without false-flagging
    # encodes that drop a few sub-second trailing frames at the boundary.
    ratio = out_dur / src_dur if src_dur > 0 else 0.0
    if not (0.95 <= ratio <= 1.05):
        return False, (f"duration mismatch: source {src_dur:.1f}s, "
                       f"output {out_dur:.1f}s (ratio {ratio:.2f})")
    # Output ≥ source guard: AV1 should net smaller. ≥ source means the
    # encode misbehaved; fail so the user can inspect before cleanup
    # disposes of the original.
    try:
        out_size = output_path.stat().st_size
    except OSError as e:
        return False, f"cannot stat output: {e}"
    if probe.size and out_size >= probe.size:
        delta_mb = (out_size - probe.size) / (1024 * 1024)
        return False, (f"output ≥ source ({out_size / 1024**3:.2f} GB "
                       f"vs {probe.size / 1024**3:.2f} GB; "
                       f"+{delta_mb:.0f} MB) — encode bloated, "
                       f"likely CQ too low for grainy content")
    return True, ""


def build_dv_p7_strip_raw_command(p8_hevc_path: Path,
                                  stripped_hevc_path: Path) -> list[str]:
    """Return ffmpeg argv: read raw P8 HEVC, strip RPU, write raw HEVC.

    Stage 3a of the Profile 7 pipeline. `-c copy -f hevc` keeps the
    output in raw annex-B form so the matroska timestamp problem
    doesn't apply (no container, no PTS/DTS to worry about). The
    stripped bitstream is plain HDR10, ready for mkvmerge to mux
    alongside the source's audio/subs.

    `-progress pipe:1 -nostats` is mandatory for `run_ffmpeg`'s stall
    watchdog to do anything useful. Without progress events on
    stdout, the watchdog has no signal to monitor and a hung NAS
    write at the tail of the strip can park ffmpeg indefinitely
    (observed on a Princess Bride strip that hung for 56+ minutes
    after writing 99.87% of the output).
    """
    return [
        "ffmpeg", "-hide_banner", "-nostdin", "-y",
        "-i", str(p8_hevc_path),
        "-c", "copy",
        "-bsf:v", "dovi_rpu=strip=true",
        "-progress", "pipe:1", "-nostats",
        "-f", "hevc",
        str(stripped_hevc_path),
    ]


def build_dv_p7_mkvmerge_command(probe: ProbeResult,
                                 stripped_hevc_path: Path,
                                 prepared_path: Path) -> list[str]:
    """Return mkvmerge argv: mux stripped HEVC + source audio/subs.

    Stage 3b of the Profile 7 pipeline. mkvmerge handles raw HEVC
    cleanly, by design — it parses the bitstream and synthesises the
    matroska timestamps from the SPS/VUI rate, which ffmpeg's matroska
    muxer doesn't do. Audio and subtitles come from the original
    source; the `--no-video` switch on the source input keeps
    mkvmerge from attaching the original (unstripped) DV-tagged HEVC
    a second time.
    """
    return [
        "mkvmerge", "-q",  # -q suppresses chatty progress
        "-o", str(prepared_path),
        # Stripped HEVC video (only video; mkvmerge auto-detects rate).
        "--no-audio", "--no-subtitles", "--no-chapters", "--no-attachments",
        str(stripped_hevc_path),
        # Original source: take its audio + subs + chapters; drop the
        # original (unstripped) video stream.
        "--no-video", "--no-attachments",
        probe.path,
    ]


# --------------------------------------------------------------------------- #
# Stream mapping
# --------------------------------------------------------------------------- #


def _color_passthrough_args(probe: ProbeResult) -> list[str]:
    """Mirror the source's color tagging to the output, when present.

    Forwards color_primaries / color_transfer / color_space from the probed
    source so HDR sources stay correctly tagged (bt2020/smpte2084/bt2020nc)
    and SDR sources keep their bt709/etc. metadata. Forcing fixed HDR values
    on every encode (as the reference shell script does) would mis-tag SDR
    output; passthrough is the right behaviour for a generic tool.
    """
    args: list[str] = []
    if probe.color_primaries:
        args += ["-color_primaries", probe.color_primaries]
    if probe.color_transfer:
        args += ["-color_trc", probe.color_transfer]
    if probe.color_space:
        args += ["-colorspace", probe.color_space]
    return args


# ISO 639-1 ↔ 639-2 equivalents. ffprobe emits 3-letter codes ("eng");
# --keep-langs default is "en,und" — without this expansion non-default
# English tracks would silently fail to match.
_LANG_EQUIVS: dict[str, str] = {
    "en": "eng", "eng": "en",
    "ja": "jpn", "jpn": "ja",
    "de": "ger", "ger": "de", "deu": "de",
    "fr": "fre", "fre": "fr", "fra": "fr",
    "es": "spa", "spa": "es",
    "it": "ita", "ita": "it",
    "ru": "rus", "rus": "ru",
    "zh": "chi", "chi": "zh", "zho": "zh",
    "ko": "kor", "kor": "ko",
    "pt": "por", "por": "pt",
    "nl": "dut", "dut": "nl", "nld": "nl",
}


def _expand_langs(langs: set[str]) -> set[str]:
    """Return langs plus their ISO 639-1/2 equivalents (lowercased)."""
    out = {lang.lower() for lang in langs}
    out.update(_LANG_EQUIVS.get(lang, lang) for lang in list(out))
    return out


# Lossless / hi-res audio. DTS is conditional — DTS-HD MA reports as plain
# `dts`, only treat it as hi-res when channels >= 6 (DTS Core 2.0 isn't).
_LOSSLESS_AUDIO_CODECS = frozenset({"truehd", "mlp", "flac"})

# Higher rank = better source for the AAC compat transcode.
_AUDIO_QUALITY_RANK: dict[str, int] = {
    "truehd": 100,
    "mlp":    100,
    "flac":    90,
    "dts":     80,   # includes DTS-HD MA when channels >= 6
    "eac3":    60,
    "opus":    55,
    "ac3":     50,
    "aac":     45,
    "mp3":     30,
}


def _is_hires_lossless(track: AudioTrack) -> bool:
    """True when the track is lossless or a hi-res lossy worth shadowing."""
    codec = track.codec.lower()
    if codec in _LOSSLESS_AUDIO_CODECS or codec.startswith("pcm_"):
        return True
    return codec == "dts" and track.channels >= 6


def _is_commentary(track: AudioTrack) -> bool:
    """True when the track's title labels it as commentary.

    Commentary tracks pass the language filter (almost always tagged
    English) but are noise for an archive workflow — they bloat files
    and aren't what someone wants when they pick "English audio". Match
    on title rather than codec/channels because a commentary track and
    a primary track can share both.
    """
    return bool(track.title and "commentary" in track.title.lower())


def _audio_quality_rank(codec: str) -> int:
    """Rank an audio codec by perceptual quality; higher is better."""
    return _AUDIO_QUALITY_RANK.get(codec.lower(), 10)


def _eligible_tracks(
    probe: ProbeResult, langs: set[str],
) -> list[tuple[int, AudioTrack]]:
    """Source tracks passing the language + commentary filters.

    Used as the candidate pool for `_build_audio_ladder`. Always returns at
    least one track when the source has audio (safety: never produce a
    silent file).

    Eligibility is **language-only**: a track passes if its lang is in
    `langs`, OR — if the source has no default flag set anywhere — if it's
    the first track (a heuristic for sources missing disposition info).
    The default flag is intentionally NOT consulted: some release tools
    (BEN.THE.MEN among them) set default=True on every track, which
    flooded eligibility with foreign-lang tracks and produced ladders
    like "English DTS-HD MA s0 + Italian DTS 5.1 s1." The "no English
    audio at all" case is handled by the safety net at the bottom, not
    by the predicate.
    """
    have_default = any(a.default for a in probe.audio_tracks)
    out: list[tuple[int, AudioTrack]] = []
    for i, a in enumerate(probe.audio_tracks):
        if _is_commentary(a):
            continue
        lang = (a.language or "").lower()
        if lang in langs or (not have_default and i == 0):
            out.append((i, a))
    if not out and probe.audio_tracks:
        for i, a in enumerate(probe.audio_tracks):
            if not _is_commentary(a):
                out.append((i, a))
                break
        if not out:
            out.append((0, probe.audio_tracks[0]))
    return out


def _track_quality_key(track: AudioTrack) -> tuple:
    """Sort key for picking the 'best' track. Higher tuples sort later (use max)."""
    return (
        _is_hires_lossless(track),
        _audio_quality_rank(track.codec),
        track.channels,
        track.default,
    )


def _build_audio_ladder(
    probe: ProbeResult, langs: set[str],
) -> list[tuple[str, int, AudioTrack]]:
    """Build the standardized 3-stream audio output ladder.

    Output stream layout (deterministic, regardless of source):
      0 — best available track (lossless preferred), passthrough
      1 — 5.1 tier: a different 5.1 source if present, otherwise Opus 5.1
          encoded from stream 0 (only when stream 0 has >= 6 channels)
      2 — 2.0 tier: a different 2.0 source if present, otherwise AAC 2.0
          encoded from stream 0 (with downmix when stream 0 has > 2 channels,
          or as a lossy fallback when stream 0 is lossless 2.0)

    Streams 1 and 2 are skipped when the source can't sensibly produce them
    (e.g. stereo-only lossy source produces just stream 0). Returns a list
    of (kind, src_idx, track) tuples in output order. kind is one of:
    'copy', 'opus51', 'aac20'.
    """
    eligible = _eligible_tracks(probe, langs)
    if not eligible:
        return []

    # Stream 0: the best available track.
    s0_idx, s0_track = max(eligible, key=lambda it: _track_quality_key(it[1]))
    ladder: list[tuple[str, int, AudioTrack]] = [("copy", s0_idx, s0_track)]
    used = {s0_idx}

    # Stream 1: 5.1 tier.
    five_one = [it for it in eligible
                if it[0] not in used and it[1].channels == 6]
    if five_one:
        s1_idx, s1_track = max(five_one, key=lambda it: _track_quality_key(it[1]))
        ladder.append(("copy", s1_idx, s1_track))
        used.add(s1_idx)
    elif s0_track.channels >= 6:
        # Synthesize 5.1 from stream 0 (it has the channels for it).
        ladder.append(("opus51", s0_idx, s0_track))
    # else: source has no 5.1-capable audio; skip the 5.1 tier (no upmix).

    # Stream 2: 2.0 tier.
    stereo = [it for it in eligible
              if it[0] not in used and it[1].channels == 2]
    if stereo:
        s2_idx, s2_track = max(stereo, key=lambda it: _track_quality_key(it[1]))
        ladder.append(("copy", s2_idx, s2_track))
    elif s0_track.channels > 2 or _is_hires_lossless(s0_track):
        # Downmix-from-surround OR lossless-stereo-fallback. Skip when
        # stream 0 is already lossy stereo (a re-encode would be redundant).
        ladder.append(("aac20", s0_idx, s0_track))

    return ladder


def _kept_audio_metadata(out_idx: int, track: AudioTrack) -> list[str]:
    """Re-set language/title on a copied audio track.

    Needed because the build_*_command paths strip per-stream metadata
    (`-map_metadata:s -1`) to evict source mkvmerge statistics tags
    (BPS, NUMBER_OF_BYTES, etc.) that would otherwise carry verbatim and
    misreport bitrate on new streams.
    """
    args: list[str] = []
    lang = (track.language or "und").lower()
    args += [f"-metadata:s:a:{out_idx}", f"language={lang}"]
    if track.title:
        args += [f"-metadata:s:a:{out_idx}", f"title={track.title}"]
    return args


def _kept_subtitle_metadata(out_idx: int, track: SubtitleTrack) -> list[str]:
    """Re-set language/title on a copied subtitle track (mirrors the audio helper)."""
    args: list[str] = []
    lang = (track.language or "und").lower()
    args += [f"-metadata:s:s:{out_idx}", f"language={lang}"]
    if track.title:
        args += [f"-metadata:s:s:{out_idx}", f"title={track.title}"]
    return args


def _compat_track_args(out_idx: int, src_in_idx: int, *, codec: str,
                       channels: int, bitrate: str, lang: str,
                       title: str) -> list[str]:
    """Build the -map + per-stream codec args for one compat audio track.

    Disposition is intentionally not set here — `_audio_map_args` assigns
    dispositions to all output streams centrally after the ladder is built.
    """
    return [
        "-map", f"0:a:{src_in_idx}?",
        f"-c:a:{out_idx}", codec,
        f"-b:a:{out_idx}", bitrate,
        f"-ac:a:{out_idx}", str(channels),
        f"-ar:a:{out_idx}", "48000",
        f"-metadata:s:a:{out_idx}", f"title={title}",
        f"-metadata:s:a:{out_idx}", f"language={lang}",
    ]


def _audio_map_args(probe: ProbeResult, langs: set[str], *,
                    add_compat: bool = True) -> list[str]:
    """-map / -c:a fragment for the standardized 3-stream audio ladder.

    Output is always:
      stream 0 — the highest-quality eligible track, passthrough (default)
      stream 1 — best 5.1 in source if present, else Opus 5.1 @ 384k from
                 stream 0 (only when stream 0 has >= 6 channels)
      stream 2 — best 2.0 in source if present, else AAC 2.0 @ 256k from
                 stream 0 (downmix or lossless-fallback)

    Streams 1 and 2 may be skipped when the source can't sensibly produce
    them (stereo-only sources, or lossy stereo where the lossy fallback
    would just duplicate stream 0).

    `add_compat=False` (--no-compat-audio) collapses output to stream 0
    only — escape hatch for callers who want just the best track.
    """
    eligible = _eligible_tracks(probe, langs)
    if not eligible:
        return []

    if not add_compat:
        # Just the best track. Used by --no-compat-audio.
        s0_idx, s0_track = max(eligible, key=lambda it: _track_quality_key(it[1]))
        args = ["-map", f"0:a:{s0_idx}?", "-c:a:0", "copy"]
        args += _kept_audio_metadata(0, s0_track)
        args += ["-disposition:a:0", "default"]
        return args

    ladder = _build_audio_ladder(probe, langs)
    if not ladder:
        return []

    args: list[str] = []
    for out_idx, (kind, src_idx, track) in enumerate(ladder):
        if kind == "copy":
            args += ["-map", f"0:a:{src_idx}?",
                     f"-c:a:{out_idx}", "copy"]
            args += _kept_audio_metadata(out_idx, track)
        elif kind == "opus51":
            args += _compat_track_args(
                out_idx, src_idx,
                codec="libopus", channels=6, bitrate="384k",
                lang=(track.language or "und").lower(),
                title="Opus 5.1 (compat)",
            )
        elif kind == "aac20":
            args += _compat_track_args(
                out_idx, src_idx,
                codec="aac", channels=2, bitrate="256k",
                lang=(track.language or "und").lower(),
                title="AAC 2.0 (compat)",
            )

    # Explicit dispositions: stream 0 default, others non-default. Override
    # source disposition (a passthrough 5.1 may have been default in source).
    args += ["-disposition:a:0", "default"]
    for i in range(1, len(ladder)):
        args += [f"-disposition:a:{i}", "0"]
    return args


def _subtitle_map_args(probe: ProbeResult, langs: set[str],
                       target_container: str,
                       *, original_subs: bool = False) -> list[str]:
    """-map / -c:s fragment that keeps subs matching `langs`, dropping image
    subs on mp4 (warning to stderr) and converting text subs to mov_text.

    original_subs=True bypasses the language filter and keeps every input
    subtitle track. mp4-incompatible image subtitles are still dropped
    (the container fundamentally cannot hold them); for mkv targets every
    subtitle stream is preserved bit-perfectly.
    """
    kept: list[int] = []
    for i, s in enumerate(probe.subtitle_tracks):
        lang = (s.language or "").lower()
        if not original_subs and lang not in langs:
            continue
        if target_container == "mp4" and s.codec in _IMAGE_SUB_CODECS:
            sys.stderr.write(
                f"warning: dropping image subtitle stream {i} ({s.codec}, "
                f"lang={lang}) — cannot live in mp4\n"
            )
            continue
        kept.append(i)

    args: list[str] = []
    for out_idx, i in enumerate(kept):
        args += ["-map", f"0:s:{i}?"]
        args += _kept_subtitle_metadata(out_idx, probe.subtitle_tracks[i])
    if kept:
        # mkv preserves any sub format; mp4 needs text subs converted.
        args += ["-c:s", "copy"] if target_container == "mkv" else ["-c:s", "mov_text"]
    return args


def _kept_audio_indices(probe: ProbeResult, langs: set[str],
                        *, add_compat: bool = True) -> set[int]:
    """Source audio indices that survive into the output ladder.

    All synth tiers (Opus 5.1, AAC 2.0) read from the s0 source, so the
    kept set is a strict subset of {ladder.src_idx}. Used by the discard
    pre-strip to tell the demuxer which audio streams it can drop before
    they enter the per-stream packet queues.
    """
    if not probe.audio_tracks:
        return set()
    eligible = _eligible_tracks(probe, langs)
    if not eligible:
        return set()
    if not add_compat:
        s0_idx, _ = max(eligible, key=lambda it: _track_quality_key(it[1]))
        return {s0_idx}
    ladder = _build_audio_ladder(probe, langs)
    return {src_idx for _, src_idx, _ in ladder}


def _kept_subtitle_indices(probe: ProbeResult, langs: set[str],
                           target_container: str,
                           *, original_subs: bool = False) -> set[int]:
    """Source subtitle indices that survive into the output (mirrors the
    filter inside `_subtitle_map_args` so we can pre-strip the rest)."""
    kept: set[int] = set()
    for i, s in enumerate(probe.subtitle_tracks):
        lang = (s.language or "").lower()
        if not original_subs and lang not in langs:
            continue
        if target_container == "mp4" and s.codec in _IMAGE_SUB_CODECS:
            continue
        kept.add(i)
    return kept


def _input_discard_args(probe: ProbeResult, keep_langs: list[str],
                        target_container: str,
                        *, add_compat_audio: bool = True,
                        original_audio: bool = False,
                        original_subs: bool = False) -> list[str]:
    """Per-input `-discard:<spec> all` flags for streams we won't use.

    Drops streams at demux time before the QSV decoder's input queue
    can starve on the narrow windows between audio packets on multi-
    track sources (classic frame-0 stall on Blu-ray remuxes with 8+
    audio dubs). Must precede `-i`. Discard preserves source-side
    indexing — `-map 0:a:1?` still resolves correctly.

    `original_audio=True` keeps every audio stream; the multi-language
    stall risk re-appears, but that's the conscious tradeoff.
    """
    langs = _expand_langs({(lang or "").lower() for lang in keep_langs})
    kept_s = _kept_subtitle_indices(probe, langs, target_container,
                                    original_subs=original_subs)
    args: list[str] = []
    if not original_audio:
        kept_a = _kept_audio_indices(probe, langs, add_compat=add_compat_audio)
        for i in range(len(probe.audio_tracks)):
            if i not in kept_a:
                args += [f"-discard:a:{i}", "all"]
    for i in range(len(probe.subtitle_tracks)):
        if i not in kept_s:
            args += [f"-discard:s:{i}", "all"]
    return args


def build_stream_map_args(probe: ProbeResult, keep_langs: list[str],
                          target_container: str,
                          *, add_compat_audio: bool = True,
                          original_audio: bool = False,
                          original_subs: bool = False) -> list[str]:
    """Return -map + audio/subtitle codec args for chosen streams.

    Attachments (embedded fonts, cover art) are intentionally NOT mapped
    — a single attachment with an undeducible mimetype crashes the
    matroska muxer ("no mimetype tag … incorrect codec parameters").
    Fail cost outweighs the rare ASS/SSA font benefit.

    original_audio=True bypasses the 3-stream ladder and stream-copies
    every input audio track. Subtitle handling is unaffected.
    """
    langs = _expand_langs({(lang or "").lower() for lang in keep_langs})
    args: list[str] = ["-map", "0:v:0"]
    if original_audio:
        args += ["-map", "0:a?", "-c:a", "copy"]
    else:
        args += _audio_map_args(probe, langs, add_compat=add_compat_audio)
    args += _subtitle_map_args(probe, langs, target_container,
                               original_subs=original_subs)
    return args


# --------------------------------------------------------------------------- #
# Codec arg builder
# --------------------------------------------------------------------------- #


def _quality_default(encoder: str) -> int:
    """Return reasonable default quality value for an encoder."""
    if encoder == "libx264":
        return 23
    if encoder == "libx265":
        return 26
    if encoder in ("libsvtav1", "libaom-av1"):
        return 30
    if encoder.endswith("_qsv"):
        return 24
    if encoder.endswith("_nvenc"):
        return 24
    if encoder.endswith("_vaapi"):
        return 24
    if encoder.endswith("_videotoolbox"):
        return 65
    return 23


def _software_args(encoder: str, quality: int) -> list[str] | None:
    """Codec args for the software encoders (libx264/libx265/libsvtav1/libaom-av1)."""
    if encoder == "libx264":
        return ["-c:v", "libx264", "-preset", "medium", "-crf", str(quality)]
    if encoder == "libx265":
        return ["-c:v", "libx265", "-preset", "medium",
                "-crf", str(quality), "-tag:v", "hvc1"]
    if encoder == "libsvtav1":
        return ["-c:v", "libsvtav1", "-preset", "6",
                "-crf", str(quality), "-pix_fmt", "yuv420p10le"]
    if encoder == "libaom-av1":
        return ["-c:v", "libaom-av1", "-cpu-used", "4",
                "-crf", str(quality), "-b:v", "0"]
    return None


def _qsv_args(encoder: str, quality: int, *,
              is_uhd: bool = False, bit_depth: int = 8,
              hw_decode: bool = False,
              encoder_preset: str | None = None,
              qsv_overrides: dict[str, str] | None = None) -> list[str]:
    """Codec args for Intel Quick Sync encoders.

    Tuning resolution order: qsv_overrides → AV1_QSV_TIER (per height
    bucket) → AV1_QSV_BASE. See NOTES.md#av1_qsv-tuning.

    `encoder_preset` overrides the ladder rung (default "veryslow").

    Pure ICQ — no -maxrate / -bufsize. extbrc + ICQ + maxrate collapses
    to a hybrid VBR that under-allocates by an order of magnitude.

    pix_fmt is pinned only when hw_decode=False. Pinning under
    hw_decode=True forces a qsv→p010le conversion auto_scale can't
    bridge. See NOTES.md#bit-depth-and-hw_decode-interaction.
    """
    base = AV1_QSV_BASE
    overrides = qsv_overrides or {}
    chosen_preset = encoder_preset or AV1_QSV_DEFAULT_ENCODER_PRESET

    def pick(key: str, source: dict[str, str]) -> str:
        # Per-preset override wins; otherwise tier/base default.
        return overrides.get(key, source[key])

    # `-global_quality:v` scope is load-bearing: without `:v`, libopus
    # rejects with "Quality-based encoding not supported".
    # No `-look_ahead 1`: av1_qsv ignores it and warns (use look_ahead_depth).
    a = ["-c:v", encoder, "-preset", chosen_preset,
         "-global_quality:v", str(quality)]
    if encoder == "av1_qsv":
        tier = AV1_QSV_TIER["uhd" if is_uhd else "hd"]
        a += [
            "-look_ahead_depth", pick("look_ahead_depth", tier),
            "-extbrc", pick("extbrc", base),
            "-low_power", pick("low_power", base),
            "-adaptive_i", pick("adaptive_i", base),
            "-adaptive_b", pick("adaptive_b", base),
            "-b_strategy", pick("b_strategy", base),
            "-bf", pick("bf", base),
            "-refs", pick("refs", base),
            "-g", pick("gop", tier),
            "-profile:v", pick("profile", base),
        ]
        if bit_depth >= 10 and not hw_decode:
            a += ["-pix_fmt", "p010le"]
    if encoder == "hevc_qsv":
        a += ["-tag:v", "hvc1"]
        if bit_depth >= 10:
            # Profile is fine to set in either pipeline; only pix_fmt is
            # incompatible with the qsv-surface path.
            a += ["-profile:v", "main10"]
            if not hw_decode:
                a += ["-pix_fmt", "p010le"]
    return a


def _nvenc_args(encoder: str, quality: int) -> list[str]:
    """Codec args for NVIDIA NVENC encoders."""
    a = ["-c:v", encoder, "-preset", "p5", "-tune", "hq",
         "-rc", "vbr", "-cq", str(quality), "-b:v", "0"]
    if encoder == "hevc_nvenc":
        a += ["-tag:v", "hvc1"]
    return a


def _vaapi_args(encoder: str, quality: int) -> list[str]:
    """Codec args for VAAPI encoders (Linux, /dev/dri/renderD128)."""
    a = ["-c:v", encoder, "-qp", str(quality)]
    if encoder == "hevc_vaapi":
        a += ["-tag:v", "hvc1"]
    return a


def _videotoolbox_args(encoder: str, quality: int) -> list[str]:
    """Codec args for Apple VideoToolbox encoders."""
    a = ["-c:v", encoder, "-q:v", str(quality), "-allow_sw", "1"]
    if encoder == "hevc_videotoolbox":
        a += ["-tag:v", "hvc1", "-pix_fmt", "p010le", "-profile:v", "main10"]
    return a


def _codec_args(encoder: str, quality: int, *,
                is_uhd: bool = False, bit_depth: int = 8,
                hw_decode: bool = False,
                encoder_preset: str | None = None,
                qsv_overrides: dict[str, str] | None = None) -> list[str]:
    """Return -c:v + quality/preset arg fragment for the given encoder."""
    sw = _software_args(encoder, quality)
    if sw is not None:
        return sw
    if encoder.endswith("_qsv"):
        return _qsv_args(encoder, quality, is_uhd=is_uhd,
                         bit_depth=bit_depth, hw_decode=hw_decode,
                         encoder_preset=encoder_preset,
                         qsv_overrides=qsv_overrides)
    if encoder.endswith("_nvenc"):
        return _nvenc_args(encoder, quality)
    if encoder.endswith("_vaapi"):
        return _vaapi_args(encoder, quality)
    if encoder.endswith("_videotoolbox"):
        return _videotoolbox_args(encoder, quality)
    raise RuntimeError(f"Unhandled encoder: {encoder}")


# --------------------------------------------------------------------------- #
# Command builders
# --------------------------------------------------------------------------- #


def build_remux_command(probe: ProbeResult, output_path: Path,
                        target_container: str, keep_langs: list[str],
                        *, add_compat_audio: bool = True,
                        original_audio: bool = False,
                        original_subs: bool = False,
                        source_override: str | None = None) -> list[str]:
    """Return ffmpeg argv that stream-copies into target_container.

    source_override (when set) replaces probe.path as the `-i` input.
    Used by the DV-strip pipeline so the prepared HDR10 stream-copy
    feeds the remux rather than the original DV source. Stream layout
    decisions still come from the probe.
    """
    src = source_override if source_override is not None else probe.path
    cmd: list[str] = ["ffmpeg", "-hide_banner", "-nostdin", "-y"]
    cmd += _input_discard_args(probe, keep_langs, target_container,
                               add_compat_audio=add_compat_audio,
                               original_audio=original_audio,
                               original_subs=original_subs)
    cmd += [
        "-i", src,
        "-map_metadata", "0",
        "-map_metadata:s", "-1",
        "-map_chapters", "0",
        "-c:v", "copy",
    ]
    cmd += build_stream_map_args(probe, keep_langs, target_container,
                                 add_compat_audio=add_compat_audio,
                                 original_audio=original_audio,
                                 original_subs=original_subs)
    if target_container == "mp4":
        cmd += ["-movflags", "+faststart"]
    cmd += ["-progress", "pipe:1", "-nostats", str(output_path)]
    return cmd


def build_encode_command(probe: ProbeResult, output_path: Path,
                         encoder: str, quality: int | None,
                         keep_langs: list[str], target_container: str,
                         *, hw_decode: bool = False,
                         add_compat_audio: bool = True,
                         denoise: bool = False,
                         original_audio: bool = False,
                         original_subs: bool = False,
                         source_override: str | None = None,
                         encoder_preset: str | None = None,
                         qsv_overrides: dict[str, str] | None = None,
                         ) -> list[str]:
    """Return ffmpeg argv for a real re-encode using the given encoder.

    hw_decode=True with a QSV encoder enables zero-copy GPU decode→encode
    (`-hwaccel qsv -hwaccel_output_format qsv`). Saves CPU but can fail on
    legacy codecs that the QSV decoder doesn't support, so callers (apply)
    leave it off by default and the preset wrappers turn it on.

    denoise=True inserts an `hqdn3d` software filter into the -vf chain.
    Used by the apply layer for low-bitrate h.264 and SD content where
    pre-cleaning the source helps AV1 allocate bits to real content
    rather than h.264 macroblock noise. hqdn3d is CPU-only and won't
    compose with QSV zero-copy decode; in practice this never matters
    because every condition that triggers denoise (h.264 in [720, 1440)
    or height < 720) lands in the HD preset where hw_decode is already
    False by default.
    """
    q = quality if quality is not None else _quality_default(encoder)
    # 1440p is the cutoff: anything ≥ 1440p uses UHD-tuned encoder values
    # (deeper lookahead, longer GOP); 1080p and below get HD-tuned values.
    is_uhd = probe.height >= 1440

    # -nostdin: ffmpeg defaults to interactive controls on a TTY; in a
    # non-interactive long-running batch a stray byte on stdin can wedge
    # the process. Belt-and-braces against subprocess inheritance quirks.
    cmd: list[str] = ["ffmpeg", "-hide_banner", "-nostdin", "-y"]
    if hw_decode and encoder.endswith("_qsv"):
        cmd += ["-hwaccel", "qsv", "-hwaccel_output_format", "qsv"]
    if encoder.endswith("_vaapi"):
        cmd += ["-vaapi_device", _VAAPI_DEVICE]
    cmd += _input_discard_args(probe, keep_langs, target_container,
                               add_compat_audio=add_compat_audio,
                               original_audio=original_audio,
                               original_subs=original_subs)
    src = source_override if source_override is not None else probe.path
    cmd += ["-i", src,
            "-map_metadata", "0", "-map_metadata:s", "-1",
            "-map_chapters", "0"]

    cmd += _codec_args(encoder, q, is_uhd=is_uhd,
                       bit_depth=probe.bit_depth, hw_decode=hw_decode,
                       encoder_preset=encoder_preset,
                       qsv_overrides=qsv_overrides)
    cmd += _color_passthrough_args(probe)

    # Compose video filter chain: denoise (CPU) → vaapi format/hwupload
    # (when targeting vaapi). ffmpeg only honours one -vf flag, so build
    # the comma-separated chain in order.
    vfilters: list[str] = []
    if denoise:
        # Mild settings: light luma spatial, lighter chroma spatial,
        # moderate temporal. Strong enough to clean h.264 macroblock
        # noise without crushing film grain on prosumer sources.
        vfilters.append("hqdn3d=2:1:2:3")
    if encoder.endswith("_vaapi"):
        vfilters.append("format=nv12,hwupload")
    if vfilters:
        cmd += ["-vf", ",".join(vfilters)]

    cmd += build_stream_map_args(probe, keep_langs, target_container,
                                 add_compat_audio=add_compat_audio,
                                 original_audio=original_audio,
                                 original_subs=original_subs)

    if target_container == "mp4":
        cmd += ["-movflags", "+faststart"]

    cmd += ["-progress", "pipe:1", "-nostats", str(output_path)]
    return cmd


# --------------------------------------------------------------------------- #
# Runner with progress + timeout
# --------------------------------------------------------------------------- #


_OUT_TIME_RE = re.compile(r"^out_time_ms=(\d+)$")
_FRAME_RE = re.compile(r"^frame=\s*(\d+)$")
_FPS_RE = re.compile(r"^fps=\s*([\d.]+)$")
_SPEED_RE = re.compile(r"^speed=\s*([\d.]+)x$")
_PROGRESS_RE = re.compile(r"^progress=(\w+)$")


@dataclass
class _ProgressState:
    """Live state pulled from ffmpeg's -progress feed during one encode."""

    current_seconds: float = 0.0   # encoded position in source timeline
    frames: int = 0                # decoder-side frame count (liveness signal)
    fps: float = 0.0               # frames/second the encoder is running at
    speed: float = 0.0             # speed multiplier vs realtime (e.g. 1.8x)


# Sentinel substring marking a kill triggered by `_BloatChecker`. The
# apply layer matches on this to convert a "fail" into a retry rather
# than a terminal failure. Callers that want to act on it should look
# for `BLOAT_PROJECTION_REASON in err`, not equality — the rest of the
# string carries human-readable context.
BLOAT_PROJECTION_REASON = "bloat_projection"


class _BloatChecker:
    """Mid-encode size projection.

    At each `checkpoints` fraction (0..1) of source duration, stat the
    output file and project final size as `out_size / completion_ratio`.
    If the projection >= `source_size * threshold`, `check()` returns
    a kill verdict the progress loop honours. Each checkpoint is
    consumed once — re-entering at the same threshold is a no-op so a
    healthy encode that wobbles around 10% won't trigger repeatedly.
    """

    def __init__(self, source_size: int, output_path: Path,
                 threshold: float, checkpoints: tuple[float, ...]):
        self.source_size = source_size
        self.output_path = output_path
        self.threshold = threshold
        # Sorted ascending so we always check the next-earliest first.
        self._remaining = sorted(c for c in checkpoints if 0 < c < 1)

    def check(self, current_seconds: float,
              duration_seconds: float) -> tuple[bool, str]:
        if not self._remaining or duration_seconds <= 0 or self.source_size <= 0:
            return False, ""
        ratio_done = current_seconds / duration_seconds
        if ratio_done < self._remaining[0]:
            return False, ""
        # Crossed at least one checkpoint; consume it.
        crossed = self._remaining.pop(0)
        try:
            out_size = self.output_path.stat().st_size
        except OSError:
            return False, ""
        if ratio_done < 0.01 or out_size <= 0:
            return False, ""
        projected = out_size / ratio_done
        if projected >= self.source_size * self.threshold:
            return True, (
                f"{BLOAT_PROJECTION_REASON} at {crossed * 100:.0f}%: "
                f"output {out_size / 1024 ** 3:.2f} GB, "
                f"projecting {projected / 1024 ** 3:.1f} GB vs source "
                f"{self.source_size / 1024 ** 3:.2f} GB "
                f"(threshold {self.threshold:.2f})"
            )
        return False, ""


def _format_bar(fraction: float, width: int = 20) -> str:
    """Return a textual progress bar at the given fraction (0..1)."""
    fraction = max(0.0, min(1.0, fraction))
    filled = int(width * fraction)
    return "[" + "#" * filled + "-" * (width - filled) + "]"


def _format_secs(seconds: float) -> str:
    """Format a duration as `Hh MMm` / `Mm SSs` / `Ss` for ETA display."""
    s = int(max(0, seconds))
    h, rem = divmod(s, 3600)
    m, sec = divmod(rem, 60)
    if h > 0:
        return f"{h}h{m:02d}m"
    if m > 0:
        return f"{m}m{sec:02d}s"
    return f"{sec}s"


def _parse_progress_line(line: str, state: _ProgressState,
                         duration_seconds: float) -> _ProgressState:
    """Update the running state from one ffmpeg -progress output line."""
    m = _OUT_TIME_RE.match(line)
    if m:
        state.current_seconds = int(m.group(1)) / 1_000_000.0
        return state
    m = _FRAME_RE.match(line)
    if m:
        try:
            state.frames = int(m.group(1))
        except ValueError:
            pass
        return state
    m = _FPS_RE.match(line)
    if m:
        try:
            state.fps = float(m.group(1))
        except ValueError:
            pass
        return state
    m = _SPEED_RE.match(line)
    if m:
        try:
            state.speed = float(m.group(1))
        except ValueError:
            pass
        return state
    m = _PROGRESS_RE.match(line)
    if m and m.group(1) == "end":
        state.current_seconds = max(state.current_seconds, duration_seconds)
    return state


def _effective_position(state: _ProgressState, source_fps: float) -> float:
    """Source-timeline seconds elapsed, derived from whichever signal is ahead.

    `out_time_ms` is ffmpeg's most-recent-muxed-packet PTS. With av1_qsv
    holding many frames in lookahead + B-frame buffers, the muxer can go
    long stretches with the same PTS visible while the decoder advances
    frames steadily — Captain America 2160p run pinned `out_time` at 241s
    for an hour while the encode actually completed. The frame counter
    advances on every decoded frame and is the more reliable signal in
    that regime, so take whichever position is further along.
    """
    if source_fps > 0 and state.frames > 0:
        return max(state.current_seconds, state.frames / source_fps)
    return state.current_seconds


def _effective_speed(state: _ProgressState, source_fps: float) -> float:
    """Realtime multiplier, derived from fps when source_fps is known.

    ffmpeg's `speed` field is `out_time / wall_clock`; it collapses to
    near-zero when out_time stalls even on a healthy encode. fps-derived
    speed (`current_fps / source_fps`) tracks real throughput and stays
    accurate when out_time is misbehaving. Falls back to ffmpeg's value
    when source_fps is unknown (e.g. variable-rate sources)."""
    if source_fps > 0 and state.fps > 0:
        return state.fps / source_fps
    return state.speed


def _render_progress(state: _ProgressState, duration_seconds: float, *,
                     label: str = "", is_tty: bool = True,
                     source_fps: float = 0.0) -> None:
    """Write a progress update to stderr.

    TTY mode: in-place line with `\\r`, redrawn frequently — the
    interactive UX. Non-TTY mode (nohup, redirected stderr, `tee`):
    newline-terminated line that includes `label` (typically
    `[idx/total] filename`) so a `tail -f` of the log always answers
    "what's running now". Caller is responsible for throttling the
    non-TTY path.
    """
    effective = _effective_position(state, source_fps)
    speed = _effective_speed(state, source_fps)
    frac = (effective / duration_seconds
            if duration_seconds > 0 else 0.0)
    remaining = duration_seconds - effective
    if is_tty:
        line = (f"\r{label}{_format_bar(frac)} {frac * 100:5.1f}%  "
                f"{effective:7.1f}s/{duration_seconds:7.1f}s")
        if state.frames > 0:
            line += f"  f={state.frames}"
        if state.fps > 0:
            line += f"  {state.fps:5.1f}fps"
        if speed > 0:
            line += f"  {speed:4.2f}x"
            if remaining > 0:
                line += f"  ETA {_format_secs(remaining / speed)}"
        # Pad to clear leftover characters from a shorter previous render.
        sys.stderr.write(line.ljust(120))
    else:
        parts = [f"{label}{frac * 100:5.1f}% "
                 f"{effective:.0f}/{duration_seconds:.0f}s"]
        if state.frames > 0:
            parts.append(f"f={state.frames}")
        if state.fps > 0:
            parts.append(f"{state.fps:.1f}fps")
        if speed > 0:
            parts.append(f"{speed:.2f}x")
            if remaining > 0:
                parts.append(f"ETA {_format_secs(remaining / speed)}")
        sys.stderr.write(" ".join(parts) + "\n")
    sys.stderr.flush()


def _stream_progress_until_done(proc: subprocess.Popen,
                                duration_seconds: float,
                                timeout_seconds: int | None,
                                start: float,
                                *, label: str = "",
                                stall_seconds: int = 300,
                                source_fps: float = 0.0,
                                bloat_checker: _BloatChecker | None = None,
                                ) -> tuple[bool, str]:
    """Pump ffmpeg progress lines until EOF.

    Returns (should_kill, reason). reason is the failure description
    when should_kill is True, empty string on clean EOF. The caller
    is responsible for the actual kill + reap.
    """
    timeout_active = bool(timeout_seconds and timeout_seconds > 0)
    is_tty = sys.stderr.isatty()
    # Interactive: refresh every 0.5s for a smooth bar. Detached (log file):
    # one line every 30s — readable via `tail -f`, log size stays bounded
    # at a few thousand lines per multi-hour encode rather than 7,200/hour.
    render_interval = 0.5 if is_tty else 30.0
    last_render = 0.0
    # Stall detection: both `out_time_ms` and `frame=` must flatline.
    # Deep-lookahead encodes leave out_time_ms at 0 for minutes on a
    # working encode (Infinity War pinned this) — frame= is the real
    # liveness signal. See NOTES.md#stall-watchdog.
    last_out_seconds = 0.0
    last_frames = 0
    stall_anchor_wall = start
    state = _ProgressState()
    assert proc.stdout is not None
    for line in proc.stdout:
        state = _parse_progress_line(line.strip(), state, duration_seconds)
        now = time.monotonic()
        if (state.current_seconds > last_out_seconds
                or state.frames > last_frames):
            last_out_seconds = state.current_seconds
            last_frames = state.frames
            stall_anchor_wall = now
        if now - last_render >= render_interval:
            _render_progress(state, duration_seconds,
                             label=label, is_tty=is_tty,
                             source_fps=source_fps)
            last_render = now
        if now - stall_anchor_wall > stall_seconds:
            return True, (f"encoder stalled — no progress for "
                          f"{stall_seconds}s (out_time={last_out_seconds:.0f}s, "
                          f"frame={last_frames})")
        if timeout_active and now - start > timeout_seconds:
            return True, f"timeout after {timeout_seconds}s"
        if bloat_checker is not None:
            should_kill, reason = bloat_checker.check(
                state.current_seconds, duration_seconds,
            )
            if should_kill:
                return True, reason
    return False, ""


def run_ffmpeg(cmd: list[str], duration_seconds: float, *,
               timeout_seconds: int | None = 3600,
               stall_seconds: int = 300,
               verbose: bool = False,
               label: str = "",
               source_fps: float = 0.0,
               bloat_checker: _BloatChecker | None = None,
               ) -> tuple[bool, str]:
    """Run ffmpeg with single-line progress; enforce wall-clock + stall caps.

    timeout_seconds: positive int caps wall-clock; 0 or None disables the cap.
    stall_seconds: kill if neither out_time nor frame= advances for this many
    wall-clock seconds. Catches genuine encoder hangs while letting deep-
    lookahead buffering pass. The adaptive default is 6× source duration —
    this is the tighter check.
    source_fps: source video frame rate (frames/second). Enables a
    frame-count-derived progress fallback for sources where ffmpeg's
    out_time_ms field stalls under deep B-frame buffering. Pass 0 to use
    only out_time.
    label: prefix written on each progress update.
    Returns (success, error_message).
    """
    if verbose:
        sys.stderr.write("+ " + " ".join(cmd) + "\n")

    try:
        proc = subprocess.Popen(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, bufsize=1,
        )
    except FileNotFoundError as e:
        return False, f"ffmpeg not found: {e}"

    start = time.monotonic()
    try:
        should_kill, reason = _stream_progress_until_done(
            proc, duration_seconds, timeout_seconds, start,
            label=label, stall_seconds=stall_seconds,
            source_fps=source_fps,
            bloat_checker=bloat_checker,
        )
        if should_kill:
            proc.kill()
            proc.wait(timeout=10)
            sys.stderr.write("\n")
            return False, reason
        rc = _wait_with_optional_timeout(proc, start, timeout_seconds)
        sys.stderr.write("\n")
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)
        sys.stderr.write("\n")
        return False, f"timeout after {timeout_seconds}s"
    except Exception as e:
        # Best-effort kill of the child process on any unexpected error.
        try:
            proc.kill()
        except Exception:
            pass
        sys.stderr.write("\n")
        return False, f"runner error: {e}"
    else:
        if rc != 0:
            return False, f"ffmpeg exited {rc}\n{_read_stderr_tail(proc)}"
        return True, ""


def _wait_with_optional_timeout(proc: subprocess.Popen, start: float,
                                timeout_seconds: int | None) -> int:
    """Block on the ffmpeg process, honouring the wall-clock cap if active."""
    if timeout_seconds and timeout_seconds > 0:
        remaining = max(1, timeout_seconds - int(time.monotonic() - start))
        return proc.wait(timeout=remaining)
    return proc.wait()


def _read_stderr_tail(proc: subprocess.Popen, lines: int = 20) -> str:
    """Return the last `lines` lines of ffmpeg stderr for error reporting."""
    if proc.stderr is None:
        return ""
    return "\n".join(proc.stderr.read().splitlines()[-lines:])
