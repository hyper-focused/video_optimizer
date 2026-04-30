"""ffmpeg command builder + runner for video_optimizer."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from .models import AudioTrack, ProbeResult, SubtitleTrack
from .presets import AV1_QSV_BASE, AV1_QSV_TIER

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
        f"No usable encoder for target={target} hwaccel={hwaccel}. "
        f"Tried {candidates}. Available: {sorted(available)}"
    )


def output_extension(target: str) -> str:
    """Return file extension (with dot) for a TARGETS key."""
    if target not in TARGETS:
        raise RuntimeError(f"Unknown target {target!r}")
    return "." + TARGETS[target][1]


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


# ISO 639-1 (2-letter) ↔ ISO 639-2 (3-letter) equivalents. ffprobe usually
# emits 3-letter codes ("eng"), but our --keep-langs default is "en,und";
# without this expansion an English audio track tagged "eng" would not match
# the keep set and would only survive via the default-flag fallback (so any
# secondary English tracks were silently dropped).
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


# Codecs treated as lossless / hi-res for the compat-track logic. DTS is a
# special case — DTS-HD MA reports as plain `dts`, so we treat dts as hi-res
# only when the track has 5.1+ channels (DTS Core 2.0 is not worth shadowing).
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
                       target_container: str) -> list[str]:
    """-map / -c:s fragment that keeps subs matching `langs`, dropping image
    subs on mp4 (warning to stderr) and converting text subs to mov_text."""
    kept: list[int] = []
    for i, s in enumerate(probe.subtitle_tracks):
        lang = (s.language or "").lower()
        if lang not in langs:
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
                           target_container: str) -> set[int]:
    """Source subtitle indices that survive into the output (mirrors the
    filter inside `_subtitle_map_args` so we can pre-strip the rest)."""
    kept: set[int] = set()
    for i, s in enumerate(probe.subtitle_tracks):
        lang = (s.language or "").lower()
        if lang not in langs:
            continue
        if target_container == "mp4" and s.codec in _IMAGE_SUB_CODECS:
            continue
        kept.add(i)
    return kept


def _input_discard_args(probe: ProbeResult, keep_langs: list[str],
                        target_container: str,
                        *, add_compat_audio: bool = True) -> list[str]:
    """Per-input `-discard:<spec> all` flags for streams we won't use.

    Why: ffmpeg's demuxer interleaves packets for every active stream by
    container timestamp before any decoder sees them. On sources with
    many parallel audio tracks (8+ language dubs is common on Blu-ray
    remuxes), the QSV video decoder's input queue can starve through the
    narrow windows between audio packets and deadlock at frame 0 — the
    classic multi-language stall pattern. CPU decode survives on more
    headroom but isn't immune (the older AVC-multilang stall list also
    pre-dates hw_decode). `-discard:<spec> all` is applied at demux time,
    so dropped streams never enter the packet queue and never compete
    for scheduler attention. Must precede `-i`.

    Discard preserves source-side indexing — `-map 0:a:1?` still resolves
    to the original audio stream 1 even when audio stream 0 is discarded.
    """
    langs = _expand_langs({(lang or "").lower() for lang in keep_langs})
    kept_a = _kept_audio_indices(probe, langs, add_compat=add_compat_audio)
    kept_s = _kept_subtitle_indices(probe, langs, target_container)
    args: list[str] = []
    for i in range(len(probe.audio_tracks)):
        if i not in kept_a:
            args += [f"-discard:a:{i}", "all"]
    for i in range(len(probe.subtitle_tracks)):
        if i not in kept_s:
            args += [f"-discard:s:{i}", "all"]
    return args


def build_stream_map_args(probe: ProbeResult, keep_langs: list[str],
                          target_container: str,
                          *, add_compat_audio: bool = True) -> list[str]:
    """Return -map + audio/subtitle codec args for chosen streams.

    Attachments (embedded fonts, cover art, etc.) are intentionally NOT
    mapped, even for MKV targets. A single source attachment with a
    missing or undeducible mimetype crashes ffmpeg's matroska muxer:

      [matroska] Attachment stream N has no mimetype tag and it cannot
      be deduced from the codec id.
      [out] Could not write header (incorrect codec parameters?)

    The fail-the-whole-encode cost is far higher than the benefit of
    preserving embedded fonts (which mostly only matter for ASS/SSA
    subtitle rendering — anime, rarely live-action archive content).
    Observed in the wild on iNCEPTiON-grouped Indiana Jones 4 source.
    """
    langs = _expand_langs({(lang or "").lower() for lang in keep_langs})
    args: list[str] = ["-map", "0:v:0"]
    args += _audio_map_args(probe, langs, add_compat=add_compat_audio)
    args += _subtitle_map_args(probe, langs, target_container)
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
              hw_decode: bool = False) -> list[str]:
    """Codec args for Intel Quick Sync encoders.

    All av1_qsv tuning lives in `optimizer/presets.py` (AV1_QSV_TIER for
    HD-vs-UHD knobs; AV1_QSV_BASE for the tier-independent flag set that
    matches the validated `videos/ff_uhd_av1.sh` reference script).

    Pure ICQ, no -maxrate / -bufsize — on av1_qsv, the combination of
    extbrc + ICQ + maxrate collapses to a hybrid VBR mode that under-allocates
    by an order of magnitude (observed ~300 kb/s video at CQ 18 with maxrate
    12M on a 1080p source). CQ alone gets the expected 4–7 Mb/s.

    Bit depth handling: when hw_decode is True, the QSV decode->encode
    pipeline keeps frames in GPU-resident `qsv` surfaces that natively
    preserve source bit depth. Pinning -pix_fmt p010le in that path forces
    a qsv->p010le conversion that ffmpeg's auto_scale filter can't bridge,
    breaking the encode. So pix_fmt is only pinned when hw_decode is False
    (SW decode -> QSV encode), where it does prevent stealth downconvert.
    """
    base = AV1_QSV_BASE
    # `-global_quality` is scoped to the video stream (`:v`) so the qscale
    # flag isn't applied to every encoder in the graph. Without the scope,
    # libopus rejects with "Quality-based encoding not supported" and the
    # whole encode fails before producing any output.
    #
    # `-look_ahead 1` is intentionally absent: it's a family-level QSV
    # option that only does something on hevc_qsv / h264_qsv. av1_qsv
    # ignores it (use `-look_ahead_depth` instead) and ffmpeg surfaces a
    # `Codec AVOption look_ahead ... has not been used` warning per
    # encode if it's left on. Restore for hevc_qsv / h264_qsv if those
    # encoders ever come back into the regular path.
    a = ["-c:v", encoder, "-preset", base["preset"],
         "-global_quality:v", str(quality)]
    if encoder == "av1_qsv":
        tier = AV1_QSV_TIER["uhd" if is_uhd else "hd"]
        a += [
            "-look_ahead_depth", tier["look_ahead_depth"],
            "-extbrc", base["extbrc"],
            "-low_power", base["low_power"],
            "-adaptive_i", base["adaptive_i"],
            "-adaptive_b", base["adaptive_b"],
            "-b_strategy", base["b_strategy"],
            "-bf", base["bf"],
            "-refs", base["refs"],
            "-g", tier["gop"],
            "-profile:v", base["profile"],
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
                hw_decode: bool = False) -> list[str]:
    """Return -c:v + quality/preset arg fragment for the given encoder."""
    sw = _software_args(encoder, quality)
    if sw is not None:
        return sw
    if encoder.endswith("_qsv"):
        return _qsv_args(encoder, quality, is_uhd=is_uhd,
                         bit_depth=bit_depth, hw_decode=hw_decode)
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
                        *, add_compat_audio: bool = True) -> list[str]:
    """Return ffmpeg argv that stream-copies into target_container."""
    cmd: list[str] = ["ffmpeg", "-hide_banner", "-nostdin", "-y"]
    cmd += _input_discard_args(probe, keep_langs, target_container,
                               add_compat_audio=add_compat_audio)
    cmd += [
        "-i", probe.path,
        "-map_metadata", "0",
        "-map_metadata:s", "-1",
        "-map_chapters", "0",
        "-c:v", "copy",
    ]
    cmd += build_stream_map_args(probe, keep_langs, target_container,
                                 add_compat_audio=add_compat_audio)
    if target_container == "mp4":
        cmd += ["-movflags", "+faststart"]
    cmd += ["-progress", "pipe:1", "-nostats", str(output_path)]
    return cmd


def build_encode_command(probe: ProbeResult, output_path: Path,
                         encoder: str, quality: int | None,
                         keep_langs: list[str], target_container: str,
                         *, hw_decode: bool = False,
                         add_compat_audio: bool = True) -> list[str]:
    """Return ffmpeg argv for a real re-encode using the given encoder.

    hw_decode=True with a QSV encoder enables zero-copy GPU decode→encode
    (`-hwaccel qsv -hwaccel_output_format qsv`). Saves CPU but can fail on
    legacy codecs that the QSV decoder doesn't support, so callers (apply)
    leave it off by default and the preset wrappers turn it on.
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
                               add_compat_audio=add_compat_audio)
    cmd += ["-i", probe.path,
            "-map_metadata", "0", "-map_metadata:s", "-1",
            "-map_chapters", "0"]

    cmd += _codec_args(encoder, q, is_uhd=is_uhd,
                       bit_depth=probe.bit_depth, hw_decode=hw_decode)
    cmd += _color_passthrough_args(probe)

    if encoder.endswith("_vaapi"):
        cmd += ["-vf", "format=nv12,hwupload"]

    cmd += build_stream_map_args(probe, keep_langs, target_container,
                                 add_compat_audio=add_compat_audio)

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
    # Stall detection: if neither the encoded position (`out_time_ms`) nor
    # the decoder-side frame count (`frame=`) advances for `stall_seconds`
    # of wall-clock, the pipeline is genuinely hung. Both signals are
    # required because av1_qsv with deep lookahead (depth=100, refs=5)
    # buffers ~150 frames before any presentation timestamp surfaces to
    # the muxer, so `out_time_ms` can stay at 0 for several minutes on a
    # working encode (Avengers: Infinity War 2160p remux pinned this
    # against the v0.5.17 5-min watchdog while writing 441s of clean AV1
    # to disk). `frame=` advances on every decoded frame, so a real stall
    # — input queue starvation, hardware deadlock — flatlines both.
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
    return False, ""


def run_ffmpeg(cmd: list[str], duration_seconds: float, *,
               timeout_seconds: int | None = 3600,
               stall_seconds: int = 300,
               verbose: bool = False,
               label: str = "",
               source_fps: float = 0.0) -> tuple[bool, str]:
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
