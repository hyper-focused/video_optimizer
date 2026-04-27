"""ffmpeg command builder + runner for video_optimizer."""

from __future__ import annotations

import os
import re
import subprocess
import sys
import time
from dataclasses import dataclass
from pathlib import Path

from .models import AudioTrack, ProbeResult
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


def _audio_quality_rank(codec: str) -> int:
    """Rank an audio codec by perceptual quality; higher is better."""
    return _AUDIO_QUALITY_RANK.get(codec.lower(), 10)


def _pick_compat_source(
    hires_kept: list[tuple[int, AudioTrack]],
) -> tuple[int, AudioTrack] | None:
    """Return the (input-index, track) of the best hi-res source, if any."""
    if not hires_kept:
        return None
    return max(hires_kept, key=lambda iat: (
        _audio_quality_rank(iat[1].codec),
        iat[1].channels,
        -iat[0],          # tie-break: earlier track wins
    ))


def _select_kept_audio(
    probe: ProbeResult, langs: set[str],
) -> list[tuple[int, AudioTrack]]:
    """Pick which input audio tracks survive the language filter."""
    have_default = any(a.default for a in probe.audio_tracks)
    kept: list[tuple[int, AudioTrack]] = []
    for i, a in enumerate(probe.audio_tracks):
        lang = (a.language or "").lower()
        keep = lang in langs or a.default or (not have_default and i == 0)
        if keep:
            kept.append((i, a))
    if not kept and probe.audio_tracks:
        # Safety: never produce a silent file.
        kept = [(0, probe.audio_tracks[0])]
    return kept


def _compat_track_args(out_idx: int, src_in_idx: int, *, channels: int,
                       bitrate: str, lang: str, title: str) -> list[str]:
    """Build the -map + per-stream codec args for one AAC compat track."""
    return [
        "-map", f"0:a:{src_in_idx}?",
        f"-c:a:{out_idx}", "aac",
        f"-b:a:{out_idx}", bitrate,
        f"-ac:a:{out_idx}", str(channels),
        f"-ar:a:{out_idx}", "48000",
        f"-metadata:s:a:{out_idx}", f"title={title}",
        f"-metadata:s:a:{out_idx}", f"language={lang}",
        f"-disposition:a:{out_idx}", "0",
    ]


def _audio_map_args(probe: ProbeResult, langs: set[str], *,
                    add_compat: bool = True) -> list[str]:
    """-map / -c:a fragment for kept tracks plus optional AAC compat tracks.

    When `add_compat` is true and a kept track is hi-res lossless (TrueHD,
    DTS-HD MA, FLAC, etc.), the best such track is also re-encoded to:
      * AAC 5.1 @ 640k (only if source has ≥ 6 channels)
      * AAC 2.0 @ 320k
    Both are tagged as non-default so players still pick the original first;
    they are "cheap insurance" for downstream devices that can't decode
    lossless formats.
    """
    kept = _select_kept_audio(probe, langs)
    if not kept:
        return []

    args: list[str] = []
    for in_idx, _ in kept:
        args += ["-map", f"0:a:{in_idx}?"]
    # Blanket copy for the originals; per-stream specifiers below override
    # only the appended compat outputs.
    args += ["-c:a", "copy"]

    if not add_compat:
        return args

    hires_kept = [(i, t) for i, t in kept if _is_hires_lossless(t)]
    picked = _pick_compat_source(hires_kept)
    if picked is None:
        return args

    src_in_idx, src_track = picked
    src_lang = (src_track.language or "und").lower()
    out_idx = len(kept)

    if src_track.channels >= 6:
        args += _compat_track_args(
            out_idx, src_in_idx,
            channels=6, bitrate="640k", lang=src_lang,
            title="AAC 5.1 (compat)",
        )
        out_idx += 1

    args += _compat_track_args(
        out_idx, src_in_idx,
        channels=2, bitrate="320k", lang=src_lang,
        title="AAC 2.0 (compat)",
    )
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
    for i in kept:
        args += ["-map", f"0:s:{i}?"]
    if kept:
        # mkv preserves any sub format; mp4 needs text subs converted.
        args += ["-c:s", "copy"] if target_container == "mkv" else ["-c:s", "mov_text"]
    return args


def build_stream_map_args(probe: ProbeResult, keep_langs: list[str],
                          target_container: str,
                          *, add_compat_audio: bool = True) -> list[str]:
    """Return -map + audio/subtitle/attachment codec args for chosen streams."""
    langs = _expand_langs({(lang or "").lower() for lang in keep_langs})
    args: list[str] = ["-map", "0:v:0"]
    args += _audio_map_args(probe, langs, add_compat=add_compat_audio)
    args += _subtitle_map_args(probe, langs, target_container)
    # Attachments (e.g. embedded fonts in MKV); MKV keeps them, MP4 has none.
    if target_container == "mkv":
        args += ["-map", "0:t?", "-c:t", "copy"]
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
              is_uhd: bool = False, bit_depth: int = 8) -> list[str]:
    """Codec args for Intel Quick Sync encoders.

    All av1_qsv tuning lives in `optimizer/presets.py` (AV1_QSV_TIER for
    HD-vs-UHD knobs; AV1_QSV_BASE for the tier-independent flag set that
    matches the validated `videos/ff_uhd_av1.sh` reference script).

    Pure ICQ, no -maxrate / -bufsize — on av1_qsv, the combination of
    extbrc + ICQ + maxrate collapses to a hybrid VBR mode that under-allocates
    by an order of magnitude (observed ~300 kb/s video at CQ 18 with maxrate
    12M on a 1080p source). CQ alone gets the expected 4–7 Mb/s.

    bit_depth >= 10 pins -pix_fmt p010le so 10-bit sources don't silently
    downconvert to 8-bit through QSV's default pipeline. 8-bit sources are
    left to the encoder default.
    """
    base = AV1_QSV_BASE
    a = ["-c:v", encoder, "-preset", base["preset"],
         "-global_quality", str(quality), "-look_ahead", "1"]
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
        if bit_depth >= 10:
            a += ["-pix_fmt", "p010le"]
    if encoder == "hevc_qsv":
        a += ["-tag:v", "hvc1"]
        if bit_depth >= 10:
            a += ["-pix_fmt", "p010le", "-profile:v", "main10"]
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
                is_uhd: bool = False, bit_depth: int = 8) -> list[str]:
    """Return -c:v + quality/preset arg fragment for the given encoder."""
    sw = _software_args(encoder, quality)
    if sw is not None:
        return sw
    if encoder.endswith("_qsv"):
        return _qsv_args(encoder, quality, is_uhd=is_uhd, bit_depth=bit_depth)
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
    cmd: list[str] = [
        "ffmpeg", "-hide_banner", "-y",
        "-i", probe.path,
        "-map_metadata", "0",
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

    cmd: list[str] = ["ffmpeg", "-hide_banner", "-y"]
    if hw_decode and encoder.endswith("_qsv"):
        cmd += ["-hwaccel", "qsv", "-hwaccel_output_format", "qsv"]
    if encoder.endswith("_vaapi"):
        cmd += ["-vaapi_device", _VAAPI_DEVICE]
    cmd += ["-i", probe.path, "-map_metadata", "0", "-map_chapters", "0"]

    cmd += _codec_args(encoder, q, is_uhd=is_uhd, bit_depth=probe.bit_depth)
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
_FPS_RE = re.compile(r"^fps=\s*([\d.]+)$")
_SPEED_RE = re.compile(r"^speed=\s*([\d.]+)x$")
_PROGRESS_RE = re.compile(r"^progress=(\w+)$")


@dataclass
class _ProgressState:
    """Live state pulled from ffmpeg's -progress feed during one encode."""

    current_seconds: float = 0.0   # encoded position in source timeline
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


def _render_progress(state: _ProgressState, duration_seconds: float) -> None:
    """Write a single in-place progress line to stderr (bar / fps / speed / ETA)."""
    frac = (state.current_seconds / duration_seconds
            if duration_seconds > 0 else 0.0)
    line = (f"\r{_format_bar(frac)} {frac * 100:5.1f}%  "
            f"{state.current_seconds:7.1f}s/{duration_seconds:7.1f}s")
    if state.fps > 0:
        line += f"  {state.fps:5.1f}fps"
    if state.speed > 0:
        line += f"  {state.speed:4.2f}x"
        # ETA only meaningful once speed has stabilised and we're not done.
        remaining = duration_seconds - state.current_seconds
        if remaining > 0:
            line += f"  ETA {_format_secs(remaining / state.speed)}"
    # Pad to clear leftover characters from a shorter previous render.
    sys.stderr.write(line.ljust(100))
    sys.stderr.flush()


def _stream_progress_until_done(proc: subprocess.Popen,
                                duration_seconds: float,
                                timeout_seconds: int | None,
                                start: float) -> bool:
    """Pump ffmpeg progress lines until EOF; return True if a timeout fired."""
    timeout_active = bool(timeout_seconds and timeout_seconds > 0)
    last_render = 0.0
    state = _ProgressState()
    assert proc.stdout is not None
    for line in proc.stdout:
        state = _parse_progress_line(line.strip(), state, duration_seconds)
        now = time.monotonic()
        if now - last_render >= 0.5:
            _render_progress(state, duration_seconds)
            last_render = now
        if timeout_active and now - start > timeout_seconds:
            return True
    return False


def run_ffmpeg(cmd: list[str], duration_seconds: float, *,
               timeout_seconds: int | None = 3600,
               verbose: bool = False) -> tuple[bool, str]:
    """Run ffmpeg with single-line progress; enforce wall-clock timeout.

    timeout_seconds: positive int caps wall-clock; 0 or None disables the cap.
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
        timed_out = _stream_progress_until_done(
            proc, duration_seconds, timeout_seconds, start,
        )
        if timed_out:
            proc.kill()
            proc.wait(timeout=10)
            sys.stderr.write("\n")
            return False, f"timeout after {timeout_seconds}s"
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
