"""Tuning surface for video_optimizer.

Single file containing every knob you would realistically want to retune
without touching the encoder or rules logic. Edit values here, save, run.

Layout:

  - PRESETS              — preset definitions (CQ, height gate, target codec).
  - AV1_QSV_TIER         — per-tier (hd / uhd) av1_qsv tuning:
                           look_ahead_depth + GOP. (No maxrate/bufsize on
                           purpose — see comment below the table.)
  - AV1_QSV_BASE         — av1_qsv flags shared across tiers.
  - MIN_PROBE_SIZE_BYTES — scan-time minimum file size; smaller files
                           are recorded as skipped and never probed.
  - BITRATE_FLAG_TABLE   — rules engine's per-resolution
                           (target_mbps, flag_above_mbps) thresholds.

Anything not listed here is held constant in the encoder/rules code on
purpose — usually because changing it would invalidate the calibration
notes in the README. If you need to tune something not exposed here, that
is a code change, not a config tweak.

Deliberately a Python module rather than TOML/YAML: zero parser, zero
precedence rules, comments allowed inline next to values. If multiple
co-existing tunings (per-machine, per-library, per-content-type) become a
real need, this graduates to `~/.video_optimizer/config.toml` — the
table layout below maps cleanly.
"""

from __future__ import annotations

# --------------------------------------------------------------------------- #
# Preset definitions (consumed by cli.PRESETS)
# --------------------------------------------------------------------------- #

# `quality` is av1_qsv ICQ. Lower = larger files + higher quality.
# Calibrated against Intel Arc / Battlemage with ffmpeg 7.x for archive-
# grade live-action content:
#   HD  CQ 21 → ~5 GB/hr  (1080p SDR; v0.5.13. CQ 22 ran ahead of budget
#                          on a real campaign — nudged down for headroom
#                          on grain-heavy or motion-heavy 1080p content)
#   UHD CQ 15 → ~12 GB/hr (2160p HDR; CQ 15-16 sit on the av1_qsv ICQ
#                          quality plateau, CQ 17 falls off the knee)

PRESETS: dict[str, dict[str, object]] = {
    "hd-archive": {
        "label": "1080p / HD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 21,
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "max_height": 1439,        # SD/HD only — skip UHD, leave for uhd-archive
    },
    "uhd-archive": {
        "label": "2160p / UHD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 15,
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 1440,        # UHD/QHD only — skip HD, leave for hd-archive
        # 4K HEVC sources land in the SW HEVC decoder otherwise; on a
        # 60 Mbps 2160p stream that's 4–6 cores chewing decode while the
        # GPU encode waits, observed as fps decay during long batches.
        # Routing decode to the QSV asic via -hwaccel qsv keeps the
        # pipeline zero-copy GPU-to-GPU. Safe now that the v0.5.15 DV
        # skip filters out the sources that wedge the QSV decoder.
        # Override per run with --no-hw-decode if a specific title trips it.
        "hw_decode": True,
    },
}


# --------------------------------------------------------------------------- #
# Per-tier wall-clock estimate per file (consumed by the wizard's plan summary)
# --------------------------------------------------------------------------- #

# Estimated wall-clock seconds to encode a single representative file in
# each tier. Calibrated for the developer's hardware: Intel Battlemage
# iGPU with QSV hw decode + av1_qsv encode, ffmpeg 7.x.
#
#   hd-archive:  ~15 min/file  — 1080p Blu-ray remux at ~220 fps
#   uhd-archive: ~1 hour/file  — 2160p HDR Blu-ray remux at ~40–55 fps
#
# Older Intel iGPUs (Tiger Lake / Alder Lake), NVENC, VAAPI, or the
# software fallback (libsvtav1) will land elsewhere — sometimes by an
# order of magnitude. The wizard's plan summary should always print a
# one-line caveat alongside any total it derives from these numbers
# (something like: "based on Intel Battlemage; your hardware may vary").
#
# Single source of truth: if hardware throughput shifts materially
# (driver update, new silicon, encoder retune), update the values here.
# The wizard reads this dict — don't duplicate the numbers anywhere else.

EST_SECONDS_PER_FILE: dict[str, int] = {
    "hd-archive": 900,    # ~15 min — 1080p remux, Battlemage iGPU @ ~220 fps
    "uhd-archive": 3600,  # ~1 hour — 2160p HDR remux, Battlemage iGPU @ ~40–55 fps
}


# --------------------------------------------------------------------------- #
# av1_qsv per-tier tuning (consumed by encoder._qsv_args)
# --------------------------------------------------------------------------- #

# HD = 1080p and below; UHD = 1440p and above. The split is decided by
# probe.height >= 1440 inside encoder.build_encode_command.
#
# - look_ahead_depth: frames of motion-prediction lookahead. Deeper = better
#   compression on slow/static content, more memory and slower init.
# - gop: max keyframe interval (frames). Longer GOP compresses better but
#   makes seeks coarser. 24 fps content: 120 ≈ 5s, 240 ≈ 10s.
#
# No maxrate / bufsize on purpose. On av1_qsv, the combination
# `extbrc=1 + ICQ + maxrate` drops the encoder into a hybrid VBR mode
# that targets a *much* lower average than the maxrate suggests
# (observed: ~300 kb/s video on a 1080p source at CQ 18 with maxrate=12M).
# Pure ICQ produces the expected 4–7 Mb/s for archive-quality 1080p AV1.

AV1_QSV_TIER: dict[str, dict[str, str]] = {
    "hd": {
        "look_ahead_depth": "60",
        "gop": "120",
    },
    "uhd": {
        "look_ahead_depth": "100",
        "gop": "240",
    },
}


# Flags matching the validated `videos/ff_uhd_av1.sh` reference script.
# Stored as strings because that's what ffmpeg argv expects — saves a
# cast at every consumer site.

AV1_QSV_BASE: dict[str, str] = {
    "preset": "veryslow",
    "extbrc": "1",
    "low_power": "0",
    "adaptive_i": "1",
    "adaptive_b": "1",
    "b_strategy": "1",
    "bf": "7",                       # B-frames between references
    "refs": "5",                     # reference frames
    "profile": "main",
}


# --------------------------------------------------------------------------- #
# Probe-time size gate (consumed by cli._scan_walk_phase)
# --------------------------------------------------------------------------- #

# Files smaller than this are recorded in the `skipped_files` table at
# scan time and never ffprobe'd or rule-evaluated. Filters out trailers,
# extras, and sample files (Plex / Jellyfin download these into movie
# folders) where the projected savings don't justify the encode.
# Override per run with `scan --min-size <N>` (accepts `1G`, `500M`, `0`).
# `0` disables the gate entirely.
MIN_PROBE_SIZE_BYTES: int = 1024 ** 3   # 1 GB

# --------------------------------------------------------------------------- #
# Bitrate flag table (consumed by rules.OverBitratedRule)
# --------------------------------------------------------------------------- #

# (target_mbps, flag_above_mbps) per coarse resolution bucket.
# `target_mbps` is the bitrate the encoder aims for; `flag_above_mbps` is
# the threshold above which a source is flagged as "worth re-encoding".
# Halve the gap (e.g. flag at 8 instead of 10 for 1080p) to be more
# aggressive about catching bloated sources.

BITRATE_FLAG_TABLE: dict[str, tuple[float, float]] = {
    "480p":  (1.5, 3.0),
    "720p":  (3.0, 6.0),
    "1080p": (5.0, 10.0),
    "1440p": (9.0, 18.0),
    "2160p": (16.0, 32.0),
}
