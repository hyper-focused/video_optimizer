"""Tuning surface for video_optimizer.

Single file containing every knob you would realistically want to retune
without touching the encoder or rules logic. Edit values here, save, run.

Layout:

  - PRESETS              — preset definitions (CQ, height gate, target codec).
  - AV1_QSV_TIER         — per-tier (hd / uhd) av1_qsv tuning: maxrate /
                           bufsize / look_ahead_depth / GOP.
  - AV1_QSV_BASE         — av1_qsv flags shared across tiers.
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
# On Intel Arc + ffmpeg 7.x av1_qsv, archive-grade live-action targets are
# roughly: HD CQ 18 ≈ 5 GB/hr, UHD CQ 21 ≈ 12 GB/hr (matches the maxrate
# ceiling in AV1_QSV_TIER below).

PRESETS: dict[str, dict[str, object]] = {
    "hd-archive": {
        "label": "1080p / HD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 22,
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "max_height": 1439,        # SD/HD only — skip UHD, leave for uhd-archive
    },
    "uhd-archive": {
        "label": "2160p / UHD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 24,
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 1440,        # UHD/QHD only — skip HD, leave for hd-archive
    },
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
