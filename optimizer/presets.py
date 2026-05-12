"""Tuning surface for video_optimizer.

Single file for every knob you'd realistically retune without touching
encoder or rules logic. Edit values here, save, run.

Layout:
  - PRESETS                     — per-tier definitions (CQ, height gate, target).
  - SLOW_CPU_DECODE_CODECS      — codec-aware hw_decode override (empty today).
  - EST_SECONDS_PER_FILE        — wizard's wall-clock estimates per tier.
  - AV1_QSV_TIER / AV1_QSV_BASE — av1_qsv tuning (per-tier and shared).
  - BLOAT_*                     — UHD over-budget retry trigger + checkpoints.
  - MIN_PROBE_SIZE_BYTES        — scan-time size gate.
  - BITRATE_FLAG_TABLE          — OverBitratedRule thresholds.

Anything not exposed here is held constant on purpose — usually because
changing it would invalidate calibration. Deep rationale for the choices
below lives in NOTES.md (see section pointers).
"""

from __future__ import annotations

# --------------------------------------------------------------------------- #
# Preset definitions
# --------------------------------------------------------------------------- #
#
# CQ is av1_qsv ICQ (lower = larger + higher quality). Calibration notes
# and the UHD-FILM grain-fallback rationale: NOTES.md#preset-rationale.

PRESETS: dict[str, dict[str, object]] = {
    "SD": {
        "label": "≤719p / SD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 24,                  # looser than HD; SD is perceptually fragile
        "encoder_preset": "veryslow",
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "max_height": 719,
    },
    "HD": {
        "label": "720–1439p / HD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 21,                  # ~5 GB/hr archive-grade 1080p
        "encoder_preset": "veryslow",
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 720,
        "max_height": 1439,
    },
    "UHD": {
        "label": "≥1440p / UHD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 15,                  # on av1_qsv's ICQ quality plateau
        "encoder_preset": "veryslow",
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 1440,
        "hw_decode": True,              # see NOTES.md#hw-decode-policy
    },
    "UHD-FILM": {
        # Opt-in alternative for grain-dominated 4K remasters where the
        # default UHD CQ 15 over-allocates bits on grain.
        # See NOTES.md#preset-rationale.
        "label": "≥1440p / grainy older film at UHD (AV1 + MKV, CQ 21)",
        "target": "av1+mkv",
        "quality": 21,
        "encoder_preset": "slow",       # grain content doesn't reward veryslow
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 1440,
        "hw_decode": True,
    },
}


# --------------------------------------------------------------------------- #
# Codec-aware hw_decode override
# --------------------------------------------------------------------------- #
#
# Scaffolding for routing specific source codecs through QSV regardless
# of preset. Currently empty after vc1_qsv wedged on Lethal Weapon 1987.
# Empirical-validation gate for re-adding any codec: NOTES.md#codec-aware-override.

SLOW_CPU_DECODE_CODECS: frozenset[str] = frozenset()


# --------------------------------------------------------------------------- #
# Per-tier wall-clock estimates (wizard plan summary)
# --------------------------------------------------------------------------- #
#
# Calibrated for Intel Battlemage iGPU + av1_qsv on ffmpeg 7.x. Older /
# other hardware will land elsewhere — wizard prints a caveat alongside
# any total derived from these.

EST_SECONDS_PER_FILE: dict[str, int] = {
    "SD":       300,    # ~5 min @ 480p
    "HD":       900,    # ~15 min @ 1080p (~220 fps)
    "UHD":      3600,   # ~1 hr @ 2160p HDR (~40–55 fps)
    "UHD-FILM": 2400,   # ~40 min — looser CQ runs a touch faster
}


# --------------------------------------------------------------------------- #
# av1_qsv tuning
# --------------------------------------------------------------------------- #
#
# Tier split at probe.height >= 1440 inside encoder.build_encode_command.
# Knob meanings + the "no maxrate/bufsize" rule: NOTES.md#av1_qsv-tuning.

AV1_QSV_TIER: dict[str, dict[str, str]] = {
    "hd":  {"look_ahead_depth": "60",  "gop": "120"},   # ~5s at 24 fps
    "uhd": {"look_ahead_depth": "100", "gop": "240"},   # ~10s at 24 fps
}

# Tier-independent. Stored as strings — that's what ffmpeg argv wants.
AV1_QSV_BASE: dict[str, str] = {
    "extbrc": "1",
    "low_power": "0",
    "adaptive_i": "1",
    "adaptive_b": "1",
    "b_strategy": "1",
    "bf": "7",
    "refs": "5",
    "profile": "main",
}

# Default encoder preset for direct `apply` (no cmd_preset wrapper).
# Mirrors PRESETS["UHD"]["encoder_preset"] so library-scale runs match.
AV1_QSV_DEFAULT_ENCODER_PRESET: str = "veryslow"


# --------------------------------------------------------------------------- #
# Post/mid-encode bloat fallback (UHD only)
# --------------------------------------------------------------------------- #
#
# Healthy UHD encodes land at 0.4–0.7 of source size. If a UHD output
# projects above the threshold, kill + retry at RELAXED_UHD_CQ /
# RELAXED_UHD_ENCODER_PRESET. Threshold + checkpoint calibration history
# (TGF 1972, Princess Bride): NOTES.md#preset-rationale.

BLOAT_RATIO_THRESHOLD: float = 0.90
RELAXED_UHD_CQ: int = 21
RELAXED_UHD_ENCODER_PRESET: str = "slow"
BLOAT_CHECKPOINTS: tuple[float, ...] = (0.10, 0.20, 0.30, 0.50)


# --------------------------------------------------------------------------- #
# Scan / rules thresholds
# --------------------------------------------------------------------------- #

# Files smaller than this are skipped at scan time (trailers, samples,
# extras). Override per run with `scan --min-size <N>`; `0` disables.
MIN_PROBE_SIZE_BYTES: int = 100 * 1024 * 1024   # 100 MB

# (target_mbps, flag_above_mbps) per resolution bucket. Halve the gap
# to be more aggressive about catching bloated sources.
BITRATE_FLAG_TABLE: dict[str, tuple[float, float]] = {
    "480p":  (1.5, 3.0),
    "720p":  (3.0, 6.0),
    "1080p": (5.0, 10.0),
    "1440p": (9.0, 18.0),
    "2160p": (16.0, 32.0),
}
