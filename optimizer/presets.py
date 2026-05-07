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
    "SD": {
        "label": "≤719p / SD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 24,             # looser than HD: SD is more perceptually
                                   # fragile under heavy compression, and the
                                   # storage delta vs CQ 21 is small at SD sizes
        "encoder_preset": "veryslow",   # max RD-search depth — best efficiency
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "max_height": 719,         # SD only — leave HD/UHD for their tiers
    },
    "HD": {
        "label": "720–1439p / HD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 21,
        "encoder_preset": "veryslow",
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 720,
        "max_height": 1439,        # HD only — leave UHD for its tier
    },
    "UHD": {
        "label": "≥1440p / UHD library archive (AV1 + MKV)",
        "target": "av1+mkv",
        "quality": 15,
        "encoder_preset": "veryslow",   # archive-grade: take the time
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 1440,        # UHD/QHD only — leave HD/SD for their tiers
        # 4K HEVC sources land in the SW HEVC decoder otherwise; on a
        # 60 Mbps 2160p stream that's 4–6 cores chewing decode while the
        # GPU encode waits, observed as fps decay during long batches.
        # Routing decode to the QSV asic via -hwaccel qsv keeps the
        # pipeline zero-copy GPU-to-GPU. Safe now that the v0.5.15 DV
        # skip filters out the sources that wedge the QSV decoder.
        # Override per run with --no-hw-decode if a specific title trips it.
        "hw_decode": True,
    },
    "UHD-FILM": {
        # Looser CQ for UHD sources where the default UHD preset (CQ 15)
        # over-allocates bits. The Princess Bride 2160p remux is the
        # canonical example: heavy film grain + low scene-motion makes
        # av1_qsv try to preserve every grain particle, producing a 49 GB
        # output from a 47 GB source. CQ 21 collapses the grain budget
        # to something archive-reasonable (~20 GB/h on the same iGPU)
        # while staying perceptually transparent at typical viewing
        # distances. Use for: pre-2000 live-action 4K remasters, any
        # title where the source is grain-dominated rather than detail-
        # dominated. Opt-in only — `./video_optimizer.py UHD-FILM PATH`.
        #
        # Encoder preset is `slow`, two ladder steps faster than UHD's
        # `veryslow` (av1_qsv: veryslow → slower → slow → medium → ...).
        # Rationale: the bloat-fallback path that drops to this preset
        # is engaged precisely on content where the encoder cannot
        # compress efficiently anyway (grain-dominated). Spending
        # `veryslow` RD-search effort on bits the encoder can't compress
        # well is wasted budget — `slow` finishes ~1.5–2× faster with
        # only a few percent size penalty at this quality target.
        "label": "≥1440p / grainy older film at UHD (AV1 + MKV, CQ 21)",
        "target": "av1+mkv",
        "quality": 21,
        "encoder_preset": "slow",
        "rewrite_codec": True,
        "reencode_tag": True,
        "keep_langs": "en,und",
        "min_height": 1440,
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
#   SD:  ~5 min/file   — 480p remux on av1_qsv (CPU decode + GPU encode)
#   HD:  ~15 min/file  — 1080p Blu-ray remux at ~220 fps
#   UHD: ~1 hour/file  — 2160p HDR Blu-ray remux at ~40–55 fps
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
    "SD":       300,    # ~5 min — typical 480p, CPU decode + GPU encode
    "HD":       900,    # ~15 min — 1080p remux, Battlemage iGPU @ ~220 fps
    "UHD":      3600,   # ~1 hour — 2160p HDR remux, Battlemage iGPU @ ~40–55 fps
    "UHD-FILM": 2400,   # ~40 min — looser CQ encodes a touch faster than UHD
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
    "extbrc": "1",
    "low_power": "0",
    "adaptive_i": "1",
    "adaptive_b": "1",
    "b_strategy": "1",
    "bf": "7",                       # B-frames between references
    "refs": "5",                     # reference frames
    "profile": "main",
}

# Default encoder preset when no per-PRESETS override is supplied (e.g.
# when calling `apply` directly without going through cmd_preset). Kept
# in sync with the `encoder_preset` field on PRESETS["UHD"] so the
# direct-apply path matches the archive-grade tier behavior.
AV1_QSV_DEFAULT_ENCODER_PRESET: str = "veryslow"


# --------------------------------------------------------------------------- #
# Post-encode bloat fallback (consumed by cli._execute_encode)
# --------------------------------------------------------------------------- #

# When an encoded UHD output ends up nearly the same size as (or larger
# than) the source, the default UHD CQ (15) is over-allocating bits to
# preserve grain that doesn't perceptually need it. The Princess Bride
# 2160p remux is the canonical case: 47 GB source → 49 GB output at CQ 15.
#
# `BLOAT_RATIO_THRESHOLD` is the trigger: if `out_size >= encoder_input * this`,
# we delete the output and re-encode once at `RELAXED_UHD_CQ`. Healthy UHD
# encodes land at 0.4–0.7, so any value in the 0.85–0.95 band cleanly
# separates "encoder did its job" from "encoder gave up on grain."
#
# Lowered to 0.90 (was 0.95) after the second TGF live test: fps trajectory
# made it clear the encode was struggling by 25-30% (fps ~45 falling toward
# ~38), but the 0.95 threshold needed the encode to project to within ~5%
# of source size before tripping. At 0.90 the same case trips by 28-30%
# instead of having to wait for the 50% checkpoint, saving ~25 min of
# wasted GPU time per borderline case.
#
# Borderline retry math (CQ 15 veryslow at 0.92 vs CQ 21 slow at 0.95):
# the retry at the relaxed tuning produces a slightly LARGER output
# (~+2 GB on an 80 GB source) in roughly half the wall-clock time. For
# library-scale archive runs, the time saving is the right trade.
#
# Triggered only on UHD (>= 1440p) sources where the bloat pattern is real;
# 1080p AV1 encodes occasionally come out bigger than over-bitrated 1080p
# h264 sources but the perceptual stakes / storage delta are too small to
# justify a re-encode there. Override per run with `--no-auto-relax-cq`.

BLOAT_RATIO_THRESHOLD: float = 0.90
RELAXED_UHD_CQ: int = 21
# When the bloat fallback retries at the relaxed CQ, also drop the encoder
# preset two ladder steps from `veryslow` to `slow`. Same reasoning as the
# UHD-FILM preset itself: grain-dominated content the encoder can't
# compress efficiently doesn't reward extra RD-search effort.
RELAXED_UHD_ENCODER_PRESET: str = "slow"

# Mid-encode bloat checkpoints: fractions of source duration at which
# we stat the output file and project the final size as
# `out_size / completion_ratio`. If the projection trips
# `BLOAT_RATIO_THRESHOLD` at any checkpoint, ffmpeg is killed early and
# the apply layer retries at `RELAXED_UHD_CQ` (same path as the
# post-encode check).
#
# Four checkpoints (was two): 10% and 20% catch the obvious case where
# the encoder struggles from frame 0 (grain-dominated content
# throughout, e.g. Princess Bride). 30% and 50% catch a class the
# earlier checkpoints miss — sources where the grain density compounds
# later in the film. Empirically (TGF 1972, observed in run #168):
# fps stays near baseline (~57 fps for 2160p av1_qsv) through the
# first 25-30% of the encode, drops noticeably by 30% as the encoder
# hits denser grain in the dim interior scenes, and is clearly
# struggling by 50% (fps dropped to ~42 from ~57). Adding 50%
# specifically saves an additional ~50 minutes of wasted GPU time on
# the late-bloating cases vs only sampling at 10/20%. Each checkpoint
# is consumed once per encode; a healthy file passes through silently.
BLOAT_CHECKPOINTS: tuple[float, ...] = (0.10, 0.20, 0.30, 0.50)


# --------------------------------------------------------------------------- #
# Probe-time size gate (consumed by cli._scan_walk_phase)
# --------------------------------------------------------------------------- #

# Files smaller than this are recorded in the `skipped_files` table at
# scan time and never ffprobe'd or rule-evaluated. Filters out trailers,
# extras, and sample files (Plex / Jellyfin download these into movie
# folders) where the projected savings don't justify the encode.
# Override per run with `scan --min-size <N>` (accepts `1G`, `500M`, `0`).
# `0` disables the gate entirely.
MIN_PROBE_SIZE_BYTES: int = 100 * 1024 * 1024   # 100 MB

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
