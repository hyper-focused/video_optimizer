# video_optimizer

Probe a media library, apply rules to identify files worth re-encoding for
storage savings or container modernization, then re-encode (or remux) only
those candidates. Probe data and decisions persist in SQLite so repeat scans
of large trees are cheap and incremental.

## Requirements

- Python 3.10+ (stdlib only — no pip install needed)
- `ffmpeg` and `ffprobe` on PATH (FFmpeg 6.x or newer recommended)
- For hardware acceleration: a GPU/iGPU and the matching ffmpeg encoder
  (`av1_qsv`, `hevc_qsv`, `*_nvenc`, `*_vaapi`, `*_videotoolbox`).

## Workflow

The tool is split into subcommands run in sequence:

```
scan PATH    →    plan    →    apply  (or hd-archive / uhd-archive preset)
                              ↑
                            status (reporting)
```

1. **`scan PATH`** — recursively walk a directory, probe every video file with
   `ffprobe`, store the result in the SQLite cache. Re-running is cheap: files
   whose size+mtime match a cached row are skipped.
2. **`plan`** — run the rules engine over the probe cache, list candidates
   with reasons and projected savings, and record pending decisions.
3. **`apply`** — encode pending decisions. Per-file confirmation by default;
   `--auto` skips the prompt. `--mode side` writes outputs to a parallel tree
   (originals untouched); `--mode replace` performs an atomic in-place swap.
4. **`status`** — show recent runs and pending decisions.
5. **`list-encoders`** — pre-flight check: which ffmpeg encoders are compiled
   in, what gets picked per target with `--hwaccel auto`, VAAPI device status.
6. **`hd-archive` / `uhd-archive`** — opinionated convenience wrappers around
   `apply` with sensible defaults baked in for the two most common library
   shapes. See [Presets](#presets) below.

State lives at `~/.video_optimizer/state.db` by default (override with
`--db FILE`).

## Quick start

```bash
# 0. Pre-flight: see which encoders this ffmpeg can use
./video_optimizer.py list-encoders

# 1. Index a library (slow first time, instant on rescan)
./video_optimizer.py scan /mnt/media/Movies

# 2. See what's worth re-encoding
./video_optimizer.py plan --target av1+mkv

# 3a. Use a preset (recommended for typical libraries)
./video_optimizer.py hd-archive \
    --output-root /mnt/media/_optimized \
    --source-root /mnt/media \
    --auto

# 3b. Or use `apply` directly for full control
./video_optimizer.py apply \
    --mode side \
    --output-root /mnt/media/_optimized \
    --source-root /mnt/media \
    --hwaccel auto \
    --rewrite-codec --reencode-tag
```

## Presets

Two opinionated convenience subcommands bake in the typical settings for the
two most common library shapes. Use them when you don't want to remember the
six-flag invocation; drop down to `apply` when you need to vary something the
preset hard-codes.

| Preset | Target | Quality (AV1 CQ) | Size target | Maxrate / bufsize | Resolution gate | HW decode | Filename rewrite | REENCODE tag | Audio kept |
|--------|--------|------------------|-------------|-------------------|-----------------|-----------|------------------|--------------|-----------|
| `hd-archive` | `av1+mkv` | 18 | ~5 GB/hr | 12M / 24M | height < 1440 | on | yes (dotted) | yes | `en,und` |
| `uhd-archive` | `av1+mkv` | 21 | ~12 GB/hr | 30M / 60M | height ≥ 1440 | on | yes (dotted) | yes | `en,und` |

Both run `av1_qsv -preset veryslow` with the archive-tuned QSV flag set
(`-extbrc 1 -low_power 0 -adaptive_i 1 -adaptive_b 1 -b_strategy 1 -bf 7
-refs 5 -profile:v main`). Three values are tuned by tier: `-look_ahead_depth`
is **100** for UHD (~4s) and **60** for HD; `-g` (GOP) is **240** for UHD
and **120** for HD; `-maxrate` / `-bufsize` is **30M / 60M** for UHD and
**12M / 24M** for HD, sized to the per-tier GB/hr targets with ~35–50%
headroom — ICQ stays in charge of the quality floor, the cap only bites in
the hardest scenes. 10-bit sources are pinned to `-pix_fmt p010le` so they
don't silently downconvert through QSV's default pipeline; 8-bit sources
are left to the encoder default. The QSV decode pipeline (`-hwaccel qsv
-hwaccel_output_format qsv`) is on by default for the preset wrappers and
can be turned off with `--no-hw-decode`. Source color metadata (BT.709 vs
BT.2020/PQ) is passed through to the output rather than forced.

**Resolution gate:** when pointing a preset at a mixed-resolution library,
the gate skips files outside the preset's resolution band and **leaves them
pending** in the db. Run the matching preset afterwards to pick them up.
For example:

```bash
# Same db, mixed library — process UHD first, then HD
./video_optimizer.py scan /mnt/media
./video_optimizer.py plan --target av1+mkv

./video_optimizer.py uhd-archive --auto \
    --output-root /mnt/media/_optimized --source-root /mnt/media
# (defers 1080p candidates)

./video_optimizer.py hd-archive --auto \
    --output-root /mnt/media/_optimized --source-root /mnt/media
# (picks up the deferred 1080p candidates)
```

Software fallbacks if QSV isn't available: `libsvtav1 -preset 6
-pix_fmt yuv420p10le` or `libaom-av1 -cpu-used 4`.

Both pick `--hwaccel auto` by default and require an explicit `--output-root`
(side mode) or `--mode replace` (with optional `--backup`). HDR is skipped
unless you pass `--allow-hdr-transcode`.

```bash
# Encode pending HD candidates into a side tree, no prompts
./video_optimizer.py hd-archive --auto \
    --output-root /mnt/media/_optimized \
    --source-root /mnt/media

# UHD library, in-place with backup, override quality slightly
./video_optimizer.py uhd-archive --auto \
    --mode replace \
    --backup /mnt/media/_orig \
    --quality 20

# Either preset can be dry-run first
./video_optimizer.py hd-archive --dry-run --auto \
    --output-root /tmp/vo_dryrun --source-root /mnt/media
```

Things you can override on a preset (everything else is preset-fixed):
`--quality`, `--keep-langs`, `--hwaccel`, `--mode`, `--output-root`,
`--source-root`, `--backup`, `--limit`, `--timeout`, `--allow-hdr-transcode`,
`--no-dotted`, `--name-suffix`, `--reencode-tag-value`, `--dry-run`,
`--verbose`, `--db`. If you need to change anything else (target codec, turn
off rewrite-codec, etc.), use `apply` directly.

## Rules (v1)

| Rule | Fires when | Severity | Effect |
|------|-----------|----------|--------|
| `over_bitrate` | Video bitrate exceeds the per-resolution flag threshold (e.g. 1080p > 10 Mbps). | medium / high | Re-encode at the target's CRF/CQ. |
| `legacy_codec` | Codec is MPEG-2, MPEG-4 part 2, VC-1, WMV1/2/3, H.263, RealVideo, or Theora. | high | Re-encode to target codec. |
| `container_migration` | Container is AVI, WMV, ASF, FLV, MPEG, VOB, or MPEG-TS. | low | When this is the *only* fired rule and the video stream is already H.264 / HEVC / AV1 / VP9, do a stream-copy remux (no re-encode). |
| `hdr_advisory` | Source has HDR colour metadata (PQ / HLG / BT.2020). | medium | **Advisory only** — never the sole reason for a candidate. `apply` refuses HDR sources unless `--allow-hdr-transcode` is set, since SDR-tonemapped output of HDR is destructive if unintended. |

Use `--rules over_bitrate,legacy_codec` to restrict to a subset.

### Bitrate flag table

| Resolution | Target | Flag if > |
|------------|--------|-----------|
| 480p       | 1.5 Mbps | 3 Mbps |
| 720p       | 3 Mbps   | 6 Mbps |
| 1080p      | 5 Mbps   | 10 Mbps |
| 1440p      | 9 Mbps   | 18 Mbps |
| 2160p      | 16 Mbps  | 32 Mbps |

## Targets and hardware acceleration

`--target` selects codec + container:

- `av1+mkv` (default, best storage)
- `hevc+mp4`
- `h264+mp4` (compatibility)

`--hwaccel` selects encoder family:

- `auto` (default) — best available on this system
- `qsv` — Intel Quick Sync (`av1_qsv`, `hevc_qsv`, `h264_qsv`)
- `nvenc` — NVIDIA
- `vaapi` — Linux generic (uses `/dev/dri/renderD128`)
- `videotoolbox` — macOS
- `software` — `libsvtav1` / `libx265` / `libx264`
- `none` — alias for `software`

Falls back to software automatically if no hardware encoder is available.

## Audio and subtitle handling

`--keep-langs en,und` (default) controls which audio and subtitle tracks are
mapped into the output. Two- and three-letter language codes (`en`/`eng`,
`ja`/`jpn`, etc.) are treated as equivalents, so passing `en` will match a
track that ffprobe tagged `eng`.

- All audio tracks whose language is in the keep list are retained as-is
  (`-c:a copy`). The *default* track is always kept (so output is never
  silent). Multiple matching-language tracks are all preserved — e.g. a
  TrueHD 7.1, AC3 5.1, and DTS 2.0 commentary in English all survive.
- **Compat audio shadowing** (on by default; disable with `--no-compat-audio`):
  when any kept track is hi-res lossless — TrueHD, DTS-HD MA (DTS codec at
  ≥ 6 channels), FLAC, multichannel PCM — the best such source is also
  re-encoded into two AAC compatibility tracks appended to the output:
  - **AAC 5.1 @ 640 kbps** (only when the source has ≥ 6 channels)
  - **AAC 2.0 @ 320 kbps**

  Compat tracks are tagged non-default and labelled `AAC 5.1 (compat)` /
  `AAC 2.0 (compat)`, so players still pick the original lossless track
  first. The bitrates are intentionally generous — these are cheap insurance
  for downstream devices (older TVs, Chromecast, phones) that can't decode
  TrueHD or DTS-HD MA.
- Subtitles in the keep list are retained. MKV preserves them as-is. MP4
  outputs convert text subtitles to `mov_text`; image subtitles
  (`hdmv_pgs_subtitle`, `dvd_subtitle`) are dropped with a warning, since
  MP4 cannot carry them.

## Output modes

- `--mode side` (default, safer): outputs go under `--output-root DIR`,
  mirroring the source tree relative to `--source-root` (defaults to filesystem
  root). Originals are never touched.
- `--mode replace`: encode to a temp file, optionally backup the original to
  `--backup DIR`, then `os.replace` over the original on success. Failures
  leave the original intact.

## Other useful flags

- `--dry-run` — print planned ffmpeg commands; encode nothing.
- `--limit N` — process at most N candidates per run.
- `--quality Q` — CRF / global_quality / cq / qp / q:v override (encoder-dependent).
- `--allow-hdr-transcode` — opt in to transcoding HDR sources.
- `--name-suffix STR` — append to output stem (see Radarr/Sonarr below).
- `--timeout SECONDS` — per-file ffmpeg wall-clock cap. `0` disables. Default
  is adaptive: `max(3600, 6 × source_duration)`. A 3-hour movie therefore
  defaults to an 18-hour cap, which leaves headroom for slow software AV1
  encodes while still catching genuinely stuck processes.
- `--verbose` — log ffmpeg commands and per-file probe details.

## Radarr / Sonarr compatibility

Radarr and Sonarr identify files by name and score quality using *Custom
Formats* (regex matchers on filename / release tags). When you re-encode a
file, you want the new filename to:

1. Reflect the new codec (so Custom Format scoring picks it up).
2. Drop the *old* codec token (so the regex doesn't double-match).
3. Optionally carry a marker that lets you tell the Arr app "this is a
   re-encode — stop trying to upgrade it."

There are three composable flags for this:

| Flag | Effect |
|------|--------|
| `--rewrite-codec` | Strip foreign codec tokens (`H.264`, `H.265`, `HEVC`, `x264`, `x265`, `AVC`) and insert the canonical token for the new target (`AV1` / `HEVC` / `H.264`). Outputs Plex-style dotted filenames by default. |
| `--reencode-tag` | Append a `REENCODE` token. Use a Custom Format that matches `\bREENCODE\b` and assign it a permanent negative or saturating score — Sonarr/Radarr will then never try to "upgrade" the file. |
| `--reencode-tag-value STR` | Override the token (default `REENCODE`). |
| `--no-dotted` | Keep input whitespace style instead of forcing dots (rarely wanted). |
| `--name-suffix STR` | Free-form trailing append; runs after `--rewrite-codec` and `--reencode-tag`. |

### Example transforms (target = `av1+mkv`)

| Input stem | `--rewrite-codec` output |
|-----------|--------------------------|
| `Inception (2010) 1080p BluRay H.264-RELEASEGRP` | `Inception.(2010).1080p.BluRay-RELEASEGRP.AV1` |
| `Movie.Name.2010.2160p.HDR.HEVC.x265.10bit-GRP` | `Movie.Name.2010.2160p.HDR.10bit-GRP.AV1` |
| `Some Movie (2015) [HEVC]` | `Some.Movie.(2015).AV1` |
| `Movie.Name.2010` (no codec token) | `Movie.Name.2010.AV1` |

With `--reencode-tag` added, each gets `.REENCODE` appended:
`Inception.(2010).1080p.BluRay-RELEASEGRP.AV1.REENCODE`.

### Recommended workflow

```bash
# Re-encode the queue, replace originals (with backup), tag for Custom Formats
./video_optimizer.py apply --auto \
    --mode replace \
    --backup /mnt/media/_orig \
    --rewrite-codec \
    --reencode-tag \
    --hwaccel auto
```

Then in Radarr/Sonarr, add a Custom Format with a regex match on
`\bREENCODE\b` and either:
- Assign it a saturating positive score (so it's already at the ceiling and
  no upgrade can beat it), or
- Assign it a strong negative score in the Custom Format set used by your
  upgrade-monitoring profile (so the file is "good enough" and ignored).

**Caveat:** changing the filename means the Arr app will re-import on its
next sync — entries may briefly be flagged "missing" between scans. Run
`apply` outside of active library-sync windows for big batches, or pause the
Arr app's library polling while the run is in progress.

## Status / history

```bash
./video_optimizer.py status               # last 10 runs + pending decisions
./video_optimizer.py status --last 50
```

Each run records its arguments and a JSON summary. Decisions persist their
status (`pending`, `completed`, `failed`, `skipped`) and measured savings.

## Layout

```
video_optimizer/
├── README.md
├── version.py
├── video_optimizer.py        # entrypoint shim
└── optimizer/
    ├── cli.py                # argparse + subcommand dispatch
    ├── crawler.py            # recursive directory walk
    ├── probe.py              # ffprobe → ProbeResult
    ├── models.py             # dataclasses + JSON ser/de
    ├── presets.py            # tuning surface (CQ, maxrate, GOP, bitrate table)
    ├── rules.py              # rule engine + v1 rules
    ├── encoder.py            # ffmpeg command builder + runner
    ├── db.py                 # SQLite persistence
    └── report.py             # text + JSON candidate rendering
```

## Tuning

The knobs you would realistically retune without changing logic all live
in **`optimizer/presets.py`**:

- `PRESETS` — preset CQ, height gate, target codec.
- `AV1_QSV_TIER` — per-tier (`hd` / `uhd`) `maxrate` / `bufsize` /
  `look_ahead_depth` / `gop`.
- `AV1_QSV_BASE` — av1_qsv flags shared across tiers (`bf`, `refs`,
  `extbrc`, `preset`, `profile`, …).
- `BITRATE_FLAG_TABLE` — `(target_mbps, flag_above_mbps)` per resolution
  bucket, consumed by the rules engine.

Edit, save, run — no parsing layer, no precedence rules. CLI flags
(`--quality`, `--keep-langs`, etc.) still override on a per-run basis.

If you find yourself regularly maintaining multiple co-existing tunings
(per-machine, per-library, per-content-type), graduate this file to a
TOML config at `~/.video_optimizer/config.toml`. The shape of `presets.py`
maps cleanly to TOML tables, so the migration is mechanical when the need
arrives.

## Known limitations / deferred to v2

- No filename normalization beyond `--name-suffix` (originals' stems are
  preserved; only the suffix and extension change).
- Audio compat tracks are AAC-only (native ffmpeg encoder); no Opus or AC3
  re-encode option, no `libfdk_aac` preference even when available.
- No multi-pass software encoding.
- Apply runs sequentially (one ffmpeg at a time).
- HDR re-encode is opt-in only; HDR-preserving transcode (HDR10 → HDR10) is
  not implemented — `--allow-hdr-transcode` will produce SDR output if the
  encoder doesn't preserve HDR metadata.
- Some hardware encoders (notably `av1_qsv`) may not preserve display rotation
  metadata from phone-recorded sources.
