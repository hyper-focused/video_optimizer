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

| Preset | Target | Quality (AV1 CQ) | Size target | Resolution gate | HW decode | Filename rewrite | REENCODE tag | Audio kept |
|--------|--------|------------------|-------------|-----------------|-----------|------------------|--------------|-----------|
| `hd-archive` | `av1+mkv` | 22 | ~5 GB/hr | height < 1440 | on | yes (dotted) | yes | `en,und` |
| `uhd-archive` | `av1+mkv` | 15 | ~12 GB/hr | height ≥ 1440 | on | yes (dotted) | yes | `en,und` |

Both run `av1_qsv -preset veryslow` with the archive-tuned QSV flag set
(`-extbrc 1 -low_power 0 -adaptive_i 1 -adaptive_b 1 -b_strategy 1 -bf 7
-refs 5 -profile:v main`). Two values are tuned by tier: `-look_ahead_depth`
is **100** for UHD (~4s) and **60** for HD; `-g` (GOP) is **240** for UHD
and **120** for HD. Pure ICQ — no `-maxrate` / `-bufsize`; on av1_qsv the
combination `extbrc + ICQ + maxrate` drops the encoder into a hybrid VBR
mode that targets a much lower average than the headline cap suggests, so
the cap-and-floor pair acts as a hard ceiling instead of a peak buffer
and produces dramatically smaller files than CQ alone. 10-bit sources are
pinned to `-pix_fmt p010le` so they don't silently downconvert through
QSV's default pipeline; 8-bit sources are left to the encoder default.
CPU decode → QSV encode by default (since v0.4.1). The QSV decode pipeline
(`-hwaccel qsv -hwaccel_output_format qsv`) is available behind
`--hw-decode` but off by default — av1_qsv at preset veryslow is the
pipeline bottleneck (1–3× realtime), CPU HEVC decode runs at 5–10×
realtime, so HW decode never speeds anything up that matters here.
CPU decode also produces well-defined p010le frames (no QSV-surface
bridge issues for 10-bit sources) and preserves HDR mastering display +
MaxCLL side_data more reliably than the QSV decode path. Source color
metadata (BT.709 vs BT.2020/PQ) is passed through to the output rather
than forced.

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
(side mode) or `--mode replace` (with optional `--backup`). HDR sources are
re-encoded as HDR (10-bit AV1, BT.2020 / PQ tagging passed through).

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
| `hdr_advisory` | Source has HDR colour metadata (PQ / HLG / BT.2020). | medium | **Advisory only** — never the sole reason for a candidate. Since v0.4.0 `apply` re-encodes HDR sources as HDR (10-bit AV1, BT.2020 / PQ tagging preserved); the rule remains as informational signalling about the source. |

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

Since v0.5.0 every output has a **standardized 3-stream audio ladder**:

| Output | Role | Source |
|--------|------|--------|
| Stream 0 (default) | Highest-quality available | Passthrough — best lossless track if present, else best lossy track. Picked by `(lossless, codec_rank, channels, default_flag)`. |
| Stream 1 | 5.1 surround compat | Best 5.1 source track if present (excluding stream 0); otherwise **Opus 5.1 @ 384 kbps** encoded from stream 0 (when stream 0 has ≥ 6 channels). |
| Stream 2 | Stereo compat | Best 2.0 source track if present (excluding streams 0/1); otherwise **AAC-LC 2.0 @ 256 kbps** encoded from stream 0 (downmix when stream 0 has > 2 channels, lossy fallback when stream 0 is lossless 2.0). |

Streams 1 and 2 are skipped only when the source can't sensibly produce
them — a stereo-only lossy source produces just stream 0; an AC-3 5.1 +
AC-3 2.0 source produces 2 streams (no redundant Opus middle tier when
stream 0 is already lossy 5.1 with no separate lossy-5.1 sibling). The
common Blu-ray remux case (lossless surround source + native 5.1 + native
2.0) yields 3 fully-passthrough streams with no transcode at all.

Filter rules:

- `--keep-langs en,und` (default) controls eligibility. Two-/three-letter
  language codes (`en`/`eng`, `ja`/`jpn`) are treated as equivalents.
- **Commentary tracks are dropped** regardless of language — any audio
  stream whose title contains "commentary" (case-insensitive). The default
  track is always considered (so output is never silent); if every track
  looks like commentary the first non-commentary fallback is kept.
- Tracks in non-keep languages are dropped. Multiple matching-language
  tracks become candidates for the three ladder slots, not separate output
  streams. **Parallel lossless tracks** (TrueHD + DTS-HD MA from the same
  master, common in 4K Blu-ray remuxes) collapse to whichever ranks higher
  — TrueHD wins, the DTS-HD MA is dropped. Saves ~4 Mb/s on a typical UHD
  remux.
- `--no-compat-audio` collapses output to stream 0 only (just the best
  track, no ladder).

Compat tracks are tagged non-default and titled `Opus 5.1 (compat)` /
`AAC 2.0 (compat)` so players treat them as fallbacks behind the
high-quality stream 0.
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
- `--allow-hdr-transcode` — vestigial no-op; HDR sources have been transcoded by default since v0.4.0. Kept for compatibility with older invocations.
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
- HDR mastering display + MaxCLL/MaxFALL SEI metadata is not yet extracted
  from source or forwarded to output. Output is correctly tagged HDR
  (BT.2020 / PQ / 10-bit) and players treat it as HDR; this only affects
  fine-grained tone-mapping accuracy on non-reference displays.
- Dolby Vision metadata is not preserved. DV-on-HDR10 sources are
  transcoded as HDR10 (the base layer). DV-only sources will lose DV.
- Some hardware encoders (notably `av1_qsv`) may not preserve display rotation
  metadata from phone-recorded sources.
