# video_optimizer

Shrink a movie library by re-encoding bloated source files to AV1 — typically
~65–70% smaller on Blu-ray remuxes (a real recent run: **22 files, 409 GB
saved**) — without losing audio tracks, HDR, or subtitles, and without
risking the originals.

- **One command for new users:** `optimize PATH` does scan → plan → encode
  with safe defaults.
- **Hardware-accelerated AV1** (Intel QSV, NVENC, VAAPI) with automatic
  software fallback.
- **HDR10 preserved end-to-end.** Dolby Vision sources are auto-skipped
  (encode-path fix planned — see `TODO.md`).
- **Originals are recoverable** by default — atomically moved to a recycle
  directory, never hard-deleted unless you explicitly ask.
- **Trailers and extras are skipped automatically** — files smaller than 1 GB
  (configurable) are recorded as skipped at scan time and never re-probed
  or queued, so Plex/Jellyfin extras don't burn encode hours for marginal
  savings.
- **Radarr/Sonarr-friendly** filename rewriting (`AV1.REENCODE` markers).
- **Power-user mode:** the explicit `scan → plan → apply` stages are still
  there for fine-grained control.

Stdlib-only Python 3.10+ wrapping `ffmpeg` / `ffprobe`. Probe data and
decisions persist in SQLite so repeat scans of large trees are cheap and
incremental.

---

## Table of contents

- [Requirements](#requirements)
- [Quick start](#quick-start)
- [How it works (mental model)](#how-it-works-mental-model)
- [Common workflows](#common-workflows)
- [Presets: HD vs UHD](#presets-hd-vs-uhd)
- [Rules and the bitrate table](#rules-and-the-bitrate-table)
- [Audio, subtitles, and HDR](#audio-subtitles-and-hdr)
- [Output modes and safety](#output-modes-and-safety)
- [Watchdog, stalls, and recovery](#watchdog-stalls-and-recovery)
- [Radarr / Sonarr integration](#radarr--sonarr-integration)
- [Flag reference](#flag-reference)
- [Tuning](#tuning)
- [Repository layout](#repository-layout)
- [Known limitations](#known-limitations)

---

## Requirements

- Python 3.10+ (stdlib only — no `pip install` step)
- `ffmpeg` and `ffprobe` on PATH (FFmpeg 6.x or newer recommended; 7.x preferred)
- Optional: a GPU/iGPU and the matching ffmpeg encoder for hardware
  acceleration (`av1_qsv`, `hevc_qsv`, `*_nvenc`, `*_vaapi`, `*_videotoolbox`)
- Linux, macOS, or any platform with the above

Run `./video_optimizer.py doctor` to verify your setup before the first encode
(see [the doctor section](#doctor-preflight-checks)).

---

## Quick start

The setup is fast. The encodes are paced by your GPU and vary widely by
hardware — as a single reference point, on Intel Battlemage with hardware
decode + encode we see roughly **~15 min per 1080p Blu-ray remux**
(~220 fps) and **~1 hour per 2160p HDR remux** (~40–55 fps, ~2× realtime).
Older iGPUs, software fallback (`libsvtav1`), or NVENC will land in
different places — let one encode finish to calibrate.

A whole-library run is hours-to-days depending on backlog size; run
`optimize` overnight or under `nohup` the first time.

```bash
# 1. Verify the setup is healthy (ffmpeg, encoders, GPU, db). Takes seconds.
./video_optimizer.py doctor

# 2. Preview what *would* happen on a single 1080p file. No encodes run.
./video_optimizer.py optimize /mnt/media/Movies \
    --output /tmp/preview --dry-run --limit 1

# 3. Encode one real file into a side directory (originals untouched).
#    This is your first proof-of-life — duration depends on your GPU
#    (see throughput notes above). Watch the progress line for fps and ETA.
./video_optimizer.py optimize /mnt/media/Movies \
    --output /tmp/test --limit 1 --auto

# 4. Confirm the output is good (open it, scrub it, compare to original).
ls /tmp/test/...

# 5. Encode the whole library, in-place, with originals moved to a recycle dir.
#    Recycle directory is auto-detected; if there's an @Recycle / .@Recycle /
#    #recycle / .Trash inside the path it's used, otherwise <PATH>/.@Recycle
#    is created. Originals are *moved*, not deleted, so the run is undoable
#    until you empty the recycle dir.
./video_optimizer.py optimize /mnt/media/Movies --in-place --auto
```

`optimize` runs scan → plan → uhd-archive → hd-archive in sequence with
calibrated defaults (CQ 15 for 2160p, CQ 21 for 1080p, AV1 + MKV target,
filename rewriting on, REENCODE marker on). Each tier's resolution gate
keeps them from clobbering each other.

To stay closer to the originals (no filename rewriting, custom CQ, etc.),
use `apply` directly — see [Common workflows](#common-workflows).

### doctor (preflight checks)

Run before your first encode to surface setup problems early. Reports:

- Whether `ffmpeg` and `ffprobe` are on PATH (with version strings).
- Which video encoders are available, and which one each target picks.
- Whether `/dev/dri/renderD128` exists (VAAPI device — only matters for VAAPI).
- Whether the SQLite db is reachable, with a count of cached files and
  pending decisions.
- Optional: a real probe of a file you point it at, with summary fields.

```bash
./video_optimizer.py doctor
./video_optimizer.py doctor --probe /mnt/media/Movies/some_file.mkv
```

Exits 0 if every section is green, 1 with a punch-list of issues otherwise.

---

## How it works (mental model)

The CLI is a four-stage pipeline backed by a single SQLite db. Each stage is
independent and idempotent — re-run any stage without re-running the others.

```
            ┌─────────┐    ┌─────────┐    ┌─────────┐    ┌─────────┐
  PATH ───▶ │  scan   │ ─▶ │  plan   │ ─▶ │  apply  │ ─▶ │ status  │
            └─────────┘    └─────────┘    └─────────┘    └─────────┘
                 │              │              │
                 ▼              ▼              ▼
              files         decisions       decisions
              table       (status=pending)  (completed/failed/skipped)
```

1. **`scan PATH`** — recursively walk a directory, probe every video file
   with `ffprobe`, store the result in the cache. Re-running is cheap: files
   whose `(size, mtime)` matches a cached row are skipped. Probe runs in
   parallel (`--workers N`, default `min(8, cpu_count())`).
2. **`plan`** — run the rules engine over the probe cache. Each fired rule
   produces a candidate row in the `decisions` table with `status='pending'`.
   **`plan` clears every pending decision before re-evaluating** — re-running
   plan starts the queue over (by design; you cannot "amend" a plan).
3. **`apply`** — encode every pending decision, updating each row's status
   to `completed`, `failed`, or `skipped`. The `hd-archive` and `uhd-archive`
   presets are opinionated wrappers around `apply` with calibrated defaults.
4. **`status`** — show recent runs and pending decisions. Read-only.

`optimize PATH` runs all three live stages plus a couple of conveniences in a
single command (auto-resolves a recycle dir, sequences UHD then HD).

State lives at `~/.video_optimizer/state.db` by default. Override with
`--db FILE` on any subcommand. The path can be on a network share, but
the SQLite WAL needs the share to honor `fcntl()` — local disk is safest.

### Probe-time size gate

Files smaller than `--min-size` (default **1 GB**) are recorded in a
separate `skipped_files` table at scan time and never run through ffprobe
or rule evaluation. This filters out trailers, extras, and sample files
(Plex / Jellyfin commonly download these into movie folders) where the
projected savings don't justify the encode.

The gate is re-evaluated on every scan from the file's current size:

- A file that grows above the threshold (or a threshold lowered between
  runs) gets its skip row cleared and is probed normally.
- A file that shrinks below the threshold (or a threshold raised between
  runs) gets its probe row evicted and any pending decisions for it
  dropped — `plan` will not propose it.

`--min-size 0` disables the gate entirely. Suffixes are binary
(`1G` = `1024 ** 3`); `500M`, `750M`, `2G` etc. all work.

```bash
./video_optimizer.py scan /mnt/media --min-size 500M       # more inclusive
./video_optimizer.py scan /mnt/media --min-size 0          # probe everything
```

### Probe cache key: `(size, mtime)`

This is worth knowing because it has a footgun. Files whose size and mtime
match a cached row are **skipped without re-probing**. If a file is edited
in place with mtime preserved (rare but possible — some scrubbing tools, NAS
sync software, and rsync `--archive` operations can do it), the cache rows
will silently go stale.

Escape hatch: `reprobe PATH` (alias for `scan --no-probe-cache`) forces
re-probe of every file under PATH, ignoring cache. Use it whenever you
suspect a probe row doesn't reflect the current file content.

---

## Common workflows

### "Just shrink my Movies folder" (1080p library, in-place)

```bash
./video_optimizer.py optimize /mnt/media/Movies --in-place --auto
```

Equivalent to: scan → plan → uhd-archive (no-op if no UHD content) →
hd-archive, with originals atomically moved to `<path>/.@Recycle` (or an
existing recycle dir if one is already there). Keeps `en` + `und` audio,
adds AAC 5.1 + 2.0 compat tracks, rewrites filenames to drop the old codec
token and add `AV1.REENCODE`.

### "Mixed 4K + 1080p library, side mode for safety"

```bash
./video_optimizer.py optimize /mnt/media --output /mnt/media/_optimized --auto
```

Originals untouched; outputs go under `_optimized/` mirroring the source
tree. Re-run after inspecting; copy/move into place by hand or run `apply
--mode replace` later to commit.

### "Just one specific file" (test the pipeline first)

```bash
# 1. Probe just that file (or its containing folder).
./video_optimizer.py scan /path/to/specific/file.mkv

# 2. Plan and inspect — this prints projected savings, rules fired, target.
./video_optimizer.py plan --target av1+mkv

# 3. Dry-run to see the exact ffmpeg invocation.
./video_optimizer.py apply --dry-run --auto \
    --mode side --output-root /tmp/preview --source-root /path/to

# 4. Real run, one file only.
./video_optimizer.py apply --auto --limit 1 \
    --mode side --output-root /tmp/test --source-root /path/to
```

### "Re-encode after a Radarr import score change"

After you tweak Custom Format rules or quality scoring, the file you have
on disk may now want a re-encode. The probe data hasn't changed, so:

```bash
./video_optimizer.py reprobe /mnt/media/Movies/<title>       # only if mtime preserved
./video_optimizer.py plan --target av1+mkv                   # regenerate decisions
./video_optimizer.py apply --auto --limit 1 ...              # apply
```

### "Custom run with everything explicit"

```bash
./video_optimizer.py scan /mnt/media
./video_optimizer.py plan --target av1+mkv --rules over_bitrate
./video_optimizer.py apply --auto \
    --mode replace --recycle-to /mnt/media/@Recycle \
    --hwaccel qsv --quality 22 --keep-langs en,ja,und \
    --rewrite-codec --reencode-tag --no-dotted \
    --limit 5
```

### `archive.sh` (turnkey driver for cron / nohup)

`archive.sh` at the repo root is a shell wrapper that pre-fills site-specific
paths and runs scan → plan → confirm → apply for one preset. Edit the
`SCAN_PATH` / `SOURCE_ROOT` / `RECYCLE_TO` / `LIMIT` defaults at the top of
the file, then:

```bash
./archive.sh --preset hd --limit 25 --yes      # 25 HD encodes, no prompt
./archive.sh --preset uhd --skip-scan          # rescan-skip, useful in cron
./archive.sh --preset hd --dry-run             # preview only
```

`optimize` is the equivalent for the live, interactive case;
`archive.sh` is the equivalent for unattended / scripted runs.

---

## Presets: HD vs UHD

Two opinionated wrappers around `apply` for the two most common library
shapes. Call them with the same flag surface as `apply` minus the things
they hard-code (`--target`, `--rewrite-codec`, `--reencode-tag`).

| Preset | Target | Quality (AV1 CQ) | Size target | Resolution gate | HW decode | Filename rewrite | REENCODE tag | Audio kept |
|--------|--------|------------------|-------------|-----------------|-----------|------------------|--------------|------------|
| `hd-archive` | `av1+mkv` | 21 | ~5 GB/hr | height ≤ 1439 | **off** | yes (dotted) | yes | `en,und` |
| `uhd-archive` | `av1+mkv` | 15 | ~12 GB/hr | height ≥ 1440 | **on** | yes (dotted) | yes | `en,und` |

Both run `av1_qsv -preset veryslow` with the archive-tuned QSV flag set
(`-extbrc 1 -low_power 0 -adaptive_i 1 -adaptive_b 1 -b_strategy 1 -bf 7
-refs 5 -profile:v main`). Two values are tuned by tier:
`-look_ahead_depth` is **100** for UHD (~4s) and **60** for HD; `-g` (GOP)
is **240** for UHD and **120** for HD. Pure ICQ — no `-maxrate` /
`-bufsize`; on av1_qsv the combination `extbrc + ICQ + maxrate` collapses
to a hybrid VBR mode that under-allocates by an order of magnitude (the
cap-and-floor pair acts as a hard ceiling instead of a peak buffer).

### The hw_decode asymmetry (measured, not historical)

`uhd-archive` defaults to QSV decode → QSV encode
(`-hwaccel qsv -hwaccel_output_format qsv`). `hd-archive` defaults to CPU
decode → QSV encode and **flipping HD to HW decode actively slows it down**.
Three reasons:

1. **Media-engine contention** — QSV decode and QSV encode share the same
   fixed-function blocks. With both on the asic, each frame the decoder
   produces makes the encoder wait its turn.
2. **Surface-pipeline negotiation cost** — the qsv-surface path sets up a
   frame pool, negotiates formats, and synchronizes between decode and
   encode contexts. For HD H.264 (which SW-decodes at ~1500 fps
   single-threaded — ≥60× the rate the encoder consumes frames), that
   setup cost is a meaningful fraction of per-frame time.
3. **Asic latency vs throughput** — GPU decoders are tuned for sustained
   throughput, not single-frame latency. SW decode finishes each frame
   faster end-to-end when the work fits comfortably on 1–2 cores.

At 2160p the SW HEVC decoder takes 4–6 cores on high-bitrate sources
(50–80 Mbps Blu-ray remuxes) and competes with the libopus 5.1 + AAC 2.0
re-encode for cores; the encoder asic ends up decode-starved. Routing
decode to the asic frees the CPU and runs the encoder at its actual
ceiling — measured ~50% wall-clock improvement on representative UHD HDR
sources.

Override per run with `--no-hw-decode` / `--hw-decode`.

### Resolution gate: how mixed libraries work

When a preset's resolution band excludes a candidate, the candidate is
**deferred** — left in the `pending` queue for a follow-up run rather than
marked done or failed. This is how `optimize` chains UHD then HD: each
preset processes its own band and skips the other's, leaving those rows
for the next pass.

You can reproduce this manually:

```bash
./video_optimizer.py scan /mnt/media
./video_optimizer.py plan --target av1+mkv

./video_optimizer.py uhd-archive --auto \
    --output-root /mnt/media/_optimized --source-root /mnt/media
# (defers 1080p and below)

./video_optimizer.py hd-archive --auto \
    --output-root /mnt/media/_optimized --source-root /mnt/media
# (picks up the deferred 1080p)
```

### Software fallback

If `av1_qsv` isn't available, `select_encoder` falls back through the
`ENCODER_PREFERENCE` table to whatever is. Reasonable software fallbacks:
`libsvtav1 -preset 6 -pix_fmt yuv420p10le` or `libaom-av1 -cpu-used 4`.

### Things you can override on a preset

`--quality`, `--keep-langs`, `--hwaccel`, `--mode`, `--output-root`,
`--source-root`, `--backup`, `--recycle-to`, `--allow-hard-delete`,
`--limit`, `--timeout`, `--hw-decode` / `--no-hw-decode`, `--compat-audio` /
`--no-compat-audio`, `--no-dotted`, `--name-suffix`, `--reencode-tag-value`,
`--dry-run`, `--verbose`, `--db`. To change anything else (target codec,
disable rewrite-codec, etc.), use `apply` directly.

---

## Rules and the bitrate table

A file becomes a candidate when at least one **non-advisory** rule fires.
Advisory rules attach to a candidate but cannot create one alone.

| Rule | Fires when | Severity | Effect |
|------|-----------|----------|--------|
| `over_bitrate` | Video bitrate exceeds the per-resolution flag threshold (e.g. 1080p > 10 Mbps). | medium / high | Re-encode at the target's CRF/CQ. |
| `legacy_codec` | Codec is MPEG-2, MPEG-4 part 2, VC-1, WMV1/2/3, H.263, RealVideo, or Theora. | high | Re-encode to target codec. |
| `container_migration` | Container is AVI, WMV, ASF, FLV, MPEG, VOB, or MPEG-TS. | low | When this is the *only* fired rule and the video stream is already H.264 / HEVC / AV1 / VP9, do a stream-copy remux (no re-encode). |
| `hdr_advisory` | Source has HDR colour metadata (PQ / HLG / BT.2020). | medium | **Advisory only** — never the sole reason for a candidate. `apply` re-encodes HDR sources as HDR with metadata preserved. |

Restrict the rule set with `--rules over_bitrate,legacy_codec` on `plan`.

### Bitrate flag table

| Resolution | Target | Flag if > |
|------------|--------|-----------|
| 480p       | 1.5 Mbps | 3 Mbps |
| 720p       | 3 Mbps   | 6 Mbps |
| 1080p      | 5 Mbps   | 10 Mbps |
| 1440p      | 9 Mbps   | 18 Mbps |
| 2160p      | 16 Mbps  | 32 Mbps |

**High** severity = 2× over threshold; **medium** = 1–2× over; **low** =
container migration only. Halve the gap (flag at 8 Mbps for 1080p instead
of 10) to be more aggressive — edit `BITRATE_FLAG_TABLE` in
`optimizer/presets.py`.

---

## Audio, subtitles, and HDR

### Targets and hardware acceleration

`--target` selects codec + container:

- `av1+mkv` — default, best storage
- `hevc+mp4`
- `h264+mp4` — compatibility

`--hwaccel` selects encoder family:

- `auto` — best available on this system (default)
- `qsv` — Intel Quick Sync (`av1_qsv`, `hevc_qsv`, `h264_qsv`)
- `nvenc` — NVIDIA
- `vaapi` — Linux generic (uses `/dev/dri/renderD128`)
- `videotoolbox` — macOS
- `software` — `libsvtav1` / `libx265` / `libx264`
- `none` — alias for `software`

Falls back to software automatically if no hardware encoder is available.
`doctor` reports which encoder will be picked per target.

### Audio: the 3-stream ladder

Every output has a deterministic 3-stream layout:

| Output | Role | Source |
|--------|------|--------|
| Stream 0 (default) | Highest-quality available | Passthrough — best lossless track if present, else best lossy track. Picked by `(lossless, codec_rank, channels, default_flag)`. |
| Stream 1 | 5.1 surround compat | Best 5.1 source track if present (excluding stream 0); otherwise **Opus 5.1 @ 384 kbps** encoded from stream 0 (when stream 0 has ≥ 6 channels). |
| Stream 2 | Stereo compat | Best 2.0 source track if present (excluding streams 0/1); otherwise **AAC-LC 2.0 @ 256 kbps** encoded from stream 0. |

Streams 1 and 2 are skipped only when the source can't sensibly produce
them. Common Blu-ray remux case (lossless surround source + native 5.1 +
native 2.0) yields 3 fully-passthrough streams with no transcode at all.

Filter rules:

- `--keep-langs en,und` (default) controls eligibility. Two-/three-letter
  language codes (`en`/`eng`, `ja`/`jpn`) are equivalent.
- **Commentary tracks are dropped** regardless of language — any audio
  stream whose title contains "commentary" (case-insensitive). The default
  track is always considered (so output is never silent); if every track
  looks like commentary the first non-commentary fallback is kept.
- **Parallel lossless tracks** (TrueHD + DTS-HD MA from the same master,
  common in 4K Blu-ray remuxes) collapse to whichever ranks higher —
  TrueHD wins. Saves ~4 Mb/s on a typical UHD remux.
- `--no-compat-audio` collapses output to stream 0 only.

Compat tracks are tagged non-default and titled `Opus 5.1 (compat)` /
`AAC 2.0 (compat)` so players treat them as fallbacks.

### Pre-strip at the demuxer (multi-language sources)

Since v0.5.17, non-kept audio and subtitle streams are dropped at the
demuxer with `-discard:a:N all` / `-discard:s:N all` placed before `-i`.
This avoids a frame-zero deadlock that v0.5.16 hit on Blu-ray remuxes
with 7+ parallel audio tracks: ffmpeg's matroska demuxer interleaves
packets for every active stream by container timestamp, and on those
sources the windows between audio packets get tight enough that the QSV
video decoder's input queue starves. Pre-stripping keeps the queue fed.
Source-side indexing is preserved (no renumbering), so `-map 0:a:N?`
references stay valid.

### Subtitles

Subtitles in the keep-langs list are retained.

- **MKV outputs** preserve subtitles as-is (text + image both fine).
- **MP4 outputs** convert text subtitles to `mov_text`. Image subtitles
  (`hdmv_pgs_subtitle`, `dvd_subtitle`) are dropped with a warning — MP4
  cannot carry them.

### HDR

HDR sources are re-encoded as HDR — 10-bit AV1 with BT.2020 / PQ tagging
passed through, mastering display + MaxCLL SEI metadata propagated end-to-end
via libavcodec → av1_qsv → matroska muxer. No extra flags needed; just point
`apply` at an HDR source.

10-bit sources are pinned to `-pix_fmt p010le` so they don't silently
downconvert through QSV's default pipeline; 8-bit sources are left to the
encoder default. Source color metadata (BT.709 vs BT.2020/PQ) is passed
through to the output rather than forced.

### Dolby Vision (skipped)

Dolby Vision sources are auto-skipped at plan time. `av1_qsv` reliably
wedges on DV-tagged HEVC streams (Profile 7 stalls at frame 0; Profile 8
stalls partway in), so `plan` drops any source where the ffprobe `DOVI
configuration record` side-data is present. The skip shows up in the plan
summary as `dv_blocked: N`. Dual-layer DV files often carry an HDR10
fallback; for those, an external tool can strip the EL/RPU layer and the
remaining HDR10-only file will encode normally. See `TODO.md` (DV-aware
encode path) for the planned permanent fix.

---

## Output modes and safety

Two top-level modes; `--mode replace` has three sub-modes for what happens
to the original.

### `--mode side` (default, safer)

Outputs go under `--output-root DIR`, mirroring the source tree relative to
`--source-root` (defaults to filesystem root). Originals are never touched.
Use this for first-time runs, dry-runs, and any time you want to inspect
output before committing.

```bash
./video_optimizer.py apply --auto \
    --mode side \
    --output-root /tmp/preview --source-root /mnt/media
```

### `--mode replace`: in-place swap

Encode to the new file's final path, then dispose of the original. Failures
leave the original intact. Three options for original disposal:

| Flag | Behavior | Disk use during run | Recovery |
|------|----------|---------------------|----------|
| `--recycle-to DIR` | **Atomically move** original into `DIR`, preserving source hierarchy under it. | None extra (single rename) | Restore from `DIR` |
| `--backup DIR` | **Copy** original to `DIR` before delete. | Doubled (every original copied before delete) | Restore from `DIR` |
| (neither) | **Hard-delete** original after successful encode. | None | None — original is gone |

Mutually exclusive: `--backup` and `--recycle-to` cannot both be set.

`--recycle-to` is the right choice for NAS targets that have a
recycle-bin directory (Synology `@Recycle`, QNAP `#recycle`, etc.). The
move is atomic and instant when source and target are on the same
filesystem; files appear in the NAS recycle-bin view and age out per the
share's auto-purge policy.

`--backup` is useful when the target filesystem doesn't have a recycle
directory or you want a separate backup target (e.g., another disk).

The hard-delete default (`--mode replace` with neither flag) is dangerous
and intentionally hard to invoke unattended. Under `--auto` you must also
pass `--allow-hard-delete` to acknowledge originals will be irreversibly
deleted; otherwise `apply` refuses to start. Interactively, `apply` shows
the same warning and requires a typed `y` to proceed.

### Disposal ordering (the safety guarantee)

The original is never disposed until **the encode succeeds AND** (if
`--backup` is set) the backup copy succeeds **AND** (if `--recycle-to` is
set) the rename to the recycle directory succeeds. If any step fails, the
original stays put and the decision is marked `failed` (or `completed` with
a warning, when only the disposal step failed). Preserve this ordering if
you touch `_finalize_output` / `_execute_encode`.

---

## Watchdog, stalls, and recovery

### The 5-minute encode watchdog

Each encode is wrapped in a watchdog that kills the process if **neither**
the muxed-output timestamp **nor** the decoded frame counter advances for
300 seconds. This catches genuine encoder hangs (av1_qsv occasionally
wedges on certain bitstream patterns) without false-positiving on the
deep-lookahead warmup, where `out_time_ms` can sit at 0 for several
minutes while frames flow normally.

Stalled encodes are recorded in the `decisions` table with
`status='failed'` and an error message naming both signals at the time of
the kill: `encoder stalled — no progress for 300s (out_time=Xs, frame=N)`.
A frame-zero stall (`frame=121` after 5 minutes) usually means the source
trips the encoder at startup; a mid-encode stall (`frame=78104` mid-file)
usually means a specific bitstream pattern wedges it.

### Two-strikes auto-skip

A file that has hit the watchdog **twice** is auto-skipped on subsequent
`plan` runs (it stays in the cache, but no decision is created). The skip
appears in the plan summary as `stall_blocked: N`. The list is the
operator's signal to grab a different release of the title.

```bash
./video_optimizer.py replace-list           # see what's been auto-skipped
```

### Adaptive encode timeout

`--timeout` defaults to `max(3600, 6 × duration_seconds)`. A 3-hour movie
gets an 18-hour cap, leaving headroom for slow software AV1 encodes while
still catching genuinely stuck processes. `--timeout 0` disables the
timeout entirely; an explicit positive value wins.

### Progress display

While encoding, the per-file progress line shows percentage, position,
frames, fps, speed, and ETA:

```
[3/25] Movie.Title.mkv:  42.3% 3675/8576s f=88113 50.6fps 2.11x ETA 38m41s
```

The watchdog tracks both `out_time_ms` and frame count; the displayed
position is `max(out_time_seconds, frames / source_fps)`, so even if
ffmpeg's muxer reporting stalls (av1_qsv with deep B-frame buffering does
this) the bar keeps advancing.

### When something goes wrong

| Symptom | Likely cause | Action |
|---------|-------------|--------|
| `nothing to apply: no pending decisions` | Plan never ran, or plan ran with no candidates, or apply already drained the queue. | `./video_optimizer.py status` to inspect; `./video_optimizer.py plan` to regenerate. |
| Probe data looks stale (file changed but isn't being re-evaluated) | Edited in place with mtime preserved. | `./video_optimizer.py reprobe PATH` to bypass cache. |
| Encode wedged at 0% with low fps | Could be the multi-lang demux deadlock (pre-v0.5.17) or a watchdog-flagged encoder hang. | Check the log — if `f=` is also stuck, it's a real hang and the watchdog will kill it at 300s. |
| `apply` keeps failing on the same file | Probably a file-specific encoder issue. After 2 stalls, `plan` will auto-skip it. | `replace-list` to confirm; grab a different release. |
| DB seems corrupt / locked | Concurrent runs on the same db, or a crash mid-write. | Stop all running `apply` processes; `sqlite3 ~/.video_optimizer/state.db .schema` to verify reachable; in extreme cases, delete the db and re-scan (loses run history but not files). |

Inspect the db directly when you need to:

```bash
sqlite3 ~/.video_optimizer/state.db "SELECT path, status FROM decisions LIMIT 10;"
sqlite3 ~/.video_optimizer/state.db "SELECT * FROM runs ORDER BY id DESC LIMIT 5;"
```

---

## Radarr / Sonarr integration

Radarr and Sonarr identify files by name and score quality using *Custom
Formats* (regex matchers on filename / release tags). When you re-encode a
file, you want the new filename to:

1. Reflect the new codec (so Custom Format scoring picks it up).
2. Drop the *old* codec token (so the regex doesn't double-match).
3. Optionally carry a marker that lets you tell the Arr app "this is a
   re-encode — stop trying to upgrade it."

Five composable flags cover this:

| Flag | Effect |
|------|--------|
| `--rewrite-codec` | Strip foreign codec tokens (`H.264`, `H.265`, `HEVC`, `x264`, `x265`, `AVC`) and insert the canonical token for the new target (`AV1` / `HEVC` / `H.264`). Outputs Plex-style dotted filenames by default. |
| `--reencode-tag` | Append a `REENCODE` token. Use a Custom Format that matches `\bREENCODE\b` and assign it a permanent negative or saturating score — the Arr app then never tries to upgrade the file. |
| `--reencode-tag-value STR` | Override the token (default `REENCODE`). |
| `--no-dotted` | Keep input whitespace style instead of forcing dots. |
| `--name-suffix STR` | Free-form trailing append; runs after `--rewrite-codec` and `--reencode-tag`. |

`hd-archive` and `uhd-archive` enable `--rewrite-codec` and `--reencode-tag`
by default. `optimize` does too.

### Example transforms (target = `av1+mkv`)

| Input stem | `--rewrite-codec` output |
|-----------|--------------------------|
| `Inception (2010) 1080p BluRay H.264-RELEASEGRP` | `Inception.(2010).1080p.BluRay-RELEASEGRP.AV1` |
| `Movie.Name.2010.2160p.HDR.HEVC.x265.10bit-GRP` | `Movie.Name.2010.2160p.HDR.10bit-GRP.AV1` |
| `Some Movie (2015) [HEVC]` | `Some.Movie.(2015).AV1` |
| `Movie.Name.2010` (no codec token) | `Movie.Name.2010.AV1` |

With `--reencode-tag` added, each gets `.REENCODE` appended:
`Inception.(2010).1080p.BluRay-RELEASEGRP.AV1.REENCODE`.

### Custom Format setup

Add a Custom Format with a regex match on `\bREENCODE\b` and either:

- Assign it a saturating positive score (so it's at the ceiling and no
  upgrade can beat it), or
- Assign it a strong negative score in the Custom Format set used by your
  upgrade-monitoring profile (so the file is "good enough" and ignored).

**Caveat:** changing the filename means the Arr app re-imports on its
next sync — entries may briefly be flagged "missing" between scans. Run
`apply` outside of active library-sync windows for big batches, or pause
the Arr app's library polling while the run is in progress.

---

## Flag reference

### Subcommands

| Subcommand | Purpose |
|------------|---------|
| `optimize PATH` | One-shot scan+plan+apply with safe defaults. New-user friendly. |
| `doctor` | Preflight checks for ffmpeg, encoders, GPU, db. |
| `scan PATH` | Recursive walk + ffprobe → cache. |
| `reprobe PATH` | Force re-probe (ignores cache). Alias for `scan --no-probe-cache`. |
| `plan` | Run rules engine → pending decisions. **Destructive to the pending queue.** |
| `apply` | Encode pending decisions. |
| `hd-archive` / `uhd-archive` | Opinionated `apply` wrappers; CQ + height-gate baked in. |
| `status` | Recent runs + pending decisions. Read-only. |
| `list-encoders` | Available ffmpeg encoders + per-target picks. |
| `replace-list` | Files auto-skipped after 2+ stalls. |

### Common flags

- `--db FILE` — SQLite state file (default `~/.video_optimizer/state.db`)
- `--verbose` / `-v` — log ffmpeg commands and per-file probe details
- `--dry-run` — print planned ffmpeg commands; encode nothing

### apply / preset / optimize flags

**Workflow:**

- `--auto` — skip per-file confirmation
- `--mode side|replace` — output layout (default: `side`)
- `--output-root DIR` — required for side mode; mirrored output tree
- `--source-root DIR` — strip this prefix when computing output paths
- `--backup DIR` — copy original to `DIR` before delete (replace mode)
- `--recycle-to DIR` — atomic move into `DIR` (replace mode); preserves
  source hierarchy
- `--allow-hard-delete` — required to combine `--mode replace` with
  `--auto` when neither `--backup` nor `--recycle-to` is set
- `--limit N` — process at most N candidates (`optimize`: per tier)
- `--min-height` / `--max-height` — resolution gate (defers OoB)

**Encoding:**

- `--quality N` — encoder CQ/CRF override (lower = better quality)
- `--hwaccel auto|qsv|nvenc|vaapi|videotoolbox|software|none`
- `--keep-langs en,und` — audio/subtitle language filter
- `--timeout N` — per-file ffmpeg wall-clock cap; `0` disables; default
  adaptive (`max(3600, 6 × duration_seconds)`)
- `--hw-decode` / `--no-hw-decode` — zero-copy QSV decode→encode pipeline
- `--compat-audio` / `--no-compat-audio` — 3-stream audio ladder; default on

**Naming (Radarr/Sonarr):**

- `--rewrite-codec` — strip foreign codec tokens, insert target token
- `--reencode-tag` — append `REENCODE` token
- `--reencode-tag-value STR` — override the marker token
- `--no-dotted` — keep whitespace style instead of forcing dots
- `--name-suffix STR` — free-form trailer

### `optimize`-specific flags

- `path` (positional) — library directory to optimize
- `--output DIR` — side mode output root (mutually exclusive with `--in-place`)
- `--in-place` — replace mode with auto-resolved recycle dir
- `--recycle-to DIR` — override the auto-resolved recycle dir
- `--skip-scan` — reuse the existing probe cache; skip the scan phase
- `--min-size SIZE` — passed through to the scan phase (default `1G`;
  `0` disables; also see the dedicated section above)

### `doctor` flags

- `--probe PATH` — additional sample probe of a video file

### `scan` / `reprobe` flags

- `--workers N` — parallel ffprobe workers (default `min(8, cpu_count())`)
- `--min-size SIZE` — skip files smaller than `SIZE` (default `1G`).
  Accepts bytes or `K` / `M` / `G` / `T` suffixes; `0` disables. See
  [Probe-time size gate](#probe-time-size-gate).
- `--no-recursive` — top-level only
- `--no-probe-cache` — force re-probe of every file (alias: `reprobe`)

---

## Tuning

The knobs you would realistically retune without changing logic all live
in **`optimizer/presets.py`**:

- `PRESETS` — preset CQ, height gate, target codec, hw_decode default.
- `AV1_QSV_TIER` — per-tier (`hd` / `uhd`) `look_ahead_depth` and `gop`.
  No `maxrate` / `bufsize` on purpose — `extbrc + ICQ + maxrate` on
  av1_qsv collapses to a hybrid VBR mode that under-allocates by an order
  of magnitude. Pure ICQ produces the expected operating point.
- `AV1_QSV_BASE` — av1_qsv flags shared across tiers (`bf`, `refs`,
  `extbrc`, `preset`, `profile`, …).
- `BITRATE_FLAG_TABLE` — `(target_mbps, flag_above_mbps)` per resolution
  bucket, consumed by the rules engine.
- `MIN_PROBE_SIZE_BYTES` — scan-time minimum file size; smaller files
  are recorded in the `skipped_files` cache and never probed. Default
  `1024 ** 3` (1 GiB). Override per run with `--min-size`.

Edit, save, run — no parsing layer, no precedence rules. CLI flags
(`--quality`, `--keep-langs`, etc.) still override on a per-run basis.

If you find yourself maintaining multiple co-existing tunings (per-machine,
per-library, per-content-type), graduate this file to a TOML config at
`~/.video_optimizer/config.toml`. The shape of `presets.py` maps cleanly to
TOML tables, so the migration is mechanical when the need arrives.

---

## Repository layout

```
video_optimizer/
├── README.md
├── CLAUDE.md                 # repo-orientation notes for AI coding agents
├── TODO.md                   # actionable backlog
├── archive.sh                # turnkey driver for unattended NAS-archive runs
├── version.py
├── video_optimizer.py        # entrypoint shim
├── optimizer/
│   ├── cli.py                # argparse + subcommand dispatch
│   ├── crawler.py            # recursive directory walk
│   ├── probe.py              # ffprobe → ProbeResult
│   ├── models.py             # dataclasses + JSON ser/de
│   ├── presets.py            # tuning surface (CQ, GOP, lookahead, bitrate table)
│   ├── rules.py              # rule engine + v1 rules
│   ├── encoder.py            # ffmpeg command builder + runner; audio ladder
│   ├── db.py                 # SQLite persistence
│   └── report.py             # text + JSON candidate rendering
└── tests/                    # stdlib unittest; run with
    ├── _fixtures.py          # `python3 -m unittest discover -s tests -v`
    ├── test_audio_ladder.py
    ├── test_naming.py
    ├── test_progress.py
    └── test_qsv_args.py
```

Run tests with:

```bash
python3 -m unittest discover -s tests -v
ruff check .
```

---

## Known limitations

These are *current-state caveats* for users. Planned/actionable work lives
in `TODO.md` instead.

- **Dolby Vision sources are skipped at plan time.** av1_qsv reliably
  wedges on DV-tagged HEVC streams. Skipped sources show up in plan
  summary as `dv_blocked: N`. HDR10 metadata *is* preserved end-to-end
  for non-DV HDR sources.
- **`apply` runs sequentially** — one ffmpeg at a time. The encode itself
  is GPU-bound on Battlemage at preset veryslow, so the win from parallel
  apply only materializes on multi-engine GPUs. See `TODO.md`.
- **No filename normalization beyond `--name-suffix` + `--rewrite-codec`** —
  originals' stems are otherwise preserved.
- **No `libfdk_aac` preference** — the AAC compat track uses ffmpeg's
  native AAC encoder. libfdk_aac is GPL-incompatible and absent from most
  distros' default ffmpeg builds. The 256k bitrate is calibrated against
  the native encoder; transparent for stereo at this rate.
- **No multi-pass software encoding** — single-pass only across all
  encoders.
- **Some hardware encoders may not preserve display rotation metadata**
  from phone-recorded sources (notably `av1_qsv`).
- **Fixed audio policy** — the 3-stream ladder (best passthrough + 5.1
  compat + 2.0 compat) and the `keep-langs en,und` default are baked in.
  Users who want a different audio policy (keep all language tracks, drop
  compat tiers, different compat codecs) need to edit `encoder._build_audio_ladder`
  directly today; no CLI knob exposes it.
