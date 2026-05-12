# video_optimizer

> Point a Linux box at a movie library, walk away, come back to a smaller
> movie library. Originals stay until you say otherwise.

A stdlib-only Python 3.10+ wrapper around `ffmpeg` / `ffprobe` that
re-encodes the AV1-eligible content in your library — and only the
AV1-eligible content. Built around four ideas:

1. **You shouldn't have to think.** `./video_optimizer.py /path/to/movies`
   does the right thing. There's a wizard too, if you'd rather click
   through prompts.
2. **Calibrated presets per resolution.** UHD / HD / SD / UHD-FILM each
   have their own CQ, encoder preset, and decode pipeline tuned to
   actually finish in this lifetime.
3. **Smart skips, dumb defaults.** Auto-skips AV1 sources, prior
   outputs of itself, low-bitrate sources, and Dolby Vision Profile 5
   (where stripping the DV layer leaves a green/oversaturated mess).
   One-pass DV strip handles Profile 7 and 8 so `av1_qsv` doesn't
   wedge on them — which it absolutely will if you let it.
4. **Originals don't disappear.** Default mode writes outputs alongside
   the source as `<stem>.AV1.REENCODE.mkv` and leaves the original on
   disk. A separate `cleanup` step removes them when you're satisfied.

**Optimized for Intel Arc / Battlemage** via `av1_qsv`. NVIDIA (`av1_nvenc`
on RTX 4000+) and AMD (`av1_vaapi`) work via the encoder fallback chain;
the throughput numbers haven't been re-tuned for them. Software fallback
(`libsvtav1`) is always available if you want to excercise your CPU fan.

For design rationale, calibration history, and failure-mode catalog,
see [`NOTES.md`](NOTES.md). For the backlog see [`TODO.md`](TODO.md).
For day-to-day contracts, that's the rest of this file.

---

## System requirements

- **Linux**, recent kernel. macOS and Windows are not supported. QSV /
  VAAPI paths assume `/dev/dri/renderD128`.
- **Python 3.10+**, stdlib only — no `pip`, no virtualenv.
- **`ffmpeg` 7.0+ and `ffprobe`** on `PATH`. Older `ffmpeg` lacks the
  `dovi_rpu` bsf the DV strip pipeline needs.
- *Optional:* **Intel Arc / Battlemage** for `av1_qsv` speeds. Without
  hardware AV1, software fallback runs at "go on vacaction" speeds
  instead of "go make a sandwich" speeds.
- *Optional:* **`dovi_tool` + `mkvmerge`** only when you opt into
  `--dv-p7-convert` for stubborn Profile 7 sources.

Run `./video_optimizer.py doctor` after install — it confirms ffmpeg,
encoder availability, GPU device nodes, and the SQLite state directory.
Exits non-zero if anything's missing.

---

## Install

No installer. Clone, run.

```bash
# Debian / Ubuntu
sudo apt install ffmpeg python3 git
# Fedora (the rpmfusion ffmpeg, not ffmpeg-free)
sudo dnf install https://download1.rpmfusion.org/free/fedora/rpmfusion-free-release-$(rpm -E %fedora).noarch.rpm
sudo dnf install ffmpeg python3 git intel-media-driver
# Arch
sudo pacman -S ffmpeg python git intel-media-driver

git clone https://github.com/hyper-focused/video_optimizer ~/video_optimizer
cd ~/video_optimizer
./video_optimizer.py doctor
```

For Intel QSV on Battlemage specifically: **kernel 6.13+ (Xe driver) and
Mesa 25+**. `doctor` will tell you if `av1_qsv` isn't wired up.

For NVIDIA / AMD: the fallback chain is QSV → NVENC → VAAPI → libsvtav1.
`./video_optimizer.py list-encoders` shows which encoder gets picked on
your machine.

Optional DV-P7 tooling:

```bash
# Debian / Ubuntu
sudo apt install mkvtoolnix && cargo install dovi_tool
# Fedora
sudo dnf install mkvtoolnix && cargo install dovi_tool
# Arch
sudo pacman -S mkvtoolnix-cli && yay -S dovi-tool-bin
```

---

## Basic usage

Defaults are calibrated for "I have a movie library on a NAS, encode
the AV1-eligible content, leave originals alone until I confirm the
outputs are good."

```bash
# Whole library — UHD → HD → SD pipeline
./video_optimizer.py /mnt/nas/media/Movies

# Dry-run first to see the plan
./video_optimizer.py /mnt/nas/media/Movies --dry-run

# Just one tier
./video_optimizer.py UHD      /mnt/nas/media/Movies   # 4K, CQ 15 (archive-grade)
./video_optimizer.py UHD-FILM /mnt/nas/media/Movies   # 4K, CQ 21 (grainy older film)
./video_optimizer.py HD       /mnt/nas/media/Movies   # 1080p / 720p
./video_optimizer.py SD       /mnt/nas/media/Movies   # below 720p

# Single file (Radarr / Sonarr post-processing)
./video_optimizer.py "/movies/Foo (2023)/Foo.mkv" --replace

# Test on a few files first
./video_optimizer.py UHD /mnt/nas/media/Movies --limit 3

# Interactive wizard
./video_optimizer.py
```

`--replace` atomically moves the original to an auto-detected `@Recycle`
once the encode succeeds. Drop it into Radarr/Sonarr as a post-import
hook and forget about it.

The wizard fires with no args and a TTY: prompts for path, output mode,
tier scope, codec exemptions, count limit. Friendlier than `--help`.

### Removing originals

```bash
./video_optimizer.py cleanup            # dry-run: list what would go
./video_optimizer.py cleanup --apply    # actually unlink them
```

Reads the most recent run's completed encodes from the state db, runs a
3-check safety guard (output exists, output is non-empty, output ≠
source), and unlinks only when all three pass. The nuclear option
requires the safety word.

### Inspect

```bash
./video_optimizer.py status              # recent runs + pending decisions
./video_optimizer.py doctor              # preflight: ffmpeg / encoders / GPU / db
./video_optimizer.py list-encoders       # what encoders are available
```

### Audit for stragglers (orphan source files)

Sometimes a run finishes but originals don't get removed — `cleanup`
never invoked, partial `--replace` failure, interrupted session.
`audit_orphans.py` walks the library, finds every `*.AV1.REENCODE.mkv`,
and reports same-stem siblings still next to it.

```bash
./audit_orphans.py /mnt/nas/media/Movies                    # read-only listing
./audit_orphans.py /mnt/nas/media/Movies --json             # machine-readable
./audit_orphans.py /mnt/nas/media/Movies \
    --apply --recycle-to /mnt/nas/media/@Recycle/Movies     # actually move them
```

Read-only by default. `--apply` preserves the source-dir hierarchy
under the recycle directory so nothing collides.

---

## How it actually works

Four-stage pipeline communicating through a SQLite database at
`~/.video_optimizer/state.db`:

```
  scan  ──►  probe cache (per-file: codec, bitrate, HDR, DV profile, …)
   │
   ▼
  plan  ──►  pending decisions (which files, which target codec)
   │
   ▼
  apply ──►  encoded outputs + run report
   │
   ▼
  cleanup    (later, when you're satisfied: removes originals)
```

Path-taking subcommands (`optimize`, `SD`, `HD`, `UHD`, `UHD-FILM`, bare
invocation, `wizard`) compose all four stages. The standalone
subcommands exist as power-user escape hatches and are hidden from
`--help`.

### Output modes

| Mode | Where outputs go | Original |
|---|---|---|
| **`keep`** *(default)* | Alongside source as `<stem>.AV1.REENCODE.mkv` | Untouched. `cleanup` removes later. |
| **`replace`** | Alongside source | Atomically moved to recycle dir |
| **`side`** | Mirrored under `--output DIR` | Untouched |

Select via `--replace`, `--output DIR`, or the explicit
`--mode {keep,side,replace}`.

### What gets re-encoded

Any source that fires at least one non-advisory rule:

- **Non-AV1 video at any tier.** AV1 is wildly more efficient than
  h.264 / HEVC / MPEG-2 / VC-1 / etc., and CQ encoding preserves quality.
- **Over-bitrate sources** above the per-resolution flag threshold
  (1080p > 10 Mbps, 2160p > 32 Mbps, etc.) — including AV1 sources, if
  they're abnormally fat.
- **Container migration.** AVI / WMV / etc. get re-muxed to MKV even
  when the codec is fine.

### What gets skipped

The tool errs heavily on the side of "don't waste GPU time, don't
clobber existing work":

- Files < **100 MB** (likely trailers, samples, extras)
- Files in `Trailers/` / `Behind The Scenes/` / `Featurettes/`, or with
  `-trailer` / `-bts` / `-deleted` filename suffixes
- Files whose source codec is **already AV1**
- Files whose `.AV1.REENCODE.mkv` sibling **already exists**
- Files with `REENCODE` in their **own filename**
- Files that hit the encoder watchdog **twice** (chronic stalls; see
  `replace-list`)
- **Dolby Vision Profile 5** (no clean HDR10 fallback)
- **Low-bitrate sources** below the AV1 target for their resolution
  (1080p < 5 Mbps, 2160p < 16 Mbps, etc.) — re-encoding wouldn't yield
  meaningful savings and risks perceptual regression

Each skip class has a corresponding `--allow-*` flag — see "Advanced
options".

### Audio and subtitles: the 3-stream ladder

Every encode produces a deterministic three-stream audio output:

1. **Stream 0** — highest-quality passthrough (TrueHD / DTS-HD MA wins
   over the same master's lossy variants)
2. **Stream 1** — 5.1 tier (Opus 5.1, transcoded from stream 0 if the
   source doesn't already have one)
3. **Stream 2** — 2.0 tier (AAC 2.0, transcoded from stream 0)

Subtitles filter to `--keep-langs` (default `en,und`). Image-based subs
(PGS / VobSub) drop when targeting MP4 (which can't carry them);
otherwise they pass through.

Replaces an older "keep every audio track in your language" rule that
produced 5+ streams on Blu-ray remuxes (parallel TrueHD + DTS-HD MA +
multiple lossy + foreign-lang) and burned ~10 Mb/s on audio overhead.
`--original-audio` opts back into that.

### Dolby Vision

`av1_qsv` consistently wedges on DV — Profile 7 stalls at frame 0,
Profile 8 partway in. The one-pass DV strip removes the RPU before the
encoder sees the frames, leaving the HDR10 base layer that the wide-
gamut / high-luma stream rides on.

| DV Profile | Default behavior | Why |
|---|---|---|
| **Profile 8.x** | Strip RPU via `dovi_rpu=strip=true` bsf, encode HDR10 base to AV1 | Base layer is already HDR10 |
| **Profile 7** | Same RPU strip as P8. `--dv-p7-convert` switches to the multi-stage `dovi_tool convert --discard` + `mkvmerge` pipeline | Simple strip handles most modern P7; convert path is opt-in for stubborn sources |
| **Profile 5** | **Skipped** | Base layer isn't HDR10 — it's a custom DV-only colour space; strip leaves a green mess |

**HD DV sources skip the strip pre-pass.** Rare 1080p Apple TV+ Profile
8.x WEB-DLs encode correctly without it; `av1_qsv` ignores RPU side
data on encode and the HDR10 base layer flows through implicitly. Gate
is `height >= 1440`. Filename rewriter still scrubs DV tokens so the
output is labelled HDR10.

**Profile 10** (DV preserved through AV1 as OBU side-data) is on the
radar but unimplemented — waiting on `dovi_tool inject-rpu`'s AV1
support and player ecosystems recognising the resulting stream.

See [`NOTES.md#dolby-vision-pipeline`](NOTES.md#dolby-vision-pipeline)
for profile dispatch internals, bsf scoping pitfalls, and demuxer
pre-trim rationale.

#### Output filename reflects what's actually in the container

The encoded file's name is scrubbed to match what survives the av1
pipeline so Plex / Jellyfin / Radarr don't try to map streams that
aren't there:

| Source token | In the AV1 output | Why |
|---|---|---|
| `DV`, `DoVi`, `Dolby.Vision` | Removed (or substituted with `HDR10`) | RPU stripped before encode |
| `HDR10Plus`, `HDR10+` | Removed (or substituted with `HDR10`) | `av1_qsv` doesn't carry SMPTE 2094-40 |
| `HDR10`, `HDR` | Preserved | Static mastering display + MaxCLL flow through |
| `H.264` / `H.265` / `HEVC` / `x264` / `x265` / `AVC` | Removed | Foreign codec — output is AV1 |
| `MPEG2` / `MPEG-2` / `VC-1` / `XviD` / `DivX` / `VP9` / `WMV` | Removed | Legacy codecs — none survive AV1 encode |

When the source advertised DV or HDR10+ but no plain `HDR10`, the first
stripped token is replaced with `HDR10` so the file stays labelled as
HDR. DV Profile 7/8 always have an HDR10 base layer, so this reflects
the residual stream faithfully. No duplication if `HDR10` is already
there; no spurious injection on SDR sources.

Example: `Movie.2024.Remux-2160p.DV.HDR10Plus.TrueHD.HEVC.mkv` →
`Movie.2024.Remux-2160p.HDR10.TrueHD.AV1.REENCODE.mkv`.

#### Cleaning up legacy outputs: `rename-fix`

Older `.REENCODE` outputs may overstate what's in the container (claiming
DV / HDR10+ the av1_qsv pipeline stripped, or legacy codec tokens that
don't apply). `rename-fix` walks a tree, re-runs each stem through the
current naming rules, and renames in place — sidecars (`.nfo`, `.srt`,
…) come along so Radarr / Sonarr metadata stays paired.

```bash
./video_optimizer.py rename-fix /path/to/library          # preview
./video_optimizer.py rename-fix /path/to/library --apply  # do it
```

Dry-run by default. Only files whose stem carries the `REENCODE` marker
are touched, so it's safe on a mixed library. Existing targets are
never overwritten.

### The UHD bloat fallback

UHD is ambitious: CQ 15 with `veryslow`. Usually that produces a
dramatically smaller file at the same perceptual quality. Occasionally
— grain-dominated older film, mostly — `av1_qsv` spends its bit budget
preserving every grain particle and the output matches or exceeds the
source.

The bloat fallback catches this. At checkpoints (10% / 20% / 30% / 50%
of duration) the tool projects final output size from bytes-so-far. If
the projection ≥ 90% of the encoder's input, ffmpeg is killed and the
file retries once with the **UHD-FILM** tuning (CQ 21, encoder preset
`slow`). The retry finishes ~1.5–2× faster and produces a smaller
output — counter-intuitive, but the encoder stops fighting grain it
was never going to compress efficiently.

Runs silently on healthy encodes. Disable with `--no-auto-relax-cq`.
See [`NOTES.md#preset-rationale`](NOTES.md#preset-rationale) for the
threshold + checkpoint calibration history.

### The output report

After every run, a plain-text report at
`~/.video_optimizer/reports/run-<N>.txt`:

```
OK   3215 MB  /movies/foo.AV1.REENCODE.mkv  (from /movies/foo.mkv)
FAIL encoder_stalled  /movies/baz.mkv
SKIP dolby_vision     /movies/qux.mkv
```

Same data lives in the SQLite db. `cleanup` reads it to know which
originals are safe to remove.

---

## Common options

| Flag | Effect |
|---|---|
| `--dry-run` | Print planned ffmpeg commands and exit. No encoding. |
| `--limit N` | Process at most N candidates (0 = no limit) |
| `--confirm` | Prompt per-file before encoding (default is auto-yes) |
| `--cleanup-after` | Prompt to remove originals after a successful run |
| `--verbose` / `-v` | More chatter (timeout labels, preset tunings) |
| `--output DIR` | Mirror outputs into a separate tree under `DIR` |
| `--replace` | Replace mode: move originals to a recycle dir as encodes complete |
| `--recycle-to DIR` | With `--replace`: explicit recycle directory |
| `--mode {keep,side,replace}` | Explicit mode override |
| `--no-auto-relax-cq` | Disable the UHD bloat fallback |
| `--dv-p7-convert` | Use the dovi_tool convert pipeline for stubborn P7 |
| `--original-audio` | Keep every input audio track via stream-copy |
| `--original-subs` | Keep every input subtitle track via stream-copy |

---

## Advanced options

Hidden from `--help` for tidiness, but still functional. Escape hatches
for debugging or unusual library shapes.

### Re-include something the tool skips

| Flag | What it re-includes |
|---|---|
| `--allow-reencoded` | Files already tagged `REENCODE`, AND files whose `.AV1.REENCODE.mkv` sibling exists |
| `--allow-av1` | AV1 sources (default skipped — re-encoding AV1 is wasteful) |
| `--allow-extras` | Plex-style trailer/extras dirs and `-trailer` / `-bts` filenames |
| `--allow-low-bitrate` | Sources whose video bitrate is below the AV1 target for their resolution |

### Skip a codec entirely

Default is to re-encode every non-AV1 source. To leave specific codecs
alone (typical case: an HEVC archive you trust), pass `--skip-codecs`
with a comma-separated list of ffprobe codec names:

```bash
./video_optimizer.py /lib --skip-codecs hevc                  # leave HEVC alone
./video_optimizer.py /lib --skip-codecs hevc,vp9              # leave HEVC and VP9
./video_optimizer.py /lib --skip-codecs h264,mpeg2video       # only re-encode HEVC
```

Names match `ffprobe -show_streams`; case-insensitive. Common values:
`h264`, `hevc`, `vp9`, `mpeg2video`, `mpeg4`, `vc1`, `wmv3`. (Skipping
`av1` is redundant — auto-skipped anyway.)

### Tuning overrides

| Flag | Effect |
|---|---|
| `--quality N` | Override CQ (defaults: UHD 15, HD 21, SD 24, UHD-FILM 21) |
| `--keep-langs en,und` | Comma-separated language codes to retain |
| `--hwaccel {auto,qsv,nvenc,vaapi,videotoolbox,software,none}` | Force a specific hw backend |
| `--hw-decode` / `--no-hw-decode` | Override the preset's hw-decode default |
| `--min-size BYTES` | Skip files below this size at scan (default 100 MB; accepts `100M`, `1G`) |
| `--db PATH` | SQLite state file (default `~/.video_optimizer/state.db`) |
| `--timeout SEC` | Per-file ffmpeg wall-clock cap (default `max(3600, 6 × duration)`; `0` disables) |

### Pipeline-primitive subcommands

| Subcommand | What it does |
|---|---|
| `scan PATH` | Probe-cache only — populate the db without queueing anything |
| `plan` | Run rules against the cache, write pending decisions |
| `apply` | Encode pending decisions |
| `reprobe PATH` | Force-refresh the probe cache (alias for `scan --no-probe-cache`) |
| `replace-list` | Files that have hit the encoder watchdog twice (chronic stalls) |
| `rename-fix PATH` | Re-run existing `.REENCODE` outputs through the current naming rules. Dry-run by default. |

These are the building blocks the path-taking subcommands compose.
You only need them if you're iterating on rule tunings or
investigating a problem.

---

## License

MIT. See `LICENSE`.
