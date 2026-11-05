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
(`libsvtav1`) is always available if you want to exercise your CPU fan.

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
  hardware AV1, software fallback runs at "go on vacation" speeds
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

# --dry-run on a path is the preview
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

Reads completed encodes from the state db. With no `--run`, that is
the most recent run that has one. Without `--apply` it only lists
what would be removed. `--apply` unlinks those sources. It does not
ask for a confirmation.

A completed row is removed only when an output path was recorded,
that output exists, the output is non-empty, the output path is not
the source path, and the source still exists. Anything else is
printed as `SKIP` and left on disk.

### Inspect

```bash
./video_optimizer.py status              # recent runs
./video_optimizer.py doctor              # preflight: ffmpeg / encoders / GPU / db
./video_optimizer.py list-encoders       # what encoders are available
```

`status` prints recent runs: encode jobs, `scan` (including `reprobe`,
which is recorded as a scan), and `cleanup`. With no runs it prints
`no history yet.` `doctor` checks ffmpeg, encoders, the GPU device
node, and that the database opens. It reports how many files are in
the probe cache. It does not count decisions. There is no pending
queue to count.

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

A path command probes, decides, and encodes in one job, and that job is
one run. The probe cache and the outcome log live in SQLite at
`~/.video_optimizer/state.db` (override with `--db`):

```
  scan / reprobe   ──►  probe cache only
  path --dry-run   ──►  preview: ffmpeg commands, no encode, no decision rows
  path             ──►  one run: encode, then the run report
  cleanup          ──►  later, when you're satisfied: removes originals
```

Path-taking commands are `optimize`, `SD`, `HD`, `UHD`, `UHD-FILM`, and
a bare path (that one is `optimize`). `optimize` encodes UHD, then HD,
then SD. A named preset encodes only its own band. `UHD-FILM` runs only
when you name it, and only on UHD-tier files (height ≥ 1440). It is not
part of the `optimize` chain, and a grainy 4K file is not sent there on
its own.

Inside a tier, the biggest projected savings go first. `--limit N` is
N files **per preset** after that sort (`0` means no limit).
`optimize /lib --limit 3` can encode up to three UHD, three HD, and
three SD. `UHD /lib --limit 3` is three UHD files.

`--dry-run` on a path is the preview. `scan` and `reprobe` only warm or
refresh the probe cache (`reprobe` ignores a size+mtime hit). `plan`
and `apply` are not commands. `cleanup --apply` and `rename-fix --apply`
are flags on those commands: they remove originals, or rename existing
outputs. They do not encode.

The wizard (no args, on a TTY) previews candidates in memory, asks how
many files to encode, then encodes through the same job. That preview
is not a queue, and it does not write decision rows.

A file is stored when its encode starts, as a `running` decision on
that one run. `decide()` skips are printed and not stored. A file
under the size gate is not one of them: scan drops it with
`record_size_skip` before `decide()` runs. `--dry-run` stores
nothing either.

Opening the database drops leftover `pending` rows.
A `running` row whose process is still alive is left alone. If that
process is gone, the row becomes `failed` with error `interrupted`.
That is not an encoder stall, so it does not count toward
`replace-list`. Completed, failed, and skipped rows stay.

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
  (1080p > 10 Mbps, 2160p > 32 Mbps, etc.). Not AV1. `decide()`
  returns `av1_source` before the rules engine unless `--allow-av1`,
  so a fat AV1 file is skipped with every other AV1 source.
- **Container migration.** AVI / WMV / etc. get re-muxed to MKV even
  when the codec is fine.

### What gets skipped

The tool errs heavily on the side of "don't waste GPU time, don't
clobber existing work":

- **Scan, size gate.** Files under **100 MB** (default `--min-size`)
  are dropped in the walk via `record_size_skip`. They are not probed
  and they are not a `decide()` reason. `--min-size 0` disables the gate.
- **Crawler, extras directories.** `Trailers/`, `Behind The Scenes/`,
  and `Featurettes/` are not walked. `decide()` does not see a directory.
- **Crawler and `decide()`, extras filenames.** A stem ending in
  `-trailer`, `-bts`, or `-deleted` is dropped on the walk. If a
  cached probe is classified anyway, `decide()` returns `extras`.
  `--allow-extras` lets both through.
- **`decide()`, already AV1.** Returned as `av1_source` before the
  rules engine, unless `--allow-av1`.
- **`decide()`, existing output.** A sibling `.AV1.REENCODE.mkv`
  returns `existing_output`, unless `--allow-reencoded`.
- **`decide()`, own filename.** `REENCODE` in the file's own name
  returns `reencoded`, unless `--allow-reencoded`.
- **`decide()`, two stalls.** `stall_count >= 2` returns `stalled`.
  See `replace-list`.
- **`decide()`, Dolby Vision.** A profile is skipped only when
  `encoder.dv_strategy` returns None. Profile 5 always does. Profiles
  7 and 8 return `p8_strip` and are encoded.
- **`decide()`, low bitrate.** Below the AV1 target for the resolution
  (1080p < 5 Mbps, 2160p < 16 Mbps, etc.), unless `--allow-low-bitrate`.

Some of these have an `--allow-*` override — see "Advanced options".

### Audio and subtitles: the 3-stream ladder

Every encode produces a deterministic three-stream audio output:

1. **Stream 0** — highest-quality passthrough (TrueHD / DTS-HD MA wins
   over the same master's lossy variants)
2. **Stream 1** — 5.1 tier (Opus 5.1, transcoded from stream 0 if the
   source doesn't already have one)
3. **Stream 2** — 2.0 tier (AAC 2.0, transcoded from stream 0)

Subtitles filter to `--keep-langs` (default `en,und`). Image-based subs
(PGS / VobSub) drop when targeting MP4 (which can't carry them);
otherwise they pass through. The one exception on MKV targets is
`mov_text` (the timed-text format inside MP4/MOV sources), which the
matroska muxer can't write — it's transcoded to SubRip rather than
copied.

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

### The bloat fallback (HD + UHD)

The default CQs are ambitious — UHD CQ 15, HD CQ 21, both `veryslow`.
Usually that produces a dramatically smaller file at the same perceptual
quality. Occasionally — grain-dominated older film and TV, mostly —
`av1_qsv` spends its bit budget preserving every grain particle and the
output matches or exceeds the source.

The bloat fallback catches this. At checkpoints (10% / 20% / 30% / 50%
of duration) the tool projects final output size from bytes-so-far. If
the projection ≥ 90% of the encoder's input, ffmpeg is killed and the
file retries once at the tier's **relaxed** tuning, encoder preset
`slow`:

| Tier | Default CQ | Relaxed CQ on bloat |
|---|---|---|
| UHD (≥ 1440p) | 15 | 21 (the **UHD-FILM** tuning) |
| HD (720–1439p) | 21 | 26 |
| SD (< 720p) | 24 | — (opts out; outputs too small to be worth a doubled encode budget) |

The retry finishes faster and produces a smaller output — counter-
intuitive, but the encoder stops fighting grain it was never going to
compress efficiently.

Runs silently on healthy encodes. Disable with `--no-auto-relax-cq`.
See [`NOTES.md#preset-rationale`](NOTES.md#preset-rationale) for the
threshold + checkpoint calibration history.

### The output report

After every run, a plain-text report at
`~/.video_optimizer/reports/run-<N>.txt`:

```
OK   3215 MB  /movies/foo.AV1.REENCODE.mkv  (from /movies/foo.mkv)
FAIL encoder_stalled  /movies/baz.mkv
SKIP user_declined    /movies/qux.mkv
```

Same data lives in the SQLite db. `cleanup` reads it to know which
originals are safe to remove. Decide-time skips are printed while the
job is choosing files; they are not lines in this report. A `SKIP`
line is a file the job had already stored and then did not encode
(for example you declined it with `--confirm`).

---

## Common options

| Flag | Effect |
|---|---|
| `--dry-run` | Print the ffmpeg commands and exit. No encoding, and no decision rows. |
| `--limit N` | At most N files per preset, biggest projected savings first (`0` = no limit). `optimize` applies that cap once per tier. |
| `--confirm` | Prompt per-file before encoding (default is auto-yes) |
| `--cleanup-after` | After the path command, unlink originals of completed encodes. No question. `--confirm` asks first (`[y/N]`) and leaves them unless you answer y. |
| `--verbose` / `-v` | More chatter (timeout labels, preset tunings) |
| `--output DIR` | Mirror outputs into a separate tree under `DIR` |
| `--replace` | Replace mode: move originals to a recycle dir as encodes complete |
| `--recycle-to DIR` | With `--replace`: explicit recycle directory |
| `--mode {keep,side,replace}` | Explicit mode override |
| `--no-auto-relax-cq` | Disable the bloat retry for HD and UHD. SD has no relaxed tuning, so this flag does not change an SD encode. |
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
| `--timeout SEC` | Per-file ffmpeg wall-clock cap. Omitted: `max(3600, 6 × duration)`. `0` disables. A positive number replaces the adaptive cap. |

### Cache and maintenance subcommands

| Subcommand | What it does |
|---|---|
| `scan PATH` | Warm or refresh the probe cache |
| `reprobe PATH` | Force-refresh the probe cache (alias for `scan --no-probe-cache`) |
| `replace-list` | Files that have hit the encoder watchdog twice (chronic stalls) |
| `rename-fix PATH` | Re-run existing `.REENCODE` outputs through the current naming rules. Dry-run by default. |

`scan` and `reprobe` only refresh the probe cache. They do not encode.
Encoding is a path command; `--dry-run` on that path is the preview.

---

## License

MIT. See `LICENSE`.
