# video_optimizer

> Point a Linux box at a movie library, walk away, come back to a smaller
> movie library. Originals are still there until you say otherwise.

`video_optimizer` is a thin wrapper around `ffmpeg` and `ffprobe` that
re-encodes the AV1-eligible content in your library — and only the
AV1-eligible content. It's stdlib-only Python 3.10+ (no `pip install`,
no virtualenv) and is built around four ideas:

1. **You shouldn't have to think.** `./video_optimizer.py /path/to/movies`
   does the right thing. There's also a wizard if you'd rather click
   through prompts.
2. **Calibrated presets per resolution.** UHD, HD, SD, and a UHD-FILM
   tier for grain-dominated 4K all have their own CQ, encoder preset,
   and decode pipeline tuned to actually finish in this lifetime.
3. **Smart skips, dumb defaults.** It auto-skips AV1 sources, prior
   outputs of itself, low-bitrate sources that wouldn't benefit, and
   Dolby Vision Profile 5 (where stripping the DV layer leaves a
   green/over-saturated mess). It also drops in a one-pass DV strip
   for Profile 7 and 8 sources so `av1_qsv` doesn't wedge on them
   (which it absolutely will if you let it).
4. **Originals don't disappear.** The default mode writes outputs
   alongside the source as `<stem>.AV1.REENCODE.mkv` and leaves the
   original on disk. A separate `cleanup` step removes the originals
   when you're satisfied.

**Optimized for Intel Arc / Battlemage** via `av1_qsv`. The presets,
lookahead depths, and decode pipelines were tuned against Intel's QSV
stack — that's what was on the workbench. NVIDIA (`av1_nvenc` on RTX
4000+) and AMD (`av1_vaapi`) work via the encoder fallback chain, but
the throughput numbers haven't been re-tuned for them. Software
fallback (`libsvtav1`) is always available and your CPU fans will let
you know.

---

## System requirements

**Required**:

- **Linux**, with a recent kernel. macOS and Windows are not tested or
  supported. The QSV/VAAPI hardware paths assume `/dev/dri/renderD128`.
- **Python 3.10+**. Stdlib only — no third-party packages, no `pip`, no
  virtualenv. If `python3 --version` returns ≥ 3.10, you're set.
- **`ffmpeg` 7.0+ and `ffprobe`** on `PATH`. Older `ffmpeg` lacks the
  `dovi_rpu` bitstream filter the DV strip pipeline needs. Distro
  versions of `ffmpeg` are sometimes a release behind; check with
  `ffmpeg -version`.

**Optional**:

- **Intel Arc / Battlemage GPU** for the `av1_qsv` fast path. Without
  hardware AV1, the tool falls back to `libsvtav1` (CPU). It still
  works; it just runs at "go make a sandwich" speeds instead of
  "go check on the kettle" speeds.
- **`dovi_tool` and `mkvmerge`** — only needed if you opt into
  `--dv-p7-convert` for stubborn Profile 7 sources. Most P7 content
  works fine with the default ffmpeg-bsf strip; install these only
  if you hit a P7 source that fails the simpler path.

Run `./video_optimizer.py doctor` after install — it confirms ffmpeg,
encoder availability, GPU device nodes, and the SQLite state directory.
Exits non-zero if anything's missing. Friendlier than reading a
checklist.

---

## Install

There's no installer. Clone the repo, run the script.

### Debian / Ubuntu

```bash
sudo apt install ffmpeg python3 git
git clone https://github.com/hyper-focused/video_optimizer ~/video_optimizer
cd ~/video_optimizer
./video_optimizer.py doctor
```

For Intel QSV on Arc / Battlemage / recent iGPUs, you'll want a recent
kernel and `intel-media-va-driver-non-free` (Debian) /
`intel-media-va-driver` (Ubuntu). **Battlemage specifically** needs
kernel 6.13+ (Xe driver) and Mesa 25+. If you're on an older kernel,
`av1_qsv` may not be wired up correctly and `doctor` will tell you so.

### Fedora

```bash
sudo dnf install https://download1.rpmfusion.org/free/fedora/rpmfusion-free-release-$(rpm -E %fedora).noarch.rpm
sudo dnf install ffmpeg python3 git intel-media-driver
git clone https://github.com/hyper-focused/video_optimizer ~/video_optimizer
cd ~/video_optimizer
./video_optimizer.py doctor
```

Fedora's stock `ffmpeg-free` is built without non-free codecs, which
the rules engine relies on heavily. RPM Fusion's full `ffmpeg` is the
one you want.

### Arch Linux

```bash
sudo pacman -S ffmpeg python git intel-media-driver
git clone https://github.com/hyper-focused/video_optimizer ~/video_optimizer
cd ~/video_optimizer
./video_optimizer.py doctor
```

### NVIDIA / AMD

The fallback chain is QSV → NVENC → VAAPI → libsvtav1. Drop in an RTX
4000-series (or newer) for `av1_nvenc`, or a recent AMD card with
`av1_vaapi` via Mesa/AMF, and `select_encoder` will route to it
automatically. `./video_optimizer.py list-encoders` shows which
encoder gets picked for each target on your specific machine.

The presets aren't *re-tuned* for non-Intel hardware — they're tuned
for Intel QSV and they work fine on other backends, just possibly not
optimally. If you have data, please send it.

### Optional: `dovi_tool` + `mkvmerge` for stubborn DV Profile 7 sources

Only install these if you hit a Profile 7 source that fails the
default ffmpeg-bsf strip path. Most P7 content doesn't trip that case.

```bash
# Debian / Ubuntu
sudo apt install mkvtoolnix
cargo install dovi_tool                       # or grab the prebuilt binary:
curl -L "https://github.com/quietvoid/dovi_tool/releases/latest/download/dovi_tool-x86_64-unknown-linux-musl.tar.gz" \
  | tar xz -C ~/.local/bin/

# Fedora
sudo dnf install mkvtoolnix && cargo install dovi_tool

# Arch
sudo pacman -S mkvtoolnix-cli && yay -S dovi-tool-bin
```

---

## Basic usage

The defaults are calibrated for "I have a movie library on a NAS, encode
the AV1-eligible content, leave originals alone until I confirm the
outputs are good." Most invocations look like one of these.

### Re-encode the whole library

```bash
./video_optimizer.py /mnt/nas/media/Movies
```

Walks the path, runs the UHD → HD → SD pipeline, writes outputs
alongside their sources as `<stem>.AV1.REENCODE.mkv`. Originals are
**not touched** — that's the whole point of the default mode. Run a
dry-run first if you'd rather see the plan:

```bash
./video_optimizer.py /mnt/nas/media/Movies --dry-run
```

### Pick a single resolution tier

```bash
./video_optimizer.py UHD      /mnt/nas/media/Movies   # 4K, CQ 15 (archive-grade)
./video_optimizer.py UHD-FILM /mnt/nas/media/Movies   # 4K, CQ 21 (looser; for grainy older film)
./video_optimizer.py HD       /mnt/nas/media/Movies   # 1080p / 720p
./video_optimizer.py SD       /mnt/nas/media/Movies   # below 720p
```

Useful when UHD is taking ~1 hour per file and you want HD's quicker
batch first, or when you've already done UHD and want to clean up the
rest of the library.

### Just one file (Radarr / Sonarr post-processing)

```bash
./video_optimizer.py "/mnt/nas/media/Movies/Foo (2023)/Foo.mkv" --replace
```

Pass a single file and it encodes just that file. With `--replace` the
original gets atomically moved to an auto-detected `@Recycle`
directory once the encode succeeds. Drop this in as a post-import
hook from Radarr or Sonarr and forget about it.

### Test on a few files first

```bash
./video_optimizer.py UHD /mnt/nas/media/Movies --limit 3
```

`--limit N` caps the number of files actually encoded. Useful before
committing to a full library run.

### The wizard, for when you don't feel like reading help

```bash
./video_optimizer.py
```

With no arguments and stdin attached to a TTY, it drops into an
interactive wizard: prompts for path, output mode, tier scope (All /
UHD / UHD-FILM / HD / SD), codec exemptions, and a count limit.
Friendlier than `--help` if you'd rather not memorize flags.

### Remove originals when you're satisfied

```bash
./video_optimizer.py cleanup            # dry-run: list what would be removed
./video_optimizer.py cleanup --apply    # actually unlink them
```

`cleanup` reads the most recent run's completed encodes from the
state database, runs a 3-check safety guard (output exists, output is
non-empty, output ≠ source), and unlinks the originals only when all
three pass. Without `--apply`, it just prints what it *would* do — the
nuclear option requires the safety word.

### Inspect

```bash
./video_optimizer.py status              # recent runs + any pending decisions
./video_optimizer.py doctor              # preflight: ffmpeg / encoders / GPU / db
./video_optimizer.py list-encoders       # what ffmpeg encoders are available
```

### Audit for stragglers (orphan source files)

Sometimes a run finishes but the source files don't get removed —
maybe `cleanup` was never invoked, maybe a `--replace` run partially
failed, maybe a previous session was interrupted between encode and
disposal. The orphan audit walks the library, finds every
`*.AV1.REENCODE.mkv`, and reports any same-stem source siblings still
sitting next to it.

```bash
./audit_orphans.py /mnt/nas/media/Movies                    # read-only listing
./audit_orphans.py /mnt/nas/media/Movies --json             # machine-readable
./audit_orphans.py /mnt/nas/media/Movies \
    --apply --recycle-to /mnt/nas/media/@Recycle/Movies     # actually move them
```

Read-only by default. `--apply` is required to move anything, and
preserves the source-dir hierarchy under the recycle directory so
nothing collides.

---

## How it actually works

The tool is a four-stage pipeline communicating through a SQLite
database at `~/.video_optimizer/state.db`:

```
  scan  ──►  probe cache (per-file: codec, bitrate, HDR, DV profile, …)
   │
   ▼
  plan  ──►  pending decisions (which files? which target codec?)
   │
   ▼
  apply ──►  encoded outputs + run report
   │
   ▼
  cleanup    (later, when you're satisfied: removes originals)
```

The path-taking subcommands (`optimize`, `SD`, `HD`, `UHD`, `UHD-FILM`,
the bare invocation, and `wizard`) compose all four stages into one
command. You don't have to think about scan/plan/apply individually.
They exist as separate subcommands but are hidden from `--help` —
power-user escape hatches for debugging rule tunings.

### Output modes

There are three. The default is `keep`, and that's the right answer
for most cases.

| Mode | Where outputs go | What happens to the original |
|---|---|---|
| **`keep`** *(default)* | Alongside the source as `<stem>.AV1.REENCODE.mkv` | Untouched. `cleanup` removes them later. |
| **`replace`** | Alongside the source | Atomically moved to a recycle dir (auto-detected `@Recycle` / `#recycle` / `.Trash`, or `.@Recycle` is created). |
| **`side`** | Mirrored under `--output DIR` | Untouched. Useful when the source filesystem is read-only. |

You select via flags: `--replace` for replace mode, `--output DIR` for
side mode, nothing for the default. Or be explicit with
`--mode {keep,side,replace}`.

### What gets re-encoded by default

Any source that fires at least one non-advisory rule. The rule set
covers the obvious cases:

- **Non-AV1 video at any tier** — UHD/HD/SD all queue any non-AV1
  source by default. AV1 is wildly more efficient than h.264 / HEVC /
  MPEG-2 / VC-1 / etc., and CQ-based encoding preserves quality.
- **Over-bitrate sources** — files whose video bitrate exceeds the flag
  threshold for their resolution (1080p > 10 Mbps, 2160p > 32 Mbps,
  etc.) — even AV1 sources, if they're abnormally fat.
- **Container migration** — files in legacy containers (AVI, WMV, etc.)
  get re-muxed to MKV even when the codec is fine.

### What gets skipped by default

The tool errs heavily on the side of *don't waste GPU time, don't
clobber existing work*. Out of the box, it skips:

- Files smaller than **100 MB** (likely trailers, samples, or extras)
- Files in `Trailers/`, `Behind The Scenes/`, `Featurettes/`, etc.,
  or with `-trailer` / `-bts` / `-deleted` filename suffixes
- Files whose source codec is **already AV1**
- Files whose `.AV1.REENCODE.mkv` sibling **already exists** (prior
  run output sitting next to the source)
- Files with the `REENCODE` token in their **own filename**
- Files that hit the encoder watchdog **twice** (chronic stalls — see
  `replace-list` for the manual-action queue)
- **Dolby Vision Profile 5** sources (no clean HDR10 fallback)
- **Low-bitrate sources** below the AV1 target for their resolution
  (1080p < 5 Mbps, 2160p < 16 Mbps, etc.) — re-encoding wouldn't yield
  meaningful savings and risks perceptual regression

Each skip class has a corresponding `--allow-*` flag if you really
want to override it (see "Advanced options" below).

### Audio and subtitles: the 3-stream ladder

Every encode produces a deterministic three-stream audio output:

1. **Stream 0**: highest-quality passthrough (TrueHD or DTS-HD MA wins
   over the same master's lossy variants)
2. **Stream 1**: 5.1 tier (Opus 5.1, transcoded from stream 0 if the
   source doesn't already have one)
3. **Stream 2**: 2.0 tier (AAC 2.0, transcoded from stream 0)

Subtitle streams are filtered to `--keep-langs` (default `en,und`).
Image-based subs (PGS / VobSub) are dropped when targeting MP4 since
MP4 can't carry them; otherwise they pass through.

This replaces an older "keep every audio track in your language" rule
that was producing 5+ streams on Blu-ray remuxes (parallel TrueHD +
DTS-HD MA + multiple lossy + foreign-lang) and burning ~10 Mb/s on
audio overhead. Override the new behavior with `--original-audio` if
you want every track preserved via stream-copy.

### Dolby Vision

`av1_qsv` consistently wedges on DV sources — Profile 7 stalls at
frame 0, Profile 8 stalls partway through. The fix is to strip the DV
metadata in a one-pass pre-encode operation so what reaches `av1_qsv`
is a plain HDR10 stream. Modern UHD content has an HDR10-compatible
base layer underneath the DV metadata, so this preserves the wide
colour gamut and high luma range.

| DV Profile | Default behavior | Why |
|---|---|---|
| **Profile 8.x** (most modern UHD WEB-DLs and recent Blu-rays) | Strip DV RPU via ffmpeg's `dovi_rpu=strip=true` bitstream filter, encode the HDR10 base to AV1 | Base layer is already HDR10-compatible; strip is a one-pass bsf operation |
| **Profile 7** (some UHD Blu-rays with FEL/MEL enhancement layers) | Same RPU strip as P8. The P7 → P8 `dovi_tool convert` pipeline is preserved but opt-in via `--dv-p7-convert` | The simple strip works on most modern P7 sources and keeps the apply pipeline single-pass |
| **Profile 5** (Apple TV+, some Vudu) | **Skipped permanently** | Base layer is *not* HDR10 — it's a custom DV-only colour space that requires the RPU to map. Stripping leaves a green/over-saturated mess; no clean fallback exists |

`--dv-p7-convert` runs the multi-stage P7 → P8 conversion using
`dovi_tool convert --discard` followed by `mkvmerge` to re-mux.
Requires both tools on `PATH`; the apply gate fails closed if either
is missing rather than silently falling back.

**Profile 10** (DV preserved through AV1 as OBU side-data) is on the
radar but not implemented. Waiting on `dovi_tool inject-rpu`'s AV1
support and on player ecosystems (Plex, Shield, etc.) recognising the
resulting Profile 10 stream.

### The UHD bloat fallback

UHD encodes are ambitious: CQ 15 with `veryslow`. Most of the time
that produces a dramatically smaller file at the same perceptual
quality. Sometimes — grain-dominated older film, mostly — `av1_qsv`
spends its bit budget trying to preserve every grain particle and the
output ends up the same size as (or larger than) the source.

The bloat fallback catches this. At checkpoints during the encode
(10%, 20%, 30%, 50%) the tool projects the final output size based on
bytes-per-second so far. If projected size ≥ 90% of the encoder's
input, ffmpeg gets killed and the file is retried once with the
**UHD-FILM** tuning: CQ 21, encoder preset `slow`. The retry finishes
~1.5–2× faster than `veryslow` would and produces a smaller output —
counter-intuitive, but the encoder stops trying to preserve grain it
was never going to compress efficiently anyway.

This runs silently. If a UHD encode looks healthy at the checkpoints,
nothing changes. Disable with `--no-auto-relax-cq` if you have a
strong opinion about it.

### The output report

After every run, a plain-text report is written to
`~/.video_optimizer/reports/run-<N>.txt`:

```
OK   3215 MB  /movies/foo.AV1.REENCODE.mkv  (from /movies/foo.mkv)
FAIL encoder_stalled  /movies/baz.mkv
SKIP dolby_vision     /movies/qux.mkv
```

The same data lives in the SQLite db. `cleanup` reads it to know
which originals are safe to remove.

---

## Common options

| Flag | Effect |
|---|---|
| `--dry-run` | Print planned ffmpeg commands and exit. No encoding. |
| `--limit N` | Process at most N candidates (0 = no limit) |
| `--confirm` | Prompt per-file before encoding (default is auto-yes) |
| `--cleanup-after` | Prompt to remove originals after a successful run |
| `--verbose` / `-v` | More chatter (timeout labels, preset tunings, etc.) |
| `--output DIR` | Mirror outputs into a separate tree under `DIR` |
| `--replace` | Replace mode: move originals to a recycle dir as encodes complete |
| `--recycle-to DIR` | With `--replace`: explicit recycle directory |
| `--mode {keep,side,replace}` | Explicit mode override (rarely needed) |
| `--no-auto-relax-cq` | Disable the UHD bloat fallback |
| `--dv-p7-convert` | Use the dovi_tool convert pipeline for stubborn P7 sources |
| `--original-audio` | Keep every input audio track via stream-copy |
| `--original-subs` | Keep every input subtitle track via stream-copy |

---

## Advanced options

These are hidden from `--help` for tidiness but remain functional.
Most are escape hatches for debugging or unusual library shapes.

### Re-include something the tool skips

| Flag | What it re-includes |
|---|---|
| `--allow-reencoded` | Files already tagged `REENCODE`, AND files whose `.AV1.REENCODE.mkv` sibling exists |
| `--allow-av1` | AV1-source files (default skipped because re-encoding AV1 is wasteful) |
| `--allow-extras` | Plex-style trailer/extras directories and `-trailer` / `-bts` filenames |
| `--allow-low-bitrate` | Sources whose video bitrate is below the AV1 target for their resolution |

### Skip a codec entirely

The default behavior is to re-encode every non-AV1 source. To leave
specific codecs alone (typical case: an HEVC archive you trust), pass
`--skip-codecs` with a comma-separated list of ffprobe codec names:

```bash
./video_optimizer.py /lib --skip-codecs hevc                  # leave HEVC alone
./video_optimizer.py /lib --skip-codecs hevc,vp9              # leave HEVC and VP9 alone
./video_optimizer.py /lib --skip-codecs h264,mpeg2video       # only re-encode HEVC
```

Names match `ffprobe -show_streams` output; case-insensitive. Common
values: `h264`, `hevc`, `vp9`, `mpeg2video`, `mpeg4`, `vc1`, `wmv3`.
(Skipping `av1` is redundant — AV1 sources are already auto-skipped.)

### Tuning overrides

| Flag | Effect |
|---|---|
| `--quality N` | Override CQ (UHD default 15, HD 21, SD 24, UHD-FILM 21; lower = better quality, larger file) |
| `--keep-langs en,und` | Comma-separated language codes to retain |
| `--hwaccel {auto,qsv,nvenc,vaapi,videotoolbox,software,none}` | Force a specific hw backend |
| `--hw-decode` / `--no-hw-decode` | Override the preset's hw-decode default |
| `--min-size BYTES` | Skip files below this size at scan time (default 100 MB; accepts `100M`, `1G`, etc.) |
| `--db PATH` | SQLite state file (default `~/.video_optimizer/state.db`) |
| `--timeout SEC` | Per-file ffmpeg wall-clock cap (default `max(3600, 6 × duration)`; `0` disables) |

### Pipeline-primitive subcommands

| Subcommand | What it does |
|---|---|
| `scan PATH` | Probe-cache only — populate the db without queueing anything |
| `plan` | Run rules against the cache, write pending decisions |
| `apply` | Encode pending decisions |
| `reprobe PATH` | Force-refresh the probe cache (alias for `scan --no-probe-cache`) |
| `replace-list` | Files that have hit the encoder watchdog twice (chronic stalls; candidates for finding a different release) |

These are the building blocks the path-taking subcommands compose.
You only need them if you're iterating on rule tunings or
investigating a problem.

---

## License

MIT. See `LICENSE`.
