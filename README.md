# video_optimizer

A point-and-shoot AV1 transcoder for personal video libraries on Linux.
Crawls a directory, identifies files worth re-encoding, and re-encodes
them to AV1 in MKV with hardware acceleration where available.

```bash
./video_optimizer.py /mnt/nas/media/Movies        # encode the whole library
./video_optimizer.py UHD /mnt/nas/media/Movies    # 4K only
./video_optimizer.py cleanup --apply              # remove originals when satisfied
```

---

## What it does

`video_optimizer` is a wrapper around `ffmpeg` and `ffprobe` that brings
three things to the table that a hand-rolled `for f in *.mkv; do ffmpeg
…; done` loop doesn't:

- **Calibrated per-tier presets.** UHD (≥1440p), HD (720–1439p), and SD
  (≤719p) each have their own CQ, GOP, lookahead, and decode pipeline
  defaults. UHD uses zero-copy QSV decode→encode; HD/SD use CPU decode →
  GPU encode (faster than zero-copy at <UHD frame sizes on Intel).
- **Smart candidate selection.** A rules engine identifies what's worth
  re-encoding (legacy codecs like MPEG-2/VC-1, h.264 at HD, anything
  non-AV1 at UHD/SD, files in legacy containers like AVI). It also skips
  things that don't need work: AV1 sources, prior outputs of this tool,
  Plex-style trailers/extras, and Dolby Vision sources (which currently
  break `av1_qsv`).
- **Library-scale defaults.** Audio collapses to a deterministic 3-stream
  ladder (best lossless passthrough + Opus 5.1 + AAC 2.0). Subtitles
  filter to `--keep-langs` (default `en,und`). Originals are preserved by
  default — outputs land alongside the source with a `.AV1.REENCODE.mkv`
  suffix, and a separate `cleanup` step removes them when you're ready.

The result is a tool you can point at a movie library and walk away from,
that catches the cases where you need to step in (encoder stalls, DV
content, ambiguous sources) and surfaces them in a per-run report.

---

## Install

Stdlib-only Python 3.10+; no `pip install`, no virtualenv. The only
runtime dependencies are `ffmpeg` and `ffprobe` on `PATH`.

### Debian / Ubuntu

```bash
sudo apt install ffmpeg python3 git
git clone https://github.com/hyper-focused/video_optimizer ~/video_optimizer
cd ~/video_optimizer
./video_optimizer.py doctor
```

For Intel QSV (Arc, Battlemage, recent iGPUs), make sure you have a
recent kernel and `intel-media-va-driver-non-free` (Debian) /
`intel-media-va-driver` (Ubuntu). Battlemage specifically needs kernel
6.13+ (Xe driver) and Mesa 25+.

### Fedora

```bash
sudo dnf install https://download1.rpmfusion.org/free/fedora/rpmfusion-free-release-$(rpm -E %fedora).noarch.rpm
sudo dnf install ffmpeg python3 git intel-media-driver
git clone https://github.com/hyper-focused/video_optimizer ~/video_optimizer
cd ~/video_optimizer
./video_optimizer.py doctor
```

(Fedora's stock `ffmpeg-free` is built without non-free codecs; RPM
Fusion's full `ffmpeg` is what you want for a transcode workflow.)

### Arch Linux

```bash
sudo pacman -S ffmpeg python git intel-media-driver
git clone https://github.com/hyper-focused/video_optimizer ~/video_optimizer
cd ~/video_optimizer
./video_optimizer.py doctor
```

### NVIDIA / AMD

The tool works with NVIDIA (`av1_nvenc` — RTX 4000-series and newer) and
AMD (`av1_vaapi` via Mesa/AMF) when the hardware supports AV1 encode. No
config required — `select_encoder` falls back through QSV → NVENC → VAAPI
→ libsvtav1 (CPU). `./video_optimizer.py list-encoders` shows which
encoder will be picked for each target on your box.

### Verifying the install

```bash
./video_optimizer.py doctor
```

`doctor` checks ffmpeg/ffprobe, encoder availability, GPU device nodes
(`/dev/dri/renderD128` for QSV/VAAPI), and the SQLite state directory
(`~/.video_optimizer/`). Exits non-zero if anything's missing.

---

## Common commands

The tool's defaults are tuned for "I have a movie library on a NAS,
encode the AV1-eligible content, leave originals alone until I confirm
the outputs are good." Most invocations look like one of these.

### Encode the entire library, leave originals untouched

```bash
./video_optimizer.py /mnt/nas/media/Movies
```

Runs the UHD → HD → SD pipeline, writes outputs alongside their sources
as `<original-stem>.AV1.REENCODE.mkv`, leaves originals untouched. Run
the dry-run first if you want to see what would happen:

```bash
./video_optimizer.py /mnt/nas/media/Movies --dry-run
```

### Encode only one resolution tier

```bash
./video_optimizer.py UHD /mnt/nas/media/Movies        # 4K only
./video_optimizer.py HD  /mnt/nas/media/Movies        # 1080p / 720p
./video_optimizer.py SD  /mnt/nas/media/Movies        # below 720p
```

Useful when UHD is taking ~1h/file and you want HD's quicker batch first,
or when you've already done UHD and want to clean up the rest.

### Test on a few files first

```bash
./video_optimizer.py UHD /mnt/nas/media/Movies --limit 3
```

### Encode a single file (Radarr / Sonarr post-processing hook)

```bash
./video_optimizer.py "/mnt/nas/media/Movies/Foo (2023)/Foo.mkv" --in-place
```

The tool accepts a single video file as the path argument; `--in-place`
moves the original to an auto-detected `@Recycle` directory once the
encode succeeds. Suitable as a post-import hook from Radarr/Sonarr.

### Replace originals as you go

```bash
./video_optimizer.py /mnt/nas/media/Movies --in-place
```

`--in-place` recycles each original (atomically, into
`<library>/.@Recycle` by default) once its encode completes successfully.
The file is moved, not deleted — recoverable until you empty the
recycle directory.

### Mirror outputs into a separate tree

```bash
./video_optimizer.py /mnt/nas/media/Movies --output /mnt/backup/encoded
```

Mirrors the directory structure under `--output`. Originals are
untouched. Useful when the source filesystem is read-only or when you
want to keep the encoded library separate.

### Remove originals after a run

```bash
./video_optimizer.py cleanup            # dry-run: list what would be removed
./video_optimizer.py cleanup --apply    # actually unlink them
```

`cleanup` reads the most recent run's completed encodes from the
database, runs a 3-check safety guard (output exists, output is
non-empty, output ≠ source), and unlinks the originals only when all
three pass. `--apply` is required to actually delete; without it you
get a dry-run listing.

### Interactive guided run

```bash
./video_optimizer.py
```

With no arguments and stdin attached to a terminal, drops into a wizard
that prompts for path, output mode, and tier scope, then runs the full
pipeline.

### Resume / inspect

```bash
./video_optimizer.py status              # recent runs + pending decisions
./video_optimizer.py list-encoders       # what ffmpeg encoders are available
./video_optimizer.py replace-list        # files that have stalled twice
```

---

## Flags reference

Every path-taking subcommand (`SD`, `HD`, `UHD`, `optimize`, plus the
bare invocation) shares the same flag surface. Run any of them with
`--help` for the full inline reference; the most-used flags are
summarized here.

### Output mode (pick at most one)

| Flag | Effect |
|---|---|
| _(none)_ | **Default**: `beside` mode — outputs land alongside source as `<stem>.AV1.REENCODE.mkv`; originals untouched |
| `--output DIR` | Mirror outputs into a separate directory tree under `DIR` |
| `--in-place` | Replace mode — outputs land alongside source, originals moved to a recycle dir |
| `--mode {beside,side,replace}` | Explicit mode override |
| `--recycle-to DIR` | With `--in-place`: explicit recycle directory (default: auto-detect `@Recycle` / `#recycle` / `.Trash` under source, or create `.@Recycle`) |

### Run control

| Flag | Effect |
|---|---|
| `--dry-run` | Print planned ffmpeg commands and exit |
| `--limit N` | Process at most N candidates (0 = no limit) |
| `--confirm` | Prompt per-file before encoding (default is auto-yes) |
| `--cleanup-after` | Prompt to remove originals after a successful run |
| `--verbose` / `-v` | More chatter (timeout labels, preset tunings, etc.) |

### Audio / subtitle overrides

| Flag | Effect |
|---|---|
| `--original-audio` | Keep every input audio track via stream-copy (default strips to `--keep-langs` and rebuilds a 3-stream ladder) |
| `--original-subs` | Keep every input subtitle track via stream-copy (default strips to `--keep-langs`) |

### Skip / inclusion overrides (advanced; default behavior is what you want)

These flags re-include content the tool skips by default. All hidden
from `--help` for tidiness, but functional.

| Flag | What it re-includes |
|---|---|
| `--allow-reencoded` | Files already tagged `REENCODE` in their filename, AND files whose `.AV1.REENCODE.mkv` sibling already exists |
| `--allow-av1` | AV1-source files (default: skipped because re-encoding AV1 is wasteful) |
| `--allow-extras` | Plex-style trailer/extras directories and `-trailer` / `-bts` / etc. filenames |

### Tuning overrides (advanced)

| Flag | Effect |
|---|---|
| `--quality N` | Override CQ (UHD default 15, HD 21, SD 24; lower = better quality, larger file) |
| `--keep-langs en,und` | Comma-separated language codes to retain (default `en,und`) |
| `--hwaccel {auto,qsv,nvenc,vaapi,videotoolbox,software,none}` | Force a specific hw backend (default auto) |
| `--hw-decode` / `--no-hw-decode` | Override the preset's hw-decode default |
| `--workers N` | Parallel ffprobe workers during scan (default `min(8, cpu_count)`) |
| `--min-size BYTES` | Skip files below this size at scan time (default 100 MB; accepts `100M`, `1G`, etc.) |
| `--db PATH` | SQLite state file (default `~/.video_optimizer/state.db`) |
| `--timeout SEC` | Per-file ffmpeg wall-clock cap (default `max(3600, 6 × duration)`; `0` disables) |

### Subcommands

| Subcommand | What it does |
|---|---|
| `<bare path>` | Implicit `optimize` (all three tiers) |
| `optimize PATH` | Explicit form of the above |
| `SD PATH` / `HD PATH` / `UHD PATH` | Same pipeline, single tier |
| `wizard` | Interactive prompts → full pipeline |
| `cleanup` | Remove originals from the most recent (or `--run N`) successful run |
| `doctor` | Preflight checks: ffmpeg, encoders, GPU device, db |
| `status` | Recent runs + pending decisions |
| `list-encoders` | What ffmpeg encoders are available, and which gets picked per target |
| `replace-list` | Files that have stalled twice (candidates for finding a different release) |
| `scan` / `reprobe` / `plan` / `apply` | Pipeline primitives — useful for power users iterating on rule tunings |

---

## Output report

After every successful run, `video_optimizer` writes a per-run report to
`~/.video_optimizer/reports/run-<N>.txt` with one line per file:

```
OK   3215 MB  /movies/foo.AV1.REENCODE.mkv  (from /movies/foo.mkv)
FAIL encoder_stalled  /movies/baz.mkv
SKIP dolby_vision     /movies/qux.mkv
```

The same data is in the SQLite db under `decisions_for_run(N)`. The
`cleanup` subcommand uses it to know which originals are safe to remove.

---

## Default skip behavior at a glance

The tool errs heavily on the side of "don't waste GPU time and don't
clobber existing work." Out of the box, it will not encode:

- Files smaller than 100 MB (likely trailers, samples, or extras)
- Files in `Trailers/`, `Behind The Scenes/`, `Featurettes/`, etc.
  directories, or with `-trailer` / `-bts` / `-deleted` filename suffixes
- Files whose source codec is already AV1
- Files whose `.AV1.REENCODE.mkv` sibling already exists (prior run output)
- Files with the `REENCODE` token in their own filename
- Files that have hit the encoder watchdog twice (chronic stalls — see
  `replace-list` for the manual-action queue)
- Dolby Vision sources (`av1_qsv` wedges on DV; awaiting a DV-aware encode
  path)

Each skip class has an `--allow-*` opt-in flag if you genuinely want to
re-encode those files anyway.

---

## License

MIT. See `LICENSE`.
