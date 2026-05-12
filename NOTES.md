# Internals & design notes

Long-form context for decisions whose *why* doesn't fit comfortably
inline. Code-side comments point here with `# see NOTES.md#<slug>`.

This file is **not authoritative for behavior** — constants, thresholds,
and tuning values live in code (`optimizer/presets.py`). When a section
here names a function or flag, it's a soft pointer; verify against the
current source.

The companion files split the documentation surface as:

- `README.md` — user-facing flags, contracts, tables.
- `CLAUDE.md` — architecture map, module ownership, conventions.
- `TODO.md` — backlog (planned / in-flight / known issues).
- `NOTES.md` — *this file*: design rationale and historical context.

---

## HW decode policy

The default is **frame-size-driven**: HD (and below) decodes on CPU,
UHD decodes on the QSV asic. On Battlemage iGPU (shared LLC, no PCIe
transit) CPU decode for H.264/HEVC/MPEG-2 at 1080p actually beats GPU
decode end-to-end — better frame threading, lower setup cost, the
"upload" to the encoder is a cache flush. UHD frame sizes flip the
math: per-frame decode workload grows enough that CPU decode competes
with the encoder for cores.

The `UHD` preset defaults `hw_decode=True` for this reason. HD / SD
default to `False`. Discrete Arc cards (PCIe transit cost meaningful)
would flip the calculus at HD too — captured as future work.

### Codec-aware override

`optimizer/presets.SLOW_CPU_DECODE_CODECS` is scaffolding for routing
specific codecs through the QSV decoder regardless of preset. Currently
empty.

Past membership: `vc1` and `vp9` were added on the theory that their
weakly-threaded libavcodec decoders starve `av1_qsv` at HD. The
analysis was clean on paper (libavcodec VC-1 caps one core at ~126 fps
vs the ~220 fps Battlemage HD encoder supply rate; tile-less VP9
similar). Empirically, `vc1_qsv` wedged at frame 0 on Lethal Weapon
1987 — a representative 1080p VC-1 Blu-ray remux. 13+ minutes of
0-byte output, no progress lines, watchdog couldn't fire (see
[Stall watchdog](#stall-watchdog) below). Same failure signature as
DV-on-UHD.

**Lesson**: "ffmpeg exposes `<codec>_qsv`" is necessary but not
sufficient. The QSV decoder paths for legacy codecs on Battlemage are
corner-case-prone in ways invisible from the spec sheet.

**Empirical validation gate for re-adding any codec**:

```bash
ffmpeg -hwaccel qsv -hwaccel_output_format qsv \
    -i SAMPLE.mkv -f null - -t 60
```

Two or three samples per codec at minimum (different release groups,
different decades). Clean completion required before adding to the
set.

**Throughput cost we're accepting**: VC-1 HD encodes run at ~126 fps
rather than the ~220 fps ceiling. Slower but reliable.

---

## Dolby Vision pipeline

`av1_qsv` wedges on DV sources at UHD frame sizes — Profile 7 stalls at
frame 0, Profile 8 stalls partway through. The DV strip pre-pass is a
one-pass stream-copy that uses ffmpeg's `dovi_rpu=strip=true` bitstream
filter to remove the RPU so what reaches `av1_qsv` is plain HDR10. Modern
DV Profile 7/8 sources carry an HDR10 base layer, so the strip preserves
wide gamut and HDR luma.

### UHD-only gate

The strip pre-pass is `pr.height >= 1440`-gated. At HD we don't engage
QSV decode by default (UHD-DV's wedge mode), `av1_qsv` ignores RPU side
data on encode anyway, and static HDR10 mastering display + MaxCLL flow
through implicitly. Running the strip at HD would just write a multi-GB
temp file for no functional gain.

Realistic HD-DV vectors (Apple TV+ 1080p WEB-DLs with Profile 8.x, the
occasional 1080p DoVi broadcast rip, iPhone-recorded video) encode
correctly without the strip. The filename rewriter still scrubs DV /
HDR10+ tokens on the AV1 target so the output is labelled `HDR10`.

### Profile dispatch

| Profile | Strategy | Why |
|---|---|---|
| 8.x | `dovi_rpu=strip=true` bsf | HDR10 base survives strip |
| 7 | Same bsf by default; opt-in `--dv-p7-convert` runs `dovi_tool convert --discard` + `mkvmerge` | Simple strip works on most modern P7 |
| 5 | **Skipped** | No HDR10 base — custom DV colour space; strip leaves green mess |

The simple-strip P7 path handles most modern P7 sources. The P7→P8
conversion via `dovi_tool` exists for stubborn cases but requires
`dovi_tool` + `mkvmerge` on `PATH`; the gate fails closed if missing.

### bsf scoping pitfall

`build_dv_strip_command` scopes the strip to `-bsf:v:0`, not the bare
`-bsf:v`. Bare-form applies to every video stream — including embedded
JPEG cover art (a real case: The Housemaid 2025, P7 + JPEG cover, run
#166). `dovi_rpu` only handles HEVC + AV1, so it crashes on the mjpeg
stream. Pinned by test (`test_dv_skip.py`).

### Demuxer pre-trim during strip

The strip command pre-discards audio / subtitle streams that the
downstream encode wouldn't keep anyway, using `-discard:a:N all` /
`-discard:s:N all` *during* the strip rather than after. On multi-track
sources (e.g. Saving Private Ryan with 9 audio + 50 subs) this saves
the equivalent NAS write+read of every discarded stream. `--original-audio`
and `--original-subs` round-trip into the strip — those overrides
suppress the pre-discard.

---

## Log descriptor format

The per-file log preamble in `_apply_one` reads:

```
[N/M] /path/to/source.mkv
    rules: [...]  target: ...
    projected savings: X.X GB
    decode <codec> (<CPU|QSV>) → encode via <encoder_name> [(+ extras)]
```

The pipeline-shaped descriptor (`decode … → encode …`) is built in
`_build_apply_command`. Extras are tacked on after, controlled by
explicit kwargs:

- `(+ denoise pre-pass)` — `denoise=True` (CPU `hqdn3d` filter, low-bitrate H.264 / SD)
- `(+ original audio passthrough)` — `--original-audio`
- `(+ original subs passthrough)` — `--original-subs`
- `(+ DV strip pre-pass)` — `dv_pre_pass=True` (caller sets only after the strip ran)

The DV-strip suffix used to gate on `source_override is not None`, but
every regular apply call passes a non-None override (initialised to
`pr.path`), so the gate fired for every encode. The `dv_pre_pass` kwarg
replaced that — it's presentation-only, set by the caller iff
`_prepare_dv_source` actually ran.

---

## AV1_QSV tuning

Per-tier values live in `presets.AV1_QSV_TIER` (`HD` / `UHD`) and are
read by `encoder._qsv_args`. Tier-independent flags live in
`AV1_QSV_BASE`. Per-preset overrides live in `PRESETS[<name>]["qsv_overrides"]`
(empty dict = use globals).

### Knobs and what they control

- **`look_ahead_depth`** — frames of motion-prediction lookahead. Deeper
  = better compression on slow/static content, larger memory footprint,
  slower init. UHD uses deeper depth than HD because UHD content tends
  to have slower scene-change cadence and the extra latency is amortised
  over a longer encode.
- **`gop`** — max keyframe interval (frames). Longer GOP compresses
  better; too long hurts seek granularity in players. The current values
  are a compromise tuned against archive playback behaviour in Plex/Jellyfin.
- **`refs`** — reference frames the encoder can choose from. More =
  better compression on complex motion, slower encode.
- **`bf`** — max B-frames in a row. AV1's B-frame implementation is
  cheap; higher counts help on most content.

### Lookups and resolution order

`_qsv_args` resolves each value as: `qsv_overrides[key]` (per-preset) →
`AV1_QSV_TIER[tier][key]` → `AV1_QSV_BASE[key]`. Preset overrides win.
This is deliberate — a preset can change just one knob (e.g. UHD-FILM
changing `gop` for grain content) without forking the whole tier table.

### Bit depth and `hw_decode` interaction

When `hw_decode=True`, the decode→encode pipeline keeps frames in
GPU-resident `qsv` surfaces that natively carry their pixel format.
Setting `-pix_fmt` explicitly in that mode forces a `qsv → p010le`
conversion that `auto_scale` can't bridge, breaking the encode. So
`-pix_fmt` is pinned only when `hw_decode=False`.

---

## Preset rationale

`PRESETS` in `optimizer/presets.py` is the single tuning surface:
target codec + quality + height band + audio/sub policy per tier. Three
tiers map 1:1 to user-facing subcommands (`SD`, `HD`, `UHD`); a fourth
opt-in preset (`UHD-FILM`) handles grain-dominated 4K remasters.

### CQ targets

| Preset | CQ | Why |
|---|---|---|
| SD | 24 | SD is perceptually fragile under compression; storage delta vs 21 is small at SD sizes — give it headroom |
| HD | 21 | Calibrated ~5 GB/hr for archive-grade 1080p SDR. CQ 22 ran ahead of budget on a real campaign; 21 has headroom for grain/motion |
| UHD | 15 | Sits on `av1_qsv`'s ICQ quality plateau. CQ 17 falls off the knee |
| UHD-FILM | 21 | Grain-dominated source overrides; CQ 15 over-allocates bits to preserve grain particles. ~20 GB/hr instead of ~50+ |

### Encoder preset (`-preset`)

Almost everything uses `veryslow` — `av1_qsv`'s deepest RD-search
ladder, best efficiency per bit. UHD-FILM uses `slow` because grain
content is bit-budget-bound on the encoder regardless of search depth;
`veryslow` spends time the encoder can't compress better anyway.

### Why this is a Python module not TOML/YAML

Deliberately a `.py` file: zero parser, zero precedence rules, comments
allowed, type-checked by ruff alongside the rest. If multiple
co-existing tunings (per-machine, per-library) become a real need, the
graduation path is `~/.video_optimizer/config.toml` mirroring this
dict's shape.

---

## Filename rewriting

`optimizer/naming.py` rewrites the output stem so Plex / Sonarr /
Radarr Custom-Format matchers see what's actually in the container.

### Foreign codec token scrub

`_FOREIGN_CODEC_TOKENS[target_codec]` is the list of codec name
fragments scrubbed from the source stem during a rewrite. The shared
`_LEGACY_VIDEO_CODEC_TOKENS` list (MPEG-2 / XviD / DivX / VC-1 / VP9 /
WMV) is spliced into all three target codecs because none of those
legacy formats survive any modern encode. Per-target additions are
the modern-near-neighbour codecs (AV1 strips H.264 / HEVC / x264 /
x265 / AVC; HEVC strips H.264 / AVC + AV1; H.264 strips HEVC / x265 +
AV1).

### HDR metadata token substitution

When targeting AV1, the encoded output can preserve static HDR10 but
not Dolby Vision RPU and not HDR10+ dynamic metadata. The filename
should match.

`_LOST_METADATA_TOKENS["av1"]` lists DV / HDR10+ tokens. The
substitution logic in `_substitute_lost_metadata_tokens`:

1. If the source stem already contains `HDR10` / `HDR` → just strip
   the DV / HDR10+ tokens.
2. Otherwise → substitute the *first* matching token in-place with
   `HDR10`, then strip any remaining matches.

This is correct for DV Profile 7/8 (which always carry an HDR10 base
layer) and HDR10+ (which is HDR10 + dynamic metadata; dropping the
dynamic metadata yields plain HDR10). DV Profile 5 sources are
skipped upstream by the plan gate — they have no HDR10 fallback.

### `rename-fix` subcommand

`./video_optimizer.py rename-fix <PATH>` walks a tree, runs each
existing `.REENCODE` stem back through `rewrite_codec_tokens`, and
renames anything whose canonical form differs (sidecars included).
Used to back-fix legacy outputs after expanding the scrub set
(MPEG-2 / VC-1 / XviD / DivX / VP9 / WMV additions were back-applied
this way in May 2026).

---

## Stall watchdog

`encoder._run_encode_ffmpeg` checks the stall threshold (`stall_seconds`
= 300s default) *inside* its `for line in proc.stdout:` loop. Both the
output-time and frame-count signals are tracked because `av1_qsv` with
deep lookahead buffers ~150 frames before any presentation timestamp
surfaces; `frame=` advances on every decoded frame so it's the
liveness signal.

**Known limitation**: if ffmpeg never emits any `-progress` line — which
is the failure mode when the QSV decoder wedges at frame 0 — the
`for line in proc.stdout:` loop blocks on the underlying `readline()`
and the stall check never runs. Manual `kill -9 <pid>` is required to
advance the queue.

Fix is captured in `TODO.md`: replace the blocking iterator with
select/poll, or spawn a background thread that fires the stall check on
a timer independent of stdout.

---

## See also

- `CLAUDE.md` — module map, pipeline stages, conventions.
- `TODO.md` — backlog and known issues.
- `README.md` — user-facing flags and contracts.
