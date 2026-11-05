# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running and linting

The tool is stdlib-only Python 3.10+. There is no install step, no test suite, and no build artefact — `video_optimizer.py` is a shim that injects the repo onto `sys.path` and calls `optimizer.cli.main`.

```bash
./video_optimizer.py <subcommand> [...]      # run the CLI directly
ruff check .                                 # lint (config in pyproject.toml)
ruff check --select=ALL .                    # full ruleset; expected clean modulo the documented ignores
python3 -m unittest discover -s tests -v     # run the test suite
```

The test suite (`tests/`) is stdlib `unittest` — no pytest, no fixtures
beyond `tests/_fixtures.py`. Each module pins a class of bug observed in
production: `test_qsv_args.py` covers the v0.4.1 maxrate-clamping and
v0.5.4 `-global_quality` scoping regressions; `test_audio_ladder.py`
covers the v0.5.0 ladder-shape rules; `test_naming.py` mirrors the
README's documented filename-rewrite examples. Run before touching
`encoder.py`, `naming.py`, or `presets.py`.

External runtime deps: `ffmpeg` and `ffprobe` on `PATH`. `list-encoders` is the canonical pre-flight to confirm the local ffmpeg has the encoders the user expects (it parses `ffmpeg -encoders` and also reports `/dev/dri/renderD128` for VAAPI).

## Run logs

Stdout from production / test encode runs goes to the top-level `logs/`
directory (gitignored). Convention is `logs/<DDMMM>-Run<N>.log` (e.g.
`logs/8May-Run1.log`) or `logs/<short-tag>.log`. When kicking off a
backgrounded run for the user, redirect there:

```bash
./video_optimizer.py UHD /mnt/nas/media/Movies > logs/8May-Run1.log 2>&1 &
```

Don't write run output to `tests/` — that directory is for the unit
test suite only.

## Architecture: the pipeline and what glues it

A path command probes, decides, and encodes in one process. `run_job` in `optimizer/cli.py` is that entry. `optimize`, `SD`, `HD`, `UHD`, `UHD-FILM`, and a bare path (rewritten to `optimize`) all go through it. One `runs` row for the job. `plan` and `apply` are not registered. There is no pending queue.

The database (default `~/.video_optimizer/state.db`, override with `--db`) is a probe cache and an outcome log. `decide()` lives in `optimizer/decide.py`. It does not open SQLite and does not touch the filesystem; the caller passes the stall count and whether a reencode sibling already exists. Encode candidates stay in memory until `insert_running_decision` writes a row.

```
scan / reprobe PATH  ──►  files table (probe cache, keyed by path; size+mtime)
                          a scan run is recorded; no decision rows

path command         ──►  one runs row
                          decide() in memory; decide-time skips are not stored
                          each encode: decisions.status='running', then
                          completed / failed / skipped
                          --dry-run: ffmpeg argv on stdout, no decision rows

status               ──►  recent runs, or `no history yet.`
doctor               ──►  preflight; cached-file count only (not decisions)
```

`cleanup --apply` and `rename-fix --apply` are flags on those commands. They are not an encode step, and they are not a subcommand named `apply`.

Three things to internalise before changing this:

1. **The probe cache key is `(size, mtime)`, not a content hash** (`db.Database.get_cached_probe`). A file edited in place with mtime preserved will produce stale probe data — `reprobe PATH` (alias for `scan --no-probe-cache`) is the escape hatch. `scan` and `reprobe` only warm or refresh that cache. A path command probes through `_scan_for_job`, which does not open a second run.
2. **Decision rows are outcomes, not a queue.** An encode is born `running` via `Database.insert_running_decision` (`owner_pid` is this process). Decide-time skips and `--dry-run` write no decision rows. On connect, leftover `status='pending'` rows are deleted (`_delete_leftover_pending_decisions`). A live `owner_pid` stays `running`, so `status` or `scan` does not interrupt an encode that is still going. A dead owner's `running` row, or a `running` row with NULL `owner_pid`, becomes `failed` with error exactly `interrupted`. That string does not match `error LIKE '%encoder stalled%'`, so it does not count as an encoder stall. Completed, failed, and skipped rows are left alone.
3. **Tier order is the job, not a shared queue.** `cmd_optimize` calls `run_job` with `("UHD", "HD", "SD")`. `cmd_preset` calls `run_job` with `(args.cmd,)`. It does not delegate to `cmd_apply`. `UHD-FILM` runs only when that preset is named, and only for files whose tier is UHD (height ≥ 1440). It is not inferred from the file. Within a tier, biggest projected savings first (`_sorted_encodes`). `--limit N` is N files per preset, applied after that sort; `0` means no limit. Files outside the presets in this job are omitted. No row is written for them.

`status` prints recent runs (`--last`, default 10). With no runs it prints `no history yet.` `doctor` prints the probe-cache file count. It does not count pending.

The wizard previews in memory (`_wizard_preview`), asks for a limit, then encodes through `run_job` (`_run_path_pipeline`). It does not write pending rows.

Path commands accept `--allow-reencoded`, `--timeout`, `--no-auto-relax-cq`, and `--dv-p7-convert`. `--timeout` omitted is the adaptive cap in the convention below; `0` disables; a positive value wins.

## Module map (what owns what)

- **`optimizer/cli.py`** — argparse wiring, subcommand handlers, and the encode path (`run_job`, `_encode_one`, `_build_apply_command`, `_execute_encode`, `_finalize_output`). Presets are defined in `optimizer/presets.py` and imported here. `cmd_preset` calls `run_job` with `(args.cmd,)`; it does not delegate to `cmd_apply`. `cmd_optimize` calls `run_job` with `("UHD", "HD", "SD")`. The job builds each preset's `RunConfig` from `PRESETS` plus the CLI args.
- **`optimizer/presets.py`** — single tuning surface. `PRESETS` (preset CQ + height gate), `AV1_QSV_TIER` (HD/UHD `maxrate` / `bufsize` / `look_ahead_depth` / `gop`), `AV1_QSV_BASE` (tier-independent av1_qsv flags), `BITRATE_FLAG_TABLE` (rules engine thresholds). When changing tunings, edit here — `cli.py`, `encoder.py`, and `rules.py` all read from this module. Deliberately a Python module rather than a TOML/YAML config: zero parser, zero precedence rules, comments allowed. If multiple co-existing tunings (per-machine, per-library) become a real need, this is the natural step where it graduates to `~/.video_optimizer/config.toml`; the shape maps cleanly.
- **`optimizer/probe.py`** — `ffprobe` wrapper → `ProbeResult`. The probe layer is responsible for HDR detection (`is_hdr` derived from color metadata) and for synthesising a `video_bitrate` estimate when the stream-level value is absent.
- **`optimizer/rules.py`** — `Rule` subclasses + the `RULES` registry + `RulesEngine`. A `Candidate` is produced **iff at least one non-advisory rule fires**; advisory rules (currently only `hdr_advisory`) are attached to the candidate but cannot create one alone. `remux_only` is set when *only* `container_migration` fires *and* the existing video codec is in `_MODERN_CODECS` ({h264, hevc, av1, vp9}).
- **`optimizer/decide.py`** — `decide()`. Classifies one `ProbeResult` as skip, clean, or encode-plus-tier. No SQLite and no filesystem. The tier is `SD`, `HD`, or `UHD`, never `UHD-FILM`.
- **`optimizer/encoder.py`** — encoder discovery (`get_available_encoders` parses `ffmpeg -encoders` and caches the result module-wide), `select_encoder(target, hwaccel)` (falls back through `ENCODER_PREFERENCE[codec][hwaccel]` then to software), and the ffmpeg argv builders for both encode and remux paths. Also owns the **3-stream audio ladder** (`_build_audio_ladder` → stream 0 highest-quality passthrough, stream 1 5.1 tier, stream 2 2.0 tier) and the `mov_text` subtitle conversion / image-sub drop when targeting MP4. HDR metadata flows through implicitly — no extraction code needed; libavcodec lifts mastering display + MaxCLL side_data onto AVFrames and the matroska muxer writes them as container HDR tags.
- **`optimizer/db.py`** — SQLite schema (`files`, `decisions`, `runs`), context-manager wrapper, all CRUD. The database is a probe cache and an outcome log, not a work queue. Schema is created idempotently on every connect; there is no migration system. Connect deletes leftover `pending` rows and fails a dead owner's `running` row with `error='interrupted'`.
- **`optimizer/models.py`** — dataclasses (`ProbeResult`, `AudioTrack`, `SubtitleTrack`, `RuleVerdict`, `Candidate`) plus the `to_json` / `probe_from_json` helpers that let `db.py` round-trip a `ProbeResult` through a single TEXT column. `ProbeResult.resolution_class` is the canonical resolution-bucket key shared with `presets.BITRATE_FLAG_TABLE`.
- **`optimizer/naming.py`** — pure-string codec-token rewriting for `--rewrite-codec` / `--reencode-tag`. No I/O.
- **`optimizer/crawler.py`** — recursive directory walk, video-extension filter.
- **`optimizer/report.py`** — the job preview calls `format_candidates_text`. `format_candidates_json` is defined and has no caller. The post-run report is `format_run_report`.

## Conventions worth respecting

- **Not every subcommand writes a run.** `scan` and `reprobe` write one `runs` row (kind `scan`). `cleanup` writes one (kind `cleanup`) when it has a target run. A path command writes one through `run_job`: `optimize`, `SD`, `HD`, `UHD`, `UHD-FILM`, a bare path, and the wizard once it encodes. The wizard preview does not. `status`, `doctor`, and `replace-list` open the database and do not call `start_run`. `list-encoders` and `rename-fix` do not open it.
- **ffmpeg invocations live in `encoder.build_*_command`**. `cli.py` should not assemble ffmpeg argv directly. The same goes for encoder selection — call `encoder.select_encoder` rather than reading `ENCODER_PREFERENCE` from the CLI.
- **Tuning values belong in `presets.py`, not at the call site.** `_qsv_args`, `cmd_preset`, and `OverBitratedRule` all read their numeric knobs from `presets.py`. New tunable values (a new tier, a new resolution bucket, a new preset) should be defined there and consumed from there — don't reintroduce literals back into the consumer modules.
- **Output-path computation belongs to `cli._compute_output_path`.** It is the single place that branches on `--mode side` vs `--mode replace`, applies `--source-root` stripping, and composes filename rewrites with the new extension. Replicating its logic anywhere else creates skew.
- **Failure path on encode**: partial output is `unlink()`ed and the decision row gets `status='failed'` with the truncated stderr in `error`. Replace mode never disposes of the original until the encode succeeds *and* (if `--backup` is set) the backup copy succeeds *or* (if `--recycle-to` is set) the rename to the recycle directory succeeds. Preserve this ordering when touching `_finalize_output` / `_execute_encode` — the original-disposal step is the last thing that runs and only after every preserve-the-original guarantee has been honored.
- **Audio output is a deterministic 3-stream ladder, not "all matching tracks plus compat".** Every encode resolves to up to three output streams via `_build_audio_ladder` (stream 0 = best, stream 1 = 5.1, stream 2 = 2.0). Don't reintroduce the old "keep all matching-language tracks" logic — it produced 5+ streams on Blu-ray remuxes (parallel TrueHD + DTS-HD MA + DD 5.1 + DD 2.0 + foreign-lang) and ate ~10 Mb/s of audio overhead. The ladder collapses parallel lossless tracks (TrueHD wins over DTS-HD MA on the same master) and synthesizes Opus 5.1 / AAC 2.0 fallbacks only when the source doesn't already provide them.
- **Pre-strip non-kept streams at the demuxer** via `_input_discard_args` (since v0.5.17), placed *before* `-i`. ffmpeg's matroska demuxer interleaves packets for every active stream by container timestamp; on sources with 7+ parallel audio tracks the windows between audio packets get tight enough that the QSV video decoder's input queue starves and deadlocks at frame 0 (and even CPU decode wedges on the worst cases — the AVC-remux multi-lang list was the original signature). `-discard:a:N all` / `-discard:s:N all` drops streams at demux time so they never enter the packet queue. The kept set must mirror what `_build_audio_ladder` and `_subtitle_map_args` actually map — those helpers expose it via `_kept_audio_indices` / `_kept_subtitle_indices`. Don't compute it twice; the discard pre-strip and the output map have to stay in lockstep or we'll either discard a stream we still try to map (encode fails) or leak a stream past the demuxer (defeats the point). Source-side indexing is preserved by `-discard` (no renumbering), so `-map 0:a:N?` references stay valid even when earlier audio indices are dropped.
- **Adaptive timeout**: `_resolve_timeout` defaults to `max(3600, 6 × duration_seconds)`; `--timeout 0` disables; an explicit positive value wins. A 3-hour movie therefore gets an 18-hour cap.
- **Ruff `ignore` list in `pyproject.toml` is intentional** (subprocess-to-ffmpeg, module-level encoder cache, blind cleanup excepts in kill paths, etc.). Don't silence those rules per-line; if a new violation needs an exception, add it to the central list with a comment matching the existing style.

## Where work-in-progress is tracked

`TODO.md` at the repo root is the actionable backlog (planned features,
known performance work, etc.). The README's "Known limitations" section
is a *current-state* warning to users; `TODO.md` is the to-do list. When
adding a new feature item, write enough context that picking it up later
doesn't require re-deriving the why — `TODO.md`'s parallelism entry is a
template for the level of detail that's useful.

## README cross-reference

The README documents every flag, the bitrate flag table, the audio/subtitle handling contract, and the Radarr/Sonarr filename workflow. When changing user-visible behaviour (a flag's default, a rule's threshold, the bitrate table, encoder preference order, the keep-langs contract), update both the code and the matching README section — the README's tables are the user-facing source of truth and the docstrings inside the code defer to them.
