# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running and linting

The tool is stdlib-only Python 3.10+. There is no install step, no test suite, and no build artefact — `video_optimizer.py` is a shim that injects the repo onto `sys.path` and calls `optimizer.cli.main`.

```bash
./video_optimizer.py <subcommand> [...]   # run the CLI directly
ruff check .                              # lint (config in pyproject.toml)
ruff check --select=ALL .                 # full ruleset; expected clean modulo the documented ignores
```

External runtime deps: `ffmpeg` and `ffprobe` on `PATH`. `list-encoders` is the canonical pre-flight to confirm the local ffmpeg has the encoders the user expects (it parses `ffmpeg -encoders` and also reports `/dev/dri/renderD128` for VAAPI).

## Architecture: the pipeline and what glues it

The CLI is a four-stage pipeline whose stages communicate **only through the SQLite db** (default `~/.video_optimizer/state.db`, override with `--db`). There is no in-process pipeline object — each subcommand opens the db, reads what it needs, writes its output rows, and exits.

```
scan PATH  ──►  files table (probe cache, keyed by path; invalidated by size+mtime)
                │
                ▼
plan       ──►  decisions table (status='pending')      [clears prior pending first]
                │
                ▼
apply      ──►  decisions table (status='completed' / 'failed' / 'skipped')
                │
                ▼
status     ──►  read-only view of runs + pending decisions
```

Three things to internalise about this pipeline before changing it:

1. **The probe cache key is `(size, mtime)`, not a content hash** (`db.Database.get_cached_probe`). A file edited in place with mtime preserved will produce stale probe data — `reprobe PATH` (alias for `scan --no-probe-cache`) is the escape hatch.
2. **`plan` is destructive to the pending queue.** It deletes every `status='pending'` row before re-running rules (`db.clear_pending_decisions`). Half-applied queues from a previous run are preserved (their status is no longer `pending`), but you cannot "amend" a plan — re-running starts the queue over.
3. **`apply` may leave a candidate `pending` rather than terminalising it** when the resolution gate (`--min-height` / `--max-height`) defers it. This is the mechanism that lets `hd-archive` and `uhd-archive` share one queue: each preset processes its own band and skips the other's, leaving those rows for a follow-up run.

## Module map (what owns what)

- **`optimizer/cli.py`** — argparse wiring, subcommand handlers, and the apply pipeline (`_apply_one`, `_build_apply_command`, `_execute_encode`, `_finalize_output`). Presets are defined in `optimizer/presets.py` and imported here; `cmd_preset` fills missing args from the preset config and delegates to `cmd_apply`.
- **`optimizer/presets.py`** — single tuning surface. `PRESETS` (preset CQ + height gate), `AV1_QSV_TIER` (HD/UHD `maxrate` / `bufsize` / `look_ahead_depth` / `gop`), `AV1_QSV_BASE` (tier-independent av1_qsv flags), `BITRATE_FLAG_TABLE` (rules engine thresholds). When changing tunings, edit here — `cli.py`, `encoder.py`, and `rules.py` all read from this module. Deliberately a Python module rather than a TOML/YAML config: zero parser, zero precedence rules, comments allowed. If multiple co-existing tunings (per-machine, per-library) become a real need, this is the natural step where it graduates to `~/.video_optimizer/config.toml`; the shape maps cleanly.
- **`optimizer/probe.py`** — `ffprobe` wrapper → `ProbeResult`. The probe layer is responsible for HDR detection (`is_hdr` derived from color metadata) and for synthesising a `video_bitrate` estimate when the stream-level value is absent.
- **`optimizer/rules.py`** — `Rule` subclasses + the `RULES` registry + `RulesEngine`. A `Candidate` is produced **iff at least one non-advisory rule fires**; advisory rules (currently only `hdr_advisory`) are attached to the candidate but cannot create one alone. `remux_only` is set when *only* `container_migration` fires *and* the existing video codec is in `_MODERN_CODECS` ({h264, hevc, av1, vp9}).
- **`optimizer/encoder.py`** — encoder discovery (`get_available_encoders` parses `ffmpeg -encoders` and caches the result module-wide), `select_encoder(target, hwaccel)` (falls back through `ENCODER_PREFERENCE[codec][hwaccel]` then to software), and the ffmpeg argv builders for both encode and remux paths. The encode builder is also responsible for the AAC compat-track shadowing logic and for the `mov_text` subtitle conversion / image-sub drop when targeting MP4.
- **`optimizer/db.py`** — SQLite schema (`files`, `decisions`, `runs`), context-manager wrapper, all CRUD. Schema is created idempotently on every connect; there is no migration system.
- **`optimizer/models.py`** — dataclasses (`ProbeResult`, `AudioTrack`, `SubtitleTrack`, `RuleVerdict`, `Candidate`) plus the `to_json` / `probe_from_json` helpers that let `db.py` round-trip a `ProbeResult` through a single TEXT column. `ProbeResult.resolution_class` is the canonical resolution-bucket key shared with `presets.BITRATE_FLAG_TABLE`.
- **`optimizer/naming.py`** — pure-string codec-token rewriting for `--rewrite-codec` / `--reencode-tag`. No I/O.
- **`optimizer/crawler.py`** — recursive directory walk, video-extension filter.
- **`optimizer/report.py`** — text and JSON candidate-list rendering for `plan`.

## Conventions worth respecting

- **Subcommand handlers always open their own `Database`** via `with Database(args.db) as db:` and frame the work between `db.start_run(...)` and `db.end_run(...)`. Adding a new subcommand should follow this — `runs` is the audit trail.
- **ffmpeg invocations live in `encoder.build_*_command`**. `cli.py` should not assemble ffmpeg argv directly. The same goes for encoder selection — call `encoder.select_encoder` rather than reading `ENCODER_PREFERENCE` from the CLI.
- **Tuning values belong in `presets.py`, not at the call site.** `_qsv_args`, `cmd_preset`, and `OverBitratedRule` all read their numeric knobs from `presets.py`. New tunable values (a new tier, a new resolution bucket, a new preset) should be defined there and consumed from there — don't reintroduce literals back into the consumer modules.
- **Output-path computation belongs to `cli._compute_output_path`.** It is the single place that branches on `--mode side` vs `--mode replace`, applies `--source-root` stripping, and composes filename rewrites with the new extension. Replicating its logic anywhere else creates skew.
- **Failure path on encode**: partial output is `unlink()`ed and the decision row gets `status='failed'` with the truncated stderr in `error`. Replace mode never deletes the original until the encode succeeds *and* (if `--backup` is set) the backup copy succeeds. Preserve this ordering when touching `_finalize_output` / `_execute_encode`.
- **Adaptive timeout**: `_resolve_timeout` defaults to `max(3600, 6 × duration_seconds)`; `--timeout 0` disables; an explicit positive value wins. A 3-hour movie therefore gets an 18-hour cap.
- **Ruff `ignore` list in `pyproject.toml` is intentional** (subprocess-to-ffmpeg, module-level encoder cache, blind cleanup excepts in kill paths, etc.). Don't silence those rules per-line; if a new violation needs an exception, add it to the central list with a comment matching the existing style.

## README cross-reference

The README documents every flag, the bitrate flag table, the audio/subtitle handling contract, and the Radarr/Sonarr filename workflow. When changing user-visible behaviour (a flag's default, a rule's threshold, the bitrate table, encoder preference order, the keep-langs contract), update both the code and the matching README section — the README's tables are the user-facing source of truth and the docstrings inside the code defer to them.
