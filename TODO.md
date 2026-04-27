# TODO

Actionable work items, ordered by rough priority within each section.
Add new items at the bottom of the appropriate section. Prefer enough
context that whoever picks the item up doesn't have to re-derive the
why.

## Performance

- [ ] **Parallelise scan / probe** (`optimizer/cli.py:cmd_scan` /
      `optimizer/probe.py:probe_file`). Today the scan loop walks the
      tree sequentially and runs one `ffprobe` subprocess per
      uncached file (~30 ms idle, multi-second on slow files / NFS).
      For a multi-thousand-file library a cold scan is dominated by
      probe-subprocess overhead, not actual work.

      Approach: `concurrent.futures.ThreadPoolExecutor` with ~CPU-count
      workers (probe is I/O-bound — ffprobe subprocess + NFS read —
      threads are fine, no GIL contention). Cache hits should stay on
      the main thread (no upside to threading them; just adds
      coordination overhead). Probe results need to flow back to a
      single SQLite connection for `upsert_probe`; either queue them
      from the workers or batch and commit after the parallel phase
      finishes.

      Don't parallelise above ~8 workers without testing — Synology /
      QNAP NFS exports throttle concurrent reads, and ffprobe lots of
      I/O on a slow share doesn't speed up beyond the share's read
      ceiling.

      Verify with a stopwatch on `scan /mnt/nas/media` cold (post-cache
      delete). Target: 4-6× speedup at 8 workers vs current sequential.

- [ ] **Parallelise apply on multi-engine GPUs**
      (`optimizer/cli.py:cmd_apply`). Apply iterates pending decisions
      sequentially. For single-stream encodes on a single-engine GPU
      that's fine — encoder is the bottleneck. But Battlemage / Arc
      Pro and similar dual-media-engine GPUs can encode two streams
      concurrently across the engines, and the current loop leaves
      Engine 2 idle.

      Approach: a small worker pool sized to the GPU's media-engine
      count (probe Intel's `vainfo` or QuickSync stats to count;
      default to 1 if unknown). Each worker pulls the next pending
      decision from the db and calls the same `_apply_one` path.
      SQLite writes need to be serialised through the main thread or
      use `PRAGMA journal_mode=WAL` plus per-worker connections.

      Caveat: I/O contention on the source (NFS share read) can
      throttle wins. Measure on a representative library before
      committing to >2 workers. Target: ~1.7× wall-clock speedup on
      dual-engine hardware vs sequential.

## Robustness

- [ ] **Filter NAS / OS system directories during crawl**
      (`optimizer/crawler.py`). On the user's first full library scan
      ~84 % (2,026 of 2,401) of cache rows were Synology / QNAP
      thumbnail garbage from `.@__thumb/` directories. The thumbnails
      use the source filename verbatim (including `.mkv` extension)
      with JPEG content inside, so the existing extension filter
      passes them, ffprobe reports them as `mjpeg 3840x2160 0.0 Mbps`,
      and they bloat the cache + every `plan` output.

      Approach: directory-level skip-list at the entry of
      `_classify_entry`. Skip if the directory name starts with
      `.@__thumb`, `@Recycle`, `@Recently-Snapshot`, `#recycle`, or
      `.AppleDouble` (the common NAS / SMB system dirs). Plus
      `__pycache__`, `.git`, `.svn` for the basic-hygiene set.
      Single-pass: cheap, no other side effects.

      One-time cleanup of existing cache: a `reprobe --prune` mode
      (or just a SQL `DELETE FROM files WHERE path GLOB '*.@__thumb*'`)
      to evict the existing cruft. Document for users with old caches.

- [ ] **DB schema migration framework** (`optimizer/db.py`). Schema is
      created idempotently on every connect via `executescript(_SCHEMA)`,
      which only handles `CREATE TABLE IF NOT EXISTS`. Adding a column
      to an existing table won't migrate — users would have to delete
      their cache. As soon as we change the schema we'll lose every
      cached probe in the wild.

      Approach: `PRAGMA user_version` plus a numbered migration list
      (`_MIGRATIONS: list[tuple[int, str]]`) applied in order on
      connect. Each migration is a SQL string; current schema becomes
      migration 1. Future schema changes append a migration that ALTERs
      / adds tables / etc. Trivial to retrofit; the right time to do
      it is *before* the next schema change.

## Encoding

- [ ] **Drop `-look_ahead 1` from `_qsv_args`** (low priority,
      cosmetic). Every encode produces this warning:
      `Codec AVOption look_ahead (Use VBR algorithm with look ahead)
      has not been used for any stream.`

      `-look_ahead` is a family-level QSV option that exists on
      `h264_qsv` and `hevc_qsv` (where it engages a lookahead rate-
      control variant) but is **not implemented on `av1_qsv`** — the
      AV1 encoder ignores it and ffmpeg surfaces "has not been used"
      as a warning. We're already in ICQ mode (via `-global_quality`)
      with an active lookahead window (via `-look_ahead_depth`, which
      av1_qsv *does* support); `-look_ahead 1` adds nothing on top.

      Removing the two-token literal `["-look_ahead", "1"]` from
      `_qsv_args` cleans up log output with no behavioural change on
      av1_qsv. Verify with a before/after argv diff + one re-encode
      to confirm the same output bitrate. Caveat: if we ever wire up
      `hevc_qsv` or `h264_qsv` for some target, we'd want to keep
      `-look_ahead 1` *for those encoders* — currently neither is in
      regular use, but worth a comment if/when removed.

- [ ] **Filename normalization beyond `--name-suffix`**
      (`optimizer/naming.py`). Currently we strip foreign codec tokens
      and append AV1 / REENCODE markers, but otherwise preserve the
      release-group filename verbatim — including bracket cruft, year
      placement quirks, extra periods, and so on. A Plex-friendly
      pass would normalize `Movie Name (2010) [HDR][REMUX]-GRP` →
      `Movie.Name.2010-GRP.AV1` consistently.

      Approach: pass over the stripped stem after `_strip_foreign_tokens`
      and `_cleanup_separators`. Rules: collapse `[..]` and `(..)`
      groups around tags (keep year), normalize whitespace, dedupe
      consecutive separators. Keep behind a flag (`--normalize-name`)
      so users with delicate Sonarr matching can opt in.

- [ ] **Two-pass software encoding for `libsvtav1` / `libx265`**.
      The QSV path uses ICQ (single-pass quality target) and works
      well. Software-encoder fallbacks (`libsvtav1 -preset 6
      -crf` / `libx265 -preset medium -crf`) are also single-pass
      CRF, which is fine — but for users with hard size targets
      rather than quality targets, two-pass ABR delivers more
      predictable file sizes. Significant work: pass 1 produces a
      stats file that pass 2 reads, both run ffmpeg, decision row
      tracks intermediate state in case pass 2 fails. Probably wait
      until a user actually asks for it.

## Tooling

- [ ] **`archive-uhd.sh` companion to `archive-hd.sh`**, OR
      generalise the existing script to take a `--preset hd|uhd`
      flag. Right now `archive-hd.sh` is hardcoded to the `hd-archive`
      preset; running the same workflow against the UHD library
      requires editing the script in place. Two-line generalisation:
      add `--preset NAME` defaulting to `hd`, swap `hd-archive` for
      `${PRESET}-archive` in the apply args. Update the script's help
      text to mention both.

- [ ] **Formalise synthetic probe tests** (new `tests/` directory).
      Throughout the v0.4–v0.5 work cycle every check ran ad-hoc via
      Python heredocs (synthetic `ProbeResult` → `_build_audio_ladder`
      / `_qsv_args` / `build_encode_command` → assert argv). Those
      checks lived in conversation history, not the repo.

      Approach: `tests/test_audio_ladder.py`, `tests/test_qsv_args.py`,
      `tests/test_naming.py` covering the three cases that have caught
      regressions in this cycle: ladder-shape for various source
      audio configs, av1_qsv argv with hw_decode + 10-bit + HDR, and
      filename rewrite for the documented input-stem table. Use
      stdlib `unittest` (consistent with the project's stdlib-only
      stance). Wire into `pyproject.toml` / a CI workflow when there
      is one.
