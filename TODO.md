# TODO

Actionable work items, ordered by rough priority within each section.
Add new items at the bottom of the appropriate section. Prefer enough
context that whoever picks the item up doesn't have to re-derive the
why.

## Performance

- [x] **Parallelise scan / probe** — landed in v0.5.14 as
      `concurrent.futures.ThreadPoolExecutor` in `cmd_scan`, gated
      behind a new `--workers` flag (default `min(8, cpu_count())`).
      Walk + cache-filter phase stays single-threaded; only uncached
      files dispatch to the pool. SQLite writes still flow through the
      main thread (worker returns `("ok"|"err", path, result)` tuples
      via `as_completed`, main thread upserts). Refactored into
      `_scan_walk_phase` + `_scan_probe_phase` helpers to satisfy C901.
      Real-fs benchmark (14 cold files): 7.97 s sequential → 0.46 s
      with 8 workers (**17× speedup**, exceeded the 4–6× target).
      Test coverage: `tests/test_scan.py` (parallel == sequential, cap,
      cache-skip, error-isolation).

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

- [x] **Filter NAS / OS system directories during crawl** — landed
      in v0.5.6 as `_SKIP_DIRS` + `_is_skipped_dir` in
      `optimizer/crawler.py`. Skips `.@__thumb`, `@Recycle`,
      `@Recently-Snapshot`, `#recycle`, `.AppleDouble`, plus
      `__pycache__` / `.git` / `.svn`. Existing cache pruned via
      `DELETE FROM files / decisions WHERE path LIKE '%.@__thumb%'`
      etc. (11,168 rows of garbage gone, ~85 % of the cache).
      Test coverage: `tests/test_crawler.py`.

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

- [ ] **Investigate AVC-remux multi-language stall pattern**. The
      two-strikes list shows a strong correlation between av1_qsv
      stalls and a specific source shape: AVC (H.264) Blu-ray remuxes
      with 4–7 parallel language audio tracks. As of v0.5.14 the
      8-title two-strikes list is dominated by `BEN.THE.MEN` releases
      (Indiana Jones: Dial of Destiny, Last Crusade; Star Wars
      Episodes I, II, IV) and the Mission: Impossible AVC-remux
      cluster (III, Fallout, Rogue Nation). All 1080p AVC, all
      multi-lang, all stall on av1_qsv either at frame 0 or partway
      through. Successful encodes in the same batches are typically
      single-language or HEVC sources.

      Hypothesis: the decode side (qsv-accelerated H.264 decoder
      consuming a stream interleaved across many audio mappings) is
      what wedges, not the AV1 encoder itself. Worth checking by:
        1. Re-running one stalled title with `--hwaccel none` (sw
           decode → hw encode) — if it completes, the QSV decoder
           is the culprit.
        2. Re-running with all but one audio track stripped — if it
           completes, the demuxer / mapping interaction is implicated.
        3. Checking ffmpeg `-loglevel debug` for the last decoded
           PTS before the stall.

      Not blocking — the watchdog + two-strikes auto-skip already
      contains the damage. But identifying the root cause might
      yield a workaround (e.g. force sw decode for AVC sources with
      >3 audio streams, or a probe-time flag that routes them to a
      different encoder path).

- [ ] **DV-aware encode path for av1_qsv** (replaces the v0.5.15
      plan-time skip). v0.5.15 detects `dv_profile` from the ffprobe
      `DOVI configuration record` side-data and drops every DV source
      from the queue, because av1_qsv consistently wedges on them:
      Profile 7 (BL+EL, dual-layer) stalls at frame 0; Profile 8
      (single-layer + RPU) makes initial progress and stalls partway
      in. Confirmed against five titles in the run-3 / run-6 archive
      logs (LOTR x265-NAHOM trilogy → P7, The Housemaid 2025 → P7,
      Hobbit Desolation of Smaug → P8). The skip rule keeps the queue
      moving but means DV sources are never re-encoded — a permanent
      gap until this work lands.

      Two viable paths, both worth prototyping on a single P7 title
      first to confirm the encoder accepts the modified bitstream:

      1. **Bitstream-filter strip** — prepend `-bsf:v
         'filter_units=remove_types=62'` (Dolby Vision EL NAL type for
         HEVC) before `-c:v av1_qsv`. Cheapest fix if the QSV pipeline
         is happy with a BL-only HEVC frame stream. P7 → strip EL +
         RPU; P8 → strip RPU only. Risk: the BL alone may still carry
         RPU-side-data references that confuse the encoder.

      2. **External `dovi_tool` pre-pass** — `ffmpeg -i src -c copy -bsf
         hevc_mp4toannexb -f hevc - | dovi_tool remove --rpu-out /dev/null
         - -o bl.hevc`, then re-mux + encode bl.hevc. Canonical fix for
         Profile 7 but adds an external dependency (`dovi_tool` from
         GitHub releases, not in apt). Also doubles the I/O — read
         source twice on every encode.

      Whichever path lands: keep the plan-time skip behind a flag
      (`--allow-dv` opt-in) so users on machines without the workaround
      don't get the stall pattern back. Update `replace-list` to also
      surface DV sources (alongside two-strikes failures). Worth
      probing whether `hevc_vaapi` or libsvtav1 handles DV cleanly —
      if so, route DV sources to a software fallback rather than
      av1_qsv.

      Outstanding research: capture an `ffmpeg -loglevel debug`
      transcript of one stalled DV encode to identify exactly which
      NAL/RPU sequence wedges the av1_qsv intake. Without that, we're
      guessing about which NAL types to strip.

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

- [x] **`archive-uhd.sh` companion to `archive-hd.sh`** — landed
      in v0.5.14 by consolidating into a single `archive.sh` with a
      `--preset hd|uhd` flag (default `hd` for backwards-compat
      ergonomics). Old `archive-hd.sh` removed. Default `SCAN_PATH`
      widened to `/mnt/nas/media` so the same script can drive both
      libraries; resolution gate in each preset filters out the other
      band.

- [ ] **Sonarr/Radarr import hook**: per-file re-encode triggered on
      Arr import. Once the backlog is encoded, the steady-state goal
      is that newly-downloaded files run through the same rules engine
      + audio ladder + HDR pipeline before they ever look like a
      "finished" import to the rest of the library.

      Trigger surface: both apps have *Settings → Connect → Custom
      Script* with a "Download / Import / Upgrade" event. They pass
      the imported file path via environment variables
      (`sonarr_episodefile_path`, `sonarr_episodefile_sourcefolder`,
      `radarr_moviefile_path`, `radarr_movie_path`, etc.). Script
      must return 0 quickly — encode is hours-long, so the hook
      enqueues and detaches.

      Approach (sketched, not committed):
      1. New `tools/arr-import-hook.sh` reads the relevant Arr env
         vars, normalises the import path, exits 0 immediately.
      2. Either fire-and-forget a background job
         (`nohup video_optimizer.py scan "$file" && plan && apply
         --auto --mode replace --recycle-to ... &`) or push a row
         into a new `import_queue` table that a periodic worker
         drains. Worker is preferable — avoids overlapping encodes
         when the Arr app imports a season pack of 20 episodes.
      3. New CLI surface either way: a single-file shortcut like
         `video_optimizer.py one-shot PATH` that runs scan + plan +
         apply for one path, respecting the existing decision /
         status / recycle plumbing. The Arr hook calls this; cron
         or systemd-timer drains the queue at off-hours.

      Quality gate: only re-encode when the rules engine would have
      flagged the file in a normal `plan` pass — over_bitrate or
      legacy_codec firing. Pass-through everything that's already
      archive-grade (modern codec at sane bitrate). The hook should
      *not* fire a re-encode on imports that don't meet the gate; it
      should exit 0 silently and leave the file alone.

      Failure handling: the Arr app has already imported the file
      to its library path by the time our hook runs. If our re-encode
      fails, the original stays at the library path (replace mode
      never deletes until encode succeeds), the decision row is
      `failed`, and the Arr app is none the wiser. Worker retries
      on the next drain cycle; if persistently failing, manual
      review via `status`.

      Caveats / open questions:
      - Sonarr's "Upgrade" event re-fires the hook when a better
        source replaces an existing file. Want to handle: re-encode
        the new source, recycle the old encoded file (which may
        itself be ours from a prior import). Need to detect the
        `.AV1.REENCODE` marker to avoid encoding our own output.
      - Concurrent imports (season packs): single-worker queue
        avoids contention; multi-engine GPU could parallelize but
        see the apply-parallelism TODO item — they share the same
        underlying refactor.
      - Arr apps may scan/index the new encoded file as a "different
        file" since the codec rewrite changes the filename. Document
        the Custom Format setup needed (already covered in README's
        Radarr/Sonarr section).

- [x] **Formalise synthetic probe tests** — landed in v0.5.5 as
      `tests/test_audio_ladder.py`, `tests/test_qsv_args.py`,
      `tests/test_naming.py` plus shared `tests/_fixtures.py`. 32
      assertions covering the v0.4.1 maxrate, v0.4.1 hw_decode pix_fmt,
      and v0.5.4 `-global_quality:v` regressions (each test with a
      docstring tying it to the bug it pins). Run with
      `python3 -m unittest discover -s tests -v`.
