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

- [ ] **WMV3 → vc1_qsv via bitstream filter (HW-decode fallback for legacy WMV)**
      `optimizer/presets.SLOW_CPU_DECODE_CODECS` flips VC-1 and VP9 to
      `-hwaccel qsv -hwaccel_output_format qsv` automatically — those are
      the only single-threaded-decode codecs that *also* have a QSV decoder
      exposed on Battlemage (`vc1_qsv`, `vp9_qsv`). WMV3 (and to a lesser
      extent WMV2) hits the same single-threaded software-decode trap but
      has no `wmv3_qsv`, so it's currently excluded.

      WMV3 *is* VC-1 Simple/Main profile on the wire — only the codec ID
      differs. A bitstream filter (`-bsf:v vc1_asftorcv` or repackaging via
      `-codec:v vc1` on input) can present a WMV3 stream to `vc1_qsv`.
      Worth a feasibility spike on one of the user's WMV3 files (if any
      exist — modern libraries rarely have any) before committing code.

      If it works: add `wmv3` to `SLOW_CPU_DECODE_CODECS` plus a small
      branch in `_build_apply_command` that injects the bsf when source
      codec is `wmv3`. If it doesn't: leave WMV3 on the CPU-decode floor
      and add a one-line note to README "Known limitations" that legacy
      WMV files encode at ~half the typical HD rate.

      Skip if no WMV3 sources exist in the user's library — purely
      preventive code with no measured benefit is the kind of complexity
      `feedback_flag_minimalism` argues against.

## Robustness

- [x] **Filter NAS / OS system directories during crawl** — landed
      in v0.5.6 as `_SKIP_DIRS` + `_is_skipped_dir` in
      `optimizer/crawler.py`. Skips `.@__thumb`, `@Recycle`,
      `@Recently-Snapshot`, `#recycle`, `.AppleDouble`, plus
      `__pycache__` / `.git` / `.svn`. Existing cache pruned via
      `DELETE FROM files / decisions WHERE path LIKE '%.@__thumb%'`
      etc. (11,168 rows of garbage gone, ~85 % of the cache).
      Test coverage: `tests/test_crawler.py`.

- [ ] **Post-stall source-corruption probe + actionable error message**
      (`optimizer/cli.py:_apply_one`, `optimizer/cli.py:cmd_replace_list`).
      The current stall path records `error='encoder stalled — no
      progress for 300s (out_time=Xs, frame=N)'` and the operator-facing
      `replace-list` says "consider replacing with a different release"
      with no explanation of *why* the source is bad.

      Investigation in v0.6.1 found that the three "head scratcher"
      stall candidates (Indiana Jones - Last Crusade, Guardians of the
      Galaxy, Avengers Infinity War) all have source-side bitstream
      corruption that the qsv hardware H.264/HEVC decoder honestly
      refuses to handle: corrupt SPS (`sps_id 1 out of range`),
      DTS-HD MA decode failures with concealed h264 macroblock errors,
      and non-monotonic DTS timestamps respectively. CPU-side decode
      (libavcodec h264) is permissive — interpolates over missing
      macroblocks and corrects timestamps — which is why the same
      source completed under av1_vaapi (CPU decode → GPU encode path)
      but stalled under av1_qsv (zero-copy GPU pipeline). VAAPI
      "succeeding" means producing output with concealed corruption,
      so it's not a viable fallback — just a different failure mode.

      Approach: on watchdog stall, before recording the failure, fire
      a quick `ffmpeg -v error -i SRC -t 60 -c copy -f null /dev/null`
      probe (cheap — copies packets, no decode). Capture the first
      stderr line and store it in `decisions.error` alongside the
      stall message:

        `encoder stalled — no progress for 300s
         (out_time=Xs, frame=N); source corruption detected: <line>`

      `replace-list` then surfaces the corruption signature in its
      output, so the user can tell "broken release, grab a different
      one" apart from "qsv driver bug, try --hwaccel vaapi" (the
      latter is rare-to-nonexistent on our current evidence, but
      keeping the phrasing distinguishable is useful).

      Caveat: this is post-stall-only — we don't want a 60-second
      probe on every encode. And the probe itself can spuriously
      report "noise" on technically-valid sources (some warnings are
      benign), so the error string should be presented as a hint,
      not a diagnosis. Probably also gate this behind `--verbose` or
      a dedicated flag if the noise rate turns out to be high in
      practice.

- [ ] **`--rescue-mode` opt-in for irreplaceable corrupt sources**
      (probably `optimizer/cli.py` apply path + a small `encoder.py`
      branch). Companion to the post-stall corruption-probe TODO above:
      that one *identifies* damaged sources; this one gives the user
      an explicit escape hatch for the case where they can't replace
      the source.

      Motivating use case: someone's only copy of a wedding video lives
      on a failing hard disk. The file decodes with concealed macroblock
      errors. The current pipeline (qsv stall → two-strikes auto-skip →
      "go grab a different release" message) is correct for the typical
      pirated-Blu-ray case but actively unhelpful when the user has no
      alternative source. We'd rather give them a recoverable AV1 file
      with a few seconds of macroblock smear than refuse to encode.

      The v0.6.1 VAAPI experiment confirmed the path works: SW H.264
      decode + av1_vaapi encode completed Indiana Jones - Last Crusade
      end-to-end despite source-side DTS-HD MA decode failures and
      h264 macroblock errors that consistently wedged the qsv hardware
      decoder. Output played the full 2h6m53s; corrupted regions were
      silently interpolated by libavcodec's concealment, exactly the
      behavior we want here.

      Approach (sketched, not committed):
      1. **New `rescue PATH` subcommand, single-file only.** Takes
         exactly one file path (positional, no recursion, errors out if
         given a directory). Does its own probe → encode → finalize for
         the one file without ever touching the pending-decisions queue
         that drives `apply` / `optimize`. Keeping it as a separate
         subcommand — rather than a `--rescue-mode` flag on `apply` /
         `optimize` — is deliberate: the workaround silently produces
         output with concealed source corruption, and that's exactly
         the kind of behavior you don't want to risk leaking into a
         batch run. By construction, `rescue` can only apply to one
         file at a time and the user has to invoke it deliberately per
         file.
      2. **Forces lenient-decode pipeline**: `--hwaccel software`
         (libavcodec, the most permissive — interpolates over corrupt
         macroblocks and corrects timestamps) decoding into either
         `av1_vaapi` or `libsvtav1` for encode. QSV path is explicitly
         disallowed for `rescue` because the qsv hardware decoder is
         what wedges on this content in the first place. Stall
         watchdog is disabled for the rescue subcommand — a slow
         concealing decode on a heavily-damaged source can look like
         a stall.
      3. **Explicit warning + confirmation** at start: "RESCUE MODE:
         source decode errors will be silently concealed; output may
         contain macroblock corruption in damaged regions. Inspect the
         result before relying on it." Interactively requires a typed
         `y` to proceed; with `--auto` requires a `--confirm-rescue`
         token so it can't trip from a stray flag in a script.
      4. **Tag the resulting `decisions` row with `status='rescued'`**
         (distinct from 'completed') so it surfaces separately in
         `status` / `replace-list` outputs — the user will want to
         re-evaluate these manually rather than treating them as
         done-done. Also bypasses the two-strikes auto-skip for the
         specific path being rescued (the whole point is overriding it).
      5. **Optional sidecar**: `.rescue.txt` next to the output with
         the ffmpeg stderr concealment lines (frame counts, offsets)
         so the user knows where to look when reviewing the result.

      Caveat / non-goals:
      - **Never expose this as a flag on `apply` / `optimize` /
        presets.** Single-file `rescue` subcommand only. The single-file
        constraint is a load-bearing safety property, not a UX choice.
      - Don't make rescue the default for any preset.
      - Don't try to estimate "how corrupt is too corrupt" — let the
        user decide after seeing the output.
      - Don't try to reconstruct the missing data; libavcodec's
        concealment is good enough and we're not in the business of
        error-correction-coded video reconstruction.

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

- [ ] **Runtime encoder fallback (av1_qsv → av1_vaapi → CPU)** — today
      `select_encoder` picks one encoder up-front from `ENCODER_PREFERENCE`
      and that's the only attempt. If `av1_qsv` wedges or stalls on a
      specific source, the file is marked failed and (after two strikes)
      permanently skipped — even when av1_vaapi or CPU might succeed on
      it. Proposed feature: on a *recoverable* failure (encoder stall, hw
      driver crash, but not source-validation issues), retry the same
      file with the next encoder in the auto chain. Caveats: on
      Battlemage specifically, av1_qsv and av1_vaapi drive the same MFX
      hardware via the same iHD driver — a wedge in one will likely
      wedge the other, so this is mostly a sideways move on Intel boxes.
      The real win is on heterogeneous setups (older Intel iGPU + AMD
      discrete, or systems where the QSV stack is broken but VAAPI
      works) and as a lifeline before falling through to libsvtav1.
      Should be opt-in via something like `--encoder-fallback-chain`
      because for the common case "this source wedges every encoder"
      we'd rather fail fast than burn 3× the wall-clock retrying.
      Implementation note: needs a "recoverable vs not" classification
      on the encode failure mode — wedging at frame 0 / qsv driver
      fault → retry; ffprobe-validation mismatch on the output → don't
      retry (the encoder did something, just wrong).

      Vendor-aware fallback ordering: `av1_vaapi` is **not** a useful
      fallback for NVIDIA hardware. NVENC isn't bridged through VAAPI
      on Linux (the existing nvidia-vaapi-driver is decode-only).
      So an NVIDIA box that fails `av1_nvenc` should skip
      `av1_vaapi` and go straight to libsvtav1. Detect via "is
      av1_vaapi available *and* is there a working VAAPI driver
      (iHD or radeonsi) for it" rather than naive list-walking.

- [ ] **`av1an` experimental backend (Battlemage dual-MFX exploit)** —
      Battlemage's BMG-G21 silicon ships with two MFX (media
      fixed-function) engines vs Alchemist's one — confirmed by Intel
      marketing and reported by the Jellyfin team on Intel contact.
      What's **not** confirmed: whether the Xe kernel driver enumerates
      them as two VCS instances (`vcs0`+`vcs1`) or collapses them into
      one logical engine. The Lunar Lake Xe2 iGPU on the dev box exposes
      a single `vcs0` via `/sys/kernel/debug/dri/0/gt1/hw_engines`, but
      Lunar Lake is the cut-down mobile variant — that doesn't generalise
      to discrete Battlemage. No public `hw_engines` dump from a B580
      owner exists, so first thing on this item is verifying the engine
      count on actual discrete hardware: `sudo cat
      /sys/kernel/debug/dri/0/gt*/hw_engines | grep -E '^(vcs|vecs)'`
      and `time ffmpeg ...` ×2 concurrent vs ×1 serial throughput
      comparison on a real source.

      Known flag worth being aware of:
      [intel/media-driver issue #1626](https://github.com/intel/media-driver/issues/1626)
      reports that two concurrent `av1_qsv` ffmpeg processes produce
      *corrupt output* on Alchemist. Open with no fix. Whether this
      bug also affects Battlemage is itself something to verify before
      committing to chunked-parallel encoding on the QSV path. The
      `av1_vaapi` path may not have the same bug since libva and
      oneVPL/MFX are different scheduling layers driving the same
      hardware — testable by running two parallel `av1_vaapi` encodes
      and validating the output via `ffprobe` + frame-hash comparison.

      Two angles worth exploring, both gated behind a flag (e.g.
      `--backend av1an`):

      **Angle 1 — chunked parallelism for raw throughput.**
      av1an splits a source into scene-aligned chunks and runs N
      encoder workers in parallel. With two `av1_qsv` workers pinned
      to separate `/dev/dri/renderD12*` device nodes (if Battlemage
      exposes both MFX engines as distinct render nodes; needs
      verifying — `vainfo` against each node), a single feature-length
      title could plausibly encode at ~1.7-1.9× our current rate.
      The same effect is achievable from our own orchestrator by
      spawning two `av1_qsv` ffmpeg children on adjacent files (see
      "apply parallelism" TODO above) — that's simpler and avoids
      av1an's chunked-quality discontinuity issue. So the throughput
      story alone probably isn't enough to justify av1an.

      **Angle 2 — scene-aware target-quality mode for grain-dominated
      content.** This is where av1an genuinely earns its place. Today
      our bloat fallback is binary: detect bloat at a checkpoint, kill
      the encode, retry the *whole file* at CQ 21 + `slow`. Most of
      the file didn't need the relaxed tuning — only the grainy
      stretches did. av1an's `--target-quality` mode does a per-chunk
      CQ search aiming at a fixed VMAF score, so a single film can
      have CQ 15 on the clean digital scenes and CQ 22-24 on the
      grain-dominated reels, without the user (or the orchestrator)
      having to pre-classify. That's a real quality+size win on
      titles like Princess Bride, Tron, The Godfather where the
      grain density varies across the runtime.

      **Caveats** — av1an's first-class encoder is `svt-av1` (CPU);
      `av1_qsv` support exists but is less well-trodden and may need
      a custom encoder template. Falling back to svt-av1 + QSV decode
      is plausible but loses the GPU encode advantage. Concat-time
      audio handling is encoded once on the coordinator so audio
      ladder logic stays where it is. External dep: `av1an` binary
      (Rust, `cargo install av1an` or prebuilt) plus `vmaf` for the
      target-quality mode. All of this should be opt-in, gated behind
      `--backend av1an` (or whatever name), and fall through to the
      current single-instance `av1_qsv` path for users who don't
      install av1an. Fail-closed at the apply gate if the binary is
      missing — same pattern as `--dv-p7-convert`.

      **Estimated scope**: probably 300-500 lines including a new
      `optimizer/backends/av1an.py` (since this is the first time we'd
      have backend pluralism, the encoder.py/_build_apply_command
      split would need to grow a backend dispatch). Cleanest path:
      get the throughput angle working end-to-end first as a proof of
      concept (validates av1_qsv + av1an integration), then layer
      target-quality on top. Bench against the existing serial path
      on the same UHD-FILM titles the bloat fallback fires on; if
      VMAF-targeted output is materially better than CQ 21 + `slow`
      retry at comparable wall-clock, the feature graduates from
      experimental to a recommended preset.


- [x] **Multi-language stall pattern (AVC + HEVC)** — landed in
      v0.5.17 as a demuxer-side pre-strip via `_input_discard_args`.
      Diagnosis: hypothesis (2) from the original entry was correct.
      The wedge is upstream of the decoder choice — ffmpeg's matroska
      demuxer interleaves packets for every active stream by container
      timestamp, and on sources with 7+ parallel audio tracks the
      windows between audio packets get tight enough that the QSV
      video decoder's input queue starves and deadlocks at frame 0.
      CPU decode survives on more headroom but isn't immune (the AVC
      remux list — Indiana Jones, MI3, Star Wars episodes — pre-dated
      the v0.5.16 hw_decode flip and stalled on CPU decode too).

      Pinned by Avengers: Infinity War in run 16 of the v0.5.16
      uhd-archive trial — HEVC Main10 + 8 audio tracks (TrueHD, DTS-HD
      MA, 4× AC3/EAC3 in eng/fre/spa/jpn), stalled at frame 0 with
      hw_decode=True. Confirmed the mechanism.

      Fix: emit `-discard:a:N all` / `-discard:s:N all` for every audio
      and subtitle stream not selected by `_build_audio_ladder` /
      `_subtitle_map_args`. The discards apply at demux time so dropped
      streams never enter the packet queue. Source-side indexing is
      preserved (discard doesn't renumber), so existing `-map 0:a:N?`
      references stay valid. Resolves both AVC and HEVC variants in
      one shot — they were always the same root cause.

      Existing two-strikes auto-skip and watchdog stay as belt-and-
      braces. Replace-list still surfaces any future stall pattern.

*Original entry retained for context:*

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

- [ ] **DV strip-and-encode (P7/P8 → HDR10) as the default DV path**
      (replaces the current plan-time `dv` skip). The probe layer
      already detects `dv_profile`; today the plan-gate drops every
      DV source from the queue because av1_qsv wedges on them
      (Profile 7 stalls at frame 0, Profile 8 stalls partway in).
      Strategy decided 2026-05-06: strip the DV RPU, encode the
      HDR10-compatible base layer to AV1. Most modern UHD content
      has a clean HDR10 base layer; players that don't speak DV
      already fall back to it, so this is a no-loss path for the
      common case.

      Per-profile routing (matches the README's "Dolby Vision"
      section):

      - **Profile 8.x** (most modern UHD WEB-DL / Blu-ray): one-pass
        strip via `-bsf:v dovi_rpu=strip` + the existing av1_qsv
        encode. Base layer is HDR10 already; strip is the only
        bitstream surgery needed. Wire as a new `strip_dv=True`
        branch in `encoder.build_encode_command` driven by
        `pr.dv_profile in {8, "8.1", "8.4", ...}`.

      - **Profile 7** (some UHD Blu-rays with FEL/MEL enhancement
        layers): plain strip leaves orphan enhancement-layer NALs
        that confuse the encoder. Need `dovi_tool convert -m 2` to
        flatten P7 → P8 first, then run the same strip path. New
        external dep (`dovi_tool` from GitHub, not in apt) — `doctor`
        gains a check; sources fall back to `dv` skip when it's
        absent.

      - **Profile 5** (Apple TV+, some Vudu): keep skipping. Base
        layer is *not* HDR10 — it's a custom DV-only colour space
        that requires the RPU to map. Stripping leaves a green/
        over-saturated mess; no clean HDR10 fallback exists.

      Implementation notes:

      - `_plan_probe_gate` refines the `dv` verdict: P5 always
        skipped; P7 skipped only when `dovi_tool` is missing on
        PATH; P8 admitted to rule evaluation, with the apply layer
        knowing to use the strip path.
      - `--allow-dv` flag preserved for forcing the legacy
        skip-everything behavior (in case the strip path causes
        regressions on a particular title).
      - The strip path is essentially a two-pass internally:
        stream-copy with bsf to strip RPU → AV1 encode. Pipe the
        first pass directly into the second to avoid temp-file I/O
        on a 50 GB UHD remux.
      - `replace-list` extension: surface DV-skipped (P5) sources
        alongside the stall list so the user can find different
        releases.

      Future direction (not part of this work): **Profile 10
      preservation** — carry DV metadata through the AV1 encode as
      Profile 10 OBU side-data, so DV-capable players continue to
      see DV after the transcode. Depends on `dovi_tool inject-rpu`
      AV1 support and on Plex/PMS / Shield TV / Apple TV recognising
      the resulting Profile 10 stream. Revisit when the tooling and
      player ecosystems stabilise.

- [x] **Drop `-look_ahead 1` from `_qsv_args`** — landed in v0.5.16
      together with the uhd-archive `hw_decode=True` flip and the
      `-nostdin` addition. Comment in `_qsv_args` notes the option is
      preserved for the day hevc_qsv / h264_qsv come back into the
      regular path.

      *Original entry retained for context:* (low priority,
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

- [ ] **`audit` subcommand: library health summary** — read-only
      report on the state of a library, sourced entirely from the
      existing probe cache + rules engine. Output covers:
        - file count, total size, codec breakdown (av1 / hevc / h264 /
          legacy), tier breakdown (UHD / HD / SD)
        - per-tier candidate counts and projected savings
        - skip-bucket counts with the reason and the override flag
          that re-includes them (already-AV1, sub-target bitrate,
          DV Profile 5, already-reencoded, etc.)
        - "projected total savings if you encoded everything" headline
      `--json` flag for the same data as a single object so users can
      pipe it through awk/jq/Grafana without us having to maintain a
      dashboard. Stretch: per-codec / per-resolution histogram so
      grain-prone titles (legacy h264 at HD with 25+ Mbps) surface
      visually. The motivating use case is the Tdarr-style "what's
      the state of my library?" question — we already have the data
      from the probe cache, we just don't summarize it. New subcommand
      should follow the existing pattern: open the db with the
      `Database` context manager, frame the work between
      `start_run("audit")` / `end_run()`, render via a new helper in
      `optimizer/report.py`. No new state, no new flags on the existing
      subcommands. Estimated ~150 lines.

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
