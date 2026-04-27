# TODO

Actionable work items, ordered by rough priority. Add new items at the
bottom of the appropriate section. Prefer enough context that whoever
picks the item up doesn't have to re-derive the why.

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
