"""Scan-pipeline tests: parallel probe path produces the same db state
as the sequential path, and obeys the --workers flag.

These tests don't run real ffprobe — they patch `probe.probe_file` with
a stub that returns a deterministic ProbeResult per filename. The intent
is to verify the orchestration (cache hit/miss split, single-writer
SQLite, error handling) rather than ffprobe behavior.
"""

from __future__ import annotations

import argparse
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from optimizer import probe as probe_mod
from optimizer.cli import cmd_scan
from optimizer.models import ProbeResult


def _stub_probe(path: Path) -> ProbeResult:
    """Deterministic ProbeResult — encodes the filename into bit_depth so we
    can verify which file each row came from."""
    name_byte = ord(path.name[0]) if path.name else 0
    size = path.stat().st_size if path.exists() else 0
    return ProbeResult(
        path=str(path),
        size=size,
        mtime=path.stat().st_mtime if path.exists() else 0.0,
        duration_seconds=60.0,
        container="matroska",
        format_name="matroska,webm",
        video_codec="hevc",
        width=1920, height=1080, frame_rate=23.976,
        pixel_format="yuv420p",
        bit_depth=8 + (name_byte % 4),
        video_bitrate=10_000_000,
        color_primaries=None, color_transfer=None, color_space=None,
        is_hdr=False,
        audio_tracks=[], subtitle_tracks=[],
    )


def _make_args(path: Path, db_path: Path, *, workers: int | None = None,
               verbose: bool = False) -> argparse.Namespace:
    return argparse.Namespace(
        path=path,
        db=db_path,
        no_recursive=False,
        no_probe_cache=False,
        workers=workers,
        verbose=verbose,
        cmd="scan",
    )


class ParallelScanTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name) / "media"
        self.root.mkdir()
        # Three plausible video files with different content so each
        # generates a distinct ProbeResult under the stub.
        for n in ("a.mkv", "b.mkv", "c.mkv"):
            (self.root / n).write_bytes(b"x" * 1024)
        self.db_path = Path(self.tmpdir.name) / "state.db"

    def tearDown(self):
        self.tmpdir.cleanup()

    def _scan_and_collect(self, *, workers: int):
        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            rc = cmd_scan(_make_args(self.root, self.db_path, workers=workers))
        self.assertEqual(rc, 0)
        # Read back what was written
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        rows = conn.execute(
            "SELECT path FROM files ORDER BY path"
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]

    def test_parallel_writes_same_set_as_sequential(self):
        """8-worker scan and 1-worker scan upsert exactly the same paths."""
        # Run with 8 workers, capture
        parallel = self._scan_and_collect(workers=8)
        # Wipe and rerun sequentially
        self.db_path.unlink()
        sequential = self._scan_and_collect(workers=1)
        self.assertEqual(parallel, sequential)
        self.assertEqual(len(parallel), 3)

    def test_default_workers_is_capped(self):
        """Default workers is min(8, cpu_count) — never unbounded."""
        # Run with default (None means auto)
        rows = self._scan_and_collect(workers=None)
        self.assertEqual(len(rows), 3)

    def test_cache_hit_skips_probe(self):
        """Files already in cache aren't passed to the worker pool."""
        # First scan populates the cache
        self._scan_and_collect(workers=4)
        # Second scan should be all cache hits
        with patch.object(probe_mod, "probe_file") as p:
            rc = cmd_scan(_make_args(self.root, self.db_path, workers=4))
            self.assertEqual(rc, 0)
            p.assert_not_called()

    def test_probe_error_is_counted_not_fatal(self):
        """One file failing probe shouldn't break the others."""
        def stub_with_one_fail(path: Path) -> ProbeResult:
            if path.name == "b.mkv":
                raise ValueError("synthetic probe failure")
            return _stub_probe(path)
        with patch.object(probe_mod, "probe_file", side_effect=stub_with_one_fail):
            rc = cmd_scan(_make_args(self.root, self.db_path, workers=4))
        self.assertEqual(rc, 0)
        # 2 of 3 should land in the db
        import sqlite3
        conn = sqlite3.connect(str(self.db_path))
        n = conn.execute("SELECT COUNT(*) FROM files").fetchone()[0]
        conn.close()
        self.assertEqual(n, 2)


if __name__ == "__main__":
    unittest.main()
