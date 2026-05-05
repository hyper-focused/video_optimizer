"""Probe-time size gate: small files (trailers, extras, samples) are
recorded as skipped at scan time and never run through ffprobe or rule
evaluation. The gate's threshold is configurable via --min-size, with
re-evaluation on every scan so that file growth or threshold changes
move files between the probe cache and the skip cache deterministically.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from optimizer import probe as probe_mod
from optimizer.cli import _format_size, _parse_size, cmd_scan
from optimizer.models import ProbeResult


def _stub_probe(path: Path) -> ProbeResult:
    return ProbeResult(
        path=str(path),
        size=path.stat().st_size,
        mtime=path.stat().st_mtime,
        duration_seconds=60.0,
        container="matroska",
        format_name="matroska,webm",
        video_codec="hevc",
        width=1920, height=1080, frame_rate=23.976,
        pixel_format="yuv420p",
        bit_depth=8,
        video_bitrate=10_000_000,
        color_primaries=None, color_transfer=None, color_space=None,
        is_hdr=False,
        audio_tracks=[], subtitle_tracks=[],
    )


def _scan_args(path: Path, db_path: Path, *, min_size: int) -> argparse.Namespace:
    return argparse.Namespace(
        path=path, db=db_path,
        no_recursive=False, no_probe_cache=False,
        workers=1, min_size=min_size,
        verbose=False, cmd="scan",
    )


class ParseSizeTests(unittest.TestCase):
    """_parse_size handles bytes / suffixes / disable sentinel."""

    def test_bare_bytes(self):
        self.assertEqual(_parse_size("1024"), 1024)

    def test_zero_disables(self):
        self.assertEqual(_parse_size("0"), 0)

    def test_kilobytes_binary(self):
        self.assertEqual(_parse_size("1K"), 1024)

    def test_megabytes_binary(self):
        self.assertEqual(_parse_size("500M"), 500 * 1024 ** 2)

    def test_gigabytes_binary(self):
        self.assertEqual(_parse_size("1G"), 1024 ** 3)

    def test_decimal_suffix(self):
        """`1.5G` should round to (1.5 * 1 GiB)."""
        self.assertEqual(_parse_size("1.5G"), int(1.5 * 1024 ** 3))

    def test_lowercase_suffix(self):
        self.assertEqual(_parse_size("2g"), 2 * 1024 ** 3)

    def test_invalid_suffix_raises(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            _parse_size("1X")

    def test_negative_raises(self):
        with self.assertRaises(argparse.ArgumentTypeError):
            _parse_size("-1")


class FormatSizeTests(unittest.TestCase):
    def test_zero(self):
        self.assertEqual(_format_size(0), "0")

    def test_gigabyte(self):
        self.assertEqual(_format_size(1024 ** 3), "1.0G")


class SizeGateScanTests(unittest.TestCase):
    """Scan-time gate behavior: skip below threshold, probe above, and
    re-evaluate on every scan so size/threshold changes are reflected."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name) / "media"
        self.root.mkdir()
        self.db_path = Path(self.tmpdir.name) / "state.db"
        # 'movie.mkv' = 4 KB, 'trailer.mkv' = 1 KB. With a 2 KB threshold
        # only 'movie' should be probed.
        self.movie = self.root / "movie.mkv"
        self.trailer = self.root / "trailer.mkv"
        self.movie.write_bytes(b"x" * 4096)
        self.trailer.write_bytes(b"x" * 1024)

    def tearDown(self):
        self.tmpdir.cleanup()

    def _files_count(self) -> int:
        with sqlite3.connect(str(self.db_path)) as c:
            return c.execute("SELECT COUNT(*) FROM files").fetchone()[0]

    def _skipped_count(self) -> int:
        with sqlite3.connect(str(self.db_path)) as c:
            return c.execute(
                "SELECT COUNT(*) FROM skipped_files").fetchone()[0]

    def _skipped_paths(self) -> set[str]:
        with sqlite3.connect(str(self.db_path)) as c:
            rows = c.execute("SELECT path FROM skipped_files").fetchall()
        return {r[0] for r in rows}

    def test_small_file_is_recorded_as_skipped_not_probed(self):
        """A file under the threshold lands in skipped_files, not files;
        ffprobe is never called for it."""
        with patch.object(probe_mod, "probe_file",
                          side_effect=_stub_probe) as p:
            cmd_scan(_scan_args(self.root, self.db_path, min_size=2048))
        self.assertEqual(self._files_count(), 1)
        self.assertEqual(self._skipped_count(), 1)
        # probe_file called once (movie), never for trailer
        called = {Path(c.args[0]).name for c in p.call_args_list}
        self.assertEqual(called, {"movie.mkv"})
        self.assertIn(str(self.trailer), self._skipped_paths())

    def test_zero_disables_gate(self):
        """--min-size 0 means probe everything."""
        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            cmd_scan(_scan_args(self.root, self.db_path, min_size=0))
        self.assertEqual(self._files_count(), 2)
        self.assertEqual(self._skipped_count(), 0)

    def test_file_growing_above_threshold_gets_probed_next_scan(self):
        """After a file grows past the threshold, the next scan should
        clear its skip row and probe it."""
        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            cmd_scan(_scan_args(self.root, self.db_path, min_size=2048))
        self.assertIn(str(self.trailer), self._skipped_paths())

        # Trailer grows past the threshold (e.g. someone replaced the
        # placeholder with the real file). Bump mtime explicitly so the
        # filesystem reflects the change.
        self.trailer.write_bytes(b"x" * 8192)
        os.utime(self.trailer, None)

        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            cmd_scan(_scan_args(self.root, self.db_path, min_size=2048))
        self.assertEqual(self._files_count(), 2)
        self.assertEqual(self._skipped_count(), 0)

    def test_threshold_lowered_admits_previously_skipped(self):
        """If the user lowers --min-size below a previously-skipped file's
        size, the next scan should probe the file."""
        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            cmd_scan(_scan_args(self.root, self.db_path, min_size=2048))
        self.assertEqual(self._skipped_count(), 1)

        # Lower threshold below the trailer's actual size.
        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            cmd_scan(_scan_args(self.root, self.db_path, min_size=512))
        self.assertEqual(self._files_count(), 2)
        self.assertEqual(self._skipped_count(), 0)

    def test_threshold_raised_evicts_previously_probed(self):
        """If the user raises --min-size above a probed file's size, the
        next scan should evict the probe row and record it as skipped.
        Otherwise plan would still consider the now-undersize file."""
        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            cmd_scan(_scan_args(self.root, self.db_path, min_size=0))
        self.assertEqual(self._files_count(), 2)

        with patch.object(probe_mod, "probe_file", side_effect=_stub_probe):
            cmd_scan(_scan_args(self.root, self.db_path, min_size=2048))
        # Movie (4KB) stays; trailer (1KB) moves to skipped.
        self.assertEqual(self._files_count(), 1)
        self.assertEqual(self._skipped_count(), 1)
        self.assertIn(str(self.trailer), self._skipped_paths())


if __name__ == "__main__":
    unittest.main()
