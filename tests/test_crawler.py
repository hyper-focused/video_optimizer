"""Crawler directory-filtering tests.

Pinned by the v0.5.6 fix: a real-world full library scan put 11,168
rows of `.@__thumb/` Synology thumbnail garbage into the cache (85%
junk). The thumbnails masquerade as video files (`.mkv` extension,
JPEG content inside, ffprobe surfaces as 1-frame mjpeg).

These tests verify the directory-level skip-list catches the NAS /
OS system directories before they enter the crawl.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from optimizer.crawler import _SKIP_DIRS, _is_skipped_dir, crawl


class SkipListTests(unittest.TestCase):
    def test_synology_thumb_dir_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/some/path/.@__thumb")))

    def test_qnap_recycle_dir_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/Movies/@Recycle")))

    def test_synology_recycle_dir_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/share/#recycle")))

    def test_apple_metadata_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/.AppleDouble")))

    def test_normal_directory_not_skipped(self):
        self.assertFalse(_is_skipped_dir(Path("/Movies")))
        self.assertFalse(_is_skipped_dir(Path("/TV/Sons of Anarchy/Season 1")))

    def test_skip_list_membership(self):
        """Sanity-check the well-known names are present."""
        for name in (".@__thumb", "@Recycle", "@Recently-Snapshot",
                     "#recycle", ".AppleDouble"):
            self.assertIn(name, _SKIP_DIRS)


class CrawlIntegrationTests(unittest.TestCase):
    def test_thumb_dir_contents_never_yielded(self):
        """End-to-end: a real `.@__thumb/foo.mkv` file under a normal tree
        must not appear in crawl() output, even though it has a video
        extension and is non-empty."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            real = root / "Movies" / "Real Movie (2020).mkv"
            real.parent.mkdir(parents=True)
            real.write_bytes(b"x" * 100)

            thumb = root / "Movies" / ".@__thumb" / "Real Movie (2020).mkv"
            thumb.parent.mkdir(parents=True)
            thumb.write_bytes(b"x" * 100)

            recycled = root / "@Recycle" / "Old Movie (2010).mkv"
            recycled.parent.mkdir(parents=True)
            recycled.write_bytes(b"x" * 100)

            yielded = sorted(p.name for p in crawl(root))
            self.assertEqual(yielded, ["Real Movie (2020).mkv"])


if __name__ == "__main__":
    unittest.main()
