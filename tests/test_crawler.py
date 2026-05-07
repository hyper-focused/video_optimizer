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

from optimizer.crawler import (
    _EXTRAS_DIRS,
    _SKIP_DIRS,
    _is_extras_dir,
    _is_skipped_dir,
    crawl,
    is_extras_filename,
)


class SkipListTests(unittest.TestCase):
    def test_synology_thumb_dir_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/some/path/.@__thumb")))

    def test_qnap_recycle_dir_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/Movies/@Recycle")))

    def test_qnap_hidden_recycle_dir_skipped(self):
        # Newer QNAP firmware uses the dot-prefixed hidden form by
        # default. Found in the field after partial AV1 outputs and
        # orphan dv-prepped temp files moved to .@Recycle were getting
        # re-scanned and re-queued for transcoding.
        self.assertTrue(_is_skipped_dir(Path("/Movies/.@Recycle")))

    def test_qnap_hidden_recently_snapshot_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/Movies/.@Recently-Snapshot")))

    def test_synology_recycle_dir_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/share/#recycle")))

    def test_apple_metadata_skipped(self):
        self.assertTrue(_is_skipped_dir(Path("/.AppleDouble")))

    def test_normal_directory_not_skipped(self):
        self.assertFalse(_is_skipped_dir(Path("/Movies")))
        self.assertFalse(_is_skipped_dir(Path("/TV/Sons of Anarchy/Season 1")))

    def test_skip_list_membership(self):
        """Sanity-check the well-known names are present."""
        for name in (".@__thumb",
                     "@Recycle", ".@Recycle",
                     "@Recently-Snapshot", ".@Recently-Snapshot",
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


class ExtrasFilterTests(unittest.TestCase):
    """Plex-style extras directories and filename suffixes are filtered
    by default (`skip_extras=True`); --allow-extras turns the skip off."""

    def test_known_extras_dirs(self):
        for name in ("Trailers", "trailer", "Behind The Scenes",
                     "BehindTheScenes", "Featurettes", "Deleted Scenes",
                     "Interviews", "Shorts", "Bonus", "BTS", "Samples"):
            self.assertTrue(
                _is_extras_dir(Path(f"/Movies/X/{name}")),
                f"expected {name!r} to match the extras dir set",
            )

    def test_normal_dirs_not_extras(self):
        for name in ("Movies", "Season 1", "X (2020)"):
            self.assertFalse(
                _is_extras_dir(Path(f"/X/{name}")),
                f"{name!r} is a regular directory; should not match extras",
            )

    def test_extras_suffix_filenames(self):
        for name in ("Movie-trailer.mkv", "Movie-bts.mp4",
                     "Movie-deleted.mkv", "Movie-featurette.mp4",
                     "Movie-sample.mkv", "Movie-extra.mp4"):
            self.assertTrue(
                is_extras_filename(Path(name)),
                f"expected {name!r} to be classified as extras",
            )

    def test_normal_filenames_not_extras(self):
        # "trailer" without the dash prefix isn't an extras suffix.
        for name in ("Movie.mkv", "Trailer Park Boys.mkv",
                     "The Trailer (2020).mp4"):
            self.assertFalse(
                is_extras_filename(Path(name)),
                f"{name!r} should not be classified as extras",
            )

    def test_extras_dirs_skipped_during_walk(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            feature = root / "Movies" / "Real (2020).mkv"
            feature.parent.mkdir(parents=True)
            feature.write_bytes(b"x" * 100)

            trailer_dir = root / "Movies" / "Trailers" / "preview.mkv"
            trailer_dir.parent.mkdir(parents=True)
            trailer_dir.write_bytes(b"x" * 100)

            yielded = sorted(p.name for p in crawl(root))
            self.assertEqual(yielded, ["Real (2020).mkv"])

    def test_extras_filename_skipped_during_walk(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            feature = root / "Movies" / "Real (2020).mkv"
            trailer = root / "Movies" / "Real (2020)-trailer.mp4"
            feature.parent.mkdir(parents=True)
            feature.write_bytes(b"x" * 100)
            trailer.write_bytes(b"x" * 100)

            yielded = sorted(p.name for p in crawl(root))
            self.assertEqual(yielded, ["Real (2020).mkv"])

    def test_allow_extras_disables_skip(self):
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            feature = root / "Movies" / "Real (2020).mkv"
            trailer = root / "Movies" / "Real (2020)-trailer.mp4"
            feature.parent.mkdir(parents=True)
            feature.write_bytes(b"x" * 100)
            trailer.write_bytes(b"x" * 100)

            yielded = sorted(p.name for p in crawl(root, skip_extras=False))
            self.assertEqual(
                yielded,
                ["Real (2020)-trailer.mp4", "Real (2020).mkv"],
            )

    def test_extras_dir_set_is_lowercase_only(self):
        # The membership check lowercases the dir name; no UpperCase
        # entries should exist in the set or matching breaks.
        for name in _EXTRAS_DIRS:
            self.assertEqual(name, name.lower())


if __name__ == "__main__":
    unittest.main()
