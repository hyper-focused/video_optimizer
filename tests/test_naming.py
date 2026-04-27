"""Filename rewrite tests covering the README's documented examples.

The README's "Example transforms" table is the user-facing contract for
`--rewrite-codec` / `--reencode-tag`. These tests pin each row of that
table so changes to `naming.py` don't silently break documented
behaviour.
"""

from __future__ import annotations

import unittest

from optimizer.naming import (
    append_token,
    looks_dotted,
    rewrite_codec_tokens,
    to_dotted,
)


class CodecRewriteTests(unittest.TestCase):
    """Each test mirrors a row from the README's example-transforms table."""

    def test_h264_release_with_year_and_release_group(self):
        self.assertEqual(
            rewrite_codec_tokens(
                "Inception (2010) 1080p BluRay H.264-RELEASEGRP", "av1"
            ),
            "Inception.(2010).1080p.BluRay-RELEASEGRP.AV1",
        )

    def test_hevc_x265_uhd_with_dual_tokens(self):
        """Strips both HEVC and x265 (they're aliases for the same codec)."""
        self.assertEqual(
            rewrite_codec_tokens(
                "Movie.Name.2010.2160p.HDR.HEVC.x265.10bit-GRP", "av1"
            ),
            "Movie.Name.2010.2160p.HDR.10bit-GRP.AV1",
        )

    def test_bracketed_codec_token(self):
        """[HEVC] gets stripped; brackets collapse cleanly."""
        self.assertEqual(
            rewrite_codec_tokens("Some Movie (2015) [HEVC]", "av1"),
            "Some.Movie.(2015).AV1",
        )

    def test_no_existing_codec_token_just_appends(self):
        """No foreign codec tokens to strip; AV1 is appended cleanly."""
        self.assertEqual(
            rewrite_codec_tokens("Movie.Name.2010", "av1"),
            "Movie.Name.2010.AV1",
        )

    def test_canonical_token_not_duplicated(self):
        """If AV1 is already in the stem, the rewrite is idempotent."""
        self.assertEqual(
            rewrite_codec_tokens("Movie.Name.2010.AV1", "av1"),
            "Movie.Name.2010.AV1",
        )

    def test_to_hevc_target_strips_h264_and_x264(self):
        """Targeting hevc strips H.264/x264/AVC tokens, inserts HEVC."""
        self.assertEqual(
            rewrite_codec_tokens("Movie.2010.1080p.x264-GRP", "hevc"),
            "Movie.2010.1080p-GRP.HEVC",
        )


class StyleDetectionTests(unittest.TestCase):
    def test_dotted_dominant(self):
        self.assertTrue(looks_dotted("Movie.Name.2010"))

    def test_spaces_dominant(self):
        self.assertFalse(looks_dotted("Movie Name 2010"))

    def test_empty(self):
        self.assertFalse(looks_dotted(""))

    def test_to_dotted_collapses_repeats(self):
        self.assertEqual(to_dotted("Movie  Name  2010"), "Movie.Name.2010")
        self.assertEqual(to_dotted("Movie__Name_2010"), "Movie.Name.2010")


class AppendTokenTests(unittest.TestCase):
    def test_inferred_dotted_separator(self):
        self.assertEqual(
            append_token("Movie.Name.2010.AV1", "REENCODE"),
            "Movie.Name.2010.AV1.REENCODE",
        )

    def test_inferred_space_separator(self):
        self.assertEqual(
            append_token("Movie Name 2010 AV1", "REENCODE"),
            "Movie Name 2010 AV1 REENCODE",
        )

    def test_force_dotted_overrides_inference(self):
        """Used when --rewrite-codec output should stay dotted regardless."""
        self.assertEqual(
            append_token("Movie Name 2010", "REENCODE", dotted=True),
            "Movie Name 2010.REENCODE",
        )

    def test_empty_token_is_noop(self):
        self.assertEqual(append_token("Movie.Name", ""), "Movie.Name")


if __name__ == "__main__":
    unittest.main()
