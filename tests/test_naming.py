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

    def test_joined_codec_pair_drops_orphan_plus(self):
        """`HEVC+H.265` pairs collapse cleanly without leaving a stray `+`.

        Real-world example from the TRON: Ares (2025) WEB-DL release; left
        a `.+.` in the rewritten name before this was handled.
        """
        self.assertEqual(
            rewrite_codec_tokens(
                "TRON.Ares.2025.WEBDL-2160p.HEVC.EAC35.1.HEVC+H.265", "av1",
            ),
            "TRON.Ares.2025.WEBDL-2160p.EAC35.1.AV1",
        )

    def test_av1_target_substitutes_dv_hdr10plus_with_hdr10(self):
        """av1_qsv loses HDR10+ dynamic + DV RPU, but the static HDR10 base
        layer survives. Substitute the first DV/HDR10+ token with `HDR10`
        so the filename advertises what's actually in the container.
        """
        self.assertEqual(
            rewrite_codec_tokens(
                "TRON.Ares.2025.WEBDL-2160p.HEVC.DV.HDR10Plus.EAC35.1.HEVC+H.265",
                "av1",
            ),
            "TRON.Ares.2025.WEBDL-2160p.HDR10.EAC35.1.AV1",
        )

    def test_av1_target_preserves_static_hdr10_token(self):
        """Static HDR10 (mastering display + MaxCLL) IS preserved by av1_qsv."""
        self.assertEqual(
            rewrite_codec_tokens(
                "Movie.2020.Bluray-2160p.HDR10.HEVC", "av1",
            ),
            "Movie.2020.Bluray-2160p.HDR10.AV1",
        )

    def test_av1_target_substitutes_dolby_vision_with_hdr10(self):
        """`Dolby Vision` and `HDR10+` both substitute to HDR10."""
        self.assertEqual(
            rewrite_codec_tokens(
                "Movie.2024.Dolby.Vision.HDR10+.HEVC", "av1",
            ),
            "Movie.2024.HDR10.AV1",
        )

    def test_av1_target_dv_only_gains_hdr10(self):
        """DV Profile 7/8 sources have an HDR10 base layer; after RPU strip
        the file is plain HDR10 and must be labelled so."""
        self.assertEqual(
            rewrite_codec_tokens(
                "Movie.2024.Remux-2160p.DV.TrueHD.Atmos7.1.HEVC", "av1",
            ),
            "Movie.2024.Remux-2160p.HDR10.TrueHD.Atmos7.1.AV1",
        )

    def test_av1_target_hdr10plus_only_gains_hdr10(self):
        """HDR10+ source loses dynamic metadata but keeps static HDR10."""
        self.assertEqual(
            rewrite_codec_tokens(
                "Movie.2024.WEBDL-2160p.HDR10Plus.EAC3.HEVC", "av1",
            ),
            "Movie.2024.WEBDL-2160p.HDR10.EAC3.AV1",
        )

    def test_av1_target_does_not_duplicate_when_hdr10_already_present(self):
        """If the source filename already says `HDR10`, just strip the lost
        DV/HDR10+ tokens — don't insert a second HDR10."""
        self.assertEqual(
            rewrite_codec_tokens(
                "Movie.2024.Remux-2160p.DV.HDR10.TrueHD.HEVC", "av1",
            ),
            "Movie.2024.Remux-2160p.HDR10.TrueHD.AV1",
        )

    def test_av1_target_does_not_add_hdr10_to_sdr_sources(self):
        """SDR source (no DV / HDR / HDR10+ tokens) must not gain HDR10."""
        self.assertEqual(
            rewrite_codec_tokens("Movie.2010.Bluray-1080p.HEVC", "av1"),
            "Movie.2010.Bluray-1080p.AV1",
        )

    def test_dv_inside_other_words_not_stripped(self):
        """`DV` is short — make sure it doesn't eat `DVD` or similar."""
        self.assertEqual(
            rewrite_codec_tokens("Movie.2010.DVDRip.HEVC", "av1"),
            "Movie.2010.DVDRip.AV1",
        )

    def test_hevc_target_keeps_hdr10plus_and_dv(self):
        """Targeting hevc preserves DV/HDR10+ tokens (they can survive)."""
        self.assertEqual(
            rewrite_codec_tokens(
                "Movie.2024.DV.HDR10Plus.x264-GRP", "hevc",
            ),
            "Movie.2024.DV.HDR10Plus-GRP.HEVC",
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
