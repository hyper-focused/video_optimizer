"""Regression tests for `_qsv_args` argv shape.

Each test pins a specific bug class that has actually shipped to users
during the v0.4–v0.5 cycle:

  - v0.4.1: `extbrc + ICQ + maxrate` collapsed av1_qsv to a hybrid VBR
            mode producing ~300 kb/s video on 1080p sources at CQ 18.
            Test: maxrate / bufsize MUST NOT appear.

  - v0.4.1: `-pix_fmt p010le` with `-hwaccel qsv -hwaccel_output_format
            qsv` (10-bit source + HW decode) breaks the filter graph
            ("Impossible to convert between qsv and p010le"). Test:
            pix_fmt is pinned for SW decode only.

  - v0.5.4: `-global_quality` without `:v` leaks AV_CODEC_FLAG_QSCALE
            onto every encoder including libopus, which rejects with
            "Quality-based encoding not supported." Test: scoped form.
"""

from __future__ import annotations

import unittest

from optimizer.encoder import _qsv_args


class QsvArgsTests(unittest.TestCase):
    def _hd_args(self, **overrides):
        kwargs = dict(is_uhd=False, bit_depth=8, hw_decode=False)
        kwargs.update(overrides)
        return _qsv_args("av1_qsv", 22, **kwargs)

    def _uhd_args(self, **overrides):
        kwargs = dict(is_uhd=True, bit_depth=10, hw_decode=False)
        kwargs.update(overrides)
        return _qsv_args("av1_qsv", 15, **kwargs)

    def test_global_quality_scoped_to_video_stream(self):
        """Bare -global_quality leaks qscale onto libopus and breaks the encode."""
        args = self._hd_args()
        self.assertIn("-global_quality:v", args)
        # The bare form (without :v) must not appear:
        self.assertNotIn("-global_quality", args)

    def test_no_maxrate_or_bufsize_on_av1_qsv_icq(self):
        """`extbrc + ICQ + maxrate` collapses the encoder. Pure ICQ only."""
        for args in (self._hd_args(), self._uhd_args()):
            self.assertNotIn("-maxrate", args)
            self.assertNotIn("-bufsize", args)

    def test_hd_tier_lookahead_and_gop(self):
        args = self._hd_args()
        self.assertEqual(args[args.index("-look_ahead_depth") + 1], "60")
        self.assertEqual(args[args.index("-g") + 1], "120")

    def test_uhd_tier_lookahead_and_gop(self):
        args = self._uhd_args()
        self.assertEqual(args[args.index("-look_ahead_depth") + 1], "100")
        self.assertEqual(args[args.index("-g") + 1], "240")

    def test_pix_fmt_pinned_only_for_sw_decode_at_10bit(self):
        """SW decode + 10-bit source: pin p010le. HW decode + 10-bit: must NOT pin."""
        sw_10 = self._hd_args(bit_depth=10, hw_decode=False)
        self.assertIn("-pix_fmt", sw_10)
        self.assertEqual(sw_10[sw_10.index("-pix_fmt") + 1], "p010le")

        hw_10 = self._hd_args(bit_depth=10, hw_decode=True)
        self.assertNotIn("-pix_fmt", hw_10)

    def test_no_pix_fmt_pin_for_8bit_sources(self):
        """8-bit source: encoder default is fine in either decode path."""
        self.assertNotIn("-pix_fmt", self._hd_args(bit_depth=8, hw_decode=False))
        self.assertNotIn("-pix_fmt", self._hd_args(bit_depth=8, hw_decode=True))

    def test_av1_main_profile(self):
        """AV1 Main carries 8-bit and 10-bit; explicit profile prevents drift."""
        args = self._hd_args()
        self.assertIn("-profile:v", args)
        self.assertEqual(args[args.index("-profile:v") + 1], "main")

    def test_archive_flag_set_present(self):
        """The validated archive-tuned QSV flag set from the reference script."""
        args = self._hd_args()
        # extbrc=1, low_power=0, adaptive I/B, B-strategy, 7 B-frames, 5 refs
        self.assertEqual(args[args.index("-extbrc") + 1], "1")
        self.assertEqual(args[args.index("-low_power") + 1], "0")
        self.assertEqual(args[args.index("-adaptive_i") + 1], "1")
        self.assertEqual(args[args.index("-adaptive_b") + 1], "1")
        self.assertEqual(args[args.index("-b_strategy") + 1], "1")
        self.assertEqual(args[args.index("-bf") + 1], "7")
        self.assertEqual(args[args.index("-refs") + 1], "5")

    def test_hevc_qsv_main10_for_10bit(self):
        """hevc_qsv needs explicit Main10 profile for 10-bit (unlike av1)."""
        args = _qsv_args("hevc_qsv", 24, is_uhd=False, bit_depth=10, hw_decode=False)
        self.assertEqual(args[args.index("-profile:v") + 1], "main10")
        self.assertIn("-pix_fmt", args)

    def test_hevc_qsv_no_main10_for_8bit(self):
        """hevc_qsv at 8-bit: no main10 forced (encoder default Main is fine)."""
        args = _qsv_args("hevc_qsv", 24, is_uhd=False, bit_depth=8, hw_decode=False)
        self.assertNotIn("-profile:v", args)


class StreamMapTests(unittest.TestCase):
    """Output-stream mapping invariants."""

    def test_attachments_not_mapped_for_mkv(self):
        """A single source attachment with a missing mimetype tag crashes
        ffmpeg's matroska muxer with 'Could not write header' (observed
        on iNCEPTiON-grouped Indiana Jones 4 source). We accept losing
        embedded fonts to avoid the fail-the-whole-encode behavior."""
        from optimizer.encoder import build_stream_map_args
        from tests._fixtures import aud, make_probe

        probe = make_probe(audio=[aud(0, "dts", "eng", 6, default=True)])
        args = build_stream_map_args(probe, ["en", "und"], "mkv")
        # No `-map 0:t?` and no `-c:t copy` should appear:
        self.assertNotIn("0:t?", args)
        self.assertNotIn("-c:t", args)


if __name__ == "__main__":
    unittest.main()
