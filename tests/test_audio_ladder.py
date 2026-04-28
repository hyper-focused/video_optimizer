"""Audio ladder shape regression tests.

The v0.5.0 audio output is a deterministic 3-stream ladder:
  s0 = highest-quality eligible track (passthrough)
  s1 = native 5.1 if present, else Opus 5.1 synth from s0
  s2 = native 2.0 if present, else AAC 2.0 synth from s0

These tests exercise the cases that have actually mattered in real
encodes: parallel-lossless collapse on UHD remuxes (TrueHD wins,
DTS-HD MA dropped), commentary filtering, lossy-only sources, stereo-
only edge cases.
"""

from __future__ import annotations

import unittest

from optimizer.encoder import _build_audio_ladder, _expand_langs
from tests._fixtures import aud, make_probe


class AudioLadderTests(unittest.TestCase):
    LANGS = _expand_langs({"en", "und"})

    def assert_ladder(self, audio, expected):
        """Assert (kind, src_idx) tuples in output order match expected."""
        probe = make_probe(audio=audio)
        ladder = _build_audio_ladder(probe, self.LANGS)
        actual = [(kind, src_idx) for kind, src_idx, _ in ladder]
        self.assertEqual(actual, expected,
                         f"\n  expected: {expected}\n  actual:   {actual}")

    def test_endgame_remux_collapses_parallel_lossless(self):
        """TrueHD ranks higher than DTS-HD MA; DTS dropped, native 5.1 + 2.0 used."""
        self.assert_ladder(
            audio=[
                aud(0, "truehd", "eng", 8, title="TrueHD 7.1 Atmos", default=True),
                aud(1, "dts",    "eng", 8, title="DTS-HD MA 7.1"),
                aud(2, "ac3",    "eng", 6, title="DD 5.1"),
                aud(3, "ac3",    "eng", 2, title="DD 2.0"),
                aud(4, "ac3",    "fre", 6, title="DD 5.1 fr"),
                aud(5, "eac3",   "spa", 8, title="DDP 7.1 es"),
            ],
            expected=[("copy", 0), ("copy", 2), ("copy", 3)],
        )

    def test_dts_hd_ma_5_1_only_synthesizes_both_compats(self):
        """Single lossless surround source → DTS + Opus 5.1 + AAC 2.0 synth."""
        self.assert_ladder(
            audio=[aud(0, "dts", "eng", 6, title="DTS-HD MA 5.1", default=True)],
            expected=[("copy", 0), ("opus51", 0), ("aac20", 0)],
        )

    def test_commentary_track_dropped_even_with_matching_language(self):
        """Title contains 'commentary' (any case) → excluded."""
        self.assert_ladder(
            audio=[
                aud(0, "dts", "eng", 6, title="DTS-HD MA 5.1", default=True),
                aud(1, "ac3", "eng", 2, title="Commentary by Director"),
            ],
            expected=[("copy", 0), ("opus51", 0), ("aac20", 0)],
        )

    def test_lossy_5_1_plus_lossy_2_0_uses_natives(self):
        """AC3 5.1 + AC3 2.0: passthrough both, plus Opus 5.1 synth from s0."""
        self.assert_ladder(
            audio=[
                aud(0, "ac3", "eng", 6, title="DD 5.1", default=True),
                aud(1, "ac3", "eng", 2, title="DD 2.0"),
            ],
            expected=[("copy", 0), ("opus51", 0), ("copy", 1)],
        )

    def test_lossy_5_1_only_synthesizes_both_compats(self):
        self.assert_ladder(
            audio=[aud(0, "ac3", "eng", 6, title="DD 5.1", default=True)],
            expected=[("copy", 0), ("opus51", 0), ("aac20", 0)],
        )

    def test_lossy_stereo_only_yields_one_stream(self):
        """Stereo lossy source: no 5.1 to synthesize from; redundant AAC skipped."""
        self.assert_ladder(
            audio=[aud(0, "aac", "eng", 2, title="AAC 2.0", default=True)],
            expected=[("copy", 0)],
        )

    def test_lossless_stereo_keeps_aac_fallback(self):
        """FLAC stereo: no 5.1 source, but lossless → keep AAC 2.0 lossy fallback."""
        self.assert_ladder(
            audio=[aud(0, "flac", "eng", 2, title="FLAC stereo", default=True)],
            expected=[("copy", 0), ("aac20", 0)],
        )

    def test_truehd_outranks_dts_when_both_lossless(self):
        """Quality rank: truehd=100 > dts=80, regardless of declaration order."""
        # DTS-HD MA listed first should NOT win over a later TrueHD.
        self.assert_ladder(
            audio=[
                aud(0, "dts",    "eng", 8, title="DTS-HD MA 7.1", default=True),
                aud(1, "truehd", "eng", 8, title="TrueHD 7.1 Atmos"),
                aud(2, "ac3",    "eng", 6, title="DD 5.1"),
                aud(3, "ac3",    "eng", 2, title="DD 2.0"),
            ],
            expected=[("copy", 1), ("copy", 2), ("copy", 3)],
        )

    def test_all_default_true_does_not_admit_foreign_lang(self):
        """BEN.THE.MEN releases set default=True on EVERY audio track, which
        used to flood eligibility with foreign-lang tracks via the
        `or a.default` clause. The s1 5.1 slot would then prefer Italian
        DTS 5.1 (codec rank 80) over English EAC3 5.1 (rank 60).

        With v0.5.11's fix, eligibility is language-only and foreign
        tracks are skipped regardless of their default flag."""
        self.assert_ladder(
            audio=[
                aud(0, "dts",  "eng", 7, title="BTM DTS-HD Master", default=True),
                aud(1, "eac3", "eng", 6, title="BTM DDP5.1",        default=True),
                aud(2, "eac3", "spa", 6, title="BTM DDP5.1",        default=True),
                aud(3, "eac3", "cze", 6, title="BTM DDP5.1",        default=True),
                aud(4, "dts",  "ita", 6, title="BTM DTS 5.1",       default=True),
                aud(5, "eac3", "ita", 6, title="BTM DDP5.1",        default=True),
                aud(6, "eac3", "rus", 6, title="BTM DDP5.1",        default=True),
                aud(7, "ac3",  "eng", 2, title="BTM Commentary",    default=True),
            ],
            # s0 = English DTS-HD MA (track 0). s1 = English EAC3 5.1
            # (track 1), NOT Italian DTS 5.1 (track 4). s2 = synth AAC 2.0
            # because no native English 2.0 (the 2-channel commentary at
            # track 7 is filtered by the commentary rule).
            expected=[("copy", 0), ("copy", 1), ("aac20", 0)],
        )

    def test_foreign_only_default_falls_through_to_safety_net(self):
        """Source where all-default-true tracks are foreign (e.g. anime with
        Japanese-only audio, no English at all): output isn't silent.

        The safety net at the bottom of _eligible_tracks keeps only the
        first non-commentary track (just enough to avoid silence). The
        ladder then synthesizes both compat tiers from it. The Japanese
        AC3 2.0 (track 1) is NOT used as the s2 passthrough because it
        isn't eligible — language filter rejected it. To get the foreign
        2.0 retained, the user would pass --keep-langs jpn,eng."""
        self.assert_ladder(
            audio=[
                aud(0, "flac", "jpn", 6, title="Japanese FLAC 5.1", default=True),
                aud(1, "ac3",  "jpn", 2, title="Japanese DD 2.0",   default=True),
            ],
            expected=[("copy", 0), ("opus51", 0), ("aac20", 0)],
        )


if __name__ == "__main__":
    unittest.main()
