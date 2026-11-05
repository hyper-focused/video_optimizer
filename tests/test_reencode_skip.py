"""Plan-time gate: files already tagged REENCODE (this tool's output)
are skipped permanently to prevent re-encode loops in replace runs
where originals were preserved (recycle / backup) — without the gate,
the next decide would surface its own outputs as candidates.

The gate is overridable via ``allow_reencoded=True`` for the rare case
where a re-encode of an already-tagged file is genuinely intended.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from optimizer.cli import _is_reencoded_filename
from optimizer.decide import decide
from optimizer.models import ProbeResult


def _probe_for(path: Path, *, dv_profile: int | None = None) -> ProbeResult:
    return ProbeResult(
        path=str(path), size=path.stat().st_size, mtime=path.stat().st_mtime,
        duration_seconds=3600.0, container="matroska", format_name="matroska,webm",
        video_codec="hevc", width=3840, height=2160, frame_rate=23.976,
        pixel_format="yuv420p10le", bit_depth=10,
        video_bitrate=40_000_000,
        color_primaries="bt2020", color_transfer="smpte2084",
        color_space="bt2020nc", is_hdr=True,
        audio_tracks=[], subtitle_tracks=[],
        dv_profile=dv_profile,
    )


class IsReencodedFilenameTests(unittest.TestCase):
    def test_canonical_marker(self):
        self.assertTrue(_is_reencoded_filename(
            "/movies/Wonder Woman/Wonder.Woman.2017.AV1.REENCODE.mkv"))

    def test_double_marker_still_matches(self):
        """Defensive: even our own bug-output ('.REENCODE.REENCODE.') counts."""
        self.assertTrue(_is_reencoded_filename(
            "/m/Van Wilder/00278.AV1.REENCODE.REENCODE.mkv"))

    def test_lowercase(self):
        self.assertTrue(_is_reencoded_filename(
            "/m/movie.av1.reencode.mkv"))

    def test_word_boundary_required(self):
        """A title like 'PREENCODER.mkv' should NOT trip the gate."""
        self.assertFalse(_is_reencoded_filename(
            "/m/Some.PREENCODER.Movie.mkv"))

    def test_pristine_source(self):
        self.assertFalse(_is_reencoded_filename(
            "/movies/Heat (1995)/Heat.1995.Remux-2160p.HEVC.HDR10.mkv"))


class PlanGateReencodeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.reenc = self.root / "old.AV1.REENCODE.mkv"
        self.reenc.write_bytes(b"x" * 4096)
        self.fresh = self.root / "Heat.1995.HEVC.mkv"
        self.fresh.write_bytes(b"x" * 4096)

    def tearDown(self):
        self.tmp.cleanup()

    def test_gate_skips_reencoded_by_default(self):
        d = decide(_probe_for(self.reenc), stall_count=0)
        self.assertEqual(d.kind, "skip")
        self.assertEqual(d.reason, "reencoded")
        self.assertIsNone(d.candidate)

    def test_gate_admits_reencoded_with_override(self):
        d = decide(_probe_for(self.reenc), stall_count=0,
                   allow_reencoded=True)
        self.assertEqual(d.kind, "encode")
        self.assertEqual(d.tier, "UHD")
        self.assertIsNotNone(d.candidate)

    def test_gate_admits_pristine_source(self):
        d = decide(_probe_for(self.fresh), stall_count=0)
        self.assertEqual(d.kind, "encode")
        self.assertEqual(d.tier, "UHD")

    def test_gate_dv_takes_precedence_over_reencoded(self):
        """A DV-tagged file that also has REENCODE in the name should
        report 'dv' (the higher-priority skip), not 'reencoded'. Order
        matters because the operator-facing remediation is different.

        Profile 5 always skips — no clean HDR10 fallback. Profiles 7
        and 8 do not: dv_strategy returns "p8_strip" without consulting
        dovi_tool.
        """
        d = decide(_probe_for(self.reenc, dv_profile=5), stall_count=0)
        self.assertEqual(d.kind, "skip")
        self.assertEqual(d.reason, "dv")

    def test_profile_7_and_8_are_not_dv_skips(self):
        for profile in (7, 8):
            d = decide(_probe_for(self.fresh, dv_profile=profile),
                       stall_count=0)
            self.assertEqual(d.kind, "encode", profile)
            self.assertEqual(d.tier, "UHD", profile)
        # A REENCODE name still skips, but as reencoded, not dv.
        d7 = decide(_probe_for(self.reenc, dv_profile=7), stall_count=0)
        self.assertEqual(d7.reason, "reencoded")


if __name__ == "__main__":
    unittest.main()
