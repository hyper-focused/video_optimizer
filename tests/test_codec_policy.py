"""Codec policy: SD / HD / UHD non-AV1 rules + AV1 plan-gate.

The goal of this policy:
  - Any non-AV1 source at SD / HD / UHD heights is a re-encode candidate.
    The three tiers differ only in their height band and downstream encoding
    settings (CQ, encoder preset).
  - AV1 source → plan-time skip (already at the target codec); --allow-av1 to override.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from optimizer import encoder
from optimizer.cli import _plan_probe_gate, _should_apply_denoise
from optimizer.db import Database
from optimizer.rules import (
    HdNonAv1Rule,
    SdNonAv1Rule,
    UhdNonAv1Rule,
)
from tests._fixtures import make_probe

# --------------------------------------------------------------------------- #
# HdNonAv1Rule (any non-AV1 at HD)
# --------------------------------------------------------------------------- #


class HdNonAv1RuleTests(unittest.TestCase):
    """Anything-but-AV1 in [720, 1440) fires; AV1 + out-of-band heights miss."""

    def setUp(self):
        self.rule = HdNonAv1Rule()

    def test_h264_1080p_fires(self):
        v = self.rule.evaluate(make_probe(codec="h264", height=1080))
        self.assertTrue(v.fired)
        self.assertGreater(v.projected_savings_mb, 0)

    def test_h264_720p_fires(self):
        v = self.rule.evaluate(make_probe(codec="h264", height=720))
        self.assertTrue(v.fired)

    def test_hevc_at_hd_fires(self):
        # HEVC at HD is now in scope (default policy: encode any non-AV1
        # at any tier). Was previously opt-in via --allow-hd-hevc.
        v = self.rule.evaluate(make_probe(codec="hevc", height=1080))
        self.assertTrue(v.fired)

    def test_h264_below_hd_does_not_fire(self):
        # 480p is SD; SdNonAv1 covers it.
        v = self.rule.evaluate(make_probe(codec="h264", height=480))
        self.assertFalse(v.fired)

    def test_h264_at_uhd_does_not_fire(self):
        # UhdNonAv1Rule covers UHD; this rule must not double-fire.
        v = self.rule.evaluate(make_probe(codec="h264", height=2160))
        self.assertFalse(v.fired)

    def test_av1_at_hd_does_not_fire(self):
        v = self.rule.evaluate(make_probe(codec="av1", height=1080))
        self.assertFalse(v.fired)


# --------------------------------------------------------------------------- #
# UhdNonAv1Rule (any non-AV1 at UHD)
# --------------------------------------------------------------------------- #


class UhdNonAv1RuleTests(unittest.TestCase):
    """Anything-but-AV1 at height >= 1440 fires."""

    def setUp(self):
        self.rule = UhdNonAv1Rule()

    def test_hevc_at_2160p_fires(self):
        v = self.rule.evaluate(make_probe(codec="hevc", height=2160))
        self.assertTrue(v.fired)
        self.assertGreater(v.projected_savings_mb, 0)

    def test_h264_at_2160p_fires(self):
        v = self.rule.evaluate(make_probe(codec="h264", height=2160))
        self.assertTrue(v.fired)

    def test_vp9_at_2160p_fires(self):
        v = self.rule.evaluate(make_probe(codec="vp9", height=2160))
        self.assertTrue(v.fired)

    def test_av1_at_2160p_does_not_fire(self):
        # Defensive — plan-gate should skip av1 sources before this rule
        # is reached, but if it ever runs, it must still not fire.
        v = self.rule.evaluate(make_probe(codec="av1", height=2160))
        self.assertFalse(v.fired)

    def test_hevc_at_1080p_does_not_fire(self):
        v = self.rule.evaluate(make_probe(codec="hevc", height=1080))
        self.assertFalse(v.fired)

    def test_hevc_at_1440p_fires(self):
        # 1440p sits at the UHD boundary — the explicit UHD rule fires
        # there because the AV1-at-UHD savings deltas are still large.
        v = self.rule.evaluate(make_probe(codec="hevc", height=1440))
        self.assertTrue(v.fired)


# --------------------------------------------------------------------------- #
# SdNonAv1Rule (any non-AV1 at SD)
# --------------------------------------------------------------------------- #


class SdNonAv1RuleTests(unittest.TestCase):
    """SD non-AV1 fires; AV1 SD never fires; HD/UHD heights never fire."""

    def setUp(self):
        self.rule = SdNonAv1Rule()

    def test_h264_at_480p_fires(self):
        v = self.rule.evaluate(make_probe(codec="h264", height=480))
        self.assertTrue(v.fired)
        self.assertGreater(v.projected_savings_mb, 0)

    def test_mpeg2_at_576p_fires(self):
        v = self.rule.evaluate(make_probe(codec="mpeg2video", height=576))
        self.assertTrue(v.fired)

    def test_av1_at_480p_does_not_fire(self):
        v = self.rule.evaluate(make_probe(codec="av1", height=480))
        self.assertFalse(v.fired)

    def test_h264_at_720p_does_not_fire(self):
        # 720p is the HD floor; SdNonAv1 must stop short of it.
        v = self.rule.evaluate(make_probe(codec="h264", height=720))
        self.assertFalse(v.fired)

    def test_h264_at_1080p_does_not_fire(self):
        v = self.rule.evaluate(make_probe(codec="h264", height=1080))
        self.assertFalse(v.fired)


# --------------------------------------------------------------------------- #
# Plan-gate AV1 + extras skips
# --------------------------------------------------------------------------- #


class _GateTestBase(unittest.TestCase):
    """Build a real on-disk Database so _plan_probe_gate's stall query runs."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        # We need a real path that exists on disk so the gate's
        # Path(pr.path).exists() short-circuit doesn't fire.
        self.fake_file = Path(self.tmp.name) / "x.mkv"
        self.fake_file.write_bytes(b"x")
        self.db_path = Path(self.tmp.name) / "test.db"

    def _probe(self, **kwargs) -> "ProbeResult":  # noqa: F821
        kwargs.setdefault("codec", "hevc")
        kwargs.setdefault("height", 1080)
        p = make_probe(**kwargs)
        p.path = str(self.fake_file)
        return p


class PlanGateAv1Tests(_GateTestBase):
    def test_av1_source_skipped_by_default(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(codec="av1")),
                "av1_source",
            )

    def test_av1_admitted_with_override(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(codec="av1"),
                                 allow_av1=True),
                "ok",
            )

    def test_non_av1_unaffected(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(codec="h264")),
                "ok",
            )


class PlanGateLowBitrateTests(_GateTestBase):
    """Sources whose video bitrate is below the AV1 target for their
    resolution bucket are skipped — re-encoding can't yield meaningful
    savings and risks perceptual regression on already-compressed sources.
    """

    def test_1080p_3mbps_skipped(self):
        # 1080p AV1 target is 5 Mbps; 3 Mbps is below.
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="h264", height=1080,
                                    video_bitrate=3_000_000),
                ),
                "low_bitrate",
            )

    def test_1080p_3mbps_admitted_with_override(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="h264", height=1080,
                                    video_bitrate=3_000_000),
                    allow_low_bitrate=True,
                ),
                "ok",
            )

    def test_2160p_below_target_skipped(self):
        # 2160p target is 16 Mbps; 12 Mbps falls below.
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="hevc", height=2160,
                                    video_bitrate=12_000_000),
                ),
                "low_bitrate",
            )

    def test_at_target_bitrate_admitted(self):
        # Sources at-or-above the AV1 target are admitted.
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="hevc", height=1080,
                                    video_bitrate=5_500_000),
                ),
                "ok",
            )

    def test_unknown_bitrate_admitted(self):
        # Source bitrate == 0 means we can't decide; admit (the rule
        # engine will still gate on its own thresholds).
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="h264", height=1080,
                                    video_bitrate=0),
                ),
                "ok",
            )


class PlanGateSkipCodecsTests(_GateTestBase):
    """`--skip-codecs hevc,h264,…` filters by source codec at plan time."""

    def test_hevc_skipped_when_in_skip_set(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="hevc", height=1080,
                                    video_bitrate=20_000_000),
                    skip_codecs=frozenset({"hevc"}),
                ),
                "skipped_codec",
            )

    def test_h264_admitted_when_only_hevc_skipped(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="h264", height=1080,
                                    video_bitrate=20_000_000),
                    skip_codecs=frozenset({"hevc"}),
                ),
                "ok",
            )

    def test_av1_skip_still_takes_precedence(self):
        # AV1 sources hit the "av1_source" gate before the codec-skip
        # check, even when "av1" appears in the skip set.
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="av1", height=1080,
                                    video_bitrate=20_000_000),
                    skip_codecs=frozenset({"av1"}),
                ),
                "av1_source",
            )

    def test_empty_skip_set_is_a_noop(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(
                    db, self._probe(codec="hevc", height=1080,
                                    video_bitrate=20_000_000),
                    skip_codecs=frozenset(),
                ),
                "ok",
            )


class PlanGateSdTests(_GateTestBase):
    """SD content is now first-class: admitted at the plan gate, processed
    by the SdNonAv1Rule (or the SD preset, depending on the entry point)."""

    def test_sd_admitted_to_rules(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(height=480)),
                "ok",
            )

    def test_av1_skip_takes_precedence_at_sd(self):
        # An SD AV1 source should still report "av1_source" (already at
        # the target codec, regardless of resolution).
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(codec="av1", height=480)),
                "av1_source",
            )


class PlanGateExistingOutputTests(_GateTestBase):
    """A source with an AV1 REENCODE sibling is skipped (keep-mode prior run).

    This is the load-bearing test for the bug that re-encoded 47 Ronin
    overnight: source filename had no REENCODE marker, but the AV1
    output sat next to it, and the plan-gate blindly re-queued the source.
    """

    def _setup_source_and_sibling(self, src_name: str,
                                  sibling_name: str | None) -> Path:
        src = self.fake_file.parent / src_name
        src.write_bytes(b"x" * 100)
        if sibling_name is not None:
            sibling = self.fake_file.parent / sibling_name
            sibling.write_bytes(b"y" * 100)
        p = make_probe(codec="hevc", height=2160)
        p.path = str(src)
        return p

    def test_existing_av1_reencode_sibling_skipped(self):
        # The naming pipeline strips HEVC tokens and inserts AV1.REENCODE.
        # Source: Foo.HEVC.mkv → expected sibling: Foo.AV1.REENCODE.mkv.
        pr = self._setup_source_and_sibling(
            "Foo.HEVC.mkv", "Foo.AV1.REENCODE.mkv",
        )
        with Database(self.db_path) as db:
            self.assertEqual(_plan_probe_gate(db, pr), "existing_output")

    def test_existing_output_admitted_with_allow_reencoded(self):
        pr = self._setup_source_and_sibling(
            "Foo.HEVC.mkv", "Foo.AV1.REENCODE.mkv",
        )
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, pr, allow_reencoded=True),
                "ok",
            )

    def test_no_sibling_passes_to_rules(self):
        pr = self._setup_source_and_sibling("Foo.HEVC.mkv", sibling_name=None)
        with Database(self.db_path) as db:
            self.assertEqual(_plan_probe_gate(db, pr), "ok")

    def test_unrelated_av1_file_in_dir_does_not_match(self):
        # Some other AV1.REENCODE.mkv in the directory shouldn't trip
        # the gate for a different source — the naming pipeline maps
        # 1:1 by stem, not "any *.AV1.REENCODE.mkv nearby".
        pr = self._setup_source_and_sibling(
            "Foo.HEVC.mkv", "Bar.AV1.REENCODE.mkv",
        )
        with Database(self.db_path) as db:
            self.assertEqual(_plan_probe_gate(db, pr), "ok")


class PlanGateExtrasTests(_GateTestBase):
    """Extras-suffixed filenames (e.g. `Movie-trailer.mp4`) are skipped."""

    def _probe_extras(self, name="Movie-trailer.mkv", **kwargs):
        # Override the path to a file with an extras suffix.
        extras_path = self.fake_file.parent / name
        extras_path.write_bytes(b"x")
        kwargs.setdefault("codec", "h264")
        kwargs.setdefault("height", 1080)
        p = make_probe(**kwargs)
        p.path = str(extras_path)
        return p

    def test_trailer_suffix_skipped_by_default(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe_extras("Movie-trailer.mkv")),
                "extras",
            )

    def test_extras_admitted_with_override(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe_extras("Movie-trailer.mkv"),
                                 allow_extras=True),
                "ok",
            )

    def test_non_extras_filename_unaffected(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe_extras("Movie.mkv")),
                "ok",
            )


# --------------------------------------------------------------------------- #
# Denoise decision (low-bitrate h.264 + SD)
# --------------------------------------------------------------------------- #


class DenoiseDecisionTests(unittest.TestCase):
    """`_should_apply_denoise` triggers on SD or low-bitrate HD h.264."""

    def test_h264_1080p_low_bitrate_denoises(self):
        # 1080p AV1 target is 5 Mbps; a 3 Mbps source is below that.
        pr = make_probe(codec="h264", height=1080,
                        video_bitrate=3_000_000)
        self.assertTrue(_should_apply_denoise(pr))

    def test_h264_1080p_high_bitrate_does_not_denoise(self):
        pr = make_probe(codec="h264", height=1080,
                        video_bitrate=12_000_000)
        self.assertFalse(_should_apply_denoise(pr))

    def test_h264_at_av1_target_does_not_denoise(self):
        # Boundary: at exactly the target bitrate the source has just
        # enough headroom for a clean re-encode.
        pr = make_probe(codec="h264", height=1080,
                        video_bitrate=5_000_000)
        self.assertFalse(_should_apply_denoise(pr))

    def test_hevc_low_bitrate_does_not_denoise(self):
        # Denoise is h.264-specific in the HD band.
        pr = make_probe(codec="hevc", height=1080,
                        video_bitrate=2_000_000)
        self.assertFalse(_should_apply_denoise(pr))

    def test_unknown_bitrate_does_not_denoise(self):
        pr = make_probe(codec="h264", height=1080, video_bitrate=0)
        self.assertFalse(_should_apply_denoise(pr))

    def test_sd_always_denoises(self):
        # SD content benefits universally from cleanup.
        pr = make_probe(codec="hevc", height=480, video_bitrate=1_500_000)
        self.assertTrue(_should_apply_denoise(pr))

    def test_h264_uhd_does_not_denoise(self):
        # UHD content has enough headroom; UhdNonAv1Rule covers it
        # without preprocessing.
        pr = make_probe(codec="h264", height=2160,
                        video_bitrate=15_000_000)
        self.assertFalse(_should_apply_denoise(pr))


# --------------------------------------------------------------------------- #
# Encoder integration (build_encode_command honours denoise=True)
# --------------------------------------------------------------------------- #


class EncoderDenoiseIntegrationTests(unittest.TestCase):
    """build_encode_command inserts hqdn3d into the -vf chain on denoise=True."""

    def _build(self, **kwargs):
        pr = make_probe(codec="h264", height=1080)
        from pathlib import Path
        return encoder.build_encode_command(
            pr, Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"],
            target_container="mkv",
            **kwargs,
        )

    def test_denoise_false_omits_hqdn3d(self):
        cmd = self._build(denoise=False)
        joined = " ".join(cmd)
        self.assertNotIn("hqdn3d", joined)

    def test_denoise_true_inserts_hqdn3d(self):
        cmd = self._build(denoise=True)
        # hqdn3d=... must appear inside the -vf argument.
        self.assertIn("-vf", cmd)
        vf_idx = cmd.index("-vf")
        self.assertIn("hqdn3d", cmd[vf_idx + 1])

    def test_denoise_true_excludes_qsv_hwaccel(self):
        # When denoise is on, the caller is supposed to pass
        # hw_decode=False; verify the encoder honours that and doesn't
        # silently insert -hwaccel qsv.
        cmd = self._build(denoise=True, hw_decode=False)
        self.assertNotIn("qsv", " ".join(cmd))


if __name__ == "__main__":
    unittest.main()
