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


# --------------------------------------------------------------------------- #
# Codec-aware HW decode override (_build_apply_command)
# --------------------------------------------------------------------------- #


class CodecAwareHwDecodeTests(unittest.TestCase):
    """`SLOW_CPU_DECODE_CODECS` is currently empty after the VC-1 hwaccel
    attempt wedged at frame 0 on Lethal Weapon 1987. These tests pin two
    things: (a) the override mechanism in `_build_apply_command` still
    works (so any future additions to the set will route correctly), and
    (b) the codecs we previously had in the set (VC-1, VP9) now stay on
    the preset's `hw_decode` default — CPU decode for HD.
    """

    def _build(self, codec: str, *, hw_decode_default: bool = False,
               denoise_codec: str | None = None,
               return_desc: bool = False,
               dv_pre_pass: bool = False):
        import argparse
        import json as _json
        from pathlib import Path

        from optimizer.cli import _build_apply_command

        pr = make_probe(codec=codec, height=1080, video_bitrate=20_000_000)
        # rules_fired must contain something non-container so the function
        # takes the encode path (not the remux-only short-circuit).
        dec = {"rules_fired_json": _json.dumps(["hd_non_av1"])}
        args = argparse.Namespace(
            hw_decode=hw_decode_default,
            quality=21,
            compat_audio=True,
            original_audio=False,
            original_subs=False,
            encoder_preset="veryslow",
            qsv_overrides={},
        )
        cmd, desc = _build_apply_command(
            dec, pr, Path("/tmp/out.mkv"),
            target_container="mkv",
            enc_name="av1_qsv",
            keep_langs=["en", "und"],
            args=args,
            dv_pre_pass=dv_pre_pass,
        )
        if return_desc:
            return cmd, desc
        return cmd

    def test_vc1_source_stays_on_cpu_at_hd_default(self):
        """Empirical: vc1_qsv wedges at frame 0 on at least one Blu-ray
        VC-1 source (Lethal Weapon 1987). VC-1 was removed from
        SLOW_CPU_DECODE_CODECS pending evidence the QSV path is reliable.
        """
        cmd = self._build("vc1", hw_decode_default=False)
        self.assertNotIn("-hwaccel", cmd)

    def test_vp9_source_stays_on_cpu_at_hd_default(self):
        """VP9 was added defensively alongside VC-1; pulled back at the
        same time pending empirical evidence on Battlemage.
        """
        cmd = self._build("vp9", hw_decode_default=False)
        self.assertNotIn("-hwaccel", cmd)

    def test_h264_source_keeps_preset_default(self):
        """H.264 at HD stays on CPU decode (frame-threaded; keeps up with
        av1_qsv supply rate at ~220 fps)."""
        cmd = self._build("h264", hw_decode_default=False)
        self.assertNotIn("-hwaccel", cmd)

    def test_mpeg2_source_keeps_preset_default(self):
        """MPEG-2 software decode is slice-threaded; ~220 fps on Thor."""
        cmd = self._build("mpeg2video", hw_decode_default=False)
        self.assertNotIn("-hwaccel", cmd)

    def test_explicit_hw_decode_true_still_works(self):
        """UHD preset / explicit user override still routes through QSV.
        The empty SLOW_CPU_DECODE_CODECS only affects the codec-aware
        upward override; an explicit True from the preset/CLI is untouched.
        """
        cmd = self._build("hevc", hw_decode_default=True)
        joined = " ".join(cmd)
        self.assertIn("-hwaccel qsv", joined)
        self.assertIn("-hwaccel_output_format qsv", joined)

    def test_override_mechanism_intact_for_future_additions(self):
        """Mechanism guard: if a codec is added back to
        SLOW_CPU_DECODE_CODECS, `_build_apply_command` must still flip
        hw_decode to True for it. Verifies via monkey-patching the set
        with a temporary entry rather than rebuilding the import.
        """
        import argparse
        import json as _json
        from pathlib import Path
        from unittest.mock import patch

        from optimizer.cli import _build_apply_command

        pr = make_probe(codec="vc1", height=1080, video_bitrate=20_000_000)
        dec = {"rules_fired_json": _json.dumps(["hd_non_av1"])}
        args = argparse.Namespace(
            hw_decode=False, quality=21, compat_audio=True,
            original_audio=False, original_subs=False,
            encoder_preset="veryslow", qsv_overrides={},
        )
        with patch("optimizer.cli.SLOW_CPU_DECODE_CODECS",
                   frozenset({"vc1"})):
            cmd, _desc = _build_apply_command(
                dec, pr, Path("/tmp/out.mkv"),
                target_container="mkv", enc_name="av1_qsv",
                keep_langs=["en", "und"], args=args,
            )
        self.assertIn("-hwaccel qsv", " ".join(cmd))

    # ----- descriptor format -----

    def test_descriptor_reports_qsv_decode_when_hw_decode_on(self):
        """Descriptor advertises QSV decode when the UHD preset / explicit
        override sets hw_decode=True. Uses a HEVC source (which the UHD
        preset would actually pick up)."""
        _cmd, desc = self._build("hevc", hw_decode_default=True, return_desc=True)
        self.assertIn("decode hevc (QSV)", desc)
        self.assertIn("encode via av1_qsv", desc)

    def test_descriptor_reports_cpu_decode_for_h264(self):
        """H.264 at HD stays on CPU; descriptor must say so."""
        _cmd, desc = self._build("h264", hw_decode_default=False, return_desc=True)
        self.assertIn("decode h264 (CPU)", desc)
        self.assertIn("encode via av1_qsv", desc)

    def test_descriptor_reports_cpu_decode_for_vc1_at_hd_default(self):
        """Companion to the policy change: VC-1 at HD now stays on CPU
        decode, so the descriptor must reflect that (was QSV pre-rollback)."""
        _cmd, desc = self._build("vc1", hw_decode_default=False, return_desc=True)
        self.assertIn("decode vc1 (CPU)", desc)

    def test_descriptor_omits_dv_strip_when_not_requested(self):
        """Regression guard: the prior `source_override is not None` check
        unconditionally appended '+ DV strip pre-pass' on every encode, even
        for plain H.264 sources. The new gate is `dv_pre_pass=True` only."""
        _cmd, desc = self._build("h264", return_desc=True, dv_pre_pass=False)
        self.assertNotIn("DV strip pre-pass", desc)

    def test_descriptor_includes_dv_strip_when_requested(self):
        """When the caller actually ran the strip, the descriptor advertises it."""
        _cmd, desc = self._build("hevc", return_desc=True, dv_pre_pass=True)
        self.assertIn("(+ DV strip pre-pass)", desc)


# --------------------------------------------------------------------------- #
# Candidate.total_projected_savings_mb
# --------------------------------------------------------------------------- #


class CandidateProjectedSavingsTests(unittest.TestCase):
    """Per-rule projections are competing estimates of the same encode, not
    additive contributions. Pin: take max, cap at 95% of source size.

    Canary case from production: Lethal Weapon 1987 — 23 GB source, three
    rules fired (over_bitrate, legacy_codec, hd_non_av1) summing to 32.4
    GB of "projected savings", impossibly larger than the source.
    """

    def _candidate(self, *, source_mb: float, per_rule_mb: list[float],
                   duration_s: float = 3600.0, height: int = 1080):
        """Build a Candidate with N fired rules, each projecting given MB."""
        from optimizer.models import Candidate, RuleVerdict
        pr = make_probe(codec="vc1", height=height, duration=duration_s)
        pr.size = int(source_mb * 1024 * 1024)
        fired = [
            RuleVerdict(rule=f"rule_{i}", fired=True,
                        reason="t", severity="medium",
                        projected_savings_mb=mb, notes={})
            for i, mb in enumerate(per_rule_mb)
        ]
        return Candidate(probe=pr, fired=fired, target="av1+mkv",
                         remux_only=False, is_hdr=False)

    def test_takes_max_not_sum_of_overlapping_estimates(self):
        """Rules predicting 18/11/5 GB don't add — they're competing
        estimates of the same encode. 1080p × 1hr → 5 GB expected output
        → realistic ceiling 18 GB; max-rule 18 GB lands under it."""
        cand = self._candidate(
            source_mb=23_000,
            per_rule_mb=[18_000, 11_000, 5_000],
        )
        self.assertEqual(cand.total_projected_savings_mb, 18_000)

    def test_caps_at_realistic_av1_output_for_tier(self):
        """Die Hard 3 canary: 1080p AVC remux, ~2.18 hr, source ~30 GB,
        over_bitrate rule projected 23.7 GB savings. Realistic ceiling:
        source - 2.18hr × 5000 MB/hr = ~19 GB. The rule's overshoot
        gets pulled back to that ceiling."""
        # source 30 GB, duration 2.18 hr at 1080p (5000 MB/hr rate)
        cand = self._candidate(
            source_mb=30_000,
            per_rule_mb=[23_700],
            duration_s=3600 * 2.18,
        )
        expected_output_mb = (3600 * 2.18 / 3600.0) * 5000.0   # = 10900
        expected_savings = 30_000 - expected_output_mb         # = 19_100
        self.assertAlmostEqual(cand.total_projected_savings_mb,
                               expected_savings, places=1)

    def test_caps_at_95_percent_when_tier_ceiling_unavailable(self):
        """Defensive: zero-duration source bypasses the tier-aware cap
        (can't compute expected output without duration) and falls
        through to the 95%-of-source guard."""
        cand = self._candidate(
            source_mb=10_000,
            per_rule_mb=[25_000],
            duration_s=0,           # bypasses tier ceiling
        )
        self.assertEqual(cand.total_projected_savings_mb, 9_500)

    def test_handles_none_savings_from_advisory_rules(self):
        """hdr_advisory has projected_savings_mb=None and must be ignored,
        not crash the max() call."""
        from optimizer.models import Candidate, RuleVerdict
        pr = make_probe(codec="hevc", height=2160)
        pr.size = 50_000 * 1024 * 1024
        fired = [
            RuleVerdict(rule="hdr_advisory", fired=True, reason="t",
                        severity="medium", projected_savings_mb=None,
                        notes={}),
            RuleVerdict(rule="uhd_non_av1", fired=True, reason="t",
                        severity="high", projected_savings_mb=15_000,
                        notes={}),
        ]
        cand = Candidate(probe=pr, fired=fired, target="av1+mkv",
                         remux_only=False, is_hdr=True)
        self.assertEqual(cand.total_projected_savings_mb, 15_000)

    def test_returns_zero_for_no_fired_rules(self):
        """Defensive: degenerate candidate with no rules → 0 savings.
        (In normal flow we wouldn't construct such a Candidate, but
        max([]) crashes; verify the guard.)"""
        cand = self._candidate(source_mb=10_000, per_rule_mb=[])
        self.assertEqual(cand.total_projected_savings_mb, 0.0)


if __name__ == "__main__":
    unittest.main()
