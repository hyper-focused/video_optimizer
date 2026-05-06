"""Codec policy: inefficient h.264 (HD) and non-AV1 (UHD) rules + AV1/SD gates.

The goal of this policy:
  - HD h.264 → always a re-encode candidate (CQ-based encode preserves quality;
    storage savings are typical but not guaranteed for already-compressed sources).
  - UHD anything-but-AV1 → re-encode candidate (the savings deltas are large at 4K).
  - AV1 source → plan-time skip (already at the target codec); --allow-av1 to override.
  - SD source (height < 720) → plan-time skip (would need its own preset/rule set);
    --allow-sd to override.
"""

from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from optimizer.cli import _plan_probe_gate
from optimizer.db import Database
from optimizer.rules import (
    InefficientCodecRule,
    UhdNonAv1Rule,
)
from tests._fixtures import make_probe

# --------------------------------------------------------------------------- #
# InefficientCodecRule (h.264 at HD)
# --------------------------------------------------------------------------- #


class InefficientCodecRuleTests(unittest.TestCase):
    """h.264 in [720, 1440) fires; everything else is a miss."""

    def setUp(self):
        self.rule = InefficientCodecRule()

    def test_h264_1080p_fires(self):
        v = self.rule.evaluate(make_probe(codec="h264", height=1080))
        self.assertTrue(v.fired)
        self.assertGreater(v.projected_savings_mb, 0)

    def test_h264_720p_fires(self):
        v = self.rule.evaluate(make_probe(codec="h264", height=720))
        self.assertTrue(v.fired)

    def test_h264_below_hd_does_not_fire(self):
        # 480p falls into SD range; the plan-gate skips it before rule
        # evaluation, but the rule itself should also not fire on it.
        v = self.rule.evaluate(make_probe(codec="h264", height=480))
        self.assertFalse(v.fired)

    def test_h264_at_uhd_does_not_fire(self):
        # UhdNonAv1Rule covers UHD; this rule must not double-fire.
        v = self.rule.evaluate(make_probe(codec="h264", height=2160))
        self.assertFalse(v.fired)

    def test_hevc_at_hd_does_not_fire(self):
        v = self.rule.evaluate(make_probe(codec="hevc", height=1080))
        self.assertFalse(v.fired)

    def test_av1_at_hd_does_not_fire(self):
        v = self.rule.evaluate(make_probe(codec="av1", height=1080))
        self.assertFalse(v.fired)

    def test_vp9_at_hd_does_not_fire(self):
        v = self.rule.evaluate(make_probe(codec="vp9", height=1080))
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
# Plan-gate AV1 + SD skips
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


class PlanGateSdTests(_GateTestBase):
    def test_sd_source_skipped_by_default(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(height=480)),
                "sd_source",
            )

    def test_sd_admitted_with_override(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(height=480),
                                 allow_sd=True),
                "ok",
            )

    def test_720p_is_not_sd(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(height=720)),
                "ok",
            )

    def test_av1_skip_takes_precedence_over_sd_skip(self):
        # An SD AV1 source should report "av1_source" (the higher-
        # information skip — AV1 means it's already optimised, even at
        # SD), not "sd_source".
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, self._probe(codec="av1", height=480)),
                "av1_source",
            )


if __name__ == "__main__":
    unittest.main()
