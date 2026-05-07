"""Plan-time Dolby Vision skip filter.

Sources with a DV configuration record consistently wedge av1_qsv
(Profile 7 stalls at frame 0, Profile 8 stalls partway in). Pinned in
v0.5.15 after run-3 / run-6 of the UHD-archive test queue produced
seven stalls, all on DV titles (LOTR x265-NAHOM trilogy, Hobbit
Desolation of Smaug, The Housemaid 2025).

These tests verify plan() drops DV probes from the queue before rule
evaluation, leaving non-DV candidates unaffected.
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
import unittest
from pathlib import Path

from optimizer.cli import cmd_plan
from optimizer.db import Database
from tests._fixtures import make_probe


def _plan_args(db_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        db=db_path, rules=None, target="av1+mkv", json=False, cmd="plan",
    )


class DvSkipTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "state.db"
        # Seed two probes that both clear OverBitratedRule (2160p well
        # above the 32 Mbps flag threshold) — only the DV gate should
        # decide which one becomes a pending decision.
        self.non_dv = "/tmp/non_dv.mkv"
        self.dv = "/tmp/dv_p7.mkv"
        # Real files so the plan-time existence check passes.
        Path(self.non_dv).write_bytes(b"x")
        Path(self.dv).write_bytes(b"x")
        with Database(self.db_path) as db:
            db.upsert_probe(_p(self.non_dv, dv_profile=None))
            # Profile 5: always skipped (no clean HDR10 base layer to
            # fall back to). Profile 7 is environment-dependent (admitted
            # iff dovi_tool is on PATH); P5 keeps the test deterministic.
            db.upsert_probe(_p(self.dv, dv_profile=5))

    def tearDown(self):
        for p in (self.non_dv, self.dv):
            Path(p).unlink(missing_ok=True)
        self.tmpdir.cleanup()

    def _pending_paths(self) -> list[str]:
        conn = sqlite3.connect(str(self.db_path))
        rows = conn.execute(
            "SELECT path FROM decisions WHERE status='pending'",
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]

    def test_dv_source_not_queued(self):
        rc = cmd_plan(_plan_args(self.db_path))
        self.assertEqual(rc, 0)
        pending = self._pending_paths()
        self.assertIn(self.non_dv, pending)
        self.assertNotIn(self.dv, pending)

    def test_summary_records_dv_blocked(self):
        cmd_plan(_plan_args(self.db_path))
        conn = sqlite3.connect(str(self.db_path))
        row = conn.execute(
            "SELECT summary_json FROM runs WHERE kind='plan' "
            "ORDER BY started_at DESC LIMIT 1",
        ).fetchone()
        conn.close()
        import json
        summary = json.loads(row[0])
        self.assertEqual(summary.get("dv_blocked"), 1)


class DvStripCommandShapeTests(unittest.TestCase):
    """`build_dv_strip_command` must scope the bsf to v:0, not bare v.

    Run #166 caught this: The Housemaid 2025 (DV P7 + JPEG cover art)
    crashed the strip with `Error initializing bitstream filter:
    dovi_rpu — Codec 'mjpeg' (7) is not supported`. The bsf only
    handles hevc + av1; bare `-bsf:v` applies to every video stream
    including the embedded JPEG cover, while `-bsf:v:0` targets only
    the primary HEVC track and leaves the cover art untouched.
    """

    def test_bsf_targets_first_video_stream_only(self):
        from optimizer.encoder import build_dv_strip_command
        from tests._fixtures import make_probe
        pr = make_probe(height=2160, dv_profile=8)
        pr.path = "/movies/x.mkv"
        cmd = build_dv_strip_command(pr, Path("/movies/x.dv-prepped.mkv"))
        # The whole-stream form must NOT be present.
        self.assertNotIn("-bsf:v", cmd,
                         "bare -bsf:v leaked; will crash on sources with "
                         "non-DV video streams (e.g. JPEG cover art)")
        # The scoped form must be present and followed by the strip filter.
        self.assertIn("-bsf:v:0", cmd)
        idx = cmd.index("-bsf:v:0")
        self.assertEqual(cmd[idx + 1], "dovi_rpu=strip=true")

    def test_no_keep_langs_means_no_discards(self):
        """Backwards-compat: omitting keep_langs preserves the original
        "copy every stream" behavior so callers that want a faithful
        clone of the source minus the RPU still get one."""
        from optimizer.encoder import build_dv_strip_command
        from tests._fixtures import make_probe
        pr = make_probe(height=2160, dv_profile=8)
        pr.path = "/movies/x.mkv"
        cmd = build_dv_strip_command(pr, Path("/o.mkv"))
        self.assertFalse(any(c.startswith("-discard") for c in cmd),
                         "no keep_langs → no demuxer discards expected")

    def test_keep_langs_drops_unwanted_audio_at_demuxer(self):
        """The whole point of the optimization: with keep_langs set,
        the strip demuxer drops unwanted audio so they're never read
        from the NAS or written to the temp file. SPR-shaped: 9 audio
        streams (TrueHD primary + DTS + 6× AC3 commentary) → keep
        primary + a 5.1 fallback, discard the rest."""
        from optimizer.encoder import build_dv_strip_command
        from optimizer.models import AudioTrack
        from tests._fixtures import make_probe

        def at(i: int, codec: str, ch: int = 6) -> AudioTrack:
            return AudioTrack(
                index=i, codec=codec, language="eng", channels=ch,
                channel_layout=f"{ch}.0", bitrate=1_000_000,
                default=(i == 0), title=None,
            )

        pr = make_probe(height=2160, dv_profile=7)
        pr.path = "/movies/x.mkv"
        pr.audio_tracks = (
            [at(0, "truehd", 8), at(1, "dts", 6)]
            + [at(i, "ac3", 6) for i in range(2, 9)]
        )
        cmd = build_dv_strip_command(
            pr, Path("/o.mkv"),
            keep_langs=["en", "und"], target_container="mkv",
        )
        discards = {cmd[i + 1] for i, t in enumerate(cmd[:-1])
                    if t.startswith("-discard:a:")}
        # The 7 commentary AC3 tracks (a:2..a:8) should be discarded;
        # primary TrueHD (a:0) and a 5.1 fallback (a:1 dts) are kept.
        for ac3_idx in range(2, 9):
            self.assertIn(
                "all", discards,  # value side; key check below
                f"a:{ac3_idx} should be discarded",
            )
        discard_keys = [t for t in cmd if t.startswith("-discard:a:")]
        for ac3_idx in range(2, 9):
            self.assertIn(f"-discard:a:{ac3_idx}", discard_keys)
        # Primary audio (and the dts 5.1 fallback) must NOT be discarded.
        self.assertNotIn("-discard:a:0", discard_keys)


class DvStrategyDefaultsTests(unittest.TestCase):
    """`encoder.dv_strategy` per-profile dispatch.

    Default (allow_p7_convert=False) puts P7 on the same simple
    `dovi_rpu=strip=true` bsf path as P8 — the dovi_tool +
    mkvmerge pipeline is opt-in. Pinned because the historical
    default (P7 → "p7_convert" when tools are present) caused
    NAS-wedge / mkvmerge-muxer pain in the field; reverting it
    silently would re-introduce that.
    """

    def test_p5_always_skipped(self):
        from optimizer import encoder
        self.assertIsNone(encoder.dv_strategy(5))
        self.assertIsNone(encoder.dv_strategy(5, allow_p7_convert=True))

    def test_p8_is_strip(self):
        from optimizer import encoder
        self.assertEqual(encoder.dv_strategy(8), "p8_strip")

    def test_p7_default_is_strip_not_convert(self):
        from optimizer import encoder
        self.assertEqual(encoder.dv_strategy(7), "p8_strip")

    def test_p7_convert_opt_in_requires_both_tools(self):
        from unittest.mock import patch

        from optimizer import encoder
        # Both present → convert pipeline.
        with patch.object(encoder, "has_dovi_tool", return_value=True), \
             patch.object(encoder, "has_mkvmerge", return_value=True):
            self.assertEqual(
                encoder.dv_strategy(7, allow_p7_convert=True),
                "p7_convert",
            )
        # Either missing → None (fail closed; user opted in but env can't honour it).
        with patch.object(encoder, "has_dovi_tool", return_value=False), \
             patch.object(encoder, "has_mkvmerge", return_value=True):
            self.assertIsNone(
                encoder.dv_strategy(7, allow_p7_convert=True),
            )
        with patch.object(encoder, "has_dovi_tool", return_value=True), \
             patch.object(encoder, "has_mkvmerge", return_value=False):
            self.assertIsNone(
                encoder.dv_strategy(7, allow_p7_convert=True),
            )


def _p(path: str, *, dv_profile: int | None) -> object:
    """make_probe wrapper: 2160p source over the UHD bitrate flag threshold."""
    pr = make_probe(
        height=2160, codec="hevc", bit_depth=10,
        video_bitrate=60_000_000,  # well over the 32 Mbps 2160p flag
        dv_profile=dv_profile,
    )
    pr.path = path
    return pr


if __name__ == "__main__":
    unittest.main()
