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
