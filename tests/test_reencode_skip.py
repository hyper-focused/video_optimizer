"""Plan-time gate: files already tagged REENCODE (this tool's output)
are skipped permanently to prevent re-encode loops in replace runs
where originals were preserved (recycle / backup) — without the gate,
the next plan would surface its own outputs as candidates.

The gate is overridable via `plan --allow-reencoded` for the rare case
where a re-encode of an already-tagged file is genuinely intended.
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
import unittest
from pathlib import Path

from optimizer.cli import _is_reencoded_filename, _plan_probe_gate, cmd_plan
from optimizer.db import Database
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
        self.db_path = self.root / "state.db"
        self.reenc = self.root / "old.AV1.REENCODE.mkv"
        self.reenc.write_bytes(b"x" * 4096)
        self.fresh = self.root / "Heat.1995.HEVC.mkv"
        self.fresh.write_bytes(b"x" * 4096)

    def tearDown(self):
        self.tmp.cleanup()

    def test_gate_skips_reencoded_by_default(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, _probe_for(self.reenc)),
                "reencoded",
            )

    def test_gate_admits_reencoded_with_override(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, _probe_for(self.reenc),
                                 allow_reencoded=True),
                "ok",
            )

    def test_gate_admits_pristine_source(self):
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db, _probe_for(self.fresh)),
                "ok",
            )

    def test_gate_dv_takes_precedence_over_reencoded(self):
        """A DV-tagged file that also has REENCODE in the name should
        report 'dv' (the higher-priority skip), not 'reencoded'. Order
        matters because the operator-facing remediation is different.

        Uses Profile 5 (always skipped — no clean HDR10 fallback exists)
        rather than Profile 7 (environment-dependent: admitted when
        dovi_tool is on PATH) so the test is deterministic regardless
        of the host's dovi_tool install state.
        """
        with Database(self.db_path) as db:
            self.assertEqual(
                _plan_probe_gate(db,
                                 _probe_for(self.reenc, dv_profile=5)),
                "dv",
            )


def _plan_args(db_path: Path, *, allow_reencoded: bool = False,
               path: Path | None = None) -> argparse.Namespace:
    return argparse.Namespace(
        cmd="plan", path=path, rules=None,
        target="av1+mkv", json=False,
        keep_langs="en,und", allow_reencoded=allow_reencoded,
        db=db_path,
    )


class CmdPlanReencodeTests(unittest.TestCase):
    """End-to-end: cmd_plan should report reencoded_blocked in the run
    summary and not insert pending decisions for those paths."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db_path = self.root / "state.db"
        # Two files, both very over-bitrate so they'd otherwise queue.
        self.reenc = self.root / "old.AV1.REENCODE.mkv"
        self.reenc.write_bytes(b"x" * 4096)
        self.fresh = self.root / "Heat.1995.HEVC.mkv"
        self.fresh.write_bytes(b"x" * 4096)
        with Database(self.db_path) as db:
            db.upsert_probe(_probe_for(self.reenc))
            db.upsert_probe(_probe_for(self.fresh))

    def tearDown(self):
        self.tmp.cleanup()

    def _pending_paths(self) -> set[str]:
        with sqlite3.connect(str(self.db_path)) as c:
            rows = c.execute(
                "SELECT path FROM decisions WHERE status='pending'").fetchall()
        return {r[0] for r in rows}

    def _last_summary(self) -> dict:
        import json as _json
        with sqlite3.connect(str(self.db_path)) as c:
            row = c.execute(
                "SELECT summary_json FROM runs WHERE kind='plan' "
                "ORDER BY id DESC LIMIT 1").fetchone()
        return _json.loads(row[0])

    def test_default_run_skips_reencoded(self):
        cmd_plan(_plan_args(self.db_path))
        self.assertNotIn(str(self.reenc), self._pending_paths())
        self.assertIn(str(self.fresh), self._pending_paths())
        self.assertEqual(self._last_summary()["reencoded_blocked"], 1)

    def test_allow_reencoded_admits_both(self):
        cmd_plan(_plan_args(self.db_path, allow_reencoded=True))
        self.assertIn(str(self.reenc), self._pending_paths())
        self.assertIn(str(self.fresh), self._pending_paths())
        self.assertEqual(self._last_summary()["reencoded_blocked"], 0)


class CmdPlanPathScopeTests(unittest.TestCase):
    """Path-scope filter: cmd_plan only creates candidates for files under
    args.path. Without this, a path-pipeline run would re-process probes
    cached from earlier scans of unrelated directories — the bug the user
    reported where HD /movies queued candidates from /tv as well."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.db_path = self.root / "state.db"
        self.movies_dir = self.root / "Movies"
        self.tv_dir = self.root / "TV"
        self.movies_dir.mkdir()
        self.tv_dir.mkdir()
        self.movie = self.movies_dir / "Foo.HEVC.mkv"
        self.movie.write_bytes(b"x" * 4096)
        self.tv_episode = self.tv_dir / "Bar.S01E01.HEVC.mkv"
        self.tv_episode.write_bytes(b"x" * 4096)
        with Database(self.db_path) as db:
            db.upsert_probe(_probe_for(self.movie))
            db.upsert_probe(_probe_for(self.tv_episode))

    def tearDown(self):
        self.tmp.cleanup()

    def _pending_paths(self) -> set[str]:
        with sqlite3.connect(str(self.db_path)) as c:
            rows = c.execute(
                "SELECT path FROM decisions WHERE status='pending'").fetchall()
        return {r[0] for r in rows}

    def _last_summary(self) -> dict:
        import json as _json
        with sqlite3.connect(str(self.db_path)) as c:
            row = c.execute(
                "SELECT summary_json FROM runs WHERE kind='plan' "
                "ORDER BY id DESC LIMIT 1").fetchone()
        return _json.loads(row[0])

    def test_no_path_arg_includes_everything(self):
        # Standalone `plan` (no path) iterates the whole cache —
        # backwards-compat for users who scan/plan separately.
        cmd_plan(_plan_args(self.db_path))
        self.assertIn(str(self.movie), self._pending_paths())
        self.assertIn(str(self.tv_episode), self._pending_paths())
        self.assertEqual(self._last_summary()["out_of_scope_blocked"], 0)

    def test_path_arg_excludes_other_dirs(self):
        # When path is /Movies, /TV probes get bucketed as out_of_scope
        # and don't make it into the candidate list.
        cmd_plan(_plan_args(self.db_path, path=self.movies_dir))
        self.assertIn(str(self.movie), self._pending_paths())
        self.assertNotIn(str(self.tv_episode), self._pending_paths())
        self.assertEqual(self._last_summary()["out_of_scope_blocked"], 1)

    def test_single_file_path_admits_only_that_file(self):
        # Path can be a single file (Radarr/Sonarr post-processing hook).
        cmd_plan(_plan_args(self.db_path, path=self.movie))
        self.assertIn(str(self.movie), self._pending_paths())
        self.assertNotIn(str(self.tv_episode), self._pending_paths())
        self.assertEqual(self._last_summary()["out_of_scope_blocked"], 1)


if __name__ == "__main__":
    unittest.main()
