"""Cleanup subcommand: post-encode original-removal pipeline.

The 3-check guard in `_classify_cleanup_decision` is the safety contract
that keeps a botched cleanup from nuking originals when the new file is
unusable. Each guard branch (output missing, output zero-byte, output
== source) gets a positive + negative test so a regression that softens
the guard fails loudly instead of quietly deleting user data.

Also covers the surrounding plumbing: dry-run vs --apply, --run N
targeting, and the no-eligible-run early exit.
"""

from __future__ import annotations

import argparse
import io
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from optimizer.cli import cmd_cleanup
from optimizer.db import Database


def _make_args(db_path: Path, *, run: int | None = None,
               apply: bool = False) -> argparse.Namespace:
    """Construct the Namespace cmd_cleanup expects."""
    return argparse.Namespace(
        cmd="cleanup",
        db=db_path,
        run=run,
        apply=apply,
    )


def _seed_files_row(db: Database, source_path: str) -> None:
    """Insert a minimal `files` row so the decisions FK is satisfied.

    decisions.path is FOREIGN KEY → files.path. Real probe payload isn't
    needed for cleanup tests (cmd_cleanup never reads it), so a stub
    JSON blob keeps the row valid without dragging in models.to_json.
    """
    db.conn.execute(
        "INSERT OR IGNORE INTO files "
        "(path, size, mtime, last_probed_at, probe_json) "
        "VALUES (?, ?, ?, ?, ?)",
        (source_path, 0, 0.0, time.time(), "{}"),
    )
    db.conn.commit()


def _seed_completed_decision(
    db: Database,
    *,
    run_id: int,
    source_path: str,
    output_path: str | None,
) -> int:
    """Insert a pending decision and flip it to completed under `run_id`.

    Mirrors the cmd_apply path: insert pending, then mark_decision flips
    status to 'completed' and stamps the apply run id onto the row so
    decisions_for_run(run_id) picks it up.
    """
    _seed_files_row(db, source_path)
    decision_id = db.insert_pending_decision(
        path=source_path,
        rules_fired=["test_rule"],
        target="hevc",
        projected_savings_mb=100.0,
        run_id=run_id,
    )
    db.mark_decision(
        decision_id,
        status="completed",
        output_path=output_path,
        actual_savings_mb=100.0,
        run_id=run_id,
    )
    return decision_id


class CleanupDryRunTests(unittest.TestCase):
    """Default mode lists cleanable sources and never touches the disk."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.db_path = self.root / "state.db"
        # Two real source files + two real output files so the guard
        # passes cleanly. Real bytes so st_size > 0.
        self.src_a = self.root / "movie_a.mkv"
        self.src_b = self.root / "movie_b.mkv"
        self.out_a = self.root / "movie_a.AV1.REENCODE.mkv"
        self.out_b = self.root / "movie_b.AV1.REENCODE.mkv"
        for p in (self.src_a, self.src_b):
            p.write_bytes(b"x" * 4096)
        for p in (self.out_a, self.out_b):
            p.write_bytes(b"y" * 1024)

        with Database(self.db_path) as db:
            self.run_id = db.start_run("apply", str(self.root), {})
            _seed_completed_decision(
                db, run_id=self.run_id,
                source_path=str(self.src_a),
                output_path=str(self.out_a),
            )
            _seed_completed_decision(
                db, run_id=self.run_id,
                source_path=str(self.src_b),
                output_path=str(self.out_b),
            )
            db.end_run(self.run_id, {"completed": 2})

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_dry_run_lists_without_deleting(self):
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(self.db_path, apply=False))
        self.assertEqual(rc, 0)
        # Both source files still on disk.
        self.assertTrue(self.src_a.exists())
        self.assertTrue(self.src_b.exists())
        # Output unchanged.
        self.assertTrue(self.out_a.exists())
        self.assertTrue(self.out_b.exists())
        out = buf.getvalue()
        # Each cleanable source surfaces with a "would remove" prefix.
        self.assertIn(str(self.src_a), out)
        self.assertIn(str(self.src_b), out)
        self.assertIn("would remove", out)
        # Summary line names the cleanable count.
        self.assertIn("summary: 2 cleanable", out)


class CleanupApplyTests(unittest.TestCase):
    """`--apply` actually unlinks sources when the guard passes."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.db_path = self.root / "state.db"
        self.src_a = self.root / "movie_a.mkv"
        self.src_b = self.root / "movie_b.mkv"
        self.out_a = self.root / "movie_a.AV1.REENCODE.mkv"
        self.out_b = self.root / "movie_b.AV1.REENCODE.mkv"
        for p in (self.src_a, self.src_b):
            p.write_bytes(b"x" * 4096)
        for p in (self.out_a, self.out_b):
            p.write_bytes(b"y" * 1024)

        with Database(self.db_path) as db:
            self.run_id = db.start_run("apply", str(self.root), {})
            _seed_completed_decision(
                db, run_id=self.run_id,
                source_path=str(self.src_a),
                output_path=str(self.out_a),
            )
            _seed_completed_decision(
                db, run_id=self.run_id,
                source_path=str(self.src_b),
                output_path=str(self.out_b),
            )
            db.end_run(self.run_id, {"completed": 2})

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_apply_unlinks_sources_when_guard_passes(self):
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(self.db_path, apply=True))
        self.assertEqual(rc, 0)
        # Sources gone.
        self.assertFalse(self.src_a.exists())
        self.assertFalse(self.src_b.exists())
        # Outputs preserved.
        self.assertTrue(self.out_a.exists())
        self.assertTrue(self.out_b.exists())
        out = buf.getvalue()
        # Tally line. Implementation says "removed N originals".
        self.assertIn("removed 2 originals", out)


class CleanupGuardOutputMissingTests(unittest.TestCase):
    """Guard (a): if output_path doesn't exist, the source must survive."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.db_path = self.root / "state.db"
        # Source exists; output_path points to a path that does not.
        self.src = self.root / "movie.mkv"
        self.src.write_bytes(b"x" * 4096)
        self.missing_out = self.root / "never_created.AV1.REENCODE.mkv"
        # Sanity: file genuinely absent before we run cleanup.
        self.assertFalse(self.missing_out.exists())

        with Database(self.db_path) as db:
            run_id = db.start_run("apply", str(self.root), {})
            _seed_completed_decision(
                db, run_id=run_id,
                source_path=str(self.src),
                output_path=str(self.missing_out),
            )
            db.end_run(run_id, {"completed": 1})

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_apply_does_not_unlink_when_output_missing(self):
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(self.db_path, apply=True))
        self.assertEqual(rc, 0)
        # Source PRESERVED — this is the load-bearing assertion.
        self.assertTrue(self.src.exists())
        out = buf.getvalue()
        self.assertIn("SKIP", out)
        self.assertIn("output missing", out)
        # Tally: zero removed.
        self.assertIn("removed 0 originals", out)


class CleanupGuardOutputZeroByteTests(unittest.TestCase):
    """Guard (b): zero-byte output is treated as a botched encode."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.db_path = self.root / "state.db"
        self.src = self.root / "movie.mkv"
        self.src.write_bytes(b"x" * 4096)
        # Output exists but is empty (the failure mode the guard exists
        # to catch — ffmpeg created the file then exited dirty).
        self.empty_out = self.root / "movie.AV1.REENCODE.mkv"
        self.empty_out.write_bytes(b"")
        self.assertEqual(self.empty_out.stat().st_size, 0)

        with Database(self.db_path) as db:
            run_id = db.start_run("apply", str(self.root), {})
            _seed_completed_decision(
                db, run_id=run_id,
                source_path=str(self.src),
                output_path=str(self.empty_out),
            )
            db.end_run(run_id, {"completed": 1})

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_apply_does_not_unlink_when_output_zero_byte(self):
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(self.db_path, apply=True))
        self.assertEqual(rc, 0)
        self.assertTrue(self.src.exists())
        out = buf.getvalue()
        self.assertIn("SKIP", out)
        self.assertIn("output zero-byte", out)
        self.assertIn("removed 0 originals", out)


class CleanupGuardSamePathTests(unittest.TestCase):
    """Guard (c): output_path == source_path → never delete."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.db_path = self.root / "state.db"
        # Single file standing in for both sides of an erroneous record.
        # Defensive: the apply path branches on side-vs-replace mode and
        # *should* never produce this row, but if it ever does, cleanup
        # must not be the thing that destroys the only copy.
        self.path = self.root / "movie.mkv"
        self.path.write_bytes(b"x" * 4096)

        with Database(self.db_path) as db:
            run_id = db.start_run("apply", str(self.root), {})
            _seed_completed_decision(
                db, run_id=run_id,
                source_path=str(self.path),
                output_path=str(self.path),
            )
            db.end_run(run_id, {"completed": 1})

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_apply_does_not_unlink_when_output_equals_source(self):
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(self.db_path, apply=True))
        self.assertEqual(rc, 0)
        # The file must still be on disk.
        self.assertTrue(self.path.exists())
        out = buf.getvalue()
        self.assertIn("SKIP", out)
        self.assertIn("output_path == source_path", out)
        self.assertIn("removed 0 originals", out)


class CleanupRunTargetingTests(unittest.TestCase):
    """`--run N` operates only on the named run's decisions."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.db_path = self.root / "state.db"
        # Two runs, each with its own source + output pair.
        self.src_run1 = self.root / "run1_movie.mkv"
        self.out_run1 = self.root / "run1_movie.AV1.REENCODE.mkv"
        self.src_run2 = self.root / "run2_movie.mkv"
        self.out_run2 = self.root / "run2_movie.AV1.REENCODE.mkv"
        for p in (self.src_run1, self.src_run2):
            p.write_bytes(b"x" * 4096)
        for p in (self.out_run1, self.out_run2):
            p.write_bytes(b"y" * 1024)

        with Database(self.db_path) as db:
            self.run1 = db.start_run("apply", str(self.root), {})
            _seed_completed_decision(
                db, run_id=self.run1,
                source_path=str(self.src_run1),
                output_path=str(self.out_run1),
            )
            db.end_run(self.run1, {"completed": 1})
            # Sleep a tick so run2's started_at is strictly later than
            # run1's — latest_run_with_completions sorts on started_at.
            time.sleep(0.01)
            self.run2 = db.start_run("apply", str(self.root), {})
            _seed_completed_decision(
                db, run_id=self.run2,
                source_path=str(self.src_run2),
                output_path=str(self.out_run2),
            )
            db.end_run(self.run2, {"completed": 1})

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_explicit_run_targets_only_that_run(self):
        """`--run run1` lists run1's source, never run2's."""
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(
                self.db_path, run=self.run1, apply=False))
        self.assertEqual(rc, 0)
        out = buf.getvalue()
        self.assertIn(str(self.src_run1), out)
        self.assertNotIn(str(self.src_run2), out)
        self.assertIn("summary: 1 cleanable", out)


class CleanupNoEligibleRunTests(unittest.TestCase):
    """Empty db / no completions → 'nothing to clean up' and exit 0."""

    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmpdir.name)
        self.db_path = self.root / "state.db"

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_empty_db_prints_nothing_to_clean_up(self):
        # Touch the db so the Database() ctor finds an existing schema,
        # but no runs / decisions inside.
        with Database(self.db_path):
            pass
        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(self.db_path, apply=False))
        self.assertEqual(rc, 0)
        self.assertIn("nothing to clean up", buf.getvalue())

    def test_run_with_only_failed_decisions_prints_nothing_to_clean_up(self):
        """A run that produced no completions has nothing to clean up.

        latest_run_with_completions returns None in this case, so the
        early-exit path is the same as the empty-db case — but exercise
        it explicitly to lock the behaviour.
        """
        with Database(self.db_path) as db:
            run_id = db.start_run("apply", str(self.root), {})
            source_path = str(self.root / "movie.mkv")
            _seed_files_row(db, source_path)
            decision_id = db.insert_pending_decision(
                path=source_path,
                rules_fired=["test_rule"],
                target="hevc",
                projected_savings_mb=100.0,
                run_id=run_id,
            )
            db.mark_decision(
                decision_id, status="failed",
                output_path=None, actual_savings_mb=None,
                error="encode bombed", run_id=run_id,
            )
            db.end_run(run_id, {"failed": 1})

        buf = io.StringIO()
        with patch("sys.stdout", buf):
            rc = cmd_cleanup(_make_args(self.db_path, apply=False))
        self.assertEqual(rc, 0)
        self.assertIn("nothing to clean up", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
