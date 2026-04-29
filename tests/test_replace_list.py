"""Tests for the two-strikes mechanism around av1_qsv encoder stalls.

`plan` skips files with 2+ stall failures (don't burn another 5 minutes
re-attempting a deterministic encoder hang). `replace-list` surfaces
those files for the operator's manual intervention (download a different
release of the same title).
"""

from __future__ import annotations

import argparse
import io
import sqlite3
import tempfile
import time
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from optimizer.cli import cmd_replace_list
from optimizer.db import Database


def _seed(db_path: Path, *,
          path: str,
          fail_count: int,
          fail_reason: str = "encoder stalled — no progress for 300s",
          ) -> None:
    """Insert one cached file plus N failed decisions for it."""
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO files (path, size, mtime, last_probed_at, probe_json) "
        "VALUES (?, 1, 0, 0, '{}')",
        (path,),
    )
    for _ in range(fail_count):
        conn.execute(
            "INSERT INTO decisions (path, decided_at, rules_fired_json, "
            "target, status, error) VALUES (?, ?, '[]', 'av1+mkv', 'failed', ?)",
            (path, time.time(), fail_reason),
        )
    conn.commit()
    conn.close()


class ReplaceListTests(unittest.TestCase):
    def test_lists_files_with_two_or_more_stall_failures(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            Database(db_path).close()  # initialise schema

            _seed(db_path, path="/x/Deadpool 2.mkv", fail_count=2)
            _seed(db_path, path="/x/Star Wars I.mkv", fail_count=3)
            _seed(db_path, path="/x/Just Once.mkv", fail_count=1)
            _seed(db_path, path="/x/Other Error.mkv", fail_count=2,
                  fail_reason="ffmpeg exited 234")

            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_replace_list(argparse.Namespace(db=db_path))
            output = buf.getvalue()
            self.assertEqual(rc, 0)
            # 2× and 3× stalls should appear:
            self.assertIn("Deadpool 2.mkv", output)
            self.assertIn("Star Wars I.mkv", output)
            # Single stall: not enough strikes yet.
            self.assertNotIn("Just Once.mkv", output)
            # Non-stall failures: outside the watchdog scope.
            self.assertNotIn("Other Error.mkv", output)

    def test_clean_run_says_so(self):
        """When nothing has hit the threshold, the command exits with a
        clear message (not a confusing empty list)."""
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            Database(db_path).close()
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_replace_list(argparse.Namespace(db=db_path))
            self.assertEqual(rc, 0)
            self.assertIn("no files have hit the stall watchdog", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
