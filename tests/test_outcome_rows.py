"""Outcome rows born as status='running', healed on a later open.

A killed encode leaves a running decision whose owner_pid is dead (or
NULL, from before that column existed). The next Database connect marks
that row failed with error exactly 'interrupted'. A live owner_pid is
left running, including when a second connect overlaps the writer.
'interrupted' must not match the two-strike stall query
(status='failed' AND error LIKE '%encoder stalled%'). A leftover
status='pending' row does not survive a reopen. Failed rows, including
existing stall failures, are left alone.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from optimizer.db import Database
from tests._fixtures import make_probe

_INSERT_RUNNING_CHILD = """\
import os
import sys
from optimizer.db import Database

db_path, path = sys.argv[1], sys.argv[2]
with Database(db_path) as db:
    run_id = db.start_run("apply", None, {})
    dec_id = db.insert_running_decision(path, "[]", "av1+mkv", 3.0, run_id)
print(dec_id)
print(os.getpid())
"""


def _seed_probe(db: Database, path: str) -> None:
    """Insert the files row decisions.path foreign-keys to."""
    probe = make_probe()
    probe.path = path
    db.upsert_probe(probe)


def _decision(db: Database, decision_id: int) -> dict:
    row = db.conn.execute(
        "SELECT * FROM decisions WHERE id = ?",
        (decision_id,),
    ).fetchone()
    if row is None:
        raise AssertionError(f"missing decision {decision_id}")
    return dict(row)


def _raw_decision(db_path: Path, decision_id: int) -> dict:
    """Read a decision without opening Database, so connect-time heal does not run."""
    row = _raw_decision_or_none(db_path, decision_id)
    if row is None:
        raise AssertionError(f"missing decision {decision_id}")
    return row


def _raw_decision_or_none(db_path: Path, decision_id: int) -> dict | None:
    """Like `_raw_decision`, but a deleted id is None."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    row = conn.execute(
        "SELECT * FROM decisions WHERE id = ?",
        (decision_id,),
    ).fetchone()
    conn.close()
    return dict(row) if row is not None else None


def _insert_running_in_child(db_path: Path, path: str) -> tuple[int, int]:
    """Have a short subprocess insert a running row, then return (id, pid)."""
    root = str(Path(__file__).resolve().parents[1])
    env = os.environ.copy()
    env["PYTHONPATH"] = root + os.pathsep + env.get("PYTHONPATH", "")
    proc = subprocess.Popen(
        [sys.executable, "-c", _INSERT_RUNNING_CHILD, str(db_path), path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=root,
        env=env,
    )
    try:
        stdout, stderr = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()
        raise
    if proc.returncode != 0:
        raise AssertionError(f"child exited {proc.returncode}: {stderr}")
    lines = stdout.splitlines()
    if len(lines) != 2:
        raise AssertionError(f"unexpected child output: {stdout!r} {stderr!r}")
    return int(lines[0]), int(lines[1])


def _prove_pid_dead(pid: int) -> None:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return
    except PermissionError as exc:
        raise AssertionError(f"pid {pid} is alive under another user") from exc
    raise AssertionError(f"pid {pid} is still alive")


class OutcomeRowTests(unittest.TestCase):
    def test_insert_running_decision_writes_status_and_run_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            with Database(db_path) as db:
                path = "/movies/running.mkv"
                _seed_probe(db, path)
                run_id = db.start_run("apply", None, {})
                rules_json = '["over_bitrated"]'
                dec_id = db.insert_running_decision(
                    path, rules_json, "av1+mkv", 12.5, run_id,
                )
                row = _decision(db, dec_id)

            self.assertEqual(row["status"], "running")
            self.assertEqual(row["run_id"], run_id)
            self.assertEqual(row["owner_pid"], os.getpid())
            self.assertEqual(row["rules_fired_json"], rules_json)
            self.assertEqual(row["target"], "av1+mkv")
            self.assertEqual(row["projected_savings_mb"], 12.5)
            self.assertEqual(row["path"], path)
            self.assertIsNone(row["error"])
            self.assertIsInstance(row["decided_at"], float)

    def test_second_connect_keeps_live_owner(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            with Database(db_path) as db_a:
                path = "/movies/live.mkv"
                _seed_probe(db_a, path)
                run_id = db_a.start_run("apply", None, {})
                dec_id = db_a.insert_running_decision(
                    path, "[]", "av1+mkv", 4.0, run_id,
                )
                before = _decision(db_a, dec_id)
                db_a.conn.commit()
                with Database(db_path) as db_b:
                    during = _decision(db_b, dec_id)
                    db_a.conn.commit()
                    from_writer = _decision(db_a, dec_id)
                self.assertEqual(during, before)
                self.assertEqual(from_writer, before)
            with Database(db_path) as db_c:
                after_close = _decision(db_c, dec_id)
            self.assertEqual(after_close, before)
            self.assertEqual(before["status"], "running")
            self.assertEqual(before["owner_pid"], os.getpid())
            self.assertIsNone(before["error"])

    def test_dead_owner_is_interrupted_and_other_rows_survive(self):
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            path = "/movies/mix.mkv"
            with Database(db_path) as db:
                _seed_probe(db, path)
                run_id = db.start_run("apply", None, {})
                stall_id = db.insert_running_decision(
                    path, '["over_bitrated"]', "av1+mkv", 1.0, run_id,
                )
                db.mark_decision(stall_id, "failed", error="encoder stalled")
                stall_before = _decision(db, stall_id)

            conn = sqlite3.connect(str(db_path))
            cur = conn.execute(
                "INSERT INTO decisions "
                "(path, decided_at, rules_fired_json, target, "
                "projected_savings_mb, status, run_id) "
                "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
                (path, 1.0, '["over_bitrated"]', "av1+mkv", 2.0, run_id),
            )
            pending_id = int(cur.lastrowid)
            conn.commit()
            conn.close()
            pending_before = _raw_decision(db_path, pending_id)
            self.assertEqual(pending_before["status"], "pending")

            dec_id, pid = _insert_running_in_child(db_path, path)
            _prove_pid_dead(pid)
            self.assertIsNone(_raw_decision_or_none(db_path, pending_id))
            running_before = _raw_decision(db_path, dec_id)
            self.assertEqual(running_before["status"], "running")
            self.assertEqual(running_before["owner_pid"], pid)
            self.assertIsNone(running_before["error"])

            with Database(db_path) as db:
                running = _decision(db, dec_id)
                self.assertIsNone(_raw_decision_or_none(db_path, pending_id))
                stall_after = _decision(db, stall_id)
                stall_hits = db.conn.execute(
                    "SELECT id FROM decisions WHERE status = 'failed' "
                    "AND error LIKE '%encoder stalled%'",
                ).fetchall()

            expected = dict(running_before)
            expected["status"] = "failed"
            expected["error"] = "interrupted"
            self.assertEqual(running, expected)
            self.assertEqual(running["error"], "interrupted")
            self.assertNotIn("encoder stalled", running["error"])
            self.assertIsNone(_raw_decision_or_none(db_path, pending_id))
            self.assertEqual(stall_after, stall_before)
            self.assertEqual(stall_after["status"], "failed")
            self.assertEqual(stall_after["error"], "encoder stalled")
            hit_ids = {row["id"] for row in stall_hits}
            self.assertEqual(hit_ids, {stall_id})
            self.assertNotIn(dec_id, hit_ids)

    def test_null_owner_on_pre_column_db_is_interrupted(self):
        """A running row from before owner_pid existed is reaped.

        The fixture is the pre-column decisions table (no AUTOINCREMENT),
        so the connect path must ALTER the column and copy it through the
        id rebuild. NULL owner_pid is not a live process.
        """
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "state.db"
            path = "/movies/old.mkv"
            conn = sqlite3.connect(str(db_path))
            conn.executescript(
                """
                CREATE TABLE files (
                    path           TEXT PRIMARY KEY,
                    size           INTEGER NOT NULL,
                    mtime          REAL    NOT NULL,
                    last_probed_at REAL    NOT NULL,
                    probe_json     TEXT    NOT NULL
                );
                CREATE TABLE decisions (
                    id                    INTEGER PRIMARY KEY,
                    path                  TEXT NOT NULL,
                    decided_at            REAL NOT NULL,
                    rules_fired_json      TEXT NOT NULL,
                    target                TEXT NOT NULL,
                    projected_savings_mb  REAL,
                    status                TEXT NOT NULL,
                    output_path           TEXT,
                    actual_savings_mb     REAL,
                    error                 TEXT,
                    run_id                INTEGER,
                    FOREIGN KEY (path) REFERENCES files(path)
                );
                """,
            )
            conn.execute(
                "INSERT INTO files (path, size, mtime, last_probed_at, probe_json) "
                "VALUES (?, 1, 0, 0, '{}')",
                (path,),
            )
            conn.execute(
                "INSERT INTO decisions (path, decided_at, rules_fired_json, "
                "target, status) VALUES (?, 0, '[]', 'av1+mkv', 'running')",
                (path,),
            )
            conn.commit()
            conn.close()

            with Database(db_path) as db:
                row = _decision(db, 1)
                sql = db.conn.execute(
                    "SELECT sql FROM sqlite_master "
                    "WHERE type = 'table' AND name = 'decisions'",
                ).fetchone()[0]
            self.assertEqual(row["status"], "failed")
            self.assertEqual(row["error"], "interrupted")
            self.assertIsNone(row["owner_pid"])
            self.assertNotIn("encoder stalled", row["error"])
            self.assertIn("owner_pid", sql)
            self.assertIn("AUTOINCREMENT", sql)


if __name__ == "__main__":
    unittest.main()
