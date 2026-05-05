"""SQLite persistence layer for video_optimizer (probe cache, decisions, runs)."""

from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Iterator, Self

from . import models
from .models import ProbeResult

DEFAULT_DB_PATH = Path.home() / ".video_optimizer" / "state.db"


_SCHEMA = """
CREATE TABLE IF NOT EXISTS files (
    path           TEXT PRIMARY KEY,
    size           INTEGER NOT NULL,
    mtime          REAL    NOT NULL,
    last_probed_at REAL    NOT NULL,
    probe_json     TEXT    NOT NULL
);

CREATE TABLE IF NOT EXISTS decisions (
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

CREATE TABLE IF NOT EXISTS runs (
    id            INTEGER PRIMARY KEY,
    kind          TEXT NOT NULL,
    started_at    REAL NOT NULL,
    ended_at      REAL,
    root          TEXT,
    args_json     TEXT,
    summary_json  TEXT
);

CREATE TABLE IF NOT EXISTS skipped_files (
    path           TEXT PRIMARY KEY,
    size           INTEGER NOT NULL,
    mtime          REAL    NOT NULL,
    reason         TEXT    NOT NULL,
    last_seen_at   REAL    NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_decisions_status ON decisions(status);
CREATE INDEX IF NOT EXISTS idx_decisions_path   ON decisions(path);
CREATE INDEX IF NOT EXISTS idx_skipped_reason   ON skipped_files(reason);
"""


class Database:
    """SQLite-backed state store for probe cache, decisions, and run history."""

    def __init__(self, db_path: Path = DEFAULT_DB_PATH) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.executescript(_SCHEMA)
        self._migrate_decisions_run_id()
        self.conn.commit()

    def _migrate_decisions_run_id(self) -> None:
        """De-facto migration: add `run_id` to existing `decisions` rows.

        Schema is created idempotently (CLAUDE.md: there is no migration
        system), but a CREATE TABLE IF NOT EXISTS won't add a column to a
        pre-existing table. ALTER and swallow the duplicate-column error
        so re-running on a fresh db is a no-op.
        """
        try:
            self.conn.execute("ALTER TABLE decisions ADD COLUMN run_id INTEGER")
        except sqlite3.OperationalError as e:
            # "duplicate column name: run_id" — already migrated.
            if "duplicate column" not in str(e).lower():
                raise

    def close(self) -> None:
        """Close the underlying SQLite connection."""
        self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---- probe cache --------------------------------------------------------

    def get_cached_probe(self, path: str, size: int,
                         mtime: float) -> ProbeResult | None:
        """Return cached probe iff (size, mtime) match exactly."""
        row = self.conn.execute(
            "SELECT size, mtime, probe_json FROM files WHERE path = ?",
            (path,),
        ).fetchone()
        if row is None:
            return None
        if row["size"] != size or row["mtime"] != mtime:
            return None
        return models.probe_from_json(row["probe_json"])

    def upsert_probe(self, probe: ProbeResult) -> None:
        """Insert or replace the cached probe row for probe.path."""
        self.conn.execute(
            "INSERT INTO files (path, size, mtime, last_probed_at, probe_json) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(path) DO UPDATE SET "
            "size=excluded.size, mtime=excluded.mtime, "
            "last_probed_at=excluded.last_probed_at, probe_json=excluded.probe_json",
            (probe.path, probe.size, probe.mtime, time.time(), models.to_json(probe)),
        )
        self.conn.commit()

    def iter_probes(self) -> Iterator[ProbeResult]:
        """Yield every cached ProbeResult."""
        cur = self.conn.execute("SELECT probe_json FROM files")
        for row in cur:
            yield models.probe_from_json(row["probe_json"])

    # ---- size-skip cache ----------------------------------------------------

    def record_size_skip(self, path: str, size: int, mtime: float,
                         reason: str = "below_min_size") -> None:
        """Mark `path` as skipped at scan time so it's not re-probed.

        Also evicts any existing probe-cache row for the same path: a file
        that fell below the threshold should no longer satisfy plan-time
        rule evaluation.
        """
        self.conn.execute(
            "INSERT INTO skipped_files (path, size, mtime, reason, last_seen_at) "
            "VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(path) DO UPDATE SET "
            "size=excluded.size, mtime=excluded.mtime, "
            "reason=excluded.reason, last_seen_at=excluded.last_seen_at",
            (path, size, mtime, reason, time.time()),
        )
        # If this path was previously above the threshold and probed, drop
        # the stale probe row + any decisions that referenced it. Decisions
        # have an FK back to files; delete them first.
        self.conn.execute("DELETE FROM decisions WHERE path = ?", (path,))
        self.conn.execute("DELETE FROM files WHERE path = ?", (path,))
        self.conn.commit()

    def clear_size_skip(self, path: str) -> bool:
        """Remove the skip row for `path`. Returns True iff a row was deleted.

        Called when a file that was previously below the threshold is now
        above it (file grew, or threshold lowered) — caller will then
        queue it for ffprobe via the normal scan path.
        """
        cur = self.conn.execute(
            "DELETE FROM skipped_files WHERE path = ?", (path,))
        self.conn.commit()
        return cur.rowcount > 0

    def is_size_skipped(self, path: str) -> bool:
        """Return True iff there's a skipped_files row for `path`."""
        row = self.conn.execute(
            "SELECT 1 FROM skipped_files WHERE path = ?", (path,)).fetchone()
        return row is not None

    def count_size_skipped(self) -> int:
        """Return the number of files currently in the skip cache."""
        return self.conn.execute(
            "SELECT COUNT(*) FROM skipped_files").fetchone()[0]

    # ---- decisions ----------------------------------------------------------

    def clear_pending_decisions(self) -> int:
        """Delete all rows where status='pending' and return the count."""
        cur = self.conn.execute("DELETE FROM decisions WHERE status = 'pending'")
        self.conn.commit()
        return cur.rowcount

    def insert_pending_decision(
        self,
        path: str,
        rules_fired: list[str],
        target: str,
        projected_savings_mb: float | None,
        run_id: int | None = None,
    ) -> int:
        """Insert a row with status='pending' and return its row id.

        `run_id` records which `runs` row created the pending row (typically
        the current `cmd_plan` run). The apply step overwrites this with its
        own run id when it terminalises the row, so `decisions_for_run` keys
        on the *apply* run — that's the run the post-run report describes.
        """
        cur = self.conn.execute(
            "INSERT INTO decisions "
            "(path, decided_at, rules_fired_json, target, "
            "projected_savings_mb, status, run_id) "
            "VALUES (?, ?, ?, ?, ?, 'pending', ?)",
            (path, time.time(), json.dumps(rules_fired),
             target, projected_savings_mb, run_id),
        )
        self.conn.commit()
        return int(cur.lastrowid or 0)

    def list_pending_decisions(self) -> list[dict]:
        """Return pending rows as dicts ordered by projected savings (desc)."""
        cur = self.conn.execute(
            "SELECT * FROM decisions WHERE status = 'pending' "
            "ORDER BY COALESCE(projected_savings_mb, 0) DESC"
        )
        return [dict(row) for row in cur]

    def mark_decision(
        self,
        decision_id: int,
        status: str,
        output_path: str | None = None,
        actual_savings_mb: float | None = None,
        error: str | None = None,
        run_id: int | None = None,
    ) -> None:
        """Update a decision row's status and outcome fields.

        If `run_id` is given, also overwrite the row's run_id with the apply
        run id so `decisions_for_run(apply_run_id)` returns exactly the rows
        terminalised in that apply.
        """
        if run_id is not None:
            self.conn.execute(
                "UPDATE decisions SET status = ?, output_path = ?, "
                "actual_savings_mb = ?, error = ?, run_id = ? WHERE id = ?",
                (status, output_path, actual_savings_mb, error,
                 run_id, decision_id),
            )
        else:
            self.conn.execute(
                "UPDATE decisions SET status = ?, output_path = ?, "
                "actual_savings_mb = ?, error = ? WHERE id = ?",
                (status, output_path, actual_savings_mb, error, decision_id),
            )
        self.conn.commit()

    def stamp_decision_run(self, decision_id: int,
                           run_id: int | None) -> None:
        """Update a decision row's run_id without touching status / outcome.

        Used by dry-run, which observes a pending row but doesn't terminalise
        it — yet still needs the row to surface in the post-run report.
        Caller passing `run_id=None` is a no-op (e.g. an apply context
        without a stashed run id, which shouldn't normally happen).
        """
        if run_id is None:
            return
        self.conn.execute(
            "UPDATE decisions SET run_id = ? WHERE id = ?",
            (run_id, decision_id),
        )
        self.conn.commit()

    def decisions_for_run(self, run_id: int) -> list[dict]:
        """Return decisions associated with `run_id` for the post-run report.

        Ordered by actual savings descending so the report's biggest wins
        appear first. NULLs sort last (failed/skipped/dry-run rows).
        """
        cur = self.conn.execute(
            "SELECT path, status, output_path, actual_savings_mb, error "
            "FROM decisions WHERE run_id = ? "
            "ORDER BY COALESCE(actual_savings_mb, -1) DESC, id ASC",
            (run_id,),
        )
        return [dict(row) for row in cur]

    def get_run(self, run_id: int) -> dict | None:
        """Fetch a single run row by id, or None if it doesn't exist."""
        row = self.conn.execute(
            "SELECT * FROM runs WHERE id = ?", (run_id,)
        ).fetchone()
        return dict(row) if row else None

    # ---- runs ---------------------------------------------------------------

    def start_run(self, kind: str, root: str | None, args: dict) -> int:
        """Insert a new run row and return its id."""
        cur = self.conn.execute(
            "INSERT INTO runs (kind, started_at, root, args_json) VALUES (?, ?, ?, ?)",
            (kind, time.time(), root, json.dumps(args)),
        )
        self.conn.commit()
        return int(cur.lastrowid or 0)

    def end_run(self, run_id: int, summary: dict) -> None:
        """Mark a run finished with end timestamp and summary JSON."""
        self.conn.execute(
            "UPDATE runs SET ended_at = ?, summary_json = ? WHERE id = ?",
            (time.time(), json.dumps(summary), run_id),
        )
        self.conn.commit()

    def recent_runs(self, limit: int = 10) -> list[dict]:
        """Return the most recent run rows as dicts."""
        cur = self.conn.execute(
            "SELECT * FROM runs ORDER BY started_at DESC LIMIT ?",
            (limit,),
        )
        return [dict(row) for row in cur]
