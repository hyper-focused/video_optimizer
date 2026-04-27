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

CREATE INDEX IF NOT EXISTS idx_decisions_status ON decisions(status);
CREATE INDEX IF NOT EXISTS idx_decisions_path   ON decisions(path);
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
        self.conn.commit()

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
    ) -> int:
        """Insert a row with status='pending' and return its row id."""
        cur = self.conn.execute(
            "INSERT INTO decisions "
            "(path, decided_at, rules_fired_json, target, "
            "projected_savings_mb, status) "
            "VALUES (?, ?, ?, ?, ?, 'pending')",
            (path, time.time(), json.dumps(rules_fired),
             target, projected_savings_mb),
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
    ) -> None:
        """Update a decision row's status and outcome fields."""
        self.conn.execute(
            "UPDATE decisions SET status = ?, output_path = ?, "
            "actual_savings_mb = ?, error = ? WHERE id = ?",
            (status, output_path, actual_savings_mb, error, decision_id),
        )
        self.conn.commit()

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
