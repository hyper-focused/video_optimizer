"""Wizard prompt-sequence tests.

The wizard (``optimizer.cli.cmd_wizard``) asks for a path, mode, tier,
and codec exemptions, previews candidates in memory, then asks for a
limit before encoding through ``run_job``. It must not insert
``status='pending'`` rows.

``input`` answers match the prompt count. An extra prompt raises
``StopIteration``. ffmpeg and ffprobe are stubbed.

Every test installs a guard that counts a pending INSERT on any
connection the wizard opens, then fails if one happened or a pending
row is left behind. If ``cmd_plan`` exists, calling it fails the test
too. Stubbing the insert to a no-op would hide the queue write.
"""

from __future__ import annotations

import argparse
import io
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from optimizer import cli as cli_mod
from optimizer import db as db_mod
from optimizer.db import Database
from optimizer.models import ProbeResult
from tests._fixtures import make_probe


def _inserts_pending(sql: object, parameters: object = ()) -> bool:
    """True when `sql` is an INSERT that stores status='pending'."""
    if not isinstance(sql, str):
        return False
    head = " ".join(sql.split()).lower()
    if not head.startswith("insert"):
        return False
    if "'pending'" in head or '"pending"' in head:
        return True
    if isinstance(parameters, dict):
        values = list(parameters.values())
    elif isinstance(parameters, (list, tuple)):
        values = list(parameters)
    else:
        return False
    return any(value == "pending" for value in values)


class _WatchConn:
    """Connection proxy that counts pending inserts. Forwards everything else."""

    def __init__(self, conn: sqlite3.Connection, state: dict[str, int]) -> None:
        object.__setattr__(self, "_conn", conn)
        object.__setattr__(self, "_state", state)

    def execute(self, sql: str, parameters: object = ()) -> sqlite3.Cursor:
        if _inserts_pending(sql, parameters):
            self._state["insert"] += 1
        return self._conn.execute(sql, parameters)

    def executemany(self, sql: str, seq: object) -> sqlite3.Cursor:
        rows = list(seq)  # type: ignore[arg-type]
        for parameters in rows:
            if _inserts_pending(sql, parameters):
                self._state["insert"] += 1
        return self._conn.executemany(sql, rows)

    def executescript(self, sql: str) -> sqlite3.Cursor:
        lowered = sql.lower()
        if "insert" in lowered and (
            "'pending'" in lowered or '"pending"' in lowered
        ):
            self._state["insert"] += 1
        return self._conn.executescript(sql)

    def __setattr__(self, name: str, value: object) -> None:
        setattr(self._conn, name, value)

    def __getattr__(self, name: str) -> object:
        return getattr(self._conn, name)


def _make_args(db_path: Path) -> argparse.Namespace:
    """Minimal Namespace for cmd_wizard (it only reads ``args.db``)."""
    return argparse.Namespace(cmd="wizard", db=db_path)


def _video(path: Path, *, height: int, bitrate: int) -> ProbeResult:
    """Sparse file at the scan size gate plus a matching probe."""
    with path.open("wb") as fh:
        fh.truncate(cli_mod.MIN_PROBE_SIZE_BYTES)
    st = path.stat()
    pr = make_probe(
        height=height, codec="hevc", video_bitrate=bitrate, duration=1.0,
    )
    pr.path = str(path)
    pr.size = st.st_size
    pr.mtime = st.st_mtime
    return pr


class WizardPromptSequenceTests(unittest.TestCase):
    """Drive cmd_wizard with mocked input. Preview uses the real decide."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_root = Path(self._tmp.name)
        self.db_path = self.tmp_root / "state.db"
        self.lib = self.tmp_root / "lib"
        self.lib.mkdir()
        self.uhd = self.lib / "uhd.mkv"
        self.sd = self.lib / "sd.mkv"
        uhd_pr = _video(self.uhd, height=2160, bitrate=40_000_000)
        sd_pr = _video(self.sd, height=480, bitrate=8_000_000)
        with Database(self.db_path) as db:
            db.upsert_probe(uhd_pr)
            db.upsert_probe(sd_pr)
        self._probes = {
            str(self.uhd): uhd_pr,
            str(self.uhd.resolve()): uhd_pr,
            str(self.sd): sd_pr,
            str(self.sd.resolve()): sd_pr,
        }

    def _patch_handlers(self, doctor_rc: int = 0) -> tuple:
        """Stub doctor, the estimator, and optimize/cleanup entry points.

        Pending inserts are counted on the real connection. Returns
        (doctor, optimize, cleanup).
        """
        doctor = patch.object(
            cli_mod, "cmd_doctor", return_value=doctor_rc,
        ).start()
        optimize = patch.object(
            cli_mod, "cmd_optimize", return_value=0,
        ).start()
        cleanup = patch.object(
            cli_mod, "cmd_cleanup", return_value=0,
        ).start()
        patch.object(
            cli_mod, "_wizard_estimate_seconds",
            return_value=(1, 1, 1, 7800),
        ).start()
        patch.object(cli_mod.Database, "recent_runs", return_value=[]).start()
        patch.object(
            cli_mod.probe, "probe_file", side_effect=self._fake_probe,
        ).start()

        state: dict[str, int] = {"plan": 0, "insert": 0}
        if hasattr(cli_mod, "cmd_plan"):
            real_plan = cli_mod.cmd_plan

            def _plan(plan_args: argparse.Namespace) -> int:
                state["plan"] += 1
                return real_plan(plan_args)

            patch.object(cli_mod, "cmd_plan", _plan).start()

        real_connect = db_mod.sqlite3.connect

        def _connect(*args: object, **kwargs: object) -> _WatchConn:
            conn = real_connect(*args, **kwargs)
            return _WatchConn(conn, state)

        patch.object(db_mod.sqlite3, "connect", _connect).start()
        self._queue_state = state
        self.addCleanup(patch.stopall)
        self.addCleanup(self._assert_no_pending_queue)
        return doctor, optimize, cleanup

    def _fake_probe(self, fp: Path) -> ProbeResult:
        pr = self._probes.get(str(fp))
        if pr is None:
            pr = self._probes.get(str(fp.resolve()))
        if pr is None:
            msg = f"unexpected probe {fp}"
            raise AssertionError(msg)
        st = fp.stat()
        pr.size = st.st_size
        pr.mtime = st.st_mtime
        return pr

    def _pending_count(self) -> int:
        if not self.db_path.exists():
            return 0
        conn = sqlite3.connect(self.db_path)
        try:
            return int(conn.execute(
                "SELECT COUNT(*) FROM decisions WHERE status = 'pending'",
            ).fetchone()[0])
        except sqlite3.OperationalError:
            return 0
        finally:
            conn.close()

    def _assert_no_pending_queue(self) -> None:
        """Fail if the wizard called plan or inserted a pending row."""
        state = getattr(self, "_queue_state", {"plan": 0, "insert": 0})
        self.assertEqual(
            (state["plan"], state["insert"], self._pending_count()),
            (0, 0, 0),
            "wizard must not call cmd_plan or insert a pending row",
        )

    def _run(self, answers: list[str]) -> tuple[int, str]:
        with patch("builtins.input", side_effect=answers), \
                patch("sys.stdout", new_callable=io.StringIO) as out:
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        return rc, out.getvalue()

    # ---- 1. happy-path-with-quit --------------------------------------------

    def test_quit_at_summary_does_not_encode(self) -> None:
        """User picks keep-mode + all-tiers then quits at the limit prompt."""
        _, optimize, cleanup = self._patch_handlers()
        answers = [str(self.lib), "1", "a", "n", "q"]
        rc, out = self._run(answers)
        self.assertEqual(rc, 0, out)
        self.assertIn("Found 2 candidate(s)", out)
        self.assertEqual(optimize.call_count, 0)
        self.assertEqual(cleanup.call_count, 0)
        self._assert_no_pending_queue()

    # ---- 2. abort at confirmation ------------------------------------------

    def test_no_at_confirmation_does_not_encode(self) -> None:
        """User reaches the final 'Proceed?' prompt and answers no."""
        _, optimize, _cleanup = self._patch_handlers()
        answers = [str(self.lib), "1", "a", "n", "a", "n"]
        rc, out = self._run(answers)
        self.assertEqual(rc, 0, out)
        self.assertIn("Found 2 candidate(s)", out)
        self.assertIn("nothing encoded", out)
        self.assertEqual(optimize.call_count, 0)
        self._assert_no_pending_queue()

    # ---- 3. doctor warning, user aborts ------------------------------------

    def test_doctor_warning_user_aborts(self) -> None:
        """Doctor fails, user answers 'n' to the continue-anyway prompt."""
        _, optimize, _cleanup = self._patch_handlers(doctor_rc=1)
        with patch("builtins.input", side_effect=["n"]), \
                patch("sys.stdout", new_callable=io.StringIO):
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(rc, 0)
        self.assertEqual(optimize.call_count, 0)
        self._assert_no_pending_queue()

    # ---- 4. doctor warning, user proceeds ----------------------------------

    def test_doctor_warning_user_proceeds(self) -> None:
        """Doctor fails, user answers 'y' and the wizard continues."""
        _, optimize, _cleanup = self._patch_handlers(doctor_rc=1)
        answers = ["y", str(self.lib), "1", "a", "n", "q"]
        rc, out = self._run(answers)
        self.assertEqual(rc, 0, out)
        self.assertIn("Found 2 candidate(s)", out)
        self.assertEqual(optimize.call_count, 0)
        self._assert_no_pending_queue()

    # ---- 5. KeyboardInterrupt -> 130 ---------------------------------------

    def test_keyboard_interrupt_exits_130(self) -> None:
        """Ctrl-C at any prompt should unwind via _WizardAbort -> exit 130."""
        self._patch_handlers()
        with patch("builtins.input", side_effect=KeyboardInterrupt), \
                patch("sys.stdout", new_callable=io.StringIO), \
                patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(ctx.exception.code, 130)
        self._assert_no_pending_queue()

    # ---- 6. EOFError -> 130 -------------------------------------------------

    def test_eof_error_exits_130(self) -> None:
        """Closed stdin (EOF) should unwind the same way as Ctrl-C."""
        self._patch_handlers()
        with patch("builtins.input", side_effect=EOFError), \
                patch("sys.stdout", new_callable=io.StringIO), \
                patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(ctx.exception.code, 130)
        self._assert_no_pending_queue()

    # ---- 7. invalid path retry cap -----------------------------------------

    def test_invalid_paths_bail_after_retry_cap(self) -> None:
        """_wizard_pick_path retries up to 3 times then aborts via SystemExit."""
        self._patch_handlers()
        bad = ["/does/not/exist", "/also/missing", "/still/missing"]
        with patch("builtins.input", side_effect=bad), \
                patch("sys.stdout", new_callable=io.StringIO), \
                patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(ctx.exception.code, 130)
        self._assert_no_pending_queue()

    # ---- 8. mode-menu re-prompt --------------------------------------------

    def test_mode_menu_invalid_choice_reprompts(self) -> None:
        """An invalid mode-menu choice should re-ask, not abort."""
        _, optimize, _cleanup = self._patch_handlers()
        answers = [str(self.lib), "9", "1", "a", "n", "q"]
        with patch("builtins.input", side_effect=answers) as inp, \
                patch("sys.stdout", new_callable=io.StringIO) as out:
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        text = out.getvalue()
        self.assertEqual(rc, 0, text)
        self.assertEqual(inp.call_count, 6)
        self.assertIn("please choose", text)
        self.assertIn("Found 2 candidate(s)", text)
        self.assertEqual(optimize.call_count, 0)
        self._assert_no_pending_queue()

    # ---- 9. tier-pick prompt routes to the right preset list ---------------

    def test_tier_pick_uhd_routes_single_preset(self) -> None:
        """Tier 'u' should run the UHD preset only, not all three."""
        _doctor, optimize, _cleanup = self._patch_handlers()
        ran: list[tuple[str, ...]] = []

        def fake_pipeline(args, presets, *, label):
            ran.append(presets)
            return 0

        with patch.object(cli_mod, "_run_path_pipeline",
                          side_effect=fake_pipeline), \
                patch("builtins.input",
                      side_effect=[str(self.lib), "1", "u", "n", "a", "y", "n"]), \
                patch("sys.stdout", new_callable=io.StringIO) as out:
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        text = out.getvalue()
        self.assertEqual(rc, 0, text)
        self.assertIn("Found 2 candidate(s)", text)
        self.assertEqual(ran, [("UHD",)])
        self.assertEqual(optimize.call_count, 0)
        self._assert_no_pending_queue()

    def test_proceed_no_does_not_invoke_pipeline(self) -> None:
        """User answers 'n' at Proceed. The encode pipeline must not fire."""
        _doctor, optimize, _cleanup = self._patch_handlers()
        ran: list[tuple[str, ...]] = []

        def fake_pipeline(args, presets, *, label):
            ran.append(presets)
            return 0

        with patch.object(cli_mod, "_run_path_pipeline",
                          side_effect=fake_pipeline), \
                patch("builtins.input",
                      side_effect=[str(self.lib), "1", "a", "n", "a", "n"]), \
                patch("sys.stdout", new_callable=io.StringIO) as out:
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        text = out.getvalue()
        self.assertEqual(rc, 0, text)
        self.assertEqual(ran, [], "pipeline must not run when the user said n")
        self.assertEqual(optimize.call_count, 0)
        self._assert_no_pending_queue()

    def test_tier_pick_all_routes_three_presets(self) -> None:
        _doctor, optimize, _cleanup = self._patch_handlers()
        ran: list[tuple[str, ...]] = []

        def fake_pipeline(args, presets, *, label):
            ran.append(presets)
            return 0

        with patch.object(cli_mod, "_run_path_pipeline",
                          side_effect=fake_pipeline), \
                patch("builtins.input",
                      side_effect=[str(self.lib), "1", "a", "n", "a", "y", "n"]), \
                patch("sys.stdout", new_callable=io.StringIO) as out:
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        text = out.getvalue()
        self.assertEqual(rc, 0, text)
        self.assertIn("Found 2 candidate(s)", text)
        self.assertEqual(ran, [("UHD", "HD", "SD")])
        self.assertEqual(optimize.call_count, 0)
        self._assert_no_pending_queue()

    def test_unselected_tier_is_not_stored(self) -> None:
        """UHD-only encode stores that file once. The SD file is absent.

        A pending row plus a completed row, or a pending row for the
        tier the user did not pick, fails this test.
        """
        self._patch_handlers()
        seen: list[int] = []

        def fake_execute(db, dec, pr, _cmd, _desc, output_path,
                         args, label="", **_kw):
            seen.append(pr.height)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_bytes(b"o" * 128)
            actual_mb = cli_mod._finalize_output(  # noqa: SLF001
                pr, output_path, args, db, dec,
            )
            return "applied", int(actual_mb * 1024 * 1024)

        answers = [str(self.lib), "1", "u", "n", "a", "y"]
        with patch.object(cli_mod.encoder, "select_encoder",
                          return_value="libsvtav1"), \
                patch.object(cli_mod.encoder, "validate_output",
                             return_value=(True, "")), \
                patch.object(cli_mod, "_execute_encode",
                             side_effect=fake_execute), \
                patch("builtins.input", side_effect=answers), \
                patch("sys.stdout", new_callable=io.StringIO) as out:
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        text = out.getvalue()
        self.assertEqual(rc, 0, text)
        self.assertIn("Found 2 candidate(s)", text)
        self.assertEqual(seen, [2160], text)

        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        decisions = [dict(r) for r in conn.execute(
            "SELECT * FROM decisions ORDER BY id",
        )]
        conn.close()
        self.assertEqual(self._pending_count(), 0, decisions)
        self.assertEqual(len(decisions), 1, decisions)
        self.assertEqual(decisions[0]["path"], str(self.uhd))
        self.assertEqual(decisions[0]["status"], "completed")
        self.assertTrue(self.uhd.exists())
        self.assertTrue(self.sd.exists())
        self._assert_no_pending_queue()


if __name__ == "__main__":
    unittest.main()
