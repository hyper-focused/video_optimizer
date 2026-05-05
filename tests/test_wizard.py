"""Wizard prompt-sequence tests (Task #13).

The wizard (`optimizer.cli.cmd_wizard`) is a Q&A surface composed of the
real subcommand handlers (`cmd_doctor`, `cmd_scan`, `cmd_plan`,
`cmd_optimize`, `cmd_cleanup`). These tests drive it via mocked
`builtins.input` and stub out the heavy handlers so we exercise the
prompt sequence and the abort/exit-130 paths without touching ffmpeg,
ffprobe, or the rules engine.

The mock list of answers for `input` must match the *expected* number of
prompts in order; if the wizard asks more than expected, `StopIteration`
surfaces as a clean test failure that points at the prompt-flow drift.
"""

from __future__ import annotations

import argparse
import io
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from optimizer import cli as cli_mod


def _make_args(db_path: Path) -> argparse.Namespace:
    """Minimal Namespace for cmd_wizard (it only reads `args.db`)."""
    return argparse.Namespace(cmd="wizard", db=db_path)


def _fake_pending() -> list[dict]:
    """Two minimal pending-decision dicts. Shape only; the wizard summary
    reads `path` via `_load_probe_for_decision` (which we bypass by
    patching `_wizard_estimate_seconds` directly)."""
    return [
        {"id": 1, "path": "/lib/a.mkv", "projected_savings_mb": 100.0},
        {"id": 2, "path": "/lib/b.mkv", "projected_savings_mb": 50.0},
    ]


class WizardPromptSequenceTests(unittest.TestCase):
    """Drive cmd_wizard end-to-end with mocked input + stubbed handlers."""

    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)
        self.tmp_root = Path(self._tmp.name)
        self.db_path = self.tmp_root / "state.db"
        # Real on-disk dir so _wizard_pick_path's is_dir() check passes.
        self.lib = self.tmp_root / "lib"
        self.lib.mkdir()

    # ---- shared patch helpers ------------------------------------------------

    def _patch_handlers(self, doctor_rc: int = 0):
        """Patch the heavy handlers + the estimator + the pending list.

        Returns a tuple of mocks: (doctor, scan, plan, optimize, cleanup,
        list_pending) so individual tests can assert on call counts.
        """
        doctor = patch.object(cli_mod, "cmd_doctor",
                              return_value=doctor_rc).start()
        scan = patch.object(cli_mod, "cmd_scan", return_value=0).start()
        plan = patch.object(cli_mod, "cmd_plan", return_value=0).start()
        optimize = patch.object(cli_mod, "cmd_optimize",
                                return_value=0).start()
        cleanup = patch.object(cli_mod, "cmd_cleanup",
                               return_value=0).start()
        # _wizard_estimate_seconds touches the probe cache; bypass it.
        patch.object(cli_mod, "_wizard_estimate_seconds",
                     return_value=(1, 1, 7200)).start()
        # list_pending_decisions returns 2 candidates so the summary fires.
        list_pending = patch.object(
            cli_mod.Database, "list_pending_decisions",
            return_value=_fake_pending(),
        ).start()
        # _wizard_run_cleanup_prompt reads recent_runs; an empty list is fine
        # because it short-circuits when encoded < 1.
        patch.object(cli_mod.Database, "recent_runs",
                     return_value=[]).start()
        self.addCleanup(patch.stopall)
        return doctor, scan, plan, optimize, cleanup, list_pending

    # ---- 1. happy-path-with-quit --------------------------------------------

    def test_quit_at_summary_does_not_encode(self) -> None:
        """User picks beside-mode then quits at the all/N/quit prompt."""
        _, scan, plan, optimize, cleanup, _ = self._patch_handlers()
        answers = [str(self.lib), "1", "q"]
        with patch("builtins.input", side_effect=answers), \
                patch("sys.stdout", new_callable=io.StringIO):
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(rc, 0)
        self.assertEqual(scan.call_count, 1)
        self.assertEqual(plan.call_count, 1)
        self.assertEqual(optimize.call_count, 0)
        self.assertEqual(cleanup.call_count, 0)

    # ---- 2. abort at confirmation ------------------------------------------

    def test_no_at_confirmation_does_not_encode(self) -> None:
        """User reaches the final 'Proceed?' prompt and answers no."""
        _, scan, plan, optimize, _, _ = self._patch_handlers()
        # path -> mode 1 -> 'a' (all) -> confirmation 'n'
        answers = [str(self.lib), "1", "a", "n"]
        with patch("builtins.input", side_effect=answers), \
                patch("sys.stdout", new_callable=io.StringIO):
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(rc, 0)
        self.assertEqual(scan.call_count, 1)
        self.assertEqual(plan.call_count, 1)
        self.assertEqual(optimize.call_count, 0)

    # ---- 3. doctor warning, user aborts ------------------------------------

    def test_doctor_warning_user_aborts(self) -> None:
        """Doctor fails, user answers 'n' to the continue-anyway prompt."""
        _, scan, plan, optimize, _, _ = self._patch_handlers(doctor_rc=1)
        with patch("builtins.input", side_effect=["n"]), \
                patch("sys.stdout", new_callable=io.StringIO):
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(rc, 0)
        self.assertEqual(scan.call_count, 0)
        self.assertEqual(plan.call_count, 0)
        self.assertEqual(optimize.call_count, 0)

    # ---- 4. doctor warning, user proceeds ----------------------------------

    def test_doctor_warning_user_proceeds(self) -> None:
        """Doctor fails, user answers 'y' and the wizard continues."""
        _, scan, plan, optimize, _, _ = self._patch_handlers(doctor_rc=1)
        # 'y' -> path -> mode 1 -> 'q' (quit at summary)
        answers = ["y", str(self.lib), "1", "q"]
        with patch("builtins.input", side_effect=answers), \
                patch("sys.stdout", new_callable=io.StringIO):
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(rc, 0)
        self.assertEqual(scan.call_count, 1)
        self.assertEqual(plan.call_count, 1)
        self.assertEqual(optimize.call_count, 0)

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

    # ---- 7. invalid path retry cap -----------------------------------------

    def test_invalid_paths_bail_after_retry_cap(self) -> None:
        """_wizard_pick_path retries up to 3 times then aborts via SystemExit."""
        self._patch_handlers()
        # All three answers are non-existent dirs -> three failed loops ->
        # _WizardAbort raised -> outer wizard exits 130.
        bad = ["/does/not/exist", "/also/missing", "/still/missing"]
        with patch("builtins.input", side_effect=bad), \
                patch("sys.stdout", new_callable=io.StringIO), \
                patch("sys.stderr", new_callable=io.StringIO):
            with self.assertRaises(SystemExit) as ctx:
                cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(ctx.exception.code, 130)

    # ---- 8. mode-menu re-prompt --------------------------------------------

    def test_mode_menu_invalid_choice_reprompts(self) -> None:
        """An invalid mode-menu choice should re-ask, not abort."""
        _, scan, plan, optimize, _, _ = self._patch_handlers()
        # path -> '9' (invalid, re-prompted) -> '1' -> 'q'
        answers = [str(self.lib), "9", "1", "q"]
        with patch("builtins.input", side_effect=answers) as inp, \
                patch("sys.stdout", new_callable=io.StringIO) as out:
            rc = cli_mod.cmd_wizard(_make_args(self.db_path))
        self.assertEqual(rc, 0)
        # All four answers consumed: path + bad-choice + good-choice + quit.
        self.assertEqual(inp.call_count, 4)
        # The "please choose" hint is the canary for the re-prompt branch
        # in `_prompt`.
        self.assertIn("please choose", out.getvalue())
        self.assertEqual(scan.call_count, 1)
        self.assertEqual(plan.call_count, 1)
        self.assertEqual(optimize.call_count, 0)


if __name__ == "__main__":
    unittest.main()
