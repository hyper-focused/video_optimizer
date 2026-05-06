"""Argv-preprocessing dispatch matrix for `_preprocess_argv`.

The CLI v2 entry point pre-processes argv before argparse sees it so a
bare `<binary> <path>` invocation rewrites to `optimize <path>
--bare-invocation`, and a bare `<binary>` invocation on a TTY drops
into the wizard. This file exercises the predicate ladder documented
in `_preprocess_argv`'s docstring.
"""

from __future__ import annotations

import unittest
from unittest.mock import patch

from optimizer.cli import KNOWN_SUBCOMMANDS, _preprocess_argv
from optimizer.presets import PRESETS


class PreprocessArgvNoArgsTest(unittest.TestCase):
    """Bare invocation (no positional args) branches on TTY-ness."""

    def test_no_args_with_tty_drops_into_wizard(self) -> None:
        with patch("optimizer.cli.sys.stdin.isatty", return_value=True), \
             patch("optimizer.cli.sys.stdout.isatty", return_value=True):
            self.assertEqual(_preprocess_argv(["bin"]), ["bin", "wizard"])

    def test_no_args_without_stdin_tty_falls_through(self) -> None:
        # Piped stdin (e.g. cron, `echo ... | video_optimizer`) must NOT
        # launch the wizard — argparse should surface its top-level help.
        with patch("optimizer.cli.sys.stdin.isatty", return_value=False), \
             patch("optimizer.cli.sys.stdout.isatty", return_value=True):
            self.assertEqual(_preprocess_argv(["bin"]), ["bin"])

    def test_no_args_without_stdout_tty_falls_through(self) -> None:
        # Output redirected to a file/pipe — also non-interactive.
        with patch("optimizer.cli.sys.stdin.isatty", return_value=True), \
             patch("optimizer.cli.sys.stdout.isatty", return_value=False):
            self.assertEqual(_preprocess_argv(["bin"]), ["bin"])

    def test_no_args_neither_tty_falls_through(self) -> None:
        with patch("optimizer.cli.sys.stdin.isatty", return_value=False), \
             patch("optimizer.cli.sys.stdout.isatty", return_value=False):
            self.assertEqual(_preprocess_argv(["bin"]), ["bin"])


class PreprocessArgvHelpAndFlagsTest(unittest.TestCase):
    """Leading `-` (help, flags) must short-circuit before the bare-path
    rewrite — argparse owns the error/help surface in those cases."""

    def test_short_help_flag_unchanged(self) -> None:
        self.assertEqual(_preprocess_argv(["bin", "-h"]), ["bin", "-h"])

    def test_long_help_flag_unchanged(self) -> None:
        self.assertEqual(_preprocess_argv(["bin", "--help"]), ["bin", "--help"])

    def test_leading_flag_unchanged(self) -> None:
        # An unknown leading flag should not be wrapped in `optimize` — let
        # argparse complain about the missing subcommand.
        self.assertEqual(
            _preprocess_argv(["bin", "--verbose"]),
            ["bin", "--verbose"],
        )


class PreprocessArgvKnownSubcommandTest(unittest.TestCase):
    """Anything in KNOWN_SUBCOMMANDS passes straight through."""

    def test_scan_subcommand_unchanged(self) -> None:
        self.assertEqual(
            _preprocess_argv(["bin", "scan", "/some/path"]),
            ["bin", "scan", "/some/path"],
        )

    def test_optimize_subcommand_unchanged(self) -> None:
        # `optimize` is itself a known subcommand — invoking it explicitly
        # must not get the bare-invocation sentinel appended.
        self.assertEqual(
            _preprocess_argv(["bin", "optimize", "/some/path"]),
            ["bin", "optimize", "/some/path"],
        )

    def test_preset_subcommand_unchanged(self) -> None:
        # Presets share the dispatcher — `HD` is a subcommand too.
        self.assertEqual(
            _preprocess_argv(["bin", "HD", "--mode", "beside"]),
            ["bin", "HD", "--mode", "beside"],
        )


class PreprocessArgvBarePathRewriteTest(unittest.TestCase):
    """The fallthrough case: a non-flag, non-subcommand argv[1] is
    treated as a path and rewritten to `optimize <path> --bare-invocation`."""

    def test_bare_path_gets_optimize_and_sentinel(self) -> None:
        self.assertEqual(
            _preprocess_argv(["bin", "/path/to/movies"]),
            ["bin", "optimize", "/path/to/movies", "--bare-invocation"],
        )

    def test_bare_path_preserves_extra_args(self) -> None:
        # Trailing flags like --dry-run must survive the rewrite — they
        # belong to the implicit `optimize` subcommand.
        self.assertEqual(
            _preprocess_argv(["bin", "/path/to/movies", "--dry-run"]),
            ["bin", "optimize", "/path/to/movies", "--dry-run", "--bare-invocation"],
        )


class KnownSubcommandsRegistryTest(unittest.TestCase):
    """The KNOWN_SUBCOMMANDS frozenset is the single source of truth for
    the dispatch predicate; this test pins its expected membership so an
    accidental rename / removal trips the suite."""

    def test_contains_all_documented_subcommands(self) -> None:
        expected_core = {
            "scan", "reprobe", "plan", "apply", "status",
            "list-encoders", "replace-list", "doctor",
            "optimize", "cleanup", "wizard",
        }
        self.assertTrue(
            expected_core.issubset(KNOWN_SUBCOMMANDS),
            f"missing core subcommands: {expected_core - set(KNOWN_SUBCOMMANDS)}",
        )

    def test_contains_all_presets(self) -> None:
        # PRESETS keys must be reachable as subcommands so the dispatcher
        # treats e.g. `hd-archive` as known rather than a bare path.
        missing = set(PRESETS.keys()) - set(KNOWN_SUBCOMMANDS)
        self.assertFalse(missing, f"missing preset subcommands: {missing}")

    def test_currently_includes_sd_hd_uhd(self) -> None:
        # Sanity check on the current preset set — if these go away or
        # rename, _preprocess_argv's dispatch table needs review too.
        self.assertIn("SD", KNOWN_SUBCOMMANDS)
        self.assertIn("HD", KNOWN_SUBCOMMANDS)
        self.assertIn("UHD", KNOWN_SUBCOMMANDS)


if __name__ == "__main__":
    unittest.main()
