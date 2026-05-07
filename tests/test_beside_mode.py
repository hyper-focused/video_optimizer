"""Beside output mode (`--mode beside`): write next to source, never
touch the original.

Beside mode was added in CLI v2 phase 1 to replace the old "side mode
into a parallel directory" default for the friendly `optimize` command.
The contract:

  * `_compute_output_path` returns a sibling of the source (same parent
    directory). Collision safety relies on the caller providing
    `--rewrite-codec` and/or `--reencode-tag` so that the output stem
    differs from the source stem (e.g. ``foo.mkv → foo.AV1.REENCODE.mkv``).
  * `_finalize_output` does *not* recycle, back up, or unlink the
    source — the original stays untouched and the operator (or a
    follow-up `cleanup --run N` command) decides when to remove it.
  * `cmd_apply` rejects the combination of ``--mode beside`` and
    ``--output-root`` because the latter is meaningless without `side`.

These tests pin all three behaviors. The end-to-end test patches
`encoder.select_encoder` and `_execute_encode` so no real ffmpeg runs;
it only verifies that `cmd_apply` plumbs the beside contract end-to-end
(source untouched, output recorded, decision marked completed).
"""

from __future__ import annotations

import argparse
import io
import json
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest.mock import patch

from optimizer import cli as cli_mod
from optimizer.cli import (
    _compute_output_path,
    _finalize_output,
    cmd_apply,
)
from optimizer.db import Database
from tests._fixtures import make_probe


def _beside_args(**overrides) -> argparse.Namespace:
    """Build a Namespace that satisfies _compute_output_path / _finalize_output.

    Default knobs match what the v2 `optimize` wizard sets for beside
    mode: rewrite_codec + reencode_tag on (so the output stem differs
    from the source stem), no side-mode roots.
    """
    ns = argparse.Namespace(
        mode="beside",
        output_root=None,
        source_root=None,
        rewrite_codec=True,
        reencode_tag=True,
        reencode_tag_value="REENCODE",
        no_dotted=False,
        name_suffix="",
        backup=None,
        recycle_to=None,
    )
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


class ComputeOutputPathBesideTests(unittest.TestCase):
    """`_compute_output_path` writes to the source's parent directory."""

    def test_output_is_sibling_of_source(self):
        pr = make_probe()
        pr.path = "/movies/foo.mkv"
        out = _compute_output_path(pr, _beside_args(), "av1+mkv")
        self.assertEqual(out.parent, Path("/movies"))

    def test_collision_safe_filename_when_extensions_match(self):
        """Source `.mkv` -> AV1+mkv target also `.mkv`. The
        --rewrite-codec / --reencode-tag pair guarantees the output
        stem differs from the source stem; assert that and that the
        directory is shared."""
        pr = make_probe()
        pr.path = "/movies/foo.mkv"
        out = _compute_output_path(pr, _beside_args(), "av1+mkv")

        self.assertNotEqual(out, Path(pr.path))
        self.assertEqual(out.parent, Path(pr.path).parent)
        self.assertEqual(out.name, "foo.AV1.REENCODE.mkv")

    def test_nested_path_keeps_directory(self):
        pr = make_probe()
        pr.path = "/data/library/Movies/Foo (2020)/Foo (2020).mkv"
        out = _compute_output_path(pr, _beside_args(), "av1+mkv")
        self.assertEqual(out.parent, Path(pr.path).parent)
        self.assertEqual(out.name, "Foo.(2020).AV1.REENCODE.mkv")


class FinalizeOutputBesideTests(unittest.TestCase):
    """`_finalize_output` records the encode without touching the original."""

    def test_source_and_output_both_remain_after_finalize(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "foo.mkv"
            output = tmp_path / "foo.AV1.REENCODE.mkv"
            source.write_bytes(b"x" * 4096)
            output.write_bytes(b"y" * 1024)

            pr = make_probe()
            pr.path = str(source)
            pr.size = source.stat().st_size

            db_path = tmp_path / "state.db"
            with Database(db_path) as db:
                # decisions has a FK on files(path); seed the probe row.
                db.upsert_probe(pr)
                run_id = db.start_run("apply", None, {})
                dec_id = db.insert_pending_decision(
                    str(source), ["over_bitrated"], "av1+mkv", 1.0,
                    run_id=run_id,
                )
                # Hand-fetch the dict shape `_finalize_output` expects.
                row = dict(db.conn.execute(
                    "SELECT * FROM decisions WHERE id = ?",
                    (dec_id,),
                ).fetchone())

                args = _beside_args()
                args._apply_run_id = run_id  # noqa: SLF001

                # Stub the post-encode ffprobe validation; the test's
                # fake output is a tiny zero-content file that wouldn't
                # pass duration-match against the synthetic 7200s probe,
                # but the test is about the no-touch contract for
                # beside mode, not about validation behavior itself.
                with patch.object(cli_mod.encoder, "validate_output",
                                  return_value=(True, "")):
                    actual_mb = _finalize_output(pr, output, args, db, row)

            # Source and output both still on disk — beside never
            # disposes of the original.
            self.assertTrue(source.exists())
            self.assertTrue(output.exists())
            # The decision row is now status='completed' with the
            # output path recorded.
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                final = dict(conn.execute(
                    "SELECT status, output_path FROM decisions WHERE id = ?",
                    (dec_id,),
                ).fetchone())
            self.assertEqual(final["status"], "completed")
            self.assertEqual(final["output_path"], str(output))
            # Savings number is positive (source was bigger than output).
            self.assertGreater(actual_mb, 0)


class CmdApplyValidationTests(unittest.TestCase):
    """`cmd_apply` rejects --mode beside paired with --output-root."""

    def test_beside_with_output_root_is_rejected(self):
        with tempfile.TemporaryDirectory() as tmp:
            args = argparse.Namespace(
                mode="beside",
                output_root=Path(tmp) / "outroot",
                source_root=None,
                backup=None,
                recycle_to=None,
                allow_hard_delete=False,
                auto=True,
                dry_run=False,
                db=Path(tmp) / "state.db",
            )
            buf = io.StringIO()
            with redirect_stderr(buf):
                rc = cmd_apply(args)
            self.assertEqual(rc, 2)
            self.assertIn("--mode beside", buf.getvalue())
            self.assertIn("--output-root", buf.getvalue())


class CmdApplyBesideE2ETests(unittest.TestCase):
    """Run cmd_apply with `_execute_encode` stubbed: source must remain."""

    def test_beside_apply_leaves_originals_untouched(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "foo.mkv"
            source.write_bytes(b"x" * 8192)

            pr = make_probe(codec="hevc")
            pr.path = str(source)
            pr.size = source.stat().st_size

            db_path = tmp_path / "state.db"
            with Database(db_path) as db:
                db.upsert_probe(pr)
                db.insert_pending_decision(
                    str(source), ["over_bitrated"], "av1+mkv", 100.0,
                )

            def fake_execute(db, dec, pr, cmd, desc, output_path,
                             args, label="", **_kw):
                """Stand-in for the real ffmpeg invocation: write a
                small output file, finalise via the real `_finalize_output`,
                return the same shape as `_execute_encode`."""
                output_path.parent.mkdir(parents=True, exist_ok=True)
                output_path.write_bytes(b"o" * 1024)
                actual_mb = cli_mod._finalize_output(  # noqa: SLF001
                    pr, output_path, args, db, dec,
                )
                return "applied", int(actual_mb * 1024 * 1024)

            args = argparse.Namespace(
                mode="beside",
                output_root=None,
                source_root=None,
                backup=None,
                recycle_to=None,
                allow_hard_delete=False,
                auto=True,
                dry_run=False,
                db=db_path,
                quality=21,
                hwaccel="auto",
                hw_decode=False,
                compat_audio=True,
                keep_langs="en,und",
                timeout=0,
                verbose=False,
                limit=0,
                min_height=None,
                max_height=None,
                rewrite_codec=True,
                reencode_tag=True,
                reencode_tag_value="REENCODE",
                no_dotted=False,
                name_suffix="",
                no_report=True,
            )

            with patch.object(cli_mod, "_execute_encode",
                              side_effect=fake_execute), \
                 patch.object(cli_mod.encoder, "select_encoder",
                              return_value="libsvtav1"), \
                 patch.object(cli_mod, "_build_apply_command",
                              return_value=(["ffmpeg"], "encode (stub)")), \
                 patch.object(cli_mod.encoder, "validate_output",
                              return_value=(True, "")):
                buf = io.StringIO()
                with redirect_stdout(buf):
                    rc = cmd_apply(args)

            self.assertEqual(rc, 0)
            # Source must still exist — beside mode never disposes of
            # the original.
            self.assertTrue(source.exists())
            expected_output = tmp_path / "foo.AV1.REENCODE.mkv"
            self.assertTrue(expected_output.exists())

            # The decision row should be completed with the beside path.
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                row = dict(conn.execute(
                    "SELECT status, output_path FROM decisions"
                ).fetchone())
            self.assertEqual(row["status"], "completed")
            self.assertEqual(row["output_path"], str(expected_output))
            # rules_fired_json round-tripped intact (sanity check on
            # the seeded row).
            with sqlite3.connect(str(db_path)) as conn:
                rules = json.loads(conn.execute(
                    "SELECT rules_fired_json FROM decisions"
                ).fetchone()[0])
            self.assertEqual(rules, ["over_bitrated"])


if __name__ == "__main__":
    unittest.main()
