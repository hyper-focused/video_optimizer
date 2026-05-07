"""DV prep failures are classified as 'failed', not 'skipped'.

Run #166 (the live UHD test) hit this: The Housemaid 2025 (DV P7 +
HDR10Plus) crashed the `dovi_rpu=strip` ffmpeg subprocess with exit
234. Pre-fix, both "no strategy applies" *and* "strip command
crashed" returned `(None, None)` from `_prepare_dv_source`, so the
caller marked every DV prep miss as `status='skipped'` with the
generic policy code `dv_no_prep_strategy` — masking the real ffmpeg
failure. The fix introduces a third tuple slot for the error message
so the caller can distinguish:

  * (None, None, None)     → no strategy → 'skipped' with policy code
  * (None, None, err)      → real failure → 'failed' with err captured
  * (work_dir, path, None) → success
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from optimizer import cli as cli_mod
from optimizer.db import Database
from tests._fixtures import make_probe


def _args() -> argparse.Namespace:
    return argparse.Namespace(
        dry_run=False,
        quality=15,
        auto_relax_cq=True,
        verbose=False,
        timeout=0,
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
        keep_langs="en,und",
        hwaccel="auto",
        hw_decode=False,
        compat_audio=True,
        original_audio=False,
        original_subs=False,
        dv_p7_convert=False,
    )


class DvNoStrategyMarksSkippedTests(unittest.TestCase):
    """When `_prepare_dv_source` returns (None, None, None) — no
    strategy applies — the row should be 'skipped' with the policy
    code, not 'failed'. Mirrors P5 sources today."""

    def test_no_strategy_marks_skipped(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "p5.mkv"
            source.write_bytes(b"x" * 1024)
            output = tmp_path / "p5.AV1.REENCODE.mkv"

            pr = make_probe(height=2160, dv_profile=5)
            pr.path = str(source)
            pr.size = source.stat().st_size

            db_path = tmp_path / "state.db"
            with Database(db_path) as db:
                db.upsert_probe(pr)
                run_id = db.start_run("apply", None, {})
                dec_id = db.insert_pending_decision(
                    str(source), ["over_bitrated"], "av1+mkv", 0.0,
                    run_id=run_id,
                )
                row = dict(db.conn.execute(
                    "SELECT * FROM decisions WHERE id = ?", (dec_id,),
                ).fetchone())

                args = _args()
                args._apply_run_id = run_id  # noqa: SLF001

                # P5 → strategy is None → (None, None, None)
                with patch.object(cli_mod, "_prepare_dv_source",
                                  return_value=(None, None, None)):
                    status, _ = cli_mod._apply_one_after_validation(
                        db, row, pr, args, run_id, output,
                        "av1+mkv", "av1_qsv", ["en", "und"], 1, 1,
                    )

            self.assertEqual(status, "skipped")
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                final = dict(conn.execute(
                    "SELECT status, error FROM decisions WHERE id = ?", (dec_id,),
                ).fetchone())
            self.assertEqual(final["status"], "skipped")
            self.assertEqual(final["error"], "dv_no_prep_strategy")


class DvStripFailureMarksFailedTests(unittest.TestCase):
    """When the strip subprocess crashes, _prepare_dv_source returns
    (None, None, err). The caller should mark the row 'failed' with
    the captured err, not 'skipped' with the policy code."""

    def test_runtime_failure_marks_failed_with_real_error(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "housemaid.mkv"
            source.write_bytes(b"x" * 1024)
            output = tmp_path / "housemaid.AV1.REENCODE.mkv"

            pr = make_probe(height=2160, dv_profile=7)
            pr.path = str(source)
            pr.size = source.stat().st_size

            db_path = tmp_path / "state.db"
            with Database(db_path) as db:
                db.upsert_probe(pr)
                run_id = db.start_run("apply", None, {})
                dec_id = db.insert_pending_decision(
                    str(source), ["over_bitrated"], "av1+mkv", 0.0,
                    run_id=run_id,
                )
                row = dict(db.conn.execute(
                    "SELECT * FROM decisions WHERE id = ?", (dec_id,),
                ).fetchone())

                args = _args()
                args._apply_run_id = run_id  # noqa: SLF001

                # Strip ffmpeg crashed — third slot carries the message.
                err_msg = "dv_strip_failed: ffmpeg exited 234"
                with patch.object(cli_mod, "_prepare_dv_source",
                                  return_value=(None, None, err_msg)):
                    status, _ = cli_mod._apply_one_after_validation(
                        db, row, pr, args, run_id, output,
                        "av1+mkv", "av1_qsv", ["en", "und"], 1, 1,
                    )

            self.assertEqual(status, "failed")
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                final = dict(conn.execute(
                    "SELECT status, error FROM decisions WHERE id = ?", (dec_id,),
                ).fetchone())
            self.assertEqual(final["status"], "failed")
            # Real ffmpeg context is preserved, not replaced by the
            # generic skip code.
            self.assertEqual(final["error"], err_msg)
            self.assertNotEqual(final["error"], "dv_no_prep_strategy")


class PrepareDvSourceShapeTests(unittest.TestCase):
    """`_prepare_dv_source` itself returns the right shape per case."""

    def test_p5_returns_no_strategy_tuple(self):
        # P5 source: dv_strategy returns None, _prepare_dv_source
        # short-circuits at the strategy check and returns the
        # all-None tuple (no work_dir was even created).
        pr = make_probe(height=2160, dv_profile=5)
        pr.path = "/tmp/__test_p5_does_not_exist__.mkv"
        result = cli_mod._prepare_dv_source(pr, _args())
        self.assertEqual(result, (None, None, None))


if __name__ == "__main__":
    unittest.main()
