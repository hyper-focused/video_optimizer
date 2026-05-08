"""--dry-run must short-circuit before DV prep.

The DV-prep stream-copy stage writes a ~50 GB temp file when the source
is a UHD remux. Before this regression pin, a dry-run run on a library
with DV titles would happily run the strip step on the first DV
candidate, defeating the entire point of `--dry-run`. The fix moves
the dry-run check ahead of `_prepare_dv_source`.
"""

from __future__ import annotations

import argparse
import io
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from optimizer import cli as cli_mod
from optimizer.db import Database
from tests._fixtures import make_probe


class DryRunSkipsDvPrepTests(unittest.TestCase):
    def test_dv_source_with_dry_run_does_not_call_prepare(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src.mkv"
            source.write_bytes(b"x" * 1024)
            output = tmp_path / "src.AV1.REENCODE.mkv"

            pr = make_probe(height=2160, dv_profile=8)
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

                args = argparse.Namespace(
                    dry_run=True,
                    quality=15,
                    auto_relax_cq=True,
                    verbose=False,
                    timeout=0,
                    mode="keep",
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

                # The whole point of the regression: _prepare_dv_source
                # must NOT be called when dry_run is set, because it
                # would otherwise write a multi-GB stream-copy temp
                # file before printing the dry-run command.
                with patch.object(cli_mod, "_prepare_dv_source") as m_prep, \
                     patch.object(cli_mod, "_build_apply_command",
                                  return_value=(["ffmpeg", "stub"],
                                                "encode (stub)")):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        status, saved = cli_mod._apply_one_after_validation(
                            db, row, pr, args, run_id, output,
                            "av1+mkv", "av1_qsv", ["en", "und"], 1, 1,
                        )

                m_prep.assert_not_called()

            self.assertEqual(status, "dry_run")
            self.assertEqual(saved, 0)
            # The decision row should be stamped with the run but not
            # transitioned to a terminal status by the dry-run path.
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                final = dict(conn.execute(
                    "SELECT status, run_id FROM decisions WHERE id = ?",
                    (dec_id,),
                ).fetchone())
            self.assertEqual(final["status"], "pending")
            self.assertEqual(final["run_id"], run_id)


if __name__ == "__main__":
    unittest.main()
