"""One job encodes every height band without a prior plan command.

No pending queue: each encoded file is born ``running`` and lands on
the single runs row. ffmpeg is stubbed the same way the other encode
tests stub it.
"""

from __future__ import annotations

import argparse
import io
import os
import sqlite3
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

from optimizer import cli as cli_mod
from optimizer.cli import cmd_optimize
from optimizer.db import Database
from optimizer.models import ProbeResult
from tests._fixtures import make_probe


def _seed(db: Database, path: Path, *, height: int,
          bitrate: int) -> ProbeResult:
    """Cache a probe whose size and mtime match the file on disk."""
    st = path.stat()
    pr = make_probe(
        height=height, codec="hevc", video_bitrate=bitrate, duration=1.0,
    )
    pr.path = str(path)
    pr.size = st.st_size
    pr.mtime = st.st_mtime
    db.upsert_probe(pr)
    return pr


class RunJobTwoBandTests(unittest.TestCase):
    """SD plus UHD, no plan, one run, zero pending rows."""

    def test_optimize_encodes_both_bands_on_one_run(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lib = root / "lib"
            lib.mkdir()
            # SD is the larger file so a savings-only sort would encode
            # it first. Preset order is UHD, then HD, then SD.
            sd = lib / "sd_feature.mkv"
            uhd = lib / "uhd_feature.mkv"
            sd.write_bytes(b"s" * (2 * 1024 * 1024))
            uhd.write_bytes(b"u" * (1 * 1024 * 1024))
            db_path = root / "state.db"
            with Database(db_path) as db:
                sd_pr = _seed(db, sd, height=480, bitrate=8_000_000)
                uhd_pr = _seed(db, uhd, height=2160, bitrate=40_000_000)
            by_path = {sd_pr.path: sd_pr, uhd_pr.path: uhd_pr}

            def fake_probe(fp: Path) -> ProbeResult:
                pr = by_path.get(str(fp))
                if pr is None:
                    msg = f"unexpected probe {fp}"
                    raise AssertionError(msg)
                return pr

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

            args = argparse.Namespace(
                path=lib,
                auto=True,
                mode="keep",
                output=None,
                replace=False,
                recycle_to=None,
                limit=0,
                dry_run=False,
                confirm=False,
                cleanup_after=False,
                verbose=False,
                workers=1,
                keep_langs=None,
                hwaccel="auto",
                hw_decode=None,
                quality=None,
                min_size=0,
                db=db_path,
                bare_invocation=False,
                skip_codecs="",
                allow_low_bitrate=False,
                allow_av1=False,
                allow_extras=False,
                original_audio=False,
                original_subs=False,
                no_report=True,
            )
            buf = io.StringIO()
            with patch.object(cli_mod.probe, "probe_file",
                              side_effect=fake_probe), \
                 patch.object(cli_mod.encoder, "select_encoder",
                              return_value="libsvtav1"), \
                 patch.object(cli_mod.encoder, "validate_output",
                              return_value=(True, "")), \
                 patch.object(cli_mod, "_execute_encode",
                              side_effect=fake_execute), \
                 redirect_stdout(buf):
                rc = cmd_optimize(args)
            out = buf.getvalue()
            self.assertEqual(rc, 0, out)
            self.assertEqual(seen, [2160, 480], out)

            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            runs = [dict(r) for r in conn.execute("SELECT * FROM runs")]
            decisions = [dict(r) for r in conn.execute(
                "SELECT * FROM decisions ORDER BY id",
            )]
            pending = conn.execute(
                "SELECT COUNT(*) FROM decisions WHERE status = 'pending'",
            ).fetchone()[0]
            conn.close()

            self.assertEqual(len(runs), 1, out)
            self.assertEqual(runs[0]["kind"], "optimize")
            self.assertIsNotNone(runs[0]["ended_at"])
            self.assertEqual(pending, 0, decisions)
            self.assertEqual(len(decisions), 2, out)
            self.assertEqual(
                {d["path"] for d in decisions}, {str(sd), str(uhd)},
            )
            run_id = runs[0]["id"]
            for dec in decisions:
                self.assertEqual(dec["status"], "completed")
                self.assertEqual(dec["run_id"], run_id)
                self.assertEqual(dec["owner_pid"], os.getpid())


class PathCommandFlagTests(unittest.TestCase):
    """Flags that used to live on plan/apply, now on the path command."""

    def test_allow_reencoded_reaches_decide(self) -> None:
        """A sibling .AV1.REENCODE.mkv is skipped until the flag is set."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            lib = root / "lib"
            lib.mkdir()
            source = lib / "Movie.mkv"
            source.write_bytes(b"x")
            sibling = lib / "Movie.AV1.REENCODE.mkv"
            sibling.write_bytes(b"y")
            self.assertEqual(
                cli_mod._existing_reencode_sibling(str(source)), sibling,
            )
            db_path = root / "state.db"
            with Database(db_path) as db:
                pr = make_probe(
                    height=1080, codec="hevc", video_bitrate=20_000_000,
                )
                pr.path = str(source)
                db.upsert_probe(pr)

            parser = cli_mod._build_parser()
            blocked = parser.parse_args(
                ["optimize", str(lib), "--db", str(db_path)],
            )
            allowed = parser.parse_args([
                "optimize", str(lib), "--db", str(db_path),
                "--allow-reencoded",
            ])
            self.assertFalse(cli_mod._decide_flags(blocked)["allow_reencoded"])
            self.assertTrue(cli_mod._decide_flags(allowed)["allow_reencoded"])

            with Database(db_path) as db, redirect_stdout(io.StringIO()):
                skipped = cli_mod._decisions_for_job(db, blocked)
                admitted = cli_mod._decisions_for_job(db, allowed)
            self.assertEqual(skipped, [])
            self.assertEqual(len(admitted), 1)
            self.assertEqual(admitted[0].kind, "encode")
            cand = admitted[0].candidate
            self.assertIsNotNone(cand)
            assert cand is not None
            self.assertEqual(cand.probe.path, str(source))

    def test_timeout_bloat_and_dv_flags_land_on_run_config(self) -> None:
        """Omitted timeout adapts. 0 disables. Positive wins. Bloat stays on."""
        parser = cli_mod._build_parser()
        path = "/tmp/library"
        bare = parser.parse_args(["HD", path])
        flagged = parser.parse_args([
            "HD", path, "--timeout", "0",
            "--no-auto-relax-cq", "--dv-p7-convert",
        ])
        positive = parser.parse_args(["HD", path, "--timeout", "90"])
        bare_cfg = cli_mod._build_run_config(
            bare, "HD", "keep", None, None, None, 1,
        )
        flagged_cfg = cli_mod._build_run_config(
            flagged, "HD", "keep", None, None, None, 1,
        )
        positive_cfg = cli_mod._build_run_config(
            positive, "HD", "keep", None, None, None, 1,
        )

        self.assertIsNone(bare_cfg.timeout)
        self.assertTrue(bare_cfg.auto_relax_cq)
        self.assertFalse(bare_cfg.dv_p7_convert)
        self.assertEqual(flagged_cfg.timeout, 0)
        self.assertFalse(flagged_cfg.auto_relax_cq)
        self.assertTrue(flagged_cfg.dv_p7_convert)
        self.assertEqual(positive_cfg.timeout, 90)
        self.assertTrue(positive_cfg.auto_relax_cq)
        self.assertFalse(positive_cfg.dv_p7_convert)

        duration = 10800.0
        self.assertEqual(
            cli_mod._resolve_timeout(bare_cfg.timeout, duration), 64800,
        )
        self.assertEqual(
            cli_mod._resolve_timeout(flagged_cfg.timeout, duration), 0,
        )
        self.assertEqual(
            cli_mod._resolve_timeout(positive_cfg.timeout, duration), 90,
        )


if __name__ == "__main__":
    unittest.main()
