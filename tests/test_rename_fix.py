"""`rename-fix` maintenance subcommand: rename existing .REENCODE outputs
through the current av1 naming rules.

Pins the safety contract: only touches files whose stem already carries
the `REENCODE` marker, moves same-stem sidecars with the video, refuses
to overwrite an existing target, and respects dry-run by default.
"""

from __future__ import annotations

import argparse
import io
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path

from optimizer.cli import cmd_rename_fix


def _make_args(path: Path, *, apply: bool = False) -> argparse.Namespace:
    return argparse.Namespace(cmd="rename-fix", path=path, apply=apply)


def _touch(p: Path, content: bytes = b"x") -> Path:
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return p


class RenameFixDryRunTests(unittest.TestCase):
    def test_lists_pending_renames_without_touching_disk(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            src = _touch(root / "Movie" /
                         "Foo.2024.DV.HDR10Plus.+.AV1.REENCODE.mkv")
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_rename_fix(_make_args(root, apply=False))
            self.assertEqual(rc, 0)
            self.assertTrue(src.exists(), "dry run must not move the file")
            self.assertIn("→", buf.getvalue())
            self.assertIn("dry run", buf.getvalue())


class RenameFixApplyTests(unittest.TestCase):
    def test_renames_video_and_sidecars_together(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stem_old = "Foo.2024.DV.HDR10Plus.+.AV1.REENCODE"
            mkv = _touch(root / "Foo" / f"{stem_old}.mkv")
            srt = _touch(root / "Foo" / f"{stem_old}.en.srt")  # different stem
            srt_match = _touch(root / "Foo" / f"{stem_old}.srt")
            nfo = _touch(root / "Foo" / f"{stem_old}.nfo")
            rc = cmd_rename_fix(_make_args(root, apply=True))
            self.assertEqual(rc, 0)
            # HDR10 substituted in for the stripped DV/HDR10Plus tokens
            # (DV Profile 7/8 sources always have an HDR10 base layer).
            new_stem = "Foo.2024.HDR10.AV1.REENCODE"
            self.assertFalse(mkv.exists())
            self.assertFalse(srt_match.exists())
            self.assertFalse(nfo.exists())
            self.assertTrue((root / "Foo" / f"{new_stem}.mkv").exists())
            self.assertTrue((root / "Foo" / f"{new_stem}.srt").exists())
            self.assertTrue((root / "Foo" / f"{new_stem}.nfo").exists())
            # The .en.srt has a different stem so it is NOT in the same
            # rename group (matching by stem equality, not prefix).
            self.assertTrue(srt.exists())

    def test_skips_files_without_reencode_marker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            untouched = _touch(root / "Movie.2024.DV.HDR10Plus.mkv")
            rc = cmd_rename_fix(_make_args(root, apply=True))
            self.assertEqual(rc, 0)
            self.assertTrue(untouched.exists())

    def test_skips_already_canonical_names(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            ok = _touch(root / "Foo.2024.AV1.REENCODE.mkv")
            rc = cmd_rename_fix(_make_args(root, apply=True))
            self.assertEqual(rc, 0)
            self.assertTrue(ok.exists())

    def test_collision_skips_and_keeps_source(self) -> None:
        """Pre-existing target name must not be clobbered."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            old = _touch(root / "Foo.2024.DV.HDR10Plus.+.AV1.REENCODE.mkv")
            blocker = _touch(root / "Foo.2024.HDR10.AV1.REENCODE.mkv")
            buf = io.StringIO()
            with redirect_stdout(buf):
                rc = cmd_rename_fix(_make_args(root, apply=True))
            # rc=0 because nothing was attempted (skip is a clean outcome).
            self.assertEqual(rc, 0)
            self.assertTrue(old.exists(), "source must survive a skipped rename")
            self.assertTrue(blocker.exists())
            self.assertIn("target already exists", buf.getvalue())

    def test_skips_nas_recycle_dirs(self) -> None:
        """NAS recycle/snapshot directories must not be walked."""
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stashed = _touch(
                root / ".@Recycle" / "Foo" /
                "Foo.2024.DV.HDR10Plus.+.AV1.REENCODE.mkv",
            )
            rc = cmd_rename_fix(_make_args(root, apply=True))
            self.assertEqual(rc, 0)
            self.assertTrue(stashed.exists(),
                            "files inside .@Recycle must be untouched")


if __name__ == "__main__":
    unittest.main()
