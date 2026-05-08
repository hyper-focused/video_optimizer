"""Post-encode bloat fallback (`--auto-relax-cq`).

When a UHD encode at the default tight CQ (15) produces an output that
is nearly the same size as the source, the encoder is over-allocating
bits to grain — re-encoding once at CQ 21 (`RELAXED_UHD_CQ`) typically
collapses the output to a sane size. The Princess Bride 2160p remux is
the canonical case (47 GB source → 49 GB CQ-15 output).

These tests verify:
  - The bloat predicate triggers only on UHD + bloat ratio + CQ < relaxed
    + auto-relax enabled + not-yet-retried.
  - `_execute_encode` returns the `bloat_retry` sentinel when bloat is
    detected and deletes the bloated output.
  - `_apply_one_after_validation` rebuilds the command at CQ 21 and
    re-runs `_execute_encode` exactly once, then restores the original
    CQ on the namespace.
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
from optimizer.cli import (
    _execute_encode,
    _should_retry_for_bloat,
)
from optimizer.db import Database
from optimizer.presets import BLOAT_RATIO_THRESHOLD, RELAXED_UHD_CQ
from tests._fixtures import make_probe


def _args(**overrides) -> argparse.Namespace:
    ns = argparse.Namespace(
        mode="keep",
        quality=15,
        auto_relax_cq=True,
        verbose=False,
        timeout=0,
        output_root=None,
        source_root=None,
        rewrite_codec=True,
        reencode_tag=True,
        reencode_tag_value="REENCODE",
        no_dotted=False,
        name_suffix="",
        backup=None,
        recycle_to=None,
        dry_run=False,
    )
    for k, v in overrides.items():
        setattr(ns, k, v)
    return ns


class ShouldRetryForBloatTests(unittest.TestCase):
    """The decision predicate is the gatekeeper for the whole feature."""

    def _setup_pair(self, *, src_size: int, out_size: int):
        self.tmp = tempfile.TemporaryDirectory()
        out = Path(self.tmp.name) / "out.mkv"
        out.write_bytes(b"x" * out_size)
        pr = make_probe(height=2160)
        pr.size = src_size
        return pr, out

    def tearDown(self):
        if hasattr(self, "tmp"):
            self.tmp.cleanup()

    def test_uhd_bloat_above_threshold_triggers(self):
        pr, out = self._setup_pair(src_size=10_000, out_size=9_700)  # ratio 0.97
        self.assertTrue(_should_retry_for_bloat(pr, out, _args()))

    def test_uhd_below_threshold_does_not_trigger(self):
        pr, out = self._setup_pair(src_size=10_000, out_size=4_000)  # ratio 0.4
        self.assertFalse(_should_retry_for_bloat(pr, out, _args()))

    def test_hd_bloat_does_not_trigger(self):
        # 1080p source: bloat policy doesn't apply. The storage delta at
        # HD doesn't justify a doubled encode budget.
        self.tmp = tempfile.TemporaryDirectory()
        out = Path(self.tmp.name) / "out.mkv"
        out.write_bytes(b"x" * 9_700)
        pr = make_probe(height=1080)
        pr.size = 10_000
        self.assertFalse(_should_retry_for_bloat(pr, out, _args()))

    def test_already_retried_does_not_trigger(self):
        pr, out = self._setup_pair(src_size=10_000, out_size=9_900)
        a = _args()
        a._cq_retried = True
        self.assertFalse(_should_retry_for_bloat(pr, out, a))

    def test_disabled_via_flag(self):
        pr, out = self._setup_pair(src_size=10_000, out_size=9_900)
        self.assertFalse(_should_retry_for_bloat(
            pr, out, _args(auto_relax_cq=False),
        ))

    def test_current_cq_already_at_relaxed_does_not_trigger(self):
        # Belt-and-braces: if the user invoked `UHD-FILM` directly and
        # got bloat anyway, retrying at the same CQ is pointless.
        pr, out = self._setup_pair(src_size=10_000, out_size=9_900)
        self.assertFalse(_should_retry_for_bloat(
            pr, out, _args(quality=RELAXED_UHD_CQ),
        ))
        self.assertFalse(_should_retry_for_bloat(
            pr, out, _args(quality=RELAXED_UHD_CQ + 5),
        ))

    def test_zero_source_size_does_not_trigger(self):
        # Defensive: a probe with size=0 shouldn't divide-by-zero.
        pr, out = self._setup_pair(src_size=0, out_size=9_900)
        self.assertFalse(_should_retry_for_bloat(pr, out, _args()))


class ExecuteEncodeBloatPathTests(unittest.TestCase):
    """`_execute_encode` returns the bloat sentinel and unlinks the output."""

    def test_bloat_returns_sentinel_and_deletes_output(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src.mkv"
            source.write_bytes(b"x" * 10_000)
            output = tmp_path / "src.AV1.REENCODE.mkv"

            pr = make_probe(height=2160)
            pr.path = str(source)
            pr.size = 10_000

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

                # Fake ffmpeg run: write a 9_900-byte output (ratio 0.99).
                def fake_run_ffmpeg(*_a, **_kw):  # noqa: ARG001
                    output.write_bytes(b"y" * 9_900)
                    return True, ""

                args = _args()
                args._apply_run_id = run_id  # noqa: SLF001

                with patch.object(cli_mod.encoder, "run_ffmpeg",
                                  side_effect=fake_run_ffmpeg):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        status, saved = _execute_encode(
                            db, row, pr, ["ffmpeg"], "encode (stub)",
                            output, args, "test: ",
                        )

            self.assertEqual(status, "bloat_retry")
            self.assertEqual(saved, 0)
            # Output was deleted before returning.
            self.assertFalse(output.exists())
            # No decision row should be marked completed yet — the retry
            # path will produce the final outcome.
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                final = dict(conn.execute(
                    "SELECT status FROM decisions WHERE id = ?", (dec_id,),
                ).fetchone())
            self.assertEqual(final["status"], "pending")

    def test_below_threshold_falls_through_to_finalize(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src.mkv"
            source.write_bytes(b"x" * 10_000)
            output = tmp_path / "src.AV1.REENCODE.mkv"

            pr = make_probe(height=2160)
            pr.path = str(source)
            pr.size = 10_000

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

                def fake_run_ffmpeg(*_a, **_kw):  # noqa: ARG001
                    output.write_bytes(b"y" * 4_000)  # ratio 0.4
                    return True, ""

                args = _args()
                args._apply_run_id = run_id  # noqa: SLF001

                with patch.object(cli_mod.encoder, "run_ffmpeg",
                                  side_effect=fake_run_ffmpeg), \
                     patch.object(cli_mod.encoder, "validate_output",
                                  return_value=(True, "")):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        status, _saved = _execute_encode(
                            db, row, pr, ["ffmpeg"], "encode (stub)",
                            output, args, "test: ",
                        )

            self.assertEqual(status, "applied")
            self.assertTrue(output.exists())


class ApplyOneRetryTests(unittest.TestCase):
    """The retry hook in `_apply_one_after_validation` re-invokes the encode."""

    def test_bloat_retry_runs_execute_encode_twice_at_relaxed_cq(self):
        from optimizer.cli import _apply_one_after_validation

        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src.mkv"
            source.write_bytes(b"x" * 10_000)
            output = tmp_path / "src.AV1.REENCODE.mkv"

            pr = make_probe(height=2160)
            pr.path = str(source)
            pr.size = 10_000

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
                args.encoder_preset = "veryslow"  # archive-grade default

                # First call: signal bloat, second call: success. Capture
                # the (quality, encoder_preset) pair on each invocation
                # so we can assert the retry bumped both knobs.
                seen: list[tuple[int, str | None]] = []

                def fake_execute(_db, _dec, _pr, _cmd, _desc,
                                 _out, args_, _label, **_kw):
                    seen.append((args_.quality,
                                 getattr(args_, "encoder_preset", None)))
                    if len(seen) == 1:
                        return "bloat_retry", 0
                    return "applied", 1024

                with patch.object(cli_mod, "_execute_encode",
                                  side_effect=fake_execute), \
                     patch.object(cli_mod, "_build_apply_command",
                                  return_value=(["ffmpeg"], "encode (stub)")):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        status, _saved = _apply_one_after_validation(
                            db, row, pr, args, run_id, output,
                            "av1+mkv", "av1_qsv",
                            ["en", "und"], 1, 1,
                        )

            self.assertEqual(status, "applied")
            # First call: archive-grade defaults. Retry: relaxed CQ + slow.
            self.assertEqual(seen, [
                (15, "veryslow"),
                (RELAXED_UHD_CQ, "slow"),
            ])
            # Both knobs restored on the namespace so the next file in
            # the queue starts at the preset defaults again.
            self.assertEqual(args.quality, 15)
            self.assertEqual(args.encoder_preset, "veryslow")
            self.assertFalse(getattr(args, "_cq_retried", False))


class MidEncodeBloatCheckerTests(unittest.TestCase):
    """`encoder._BloatChecker` projects final size at progress checkpoints."""

    def _checker(self, src_size: int, out_size: int,
                 checkpoints=(0.10, 0.20),
                 threshold=BLOAT_RATIO_THRESHOLD):
        from optimizer.encoder import _BloatChecker
        self.tmp = tempfile.TemporaryDirectory()
        out = Path(self.tmp.name) / "out.mkv"
        out.write_bytes(b"x" * out_size)
        return _BloatChecker(src_size, out, threshold, checkpoints)

    def tearDown(self):
        if hasattr(self, "tmp"):
            self.tmp.cleanup()

    def test_below_first_checkpoint_does_not_kill(self):
        ck = self._checker(src_size=10_000, out_size=900)
        kill, _ = ck.check(current_seconds=5, duration_seconds=100)  # 5%
        self.assertFalse(kill)

    def test_at_10pct_projecting_bloat_kills(self):
        # 10% complete, output 9_700 bytes → projected 97_000 vs source
        # 100_000 = ratio 0.97 → kill.
        ck = self._checker(src_size=100_000, out_size=9_700)
        kill, reason = ck.check(current_seconds=10, duration_seconds=100)
        self.assertTrue(kill)
        self.assertIn("bloat_projection", reason)
        self.assertIn("10%", reason)

    def test_at_10pct_projecting_clean_does_not_kill(self):
        # 10% complete, output 4_000 → projected 40_000 = ratio 0.40 → OK.
        ck = self._checker(src_size=100_000, out_size=4_000)
        kill, _ = ck.check(current_seconds=10, duration_seconds=100)
        self.assertFalse(kill)

    def test_each_checkpoint_consumed_once(self):
        # If 10% passes clean, the next call at 12% (still past 10%)
        # should not re-evaluate the same checkpoint — it's been popped.
        # 20% remains. Output growth between calls reflects normal
        # encoding; we check projection at 20%.
        ck = self._checker(src_size=100_000, out_size=4_000)
        kill1, _ = ck.check(current_seconds=10, duration_seconds=100)
        self.assertFalse(kill1)
        # Same call again — should be a no-op (10% already consumed,
        # haven't reached 20%).
        kill2, _ = ck.check(current_seconds=12, duration_seconds=100)
        self.assertFalse(kill2)

    def test_post_first_checkpoint_bloat_at_20pct_kills(self):
        # 10% looked clean (4 KB / 100 KB), 20% looks bloated
        # (output grew to 19_500 / 20% = 97_500 projected → ratio 0.975).
        # Simulate by re-statting the file: write a bigger output before
        # the 20% call.
        ck = self._checker(src_size=100_000, out_size=4_000)
        kill1, _ = ck.check(current_seconds=10, duration_seconds=100)
        self.assertFalse(kill1)
        # Output grew to 19_500 between checkpoints.
        out_path = next(Path(self.tmp.name).iterdir())
        out_path.write_bytes(b"y" * 19_500)
        kill2, reason = ck.check(current_seconds=20, duration_seconds=100)
        self.assertTrue(kill2)
        self.assertIn("20%", reason)

    def test_zero_duration_does_not_kill(self):
        ck = self._checker(src_size=100_000, out_size=99_000)
        kill, _ = ck.check(current_seconds=10, duration_seconds=0)
        self.assertFalse(kill)

    def test_missing_output_file_does_not_kill(self):
        from optimizer.encoder import _BloatChecker
        ck = _BloatChecker(
            source_size=100_000,
            output_path=Path("/tmp/__nonexistent_bloat_test__.mkv"),
            threshold=BLOAT_RATIO_THRESHOLD,
            checkpoints=(0.10,),
        )
        kill, _ = ck.check(current_seconds=10, duration_seconds=100)
        self.assertFalse(kill)


class MaybeMakeBloatCheckerTests(unittest.TestCase):
    """`_maybe_make_bloat_checker` mirrors `_should_retry_for_bloat`'s gate."""

    def _pr(self, **overrides):
        pr = make_probe(height=2160)
        for k, v in overrides.items():
            setattr(pr, k, v)
        return pr

    def test_uhd_returns_checker(self):
        from optimizer.cli import _maybe_make_bloat_checker
        with tempfile.TemporaryDirectory() as tmp:
            ck = _maybe_make_bloat_checker(
                self._pr(size=10_000),
                Path(tmp) / "out.mkv",
                _args(),
            )
        self.assertIsNotNone(ck)

    def test_hd_returns_none(self):
        from optimizer.cli import _maybe_make_bloat_checker
        with tempfile.TemporaryDirectory() as tmp:
            ck = _maybe_make_bloat_checker(
                self._pr(height=1080, size=10_000),
                Path(tmp) / "out.mkv",
                _args(),
            )
        self.assertIsNone(ck)

    def test_disabled_via_flag_returns_none(self):
        from optimizer.cli import _maybe_make_bloat_checker
        with tempfile.TemporaryDirectory() as tmp:
            ck = _maybe_make_bloat_checker(
                self._pr(size=10_000),
                Path(tmp) / "out.mkv",
                _args(auto_relax_cq=False),
            )
        self.assertIsNone(ck)

    def test_already_retried_returns_none(self):
        from optimizer.cli import _maybe_make_bloat_checker
        with tempfile.TemporaryDirectory() as tmp:
            a = _args()
            a._cq_retried = True
            ck = _maybe_make_bloat_checker(
                self._pr(size=10_000),
                Path(tmp) / "out.mkv",
                a,
            )
        self.assertIsNone(ck)

    def test_current_cq_at_relaxed_returns_none(self):
        from optimizer.cli import _maybe_make_bloat_checker
        with tempfile.TemporaryDirectory() as tmp:
            ck = _maybe_make_bloat_checker(
                self._pr(size=10_000),
                Path(tmp) / "out.mkv",
                _args(quality=RELAXED_UHD_CQ),
            )
        self.assertIsNone(ck)


class ExecuteEncodeMidEncodeBloatTests(unittest.TestCase):
    """`_execute_encode` translates a mid-encode bloat kill into bloat_retry."""

    def test_run_ffmpeg_returning_bloat_projection_yields_bloat_retry(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src.mkv"
            source.write_bytes(b"x" * 10_000)
            output = tmp_path / "src.AV1.REENCODE.mkv"
            output.write_bytes(b"y" * 5_000)  # partial output ffmpeg left

            pr = make_probe(height=2160)
            pr.path = str(source)
            pr.size = 10_000

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

                # ffmpeg "fails" with the bloat-projection sentinel —
                # this is what the progress loop returns when
                # _BloatChecker trips.
                fake_reason = (
                    "bloat_projection at 10%: output 0.49 GB, "
                    "projecting 4.9 GB vs source 4.7 GB (threshold 0.95)"
                )
                with patch.object(cli_mod.encoder, "run_ffmpeg",
                                  return_value=(False, fake_reason)):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        status, saved = cli_mod._execute_encode(
                            db, row, pr, ["ffmpeg"], "encode (stub)",
                            output, args, "test: ",
                        )

            self.assertEqual(status, "bloat_retry")
            self.assertEqual(saved, 0)
            # Partial output cleaned up.
            self.assertFalse(output.exists())
            # Decision row should NOT be marked failed — the retry
            # path will produce the final outcome.
            with sqlite3.connect(str(db_path)) as conn:
                conn.row_factory = sqlite3.Row
                final = dict(conn.execute(
                    "SELECT status FROM decisions WHERE id = ?", (dec_id,),
                ).fetchone())
            self.assertEqual(final["status"], "pending")


class BloatChecksAgainstEncoderInputTests(unittest.TestCase):
    """When DV strip pre-trims audio/sub streams, the encoder reads
    a smaller intermediate file than the original source. The bloat
    check must compare output size against that *encoder input*, not
    the original source — otherwise heavy-multi-track sources can
    pass the bloat threshold simply because audio stripping shrank
    the denominator, masking the case where the encoder didn't
    actually compress the video.
    """

    def test_post_encode_bloat_uses_encode_probe_size_not_pr_size(self):
        """SPR-shaped scenario: original 100 GB (with ~10 GB of audio
        we'd discard), stripped intermediate 90 GB, encoder output
        88 GB. Vs original (100 GB): ratio 0.88, would NOT trip the
        0.90 threshold. Vs stripped (90 GB): ratio 0.978, WOULD trip
        the threshold. The bloat check must use the stripped size."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src.mkv"
            source.write_bytes(b"x" * 1024)
            output = tmp_path / "src.AV1.REENCODE.mkv"
            output.write_bytes(b"y" * 88_000)  # 88 KB stand-in for 88 GB

            # `pr` reflects the original source (100 KB stand-in for 100 GB).
            pr = make_probe(height=2160)
            pr.path = str(source)
            pr.size = 100_000

            # `encode_probe` reflects the stripped intermediate
            # (90 KB stand-in for 90 GB).
            encode_probe = make_probe(height=2160)
            encode_probe.path = str(source) + ".dv-prepped.mkv"
            encode_probe.size = 90_000

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

                # ffmpeg "succeeds" (no kill); the post-encode size
                # check decides whether to retry.
                with patch.object(cli_mod.encoder, "run_ffmpeg",
                                  return_value=(True, "")):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        status, _ = cli_mod._execute_encode(
                            db, row, pr, ["ffmpeg"], "encode (stub)",
                            output, args, "test: ",
                            encode_probe=encode_probe,
                        )

            # The bloat check should fire (output 88K / encode_probe
            # 90K = 0.978 ratio, > 0.90 threshold) — even though
            # vs `pr` (100K) the ratio is only 0.88 and would NOT
            # have tripped.
            self.assertEqual(status, "bloat_retry")
            self.assertIn("encoder input", buf.getvalue())

    def test_no_dv_prep_falls_back_to_pr_size(self):
        """When no DV prep ran, encode_probe is None at the
        _execute_encode boundary, so the bloat check naturally
        falls back to pr.size — same behaviour as before this fix."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            source = tmp_path / "src.mkv"
            source.write_bytes(b"x" * 1024)
            output = tmp_path / "src.AV1.REENCODE.mkv"
            output.write_bytes(b"y" * 96_000)  # ratio 0.96 vs 100K source

            pr = make_probe(height=2160)
            pr.path = str(source)
            pr.size = 100_000

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

                with patch.object(cli_mod.encoder, "run_ffmpeg",
                                  return_value=(True, "")):
                    buf = io.StringIO()
                    with redirect_stdout(buf):
                        status, _ = cli_mod._execute_encode(
                            db, row, pr, ["ffmpeg"], "encode (stub)",
                            output, args, "test: ",
                        )

            # 0.96 vs pr.size 100_000 trips the 0.90 threshold.
            self.assertEqual(status, "bloat_retry")


class BloatThresholdSanityTests(unittest.TestCase):
    """Cheap pin: someone bumping BLOAT_RATIO_THRESHOLD to >= 1.0 would
    silently disable the fallback because the encoder can't beat its own
    input even on bloat cases (output ~equals source). Pin the range."""

    def test_threshold_within_sensible_range(self):
        self.assertGreater(BLOAT_RATIO_THRESHOLD, 0.5)
        self.assertLessEqual(BLOAT_RATIO_THRESHOLD, 1.0)


if __name__ == "__main__":
    unittest.main()
