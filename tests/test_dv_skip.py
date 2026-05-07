"""Plan-time Dolby Vision skip filter.

Sources with a DV configuration record consistently wedge av1_qsv
(Profile 7 stalls at frame 0, Profile 8 stalls partway in). Pinned in
v0.5.15 after run-3 / run-6 of the UHD-archive test queue produced
seven stalls, all on DV titles (LOTR x265-NAHOM trilogy, Hobbit
Desolation of Smaug, The Housemaid 2025).

These tests verify plan() drops DV probes from the queue before rule
evaluation, leaving non-DV candidates unaffected.
"""

from __future__ import annotations

import argparse
import sqlite3
import tempfile
import unittest
from pathlib import Path

from optimizer.cli import cmd_plan
from optimizer.db import Database
from tests._fixtures import make_probe


def _plan_args(db_path: Path) -> argparse.Namespace:
    return argparse.Namespace(
        db=db_path, rules=None, target="av1+mkv", json=False, cmd="plan",
    )


class DvSkipTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "state.db"
        # Seed two probes that both clear OverBitratedRule (2160p well
        # above the 32 Mbps flag threshold) — only the DV gate should
        # decide which one becomes a pending decision.
        self.non_dv = "/tmp/non_dv.mkv"
        self.dv = "/tmp/dv_p7.mkv"
        # Real files so the plan-time existence check passes.
        Path(self.non_dv).write_bytes(b"x")
        Path(self.dv).write_bytes(b"x")
        with Database(self.db_path) as db:
            db.upsert_probe(_p(self.non_dv, dv_profile=None))
            # Profile 5: always skipped (no clean HDR10 base layer to
            # fall back to). Profile 7 is environment-dependent (admitted
            # iff dovi_tool is on PATH); P5 keeps the test deterministic.
            db.upsert_probe(_p(self.dv, dv_profile=5))

    def tearDown(self):
        for p in (self.non_dv, self.dv):
            Path(p).unlink(missing_ok=True)
        self.tmpdir.cleanup()

    def _pending_paths(self) -> list[str]:
        conn = sqlite3.connect(str(self.db_path))
        rows = conn.execute(
            "SELECT path FROM decisions WHERE status='pending'",
        ).fetchall()
        conn.close()
        return [r[0] for r in rows]

    def test_dv_source_not_queued(self):
        rc = cmd_plan(_plan_args(self.db_path))
        self.assertEqual(rc, 0)
        pending = self._pending_paths()
        self.assertIn(self.non_dv, pending)
        self.assertNotIn(self.dv, pending)

    def test_summary_records_dv_blocked(self):
        cmd_plan(_plan_args(self.db_path))
        conn = sqlite3.connect(str(self.db_path))
        row = conn.execute(
            "SELECT summary_json FROM runs WHERE kind='plan' "
            "ORDER BY started_at DESC LIMIT 1",
        ).fetchone()
        conn.close()
        import json
        summary = json.loads(row[0])
        self.assertEqual(summary.get("dv_blocked"), 1)


class DvStripCommandShapeTests(unittest.TestCase):
    """`build_dv_strip_command` must scope the bsf to v:0, not bare v.

    Run #166 caught this: The Housemaid 2025 (DV P7 + JPEG cover art)
    crashed the strip with `Error initializing bitstream filter:
    dovi_rpu — Codec 'mjpeg' (7) is not supported`. The bsf only
    handles hevc + av1; bare `-bsf:v` applies to every video stream
    including the embedded JPEG cover, while `-bsf:v:0` targets only
    the primary HEVC track and leaves the cover art untouched.
    """

    def test_bsf_targets_first_video_stream_only(self):
        from optimizer.encoder import build_dv_strip_command
        from tests._fixtures import make_probe
        pr = make_probe(height=2160, dv_profile=8)
        pr.path = "/movies/x.mkv"
        cmd = build_dv_strip_command(pr, Path("/movies/x.dv-prepped.mkv"))
        # The whole-stream form must NOT be present.
        self.assertNotIn("-bsf:v", cmd,
                         "bare -bsf:v leaked; will crash on sources with "
                         "non-DV video streams (e.g. JPEG cover art)")
        # The scoped form must be present and followed by the strip filter.
        self.assertIn("-bsf:v:0", cmd)
        idx = cmd.index("-bsf:v:0")
        self.assertEqual(cmd[idx + 1], "dovi_rpu=strip=true")

    def test_no_keep_langs_means_no_discards(self):
        """Backwards-compat: omitting keep_langs preserves the original
        "copy every stream" behavior so callers that want a faithful
        clone of the source minus the RPU still get one."""
        from optimizer.encoder import build_dv_strip_command
        from tests._fixtures import make_probe
        pr = make_probe(height=2160, dv_profile=8)
        pr.path = "/movies/x.mkv"
        cmd = build_dv_strip_command(pr, Path("/o.mkv"))
        self.assertFalse(any(c.startswith("-discard") for c in cmd),
                         "no keep_langs → no demuxer discards expected")

    def test_original_audio_keeps_every_audio_track(self):
        """`--original-audio` must round-trip through the strip stage:
        the user asked for every audio track preserved, so the strip
        must NOT pre-discard any audio streams (even though the encode's
        kept-set logic would otherwise drop alternate-language tracks).
        Pinned because the strip pre-discard optimization could
        accidentally drop the streams the user explicitly opted to keep."""
        from optimizer.encoder import build_dv_strip_command
        from optimizer.models import AudioTrack
        from tests._fixtures import make_probe

        def at(i: int, lang: str) -> AudioTrack:
            return AudioTrack(
                index=i, codec="ac3", language=lang, channels=6,
                channel_layout="5.1", bitrate=640_000,
                default=(i == 0), title=None,
            )

        pr = make_probe(height=2160, dv_profile=7)
        pr.path = "/movies/x.mkv"
        # Multilingual remux: 5 audio tracks (eng, jpn, fre, ger, spa).
        pr.audio_tracks = [
            at(0, "eng"), at(1, "jpn"), at(2, "fre"),
            at(3, "ger"), at(4, "spa"),
        ]
        # Without original_audio, only `eng` is kept — the strip would
        # discard the other 4. With original_audio=True, none should
        # be discarded.
        cmd = build_dv_strip_command(
            pr, Path("/o.mkv"),
            keep_langs=["en", "und"], target_container="mkv",
            original_audio=True,
        )
        audio_discards = [t for t in cmd if t.startswith("-discard:a:")]
        self.assertEqual(audio_discards, [],
                         "--original-audio must suppress audio discards "
                         "in the strip stage; got: " + repr(audio_discards))
        # Sanity: the bsf is still applied (the DV strip itself didn't
        # get disabled by --original-audio).
        self.assertIn("-bsf:v:0", cmd)

    def test_original_subs_keeps_every_subtitle_track(self):
        """Same contract for `--original-subs`: the strip stage must
        not pre-discard subtitle streams when the user asked to keep
        every one."""
        from optimizer.encoder import build_dv_strip_command
        from optimizer.models import SubtitleTrack
        from tests._fixtures import make_probe

        def st(i: int, lang: str) -> SubtitleTrack:
            return SubtitleTrack(
                index=i, codec="subrip", language=lang,
                forced=False, default=(i == 0), title=None,
            )

        pr = make_probe(height=2160, dv_profile=7)
        pr.path = "/movies/x.mkv"
        pr.subtitle_tracks = [
            st(0, "eng"), st(1, "jpn"), st(2, "fre"), st(3, "ger"),
        ]
        cmd = build_dv_strip_command(
            pr, Path("/o.mkv"),
            keep_langs=["en", "und"], target_container="mkv",
            original_subs=True,
        )
        sub_discards = [t for t in cmd if t.startswith("-discard:s:")]
        self.assertEqual(sub_discards, [],
                         "--original-subs must suppress sub discards "
                         "in the strip stage; got: " + repr(sub_discards))

    def test_original_audio_and_subs_both_pass_through(self):
        """Combined: both flags simultaneously. Strip must keep every
        audio + every subtitle, while still applying the RPU strip."""
        from optimizer.encoder import build_dv_strip_command
        from optimizer.models import AudioTrack, SubtitleTrack
        from tests._fixtures import make_probe

        pr = make_probe(height=2160, dv_profile=7)
        pr.path = "/movies/x.mkv"
        pr.audio_tracks = [
            AudioTrack(index=0, codec="truehd", language="eng", channels=8,
                       channel_layout="7.1", bitrate=5_000_000,
                       default=True, title=None),
            AudioTrack(index=1, codec="ac3", language="jpn", channels=6,
                       channel_layout="5.1", bitrate=640_000,
                       default=False, title=None),
        ]
        pr.subtitle_tracks = [
            SubtitleTrack(index=0, codec="subrip", language="eng",
                          forced=False, default=True, title=None),
            SubtitleTrack(index=1, codec="subrip", language="jpn",
                          forced=False, default=False, title=None),
        ]
        cmd = build_dv_strip_command(
            pr, Path("/o.mkv"),
            keep_langs=["en", "und"], target_container="mkv",
            original_audio=True, original_subs=True,
        )
        self.assertFalse(any(c.startswith("-discard") for c in cmd),
                         "no discards expected when both original-* flags set")
        self.assertIn("-bsf:v:0", cmd)

    def test_keep_langs_drops_unwanted_audio_at_demuxer(self):
        """The whole point of the optimization: with keep_langs set,
        the strip demuxer drops unwanted audio so they're never read
        from the NAS or written to the temp file. SPR-shaped: 9 audio
        streams (TrueHD primary + DTS + 6× AC3 commentary) → keep
        primary + a 5.1 fallback, discard the rest."""
        from optimizer.encoder import build_dv_strip_command
        from optimizer.models import AudioTrack
        from tests._fixtures import make_probe

        def at(i: int, codec: str, ch: int = 6) -> AudioTrack:
            return AudioTrack(
                index=i, codec=codec, language="eng", channels=ch,
                channel_layout=f"{ch}.0", bitrate=1_000_000,
                default=(i == 0), title=None,
            )

        pr = make_probe(height=2160, dv_profile=7)
        pr.path = "/movies/x.mkv"
        pr.audio_tracks = (
            [at(0, "truehd", 8), at(1, "dts", 6)]
            + [at(i, "ac3", 6) for i in range(2, 9)]
        )
        cmd = build_dv_strip_command(
            pr, Path("/o.mkv"),
            keep_langs=["en", "und"], target_container="mkv",
        )
        discards = {cmd[i + 1] for i, t in enumerate(cmd[:-1])
                    if t.startswith("-discard:a:")}
        # The 7 commentary AC3 tracks (a:2..a:8) should be discarded;
        # primary TrueHD (a:0) and a 5.1 fallback (a:1 dts) are kept.
        for ac3_idx in range(2, 9):
            self.assertIn(
                "all", discards,  # value side; key check below
                f"a:{ac3_idx} should be discarded",
            )
        discard_keys = [t for t in cmd if t.startswith("-discard:a:")]
        for ac3_idx in range(2, 9):
            self.assertIn(f"-discard:a:{ac3_idx}", discard_keys)
        # Primary audio (and the dts 5.1 fallback) must NOT be discarded.
        self.assertNotIn("-discard:a:0", discard_keys)


class PrepareDvSourceForwardsOriginalAudioTests(unittest.TestCase):
    """End-to-end: `args.original_audio=True` must flow from the apply
    layer through `_prepare_dv_source` into the strip command, so the
    strip doesn't silently drop tracks the user explicitly opted to
    keep. Pinned because the strip pre-discard optimization (commit
    5e17efa) added a path where the strip *can* drop audio, and a
    future refactor that forgets to forward the flag would silently
    break `--original-audio` for DV sources."""

    def test_args_original_audio_suppresses_strip_discards(self):
        import argparse
        import tempfile
        from unittest.mock import patch

        from optimizer import cli as cli_mod
        from optimizer.models import AudioTrack
        from tests._fixtures import make_probe

        with tempfile.TemporaryDirectory() as tmp:
            source = Path(tmp) / "src.mkv"
            source.write_bytes(b"x" * 1024)

            pr = make_probe(height=2160, dv_profile=8)
            pr.path = str(source)
            pr.audio_tracks = [
                AudioTrack(index=i, codec="ac3", language=lang, channels=6,
                           channel_layout="5.1", bitrate=640_000,
                           default=(i == 0), title=None)
                for i, lang in enumerate(["eng", "jpn", "fre", "ger"])
            ]

            args = argparse.Namespace(
                original_audio=True,
                original_subs=False,
                compat_audio=True,
                verbose=False,
                timeout=0,
                dv_p7_convert=False,
            )

            captured: list[list[str]] = []

            def fake_run(cmd, _pr, _args, label=""):  # noqa: ARG001
                captured.append(cmd)
                # Pretend the strip succeeded so _prepare_dv_source
                # returns (work_dir, prepared, None).
                Path(cmd[cmd.index("-progress") - 1]).touch()
                return True, ""

            with patch.object(cli_mod, "_run_encode_ffmpeg",
                              side_effect=fake_run):
                _, prepared, err = cli_mod._prepare_dv_source(
                    pr, args,
                    keep_langs=["en", "und"], target_container="mkv",
                )

            self.assertIsNone(err)
            self.assertIsNotNone(prepared)
            self.assertEqual(len(captured), 1, "strip should run once")
            cmd = captured[0]
            audio_discards = [t for t in cmd if t.startswith("-discard:a:")]
            self.assertEqual(
                audio_discards, [],
                "args.original_audio=True must suppress strip-stage audio "
                "discards end-to-end; got: " + repr(audio_discards),
            )


class DvStrategyDefaultsTests(unittest.TestCase):
    """`encoder.dv_strategy` per-profile dispatch.

    Default (allow_p7_convert=False) puts P7 on the same simple
    `dovi_rpu=strip=true` bsf path as P8 — the dovi_tool +
    mkvmerge pipeline is opt-in. Pinned because the historical
    default (P7 → "p7_convert" when tools are present) caused
    NAS-wedge / mkvmerge-muxer pain in the field; reverting it
    silently would re-introduce that.
    """

    def test_p5_always_skipped(self):
        from optimizer import encoder
        self.assertIsNone(encoder.dv_strategy(5))
        self.assertIsNone(encoder.dv_strategy(5, allow_p7_convert=True))

    def test_p8_is_strip(self):
        from optimizer import encoder
        self.assertEqual(encoder.dv_strategy(8), "p8_strip")

    def test_p7_default_is_strip_not_convert(self):
        from optimizer import encoder
        self.assertEqual(encoder.dv_strategy(7), "p8_strip")

    def test_p7_convert_opt_in_requires_both_tools(self):
        from unittest.mock import patch

        from optimizer import encoder
        # Both present → convert pipeline.
        with patch.object(encoder, "has_dovi_tool", return_value=True), \
             patch.object(encoder, "has_mkvmerge", return_value=True):
            self.assertEqual(
                encoder.dv_strategy(7, allow_p7_convert=True),
                "p7_convert",
            )
        # Either missing → None (fail closed; user opted in but env can't honour it).
        with patch.object(encoder, "has_dovi_tool", return_value=False), \
             patch.object(encoder, "has_mkvmerge", return_value=True):
            self.assertIsNone(
                encoder.dv_strategy(7, allow_p7_convert=True),
            )
        with patch.object(encoder, "has_dovi_tool", return_value=True), \
             patch.object(encoder, "has_mkvmerge", return_value=False):
            self.assertIsNone(
                encoder.dv_strategy(7, allow_p7_convert=True),
            )


def _p(path: str, *, dv_profile: int | None) -> object:
    """make_probe wrapper: 2160p source over the UHD bitrate flag threshold."""
    pr = make_probe(
        height=2160, codec="hevc", bit_depth=10,
        video_bitrate=60_000_000,  # well over the 32 Mbps 2160p flag
        dv_profile=dv_profile,
    )
    pr.path = path
    return pr


if __name__ == "__main__":
    unittest.main()
