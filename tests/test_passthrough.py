"""--original-audio / --original-subs: bypass keep-langs and the audio ladder.

Defaults strip non-English audio/subs and rebuild a 3-stream audio ladder.
The two passthrough flags are explicit overrides for users who want every
input track preserved bit-perfectly.
"""

from __future__ import annotations

import unittest
from pathlib import Path

from optimizer import encoder
from tests._fixtures import aud, make_probe, sub


def _multi_lang_probe():
    """A 1080p source with 4 audio (en + 3 foreign) and 4 subs (en + 3 foreign)."""
    return make_probe(
        codec="h264",
        height=1080,
        video_bitrate=12_000_000,
        audio=[
            aud(0, "truehd", "eng", 8),
            aud(1, "ac3", "fre", 6),
            aud(2, "ac3", "spa", 6),
            aud(3, "ac3", "ger", 6),
        ],
        subs=[
            sub(0, "subrip", "eng"),
            sub(1, "subrip", "fre"),
            sub(2, "subrip", "spa"),
            sub(3, "subrip", "ger"),
        ],
    )


# --------------------------------------------------------------------------- #
# Audio passthrough
# --------------------------------------------------------------------------- #


class OriginalAudioTests(unittest.TestCase):
    def test_default_filters_non_english_audio(self):
        cmd = encoder.build_encode_command(
            _multi_lang_probe(), Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"], target_container="mkv",
        )
        joined = " ".join(cmd)
        # Default behavior: foreign audio streams are pre-stripped at demux.
        self.assertIn("-discard:a:1", joined)
        self.assertIn("-discard:a:2", joined)
        self.assertIn("-discard:a:3", joined)

    def test_original_audio_keeps_all_streams(self):
        cmd = encoder.build_encode_command(
            _multi_lang_probe(), Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"], target_container="mkv",
            original_audio=True,
        )
        joined = " ".join(cmd)
        # No audio streams are pre-stripped.
        self.assertNotIn("-discard:a:", joined)
        # The map directive picks up every audio stream and stream-copies.
        self.assertIn("-map", cmd)
        self.assertIn("0:a?", cmd)
        # -c:a copy follows the audio map, and there's no encode (libopus,
        # aac) showing up in the audio codec position.
        c_a_idx = cmd.index("-c:a")
        self.assertEqual(cmd[c_a_idx + 1], "copy")

    def test_original_audio_keeps_subs_filtered(self):
        # original_audio shouldn't bleed into the subtitle filter.
        cmd = encoder.build_encode_command(
            _multi_lang_probe(), Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"], target_container="mkv",
            original_audio=True,
        )
        joined = " ".join(cmd)
        # Foreign subtitles are still pre-stripped because original_subs=False.
        self.assertIn("-discard:s:1", joined)


# --------------------------------------------------------------------------- #
# Subtitle passthrough
# --------------------------------------------------------------------------- #


class OriginalSubsTests(unittest.TestCase):
    def test_default_filters_non_english_subs(self):
        cmd = encoder.build_encode_command(
            _multi_lang_probe(), Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"], target_container="mkv",
        )
        joined = " ".join(cmd)
        self.assertIn("-discard:s:1", joined)
        self.assertIn("-discard:s:2", joined)
        self.assertIn("-discard:s:3", joined)

    def test_original_subs_keeps_all_subtitle_streams(self):
        cmd = encoder.build_encode_command(
            _multi_lang_probe(), Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"], target_container="mkv",
            original_subs=True,
        )
        joined = " ".join(cmd)
        self.assertNotIn("-discard:s:", joined)
        # All four subtitle streams are mapped explicitly.
        self.assertEqual(joined.count("0:s:"), 4)

    def test_original_subs_keeps_audio_filtered(self):
        # original_subs shouldn't bleed into the audio filter.
        cmd = encoder.build_encode_command(
            _multi_lang_probe(), Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"], target_container="mkv",
            original_subs=True,
        )
        joined = " ".join(cmd)
        self.assertIn("-discard:a:1", joined)


# --------------------------------------------------------------------------- #
# Both flags together
# --------------------------------------------------------------------------- #


class BothPassthroughTests(unittest.TestCase):
    def test_both_flags_strip_no_streams(self):
        cmd = encoder.build_encode_command(
            _multi_lang_probe(), Path("/tmp/out.mkv"),
            "libsvtav1", quality=28,
            keep_langs=["en", "und"], target_container="mkv",
            original_audio=True, original_subs=True,
        )
        joined = " ".join(cmd)
        self.assertNotIn("-discard:a:", joined)
        self.assertNotIn("-discard:s:", joined)


if __name__ == "__main__":
    unittest.main()
