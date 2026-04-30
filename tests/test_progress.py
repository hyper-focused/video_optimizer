"""Progress feed + stall watchdog regression tests.

Pins the v0.5.18 fix: av1_qsv with deep lookahead (depth=100, refs=5)
buffers ~150 frames before any presentation timestamp surfaces to the
muxer, so `out_time_ms` can stay at 0 for several minutes on a working
encode. The Avengers: Infinity War 2160p remux pinned this against the
5-min watchdog while writing 441s of clean AV1 to disk in 3 wall-clock
minutes (fps=58 throughout the kill window). Watchdog must treat
`frame=` advancement as proof of life alongside `out_time_ms`.
"""

from __future__ import annotations

import unittest

from optimizer.encoder import _parse_progress_line, _ProgressState


def feed(state: _ProgressState, lines: list[str],
         duration: float = 8962.0) -> _ProgressState:
    for line in lines:
        state = _parse_progress_line(line, state, duration)
    return state


class ProgressParserTests(unittest.TestCase):

    def test_frame_count_is_parsed_into_state(self):
        s = feed(_ProgressState(), ["frame=  9503"])
        self.assertEqual(s.frames, 9503)

    def test_out_time_ms_is_parsed_as_microseconds(self):
        s = feed(_ProgressState(), ["out_time_ms=441408000"])
        self.assertAlmostEqual(s.current_seconds, 441.408, places=3)

    def test_qsv_lookahead_warmup_block(self):
        """A realistic block from the Infinity War run: fps + frames advance
        but out_time_ms stays at 0 (lookahead buffering ~150 frames before
        first PTS emerges). Watchdog should see frames moving."""
        s = _ProgressState()
        s = feed(s, [
            "frame=  900",
            "fps=58.1",
            "stream_0_0_q=0.0",
            "out_time_ms=0",
            "speed=2.42x",
            "progress=continue",
        ])
        self.assertEqual(s.frames, 900)
        self.assertEqual(s.current_seconds, 0.0)
        self.assertGreater(s.fps, 0)


class StallSignalTests(unittest.TestCase):
    """Mirror the watchdog's liveness check against synthetic feeds.

    The actual watchdog lives in `_stream_progress_until_done` and is
    driven by a subprocess; here we replicate its core invariant against
    `_parse_progress_line` output to keep the contract pinned.
    """

    def _is_alive(self, before: _ProgressState, after: _ProgressState) -> bool:
        """The watchdog's liveness predicate, lifted into a unit-testable form."""
        return (after.current_seconds > before.current_seconds
                or after.frames > before.frames)

    def test_frames_advancing_with_zero_out_time_is_alive(self):
        """v0.5.17 watchdog killed working encodes here. v0.5.18 must not."""
        before = _ProgressState(current_seconds=0.0, frames=900)
        after = feed(_ProgressState(current_seconds=0.0, frames=900), [
            "frame= 1800",
            "out_time_ms=0",
        ])
        self.assertTrue(self._is_alive(before, after))

    def test_out_time_advancing_is_alive(self):
        before = _ProgressState(current_seconds=120.0, frames=2880)
        after = feed(_ProgressState(current_seconds=120.0, frames=2880), [
            "out_time_ms=125000000",
        ])
        self.assertTrue(self._is_alive(before, after))

    def test_neither_advancing_is_genuinely_stalled(self):
        before = _ProgressState(current_seconds=0.0, frames=900)
        after = feed(_ProgressState(current_seconds=0.0, frames=900), [
            "frame=  900",
            "out_time_ms=0",
            "fps=0.0",
            "progress=continue",
        ])
        self.assertFalse(self._is_alive(before, after))


if __name__ == "__main__":
    unittest.main()
