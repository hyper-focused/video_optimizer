"""Pure plan decision for one probe.

Does not open SQLite and does not touch the filesystem. The caller
supplies the stall count and whether a reencode sibling already
exists; ``decide`` only classifies the probe.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from . import crawler, encoder, rules
from .models import Candidate, ProbeResult
from .presets import BITRATE_FLAG_TABLE, PRESETS

_REENCODED_MARKER_RE = re.compile(r"\bREENCODE\b", re.IGNORECASE)


@dataclass(frozen=True)
class Decision:
    """Outcome of ``decide`` for one probe.

    ``reason`` is set only when ``kind == "skip"``. ``candidate`` and
    ``tier`` are set only when ``kind == "encode"``. ``tier`` is
    ``"SD"``, ``"HD"``, or ``"UHD"`` — never ``"UHD-FILM"``.
    """

    kind: Literal["skip", "clean", "encode"]
    reason: str | None = None
    candidate: Candidate | None = None
    tier: str | None = None


def _is_reencoded_filename(path: str) -> bool:
    """True if `path` looks like one of our prior re-encode outputs.

    Matches the `REENCODE` token inserted by `--reencode-tag` (case
    insensitive, word-boundary). Used to keep the plan gate from queueing
    a file we've already processed — without this, a replace run that
    chose a non-deletable disposal mode (recycle / backup) would surface
    its own outputs back into a future plan and re-encode them, doubling
    the marker (`...AV1.REENCODE.REENCODE.mkv`) and burning hours.
    """
    return _REENCODED_MARKER_RE.search(Path(path).stem) is not None


def _source_below_target_bitrate(pr: ProbeResult) -> bool:
    """True when the source's video bitrate is below the AV1 target for its
    resolution bucket — re-encoding can't meaningfully save space and risks
    quality regression on an already-low-bitrate source.

    Threshold comes from BITRATE_FLAG_TABLE's ``target_mbps`` (the bitrate
    AV1 aims for at this resolution). Sources at or above target are always
    admitted; sources below it are skipped unless --allow-low-bitrate
    overrides at the plan layer. ``video_bitrate <= 0`` and an unknown
    resolution bucket are not low.
    """
    if pr.video_bitrate <= 0:
        return False
    bucket = pr.resolution_class
    entry = BITRATE_FLAG_TABLE.get(bucket)
    if entry is None:
        return False
    target_mbps, _flag_mbps = entry
    return (pr.video_bitrate / 1_000_000.0) < target_mbps


def _tier_for_height(height: int | None) -> str:
    """SD / HD / UHD from PRESETS height gates. Never UHD-FILM.

    ``None`` and ``0`` fall in the SD band (``max_height`` 719). The
    remainder above HD's ``max_height`` is UHD; UHD-FILM shares that
    floor but is an opt-in preset, not a resolution tier.
    """
    h = height or 0
    if h <= PRESETS["SD"]["max_height"]:
        return "SD"
    if h <= PRESETS["HD"]["max_height"]:
        return "HD"
    return "UHD"


def decide(
    pr: ProbeResult,
    *,
    stall_count: int,
    has_existing_output: bool = False,
    allow_reencoded: bool = False,
    allow_av1: bool = False,
    allow_extras: bool = False,
    allow_low_bitrate: bool = False,
    skip_codecs: frozenset[str] | None = None,
) -> Decision:
    """Classify one probe as skip, clean, or encode.

    Skip order matches the historical plan gate, minus the missing-file
    check (that one deletes cache rows and stays in the caller):

      stalled, dv, reencoded, existing_output, av1_source,
      skipped_codec, low_bitrate, extras.

    Profile 5 is ``"dv"`` because ``dv_strategy`` returns None. Profiles
    7 and 8 are not: ``dv_strategy`` returns ``"p8_strip"``. A probe no
    rule wants is ``"clean"``; a candidate is ``"encode"`` plus a tier.
    """
    if stall_count >= 2:
        return Decision(kind="skip", reason="stalled")
    if pr.dv_profile is not None and encoder.dv_strategy(pr.dv_profile) is None:
        return Decision(kind="skip", reason="dv")
    if not allow_reencoded and _is_reencoded_filename(pr.path):
        return Decision(kind="skip", reason="reencoded")
    if has_existing_output and not allow_reencoded:
        return Decision(kind="skip", reason="existing_output")
    codec = (pr.video_codec or "").lower()
    if not allow_av1 and codec == "av1":
        return Decision(kind="skip", reason="av1_source")
    if skip_codecs and codec in skip_codecs:
        return Decision(kind="skip", reason="skipped_codec")
    if not allow_low_bitrate and _source_below_target_bitrate(pr):
        return Decision(kind="skip", reason="low_bitrate")
    if not allow_extras and crawler.is_extras_filename(Path(pr.path)):
        return Decision(kind="skip", reason="extras")
    candidate = rules.RulesEngine().evaluate(pr)
    if candidate is None:
        return Decision(kind="clean")
    return Decision(
        kind="encode",
        candidate=candidate,
        tier=_tier_for_height(pr.height),
    )
