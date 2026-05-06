"""Rules engine for video_optimizer.

Each Rule.evaluate(probe) returns a RuleVerdict. The RulesEngine runs the
configured rules and, when at least one non-advisory rule fires, returns a
Candidate describing the recommended action.
"""

from __future__ import annotations

from .models import Candidate, ProbeResult, RuleVerdict
from .presets import BITRATE_FLAG_TABLE

# --------------------------------------------------------------------------- #
# Constants
# --------------------------------------------------------------------------- #


_LEGACY_CODECS: frozenset[str] = frozenset({
    "mpeg2video", "mpeg4",
    "msmpeg4v1", "msmpeg4v2", "msmpeg4v3",
    "vc1", "wmv1", "wmv2", "wmv3",
    "h263",
    "rv10", "rv20", "rv30", "rv40",
    "theora",
})


_LEGACY_CONTAINERS: frozenset[str] = frozenset({
    "avi", "wmv", "asf", "flv", "mpeg", "vob", "mpegts",
})


_MODERN_CODECS: frozenset[str] = frozenset({"h264", "hevc", "av1", "vp9"})


# Codecs older or less efficient than AV1 that benefit from a transcode
# whenever the source bitrate is at or above the AV1 target bitrate for
# the file's resolution. h.264 is the dominant case — widely used, still
# noticeably less efficient than AV1 at matched perceptual quality.
_INEFFICIENT_CODECS: frozenset[str] = frozenset({"h264"})


_MB = 1024.0 * 1024.0


# --------------------------------------------------------------------------- #
# Rule base
# --------------------------------------------------------------------------- #


class Rule:
    """Base class for rules; subclasses override name and evaluate()."""

    name: str = ""
    advisory: bool = False

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        """Inspect probe and return a RuleVerdict (fired or not)."""
        raise NotImplementedError


def _miss(name: str) -> RuleVerdict:
    """Return a non-fired verdict for the given rule name."""
    return RuleVerdict(rule=name, fired=False)


# --------------------------------------------------------------------------- #
# OverBitratedRule
# --------------------------------------------------------------------------- #


class OverBitratedRule(Rule):
    """Flag files whose video bitrate exceeds the per-resolution threshold."""

    name = "over_bitrate"
    advisory = False

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        """Fire when video_bitrate > flag_threshold for the file's resolution bucket."""
        if probe.duration_seconds <= 0 or probe.video_bitrate <= 0:
            return _miss(self.name)

        bucket = probe.resolution_class
        entry = BITRATE_FLAG_TABLE.get(bucket)
        if entry is None:
            return _miss(self.name)

        target_mbps, flag_mbps = entry
        actual_bps = probe.video_bitrate
        actual_mbps = actual_bps / 1_000_000.0
        if actual_mbps <= flag_mbps:
            return _miss(self.name)

        target_bps = int(target_mbps * 1_000_000)
        savings_bytes = (actual_bps - target_bps) * probe.duration_seconds / 8.0
        savings_mb = max(savings_bytes / _MB, 0.0)

        severity = "high" if actual_mbps > 2 * flag_mbps else "medium"
        reason = (
            f"{bucket} video at {actual_mbps:.1f} Mbps exceeds flag "
            f"threshold {flag_mbps:.1f} Mbps (target {target_mbps:.1f} Mbps)"
        )
        return RuleVerdict(
            rule=self.name,
            fired=True,
            reason=reason,
            severity=severity,
            projected_savings_mb=savings_mb,
            notes={
                "resolution_class": bucket,
                "actual_mbps": round(actual_mbps, 3),
                "target_mbps": target_mbps,
                "flag_mbps": flag_mbps,
            },
        )


# --------------------------------------------------------------------------- #
# LegacyCodecRule
# --------------------------------------------------------------------------- #


def _codec_set_verdict(probe: ProbeResult, name: str, *,
                       codec_set: frozenset[str], savings_frac: float,
                       severity: str, reason: str,
                       height_band: tuple[int, int] | None = None,
                       ) -> RuleVerdict:
    """Shared evaluator for codec-membership rules. ``reason`` may use {codec}.
    ``height_band=(low, high)`` gates on probe height in [low, high)."""
    codec = (probe.video_codec or "").lower()
    if codec not in codec_set:
        return _miss(name)
    h = probe.height or 0
    if height_band and not (height_band[0] <= h < height_band[1]):
        return _miss(name)
    notes = {"codec": codec, "height": h} if height_band else {"codec": codec}
    return RuleVerdict(
        rule=name, fired=True, severity=severity,
        reason=reason.format(codec=codec),
        projected_savings_mb=max((probe.size or 0) * savings_frac / _MB, 0.0),
        notes=notes,
    )


class LegacyCodecRule(Rule):
    """Flag files using legacy/obsolete video codecs (MPEG-2, VC-1, WMV, ...)."""

    name = "legacy_codec"
    advisory = False

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        return _codec_set_verdict(
            probe, self.name,
            codec_set=_LEGACY_CODECS,
            savings_frac=0.5, severity="high",
            reason="legacy codec {codec!r}; modern encode typically halves size",
        )


# --------------------------------------------------------------------------- #
# InefficientCodecRule / ContainerMigrationRule
# --------------------------------------------------------------------------- #


class InefficientCodecRule(Rule):
    """Flag h.264 (and similar) at HD: always a transcode candidate.

    AV1 is materially more efficient than h.264 at HD perceptual
    quality. CQ-based encoding preserves quality even when the source
    is heavily compressed (the worst case is an output similar in
    size to the source — never a perceptual regression). So fire
    unconditionally for h.264 in the HD band; SD is excluded upstream
    by the plan-time gate, and UHD is handled by UhdNonAv1Rule which
    catches a wider codec set.
    """

    name = "inefficient_codec"
    advisory = False

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        return _codec_set_verdict(
            probe, self.name,
            codec_set=_INEFFICIENT_CODECS,
            savings_frac=0.30, severity="medium",
            reason="{codec} at HD; AV1 transcode (CQ-preserved quality)",
            height_band=(720, 1440),
        )


class UhdNonAv1Rule(Rule):
    """At UHD, anything other than AV1 is a re-encode candidate.

    The savings deltas at 4K are large (HEVC→AV1 typically ~20-30%);
    AV1 is the explicit target for the UHD library. AV1 sources are
    excluded upstream by the plan-time gate, so this rule only sees
    HEVC, h.264, VP9, etc. at UHD and fires on all of them.
    """

    name = "uhd_non_av1"
    advisory = False
    _UHD_MIN_HEIGHT = 1440

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        """Fire for non-AV1 codecs at UHD frame heights."""
        codec = (probe.video_codec or "").lower()
        if codec == "av1":
            return _miss(self.name)
        height = probe.height or 0
        if height < self._UHD_MIN_HEIGHT:
            return _miss(self.name)
        savings_mb = max((probe.size or 0) * 0.25 / _MB, 0.0)
        return RuleVerdict(
            rule=self.name,
            fired=True,
            reason=f"{codec} at UHD; AV1 target codec for this tier",
            severity="high",
            projected_savings_mb=savings_mb,
            notes={"codec": codec, "height": height},
        )


class ContainerMigrationRule(Rule):
    """Flag files in legacy containers that should migrate to mp4/mkv."""

    name = "container_migration"
    advisory = False

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        """Fire when the container is on the legacy list (avi/wmv/mpeg/...)."""
        container = (probe.container or "").lower()
        if container not in _LEGACY_CONTAINERS:
            return _miss(self.name)

        savings_mb = max((probe.size or 0) * 0.02 / _MB, 0.0)
        reason = f"legacy container {container!r}; remux to modern wrapper"
        return RuleVerdict(
            rule=self.name,
            fired=True,
            reason=reason,
            severity="low",
            projected_savings_mb=savings_mb,
            notes={"container": container},
        )


# --------------------------------------------------------------------------- #
# HdrAdvisoryRule
# --------------------------------------------------------------------------- #


class HdrAdvisoryRule(Rule):
    """Advisory: annotate HDR files; never the sole reason for a candidate."""

    name = "hdr_advisory"
    advisory = True

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        """Fire when probe.is_hdr; advisory only — never sole reason for a candidate."""
        if not probe.is_hdr:
            return _miss(self.name)

        reason = "HDR source; metadata will pass through to AV1 output"
        return RuleVerdict(
            rule=self.name,
            fired=True,
            reason=reason,
            severity="medium",
            projected_savings_mb=None,
            notes={
                "color_transfer": probe.color_transfer,
                "color_primaries": probe.color_primaries,
            },
        )


# --------------------------------------------------------------------------- #
# Registry
# --------------------------------------------------------------------------- #


RULES: dict[str, Rule] = {
    "over_bitrate":         OverBitratedRule(),
    "legacy_codec":         LegacyCodecRule(),
    "inefficient_codec":    InefficientCodecRule(),
    "uhd_non_av1":          UhdNonAv1Rule(),
    "container_migration":  ContainerMigrationRule(),
    "hdr_advisory":         HdrAdvisoryRule(),
}


# --------------------------------------------------------------------------- #
# Engine
# --------------------------------------------------------------------------- #


class RulesEngine:
    """Run a set of rules against a ProbeResult and produce a Candidate."""

    def __init__(self, enabled: list[str] | None = None, target: str = "av1+mkv"):
        if enabled is None:
            self._rules = list(RULES.values())
        else:
            unknown = [k for k in enabled if k not in RULES]
            if unknown:
                raise ValueError(f"Unknown rule(s): {', '.join(unknown)}")
            self._rules = [RULES[k] for k in enabled]
        self.target = target

    def evaluate(self, probe: ProbeResult) -> Candidate | None:
        """Return a Candidate iff at least one non-advisory rule fired."""
        non_advisory_fired: list[RuleVerdict] = []
        advisory_fired: list[RuleVerdict] = []

        for rule in self._rules:
            verdict = rule.evaluate(probe)
            if not verdict.fired:
                continue
            if rule.advisory:
                advisory_fired.append(verdict)
            else:
                non_advisory_fired.append(verdict)

        if not non_advisory_fired:
            return None

        fired = list(non_advisory_fired) + list(advisory_fired)

        only_container = (
            len(non_advisory_fired) == 1
            and non_advisory_fired[0].rule == ContainerMigrationRule.name
        )
        codec = (probe.video_codec or "").lower()
        remux_only = bool(only_container and codec in _MODERN_CODECS)

        return Candidate(
            probe=probe,
            fired=fired,
            target=self.target,
            remux_only=remux_only,
            is_hdr=probe.is_hdr,
        )
