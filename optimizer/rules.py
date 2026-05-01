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


class LegacyCodecRule(Rule):
    """Flag files using legacy/obsolete video codecs."""

    name = "legacy_codec"
    advisory = False

    def evaluate(self, probe: ProbeResult) -> RuleVerdict:
        """Fire when the source codec is on the legacy list (MPEG-2, VC-1, WMV, ...)."""
        codec = (probe.video_codec or "").lower()
        if codec not in _LEGACY_CODECS:
            return _miss(self.name)

        savings_mb = max((probe.size or 0) * 0.5 / _MB, 0.0)
        reason = f"legacy codec {codec!r}; modern encode typically halves size"
        return RuleVerdict(
            rule=self.name,
            fired=True,
            reason=reason,
            severity="high",
            projected_savings_mb=savings_mb,
            notes={"codec": codec},
        )


# --------------------------------------------------------------------------- #
# ContainerMigrationRule
# --------------------------------------------------------------------------- #


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
