"""Filename rewriting helpers for codec-aware renaming.

Used when --rewrite-codec / --reencode-tag are passed to apply: strip foreign
codec tokens (e.g. H.264 / HEVC) from a filename stem and insert the canonical
token for the new target codec. Plex/Sonarr-style dotted filenames are the
default output style when rewriting.
"""

from __future__ import annotations

import re

# Tokens to strip per target codec. Each entry is a regex *fragment* (without
# the surrounding word-boundary lookarounds, which are added at compile time).
_FOREIGN_CODEC_TOKENS: dict[str, list[str]] = {
    "av1": [r"H\.?264", r"H\.?265", r"HEVC", r"x264", r"x265", r"AVC"],
    "hevc": [r"H\.?264", r"x264", r"AVC", r"AV1", r"SVT-?AV1"],
    "h264": [r"H\.?265", r"HEVC", r"x265", r"AV1", r"SVT-?AV1"],
}


# HDR/DV tokens that don't survive the target encode and should be scrubbed
# from the rewritten filename so Plex/Jellyfin don't try to map a stream that
# isn't actually there. av1_qsv (the av1 target's hardware path) carries only
# the static HDR10 mastering-display + MaxCLL side data; HDR10+ dynamic
# metadata (SMPTE 2094-40) is dropped, and the Dolby Vision RPU is always
# stripped by the DV pre-pass. Static HDR10 (`HDR`, `HDR10`) IS preserved, so
# those tokens stay in the name.
_LOST_METADATA_TOKENS: dict[str, list[str]] = {
    "av1": [
        r"HDR10Plus", r"HDR10\+",
        r"Dolby[. _-]?Vision", r"DoVi", r"DV",
    ],
}


# Canonical token written into the rewritten filename.
_CANONICAL_TOKEN: dict[str, str] = {
    "av1":  "AV1",
    "hevc": "HEVC",
    "h264": "H.264",
}


def looks_dotted(stem: str) -> bool:
    """True if dots dominate over whitespace as token separators in the stem."""
    dot_count = stem.count(".")
    space_count = sum(1 for ch in stem if ch.isspace())
    return dot_count > space_count and dot_count > 0


def to_dotted(stem: str) -> str:
    """Convert a spaced/underscored stem to dotted style."""
    out = re.sub(r"[\s_]+", ".", stem)
    out = re.sub(r"\.{2,}", ".", out)
    return out.strip(".")


def _strip_foreign_tokens(stem: str, target_codec: str) -> str:
    """Remove codec tokens that don't match the target codec."""
    tokens = _FOREIGN_CODEC_TOKENS.get(target_codec, [])
    if not tokens:
        return stem
    pattern = (
        r"(?<![A-Za-z0-9])(?:" + "|".join(tokens) + r")(?![A-Za-z0-9])"
    )
    return re.sub(pattern, "", stem, flags=re.IGNORECASE)


def _has_hdr_token(stem: str) -> bool:
    """True if the stem already advertises plain HDR / HDR10 (not HDR10+ / HDR10Plus).

    The trailing `+` exclusion matters: `HDR10+` is a *dynamic* metadata
    marker the av1_qsv pipeline can't preserve, so it must not satisfy
    the "already labelled HDR" check or we'd skip the HDR10 insert and
    leave the filename without any HDR token at all.
    """
    return bool(re.search(
        r"(?<![A-Za-z0-9])HDR(?:10|2000)?(?![A-Za-z0-9+])",
        stem, flags=re.IGNORECASE,
    ))


def _substitute_lost_metadata_tokens(stem: str, target_codec: str) -> str:
    """Replace DV / HDR10+ tokens with HDR10 (or drop them if HDR10 is already
    advertised) so the rewritten name accurately reflects what the encode path
    actually preserves.

    DV Profile 7/8 sources always carry an HDR10 base layer (Profile 5 is
    skipped upstream because it has no HDR10 fallback). After the DV RPU
    is stripped and HDR10+ dynamic metadata is dropped by av1_qsv, the
    residual stream is plain HDR10 — the filename should say so for
    Radarr Custom Format matching and downstream library tooling.

    Strategy: substitute the *first* matching token in-place with `HDR10`
    (preserves position relative to the resolution/audio tags), then
    strip any remaining matches. If an `HDR10` / `HDR` token is already
    present, just strip everything — no need to duplicate.
    """
    tokens = _LOST_METADATA_TOKENS.get(target_codec, [])
    if not tokens:
        return stem
    pattern = re.compile(
        r"(?<![A-Za-z0-9])(?:" + "|".join(tokens) + r")(?![A-Za-z0-9])",
        flags=re.IGNORECASE,
    )
    if not pattern.search(stem):
        return stem
    if _has_hdr_token(stem):
        return pattern.sub("", stem)
    stem = pattern.sub("HDR10", stem, count=1)
    return pattern.sub("", stem)


def _has_canonical_token(stem: str, canonical: str) -> bool:
    """True if the canonical codec token already appears as a standalone word."""
    pattern = (
        r"(?<![A-Za-z0-9])" + re.escape(canonical) + r"(?![A-Za-z0-9])"
    )
    return bool(re.search(pattern, stem, flags=re.IGNORECASE))


def _cleanup_separators(stem: str) -> str:
    """Collapse double-separators left behind after stripping tokens."""
    out = re.sub(r"\.{2,}", ".", stem)
    out = re.sub(r"\s{2,}", " ", out)
    out = re.sub(r"\[\s*\]", "", out)
    out = re.sub(r"\(\s*\)", "", out)
    out = re.sub(r"\s+\.|\.\s+", ".", out)
    out = re.sub(r"\s+-|-\s+", "-", out)
    out = re.sub(r"\.-|-\.", "-", out)
    # Drop orphaned `+` left behind after stripping a joined token pair
    # (e.g. `HEVC+H.265` → `+` once both sides are removed).
    out = re.sub(r"(?<![A-Za-z0-9])\+(?![A-Za-z0-9])", "", out)
    out = re.sub(r"\.{2,}", ".", out)
    return out.strip(" .-_+")


def rewrite_codec_tokens(stem: str, target_codec: str, *,
                         dotted: bool = True) -> str:
    """Strip foreign codec tokens and insert the canonical target token.

    target_codec is one of 'av1', 'hevc', 'h264'. dotted=True forces Plex-style
    dot separators on the result (recommended); set False to preserve the
    input's whitespace style.
    """
    canonical = _CANONICAL_TOKEN.get(target_codec)
    if canonical is None:
        return stem

    out = _strip_foreign_tokens(stem, target_codec)
    out = _substitute_lost_metadata_tokens(out, target_codec)
    out = _cleanup_separators(out)

    if not _has_canonical_token(out, canonical):
        sep = "." if (dotted or looks_dotted(out)) else " "
        out = f"{out}{sep}{canonical}" if out else canonical

    if dotted:
        out = to_dotted(out)

    return _cleanup_separators(out)


def append_token(stem: str, token: str, *, dotted: bool | None = None) -> str:
    """Append `token` to the stem using the inferred separator style."""
    if not token:
        return stem
    if dotted is None:
        use_dot = looks_dotted(stem)
    else:
        use_dot = dotted
    sep = "." if use_dot else " "
    return f"{stem}{sep}{token}"
