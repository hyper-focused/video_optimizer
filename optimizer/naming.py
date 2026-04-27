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
    return out.strip(" .-_")


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
