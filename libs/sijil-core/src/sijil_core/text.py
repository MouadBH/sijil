"""Text normalisation for OCR output and model-copied values."""

import re
import unicodedata

_BIDI_CONTROLS = re.compile("[\u200e\u200f\u061c\u202a-\u202e\u2066-\u2069]")
_HSPACE = re.compile("[ \t\u00a0\u202f\u2009]+")


def normalise(s: str) -> str:
    """NFKC, drop bidi controls, collapse horizontal whitespace (newlines are kept)."""
    s = unicodedata.normalize("NFKC", s)
    s = _BIDI_CONTROLS.sub("", s)
    return _HSPACE.sub(" ", s)
