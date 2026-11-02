"""Text normalisation for OCR output and model-copied values."""

import re
import unicodedata

_BIDI_CONTROLS = re.compile("[‎‏؜‪-‮⁦-⁩]")
_HSPACE = re.compile("[ \t   ]+")


def normalise(s: str) -> str:
    """NFKC, drop bidi controls, collapse horizontal whitespace (newlines are kept)."""
    s = unicodedata.normalize("NFKC", s)
    s = _BIDI_CONTROLS.sub("", s)
    return _HSPACE.sub(" ", s)
