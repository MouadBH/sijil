"""Text normalisation for OCR output and model-copied values."""

import re
import unicodedata

_BIDI_CONTROLS = re.compile("[\u200e\u200f\u061c\u202a-\u202e\u2066-\u2069]")
_HSPACE = re.compile("[ \t\u00a0\u202f\u2009]+")
# Arabic tatweel and harakat (fathatan .. sukun); the harakat are combining marks too.
_ARABIC_MARKS = re.compile("[\u0640\u064b-\u0652]")
_SEARCH_MAP = str.maketrans(
    "أإآٱىة٠١٢٣٤٥٦٧٨٩۰۱۲۳۴۵۶۷۸۹",
    "اااايه01234567890123456789",
)


def normalise(s: str) -> str:
    """NFKC, drop bidi controls, collapse horizontal whitespace (newlines are kept)."""
    s = unicodedata.normalize("NFKC", s)
    s = _BIDI_CONTROLS.sub("", s)
    return _HSPACE.sub(" ", s)


def search_normalise(s: str) -> str:
    """`normalise`, then the folding search applies to both the text and the query: casefold,
    Latin accents stripped (NFKD, combining marks dropped), Arabic tatweel and harakat dropped,
    `أ إ آ ٱ` → `ا`, `ى` → `ي`, `ة` → `ه`, Arabic-Indic and Persian digits → ASCII.

    The index database's SQL function `search_normalise(text)` (index schema v3) mirrors it for
    the generated `search_doc.tsv`; libs/sijil-core/tests/test_search_index.py checks they agree."""
    s = normalise(s).casefold()
    s = "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))
    s = _ARABIC_MARKS.sub("", s)
    return s.translate(_SEARCH_MAP)
