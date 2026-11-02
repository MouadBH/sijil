from sijil_core.text import normalise


def test_normalise_maps_presentation_forms() -> None:
    assert normalise("ﻓﺎﺗﻮﺭﺓ") == "فاتورة"


def test_normalise_collapses_nbsp_and_thin_space() -> None:
    assert normalise("a\u00a0\u00a0b\u202fc\u2009 d\t\te") == "a b c d e"
    assert normalise("a  b\nc   d") == "a b\nc d"


def test_normalise_strips_bidi_controls() -> None:
    raw = "\u202b\u202a ٦١٥٫٨٠\u202c" + "درهم\u202c"
    out = normalise(raw)
    assert out == " ٦١٥٫٨٠درهم"
    assert not any("\u202a" <= c <= "\u202e" for c in out)
    assert normalise("\u200ea\u200f\u061cb\u2066c\u2069") == "abc"
