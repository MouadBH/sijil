from sijil_core.text import normalise


def test_normalise_maps_presentation_forms() -> None:
    assert normalise("ﻓﺎﺗﻮﺭﺓ") == "فاتورة"


def test_normalise_collapses_nbsp_and_thin_space() -> None:
    assert normalise("a  b c  d\t\te") == "a b c d e"
    assert normalise("a  b\nc   d") == "a b\nc d"


def test_normalise_strips_bidi_controls() -> None:
    raw = "‫‪ ٦١٥٫٨٠‬" + "درهم‬"
    out = normalise(raw)
    assert out == " ٦١٥٫٨٠درهم"
    assert not any("‪" <= c <= "‮" for c in out)
    assert normalise("‎a‏؜b⁦c⁩") == "abc"
