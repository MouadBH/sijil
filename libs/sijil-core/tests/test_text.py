from sijil_core.text import normalise, search_normalise


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


def test_search_normalise_arabic_variants() -> None:
    assert search_normalise("فاتورة") == search_normalise("فاتوره")
    assert search_normalise("فـــاتورة") == search_normalise("فاتورة")  # tatweel
    assert search_normalise("فَاتُورَةٌ") == search_normalise("فاتورة")  # harakat
    assert search_normalise("أحمد") == search_normalise("احمد")
    assert search_normalise("إسماعيل آمنة ٱلدار") == "اسماعيل امنه الدار"
    assert search_normalise("مصطفى") == search_normalise("مصطفي")
    assert search_normalise("٢٠٢٦") == "2026"
    assert search_normalise("۲۰۲۶") == "2026"


def test_search_normalise_french_accents() -> None:
    assert search_normalise("Télécom") == "telecom"
    assert search_normalise("ÉCHANTILLON Façade Œuvre") == "echantillon facade œuvre"


def test_search_normalise_applies_normalise_first() -> None:
    assert search_normalise("ﻓﺎﺗﻮﺭﺓ  ‏F-2026") == "فاتوره f-2026"
