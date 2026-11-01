import hashlib
from datetime import UTC, datetime, timedelta, timezone
from uuid import UUID

from sijil_core import audit


def test_canonical_json_is_stable() -> None:
    obj = {
        "z": 1,
        "a": "مرحبا",
        "at": datetime(2026, 11, 1, 10, 30, 5, 123456, tzinfo=UTC),
        "id": UUID("12345678-1234-5678-1234-567812345678"),
        "n": None,
    }
    assert (
        audit.canonical_json(obj)
        == (
            '{"a":"مرحبا","at":"2026-11-01T10:30:05.123456Z",'
            '"id":"12345678-1234-5678-1234-567812345678","n":null,"z":1}'
        ).encode()
    )


def test_canonical_json_normalises_offsets_and_nesting() -> None:
    tz = timezone(timedelta(hours=1))
    a = audit.canonical_json({"b": {"y": 1, "x": datetime(2026, 1, 1, 1, 0, tzinfo=tz)}})
    assert a == b'{"b":{"x":"2026-01-01T00:00:00Z","y":1}}'


def test_event_hash_formula() -> None:
    prev = "0" * 64
    expected = hashlib.sha256(bytes.fromhex(prev) + b'{"a":1}').hexdigest()
    assert audit.event_hash(prev, {"a": 1}) == expected
