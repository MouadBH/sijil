import base64
import os

import pytest
from pydantic import ValidationError

from sijil_core.crypto import (
    DecryptionError,
    Envelope,
    KeySettings,
    LocalKeyService,
    open_envelope,
    seal,
)

CTX = "ws1:service_token"


@pytest.fixture
def ks() -> LocalKeyService:
    return LocalKeyService(os.urandom(32))


def test_roundtrip(ks: LocalKeyService) -> None:
    env = seal(b"s3cret", ks, "ws1", CTX)
    assert b"s3cret" not in env.ciphertext
    assert open_envelope(env, ks, CTX) == b"s3cret"


def test_tampered_ciphertext_fails(ks: LocalKeyService) -> None:
    env = seal(b"s3cret", ks, "ws1", CTX)
    bad = bytearray(env.ciphertext)
    bad[0] ^= 1
    tampered = Envelope(env.key_id, env.wrapped_key, env.nonce, bytes(bad))
    with pytest.raises(DecryptionError):
        open_envelope(tampered, ks, CTX)


def test_other_tenant_key_cannot_open(ks: LocalKeyService) -> None:
    env = seal(b"s3cret", ks, "ws1", CTX)
    forged = Envelope("ws2", env.wrapped_key, env.nonce, env.ciphertext)
    with pytest.raises(DecryptionError):
        open_envelope(forged, ks, CTX)


def test_wrong_context_fails(ks: LocalKeyService) -> None:
    env = seal(b"s3cret", ks, "ws1", "ws1:service_token")
    with pytest.raises(DecryptionError):
        open_envelope(env, ks, "ws1:admin_password")


def test_envelope_bytes_roundtrip(ks: LocalKeyService) -> None:
    env = seal(b"s3cret", ks, "ws1", CTX)
    raw = env.to_bytes()
    assert raw.startswith(b"SJE1")
    assert Envelope.from_bytes(raw) == env
    assert open_envelope(Envelope.from_bytes(raw), ks, CTX) == b"s3cret"


def test_from_bytes_rejects_garbage() -> None:
    for raw in (b"", b"XXXX", b"SJE1\x00", b"SJE1\x00\x03ws1\x00\x40short"):
        with pytest.raises(DecryptionError):
            Envelope.from_bytes(raw)


def test_master_key_must_be_32_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIJIL_REGION", "ma-cas")
    monkeypatch.setenv("SIJIL_ENVIRONMENT", "test")
    monkeypatch.setenv("SIJIL_MASTER_KEY", base64.b64encode(os.urandom(31)).decode())
    with pytest.raises(ValidationError):
        KeySettings()
    monkeypatch.setenv("SIJIL_MASTER_KEY", "not base64!!")
    with pytest.raises(ValidationError):
        KeySettings()
    good = os.urandom(32)
    monkeypatch.setenv("SIJIL_MASTER_KEY", base64.b64encode(good).decode())
    assert KeySettings().master_key_bytes() == good


def test_bad_master_key_not_echoed_in_error(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SIJIL_REGION", "ma-cas")
    monkeypatch.setenv("SIJIL_ENVIRONMENT", "test")
    bad = base64.b64encode(os.urandom(31)).decode()
    monkeypatch.setenv("SIJIL_MASTER_KEY", bad)
    with pytest.raises(ValidationError) as exc:
        KeySettings()
    assert bad not in str(exc.value)


def test_nonce_unique_per_seal(ks: LocalKeyService) -> None:
    nonces = {seal(b"x", ks, "ws1", CTX).nonce for _ in range(1000)}
    assert len(nonces) == 1000


def test_derive_is_32_bytes_and_domain_separated(ks: LocalKeyService) -> None:
    a = ks.derive(b"sijil:recovery-codes")
    assert len(a) == 32
    assert a != ks.derive(b"other")
