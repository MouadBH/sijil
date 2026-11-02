import re

from sijil_core import inbound

DOMAIN = "in.sijil.local"


def test_new_token_is_ten_lowercase_base32_chars() -> None:
    tokens = {inbound.new_token() for _ in range(50)}
    assert len(tokens) == 50
    assert all(re.fullmatch(r"[a-z2-7]{10}", t) for t in tokens)


def test_inbound_parse_roundtrip_and_rejects_foreign_domain() -> None:
    token = inbound.new_token()
    addr = inbound.address("atlas-sarl-ab12cd", token, DOMAIN)
    assert addr == f"atlas-sarl-ab12cd.{token}@{DOMAIN}"
    assert inbound.parse(addr, DOMAIN) == token
    assert inbound.parse(addr.upper(), DOMAIN) == token
    assert inbound.parse(addr, "IN.Sijil.Local") == token
    for bad in (
        f"atlas.{token}@evil.example",
        f"atlas.{token}@{DOMAIN}.evil.example",
        f"atlas.{token}@x{DOMAIN}",
        f"atlas.{token[:-1]}@{DOMAIN}",
        f"atlas.{token}0@{DOMAIN}",
        f"atlas.{token}@{DOMAIN} ",
        f"<atlas.{token}@{DOMAIN}>",
        f"{token}@{DOMAIN}",
        f"a@b.{token}@{DOMAIN}",
        f"atlas.{token}",
        "",
    ):
        assert inbound.parse(bad, DOMAIN) is None, bad
