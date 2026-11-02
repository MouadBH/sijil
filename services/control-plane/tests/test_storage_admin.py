"""The least-privilege object-store user that `bootstrap-storage` provisions (respx)."""

import json

import httpx
import pytest
import respx
from pydantic import AnyHttpUrl, SecretStr

from sijil_core.storage import StorageSettings
from sijil_cp import cli
from sijil_cp.storage_admin import (
    AdminError,
    AppCredentialSettings,
    app_policy,
    ensure_app_user,
)

ENDPOINT = "http://objectstore:9000"
ROOT = StorageSettings.model_construct(
    region="ma-cas",
    environment="test",
    objectstore_endpoint=AnyHttpUrl(ENDPOINT),
    objectstore_access_key=SecretStr("root-key"),
    objectstore_secret_key=SecretStr("root-secret"),
    objectstore_bucket="sijil-originals-ma-cas",
)
APP = AppCredentialSettings.model_construct(
    access_key=SecretStr("sijil-app"), secret_key=SecretStr("app-secret-123")
)
FORBIDDEN = {
    "s3:BypassGovernanceRetention",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:PutObjectLegalHold",
}


def test_policy_grants_put_read_list_only() -> None:
    policy = app_policy("sijil-originals-ma-cas")
    allowed = {
        action
        for statement in policy["Statement"]
        if statement["Effect"] == "Allow"
        for action in statement["Action"]
    }
    denied = {
        action
        for statement in policy["Statement"]
        if statement["Effect"] == "Deny"
        for action in statement["Action"]
    }
    assert allowed == {
        "s3:PutObject",
        "s3:PutObjectRetention",
        "s3:GetObject",
        "s3:GetObjectVersion",
        "s3:GetObjectRetention",
        "s3:ListBucket",
    }
    assert not any("*" in action for action in allowed)
    assert FORBIDDEN <= denied
    assert not allowed & denied
    resources = {r for s in policy["Statement"] for r in s["Resource"]}
    assert resources <= {
        "arn:aws:s3:::sijil-originals-ma-cas",
        "arn:aws:s3:::sijil-originals-ma-cas/*",
    }


def test_ensure_app_user_creates_policy_user_and_binding() -> None:
    with respx.mock(assert_all_called=True) as router:
        policy = router.put(f"{ENDPOINT}/rustfs/admin/v3/add-canned-policy").respond(200)
        user = router.put(f"{ENDPOINT}/rustfs/admin/v3/add-user").respond(200)
        bind = router.put(f"{ENDPOINT}/rustfs/admin/v3/set-user-or-group-policy").respond(200)
        ensure_app_user(ROOT, APP)
    sent = policy.calls.last.request
    assert sent.url.params["name"] == "sijil-app"
    assert json.loads(sent.content) == app_policy("sijil-originals-ma-cas")
    assert "Credential=root-key/" in sent.headers["Authorization"]  # SigV4 with the root key
    assert "x-amz-content-sha256" in sent.headers
    created = user.calls.last.request
    assert created.url.params["accessKey"] == "sijil-app"
    assert json.loads(created.content) == {"secretKey": "app-secret-123", "status": "enabled"}
    bound = bind.calls.last.request.url.params
    assert (bound["policyName"], bound["userOrGroup"], bound["isGroup"]) == (
        "sijil-app",
        "sijil-app",
        "false",
    )


def test_admin_failure_names_the_call_but_no_secret() -> None:
    with respx.mock(assert_all_called=False) as router:
        router.put(f"{ENDPOINT}/rustfs/admin/v3/add-canned-policy").respond(403, text="denied")
        with pytest.raises(AdminError) as raised:
            ensure_app_user(ROOT, APP)
    message = str(raised.value)
    assert "add-canned-policy" in message
    assert "403" in message
    assert "root-secret" not in message
    assert "app-secret-123" not in message


def test_app_credential_must_differ_from_root() -> None:
    same = AppCredentialSettings.model_construct(
        access_key=SecretStr("root-key"), secret_key=SecretStr("x")
    )
    with respx.mock(assert_all_called=False) as router, pytest.raises(ValueError):
        ensure_app_user(ROOT, same)
    assert router.calls.call_count == 0


def test_bootstrap_storage_provisions_the_app_user(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []

    class FakeStore:
        def __init__(self, settings: StorageSettings) -> None:
            pass

        async def ensure_bucket(self) -> None:
            calls.append("bucket")

    monkeypatch.setattr(cli, "S3ObjectStore", FakeStore)
    monkeypatch.setattr(cli, "StorageSettings", lambda: ROOT)
    monkeypatch.setattr(cli, "AppCredentialSettings", lambda: APP)
    monkeypatch.setattr(
        cli, "ensure_app_user", lambda root, app: calls.append(f"user {app.access_key}")
    )
    cli.bootstrap_storage()
    assert calls == ["bucket", "user **********"]


def test_bootstrap_storage_retries_while_the_admin_api_starts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    attempts: list[int] = []

    class FakeStore:
        def __init__(self, settings: StorageSettings) -> None:
            pass

        async def ensure_bucket(self) -> None:
            return None

    def flaky(root: StorageSettings, app: AppCredentialSettings) -> None:
        attempts.append(1)
        if len(attempts) == 1:
            raise httpx.ConnectError("starting")

    monkeypatch.setattr(cli, "S3ObjectStore", FakeStore)
    monkeypatch.setattr(cli, "StorageSettings", lambda: ROOT)
    monkeypatch.setattr(cli, "AppCredentialSettings", lambda: APP)
    monkeypatch.setattr(cli, "ensure_app_user", flaky)
    monkeypatch.setattr("sijil_cp.cli.time.sleep", lambda _s: None)
    cli.bootstrap_storage()
    assert len(attempts) == 2
