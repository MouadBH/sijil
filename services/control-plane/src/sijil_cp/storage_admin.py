"""The object-store users the running services hold, one per job (retention-lock layer 4).

Only the one-shot `migrate` service holds the RustFS root key. It creates (idempotently):

- `SIJIL_OBJECTSTORE_APP_ACCESS_KEY` (policy `sijil-app`), which the control plane, the main
  and extraction workers, smtp and beat hold: put, read and list originals and nothing else, no
  delete of any kind, no BypassGovernanceRetention, no legal hold, no bucket configuration.
  Putting an object with a lock needs `s3:PutObjectRetention` (S3 semantics, and RustFS 1.0.4
  enforces it); without the bypass permission the store itself refuses to shorten a GOVERNANCE
  retention or replace it, so that grant can only ever extend a lock.
- `SIJIL_OBJECTSTORE_SEAL_ACCESS_KEY` (policy `sijil-seal`), which only `worker-seal` holds:
  read and set an original's retention, with the `s3:BypassGovernanceRetention` that switching
  GOVERNANCE to COMPLIANCE needs (RustFS 1.0.4 refuses the switch without it). It never reads or
  writes bytes and every delete is denied. With the bypass it could also shorten or drop a
  GOVERNANCE (unsealed) lock; it cannot touch a COMPLIANCE one.
- `SIJIL_OBJECTSTORE_REMOVAL_ACCESS_KEY` (policy `sijil-removal`), which only `worker-removal`
  holds: read an original's retention and delete a version, with the bypass, so it can delete an
  unsealed (GOVERNANCE) original or one whose lock has expired. It never reads or writes bytes,
  never changes a retention, and the unversioned delete (a delete marker) is denied.

Nobody, the root key included, can delete or shorten a COMPLIANCE lock before its date.

RustFS 1.0.4 has a MinIO-style admin API under `/rustfs/admin/v3/` that takes plain JSON and
S3 SigV4 signatures.
"""

import json
from collections.abc import Sequence
from typing import Any

import httpx
from botocore.auth import S3SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import Credentials
from pydantic import SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from sijil_core.storage import StorageSettings

APP_POLICY_NAME = "sijil-app"
_SIGNING_REGION = "us-east-1"  # S3 signing region, not the Sijil region
_ADMIN = "/rustfs/admin/v3"

APP_ALLOWED_OBJECT_ACTIONS = (
    "s3:PutObject",
    "s3:PutObjectRetention",  # required to put with a lock; cannot shorten without the bypass
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:GetObjectRetention",
)
APP_ALLOWED_BUCKET_ACTIONS = ("s3:ListBucket",)  # head_bucket, and 404 (not 403) for absent keys
# Denied explicitly, so no other policy attached later can grant them.
APP_DENIED_ACTIONS = (
    "s3:BypassGovernanceRetention",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:PutObjectLegalHold",
    "s3:DeleteBucket",
    "s3:PutBucketObjectLockConfiguration",
    "s3:PutBucketVersioning",
    "s3:PutBucketPolicy",
    "s3:DeleteBucketPolicy",
)


SEAL_POLICY_NAME = "sijil-seal"
SEAL_ALLOWED_OBJECT_ACTIONS = (
    "s3:GetObjectRetention",
    "s3:PutObjectRetention",
    # Replacing a GOVERNANCE retention by COMPLIANCE needs it; deletes stay denied below.
    "s3:BypassGovernanceRetention",
)
_BUCKET_CONFIGURATION = (
    "s3:DeleteBucket",
    "s3:PutBucketObjectLockConfiguration",
    "s3:PutBucketVersioning",
    "s3:PutBucketPolicy",
    "s3:DeleteBucketPolicy",
)
SEAL_DENIED_ACTIONS = (
    "s3:PutObject",
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:DeleteObject",
    "s3:DeleteObjectVersion",
    "s3:PutObjectLegalHold",
    *_BUCKET_CONFIGURATION,
)

REMOVAL_POLICY_NAME = "sijil-removal"
REMOVAL_ALLOWED_OBJECT_ACTIONS = (
    "s3:GetObjectRetention",  # the lock is read before deciding (RetentionActive)
    "s3:DeleteObjectVersion",
    # The pre-seal removal deletes under GOVERNANCE; a COMPLIANCE lock ignores the bypass.
    "s3:BypassGovernanceRetention",
)
REMOVAL_DENIED_ACTIONS = (
    "s3:PutObject",
    "s3:GetObject",
    "s3:GetObjectVersion",
    "s3:PutObjectRetention",
    "s3:PutObjectLegalHold",
    "s3:DeleteObject",  # unversioned: a delete marker
    *_BUCKET_CONFIGURATION,
)


class AppCredentialSettings(BaseSettings):
    """The services' object-store credential, as `migrate` provisions it."""

    model_config = SettingsConfigDict(
        env_prefix="SIJIL_OBJECTSTORE_APP_", extra="ignore", hide_input_in_errors=True
    )

    access_key: SecretStr
    secret_key: SecretStr


class SealCredentialSettings(BaseSettings):
    """The seal worker's credential (`worker-seal` alone holds it), as `migrate` provisions it."""

    model_config = SettingsConfigDict(
        env_prefix="SIJIL_OBJECTSTORE_SEAL_", extra="ignore", hide_input_in_errors=True
    )

    access_key: SecretStr
    secret_key: SecretStr


class RemovalCredentialSettings(BaseSettings):
    """The removal worker's credential (`worker-removal` alone holds it), as `migrate`
    provisions it."""

    model_config = SettingsConfigDict(
        env_prefix="SIJIL_OBJECTSTORE_REMOVAL_", extra="ignore", hide_input_in_errors=True
    )

    access_key: SecretStr
    secret_key: SecretStr


def app_policy(bucket: str) -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Originals",
                "Effect": "Allow",
                "Action": list(APP_ALLOWED_OBJECT_ACTIONS),
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            },
            {
                "Sid": "Bucket",
                "Effect": "Allow",
                "Action": list(APP_ALLOWED_BUCKET_ACTIONS),
                "Resource": [f"arn:aws:s3:::{bucket}"],
            },
            {
                "Sid": "NeverDestroy",
                "Effect": "Deny",
                "Action": list(APP_DENIED_ACTIONS),
                "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
            },
        ],
    }


def seal_policy(bucket: str) -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "Retention",
                "Effect": "Allow",
                "Action": list(SEAL_ALLOWED_OBJECT_ACTIONS),
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            },
            {
                "Sid": "NeverDestroyNorRead",
                "Effect": "Deny",
                "Action": list(SEAL_DENIED_ACTIONS),
                "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
            },
        ],
    }


def removal_policy(bucket: str) -> dict[str, Any]:
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "RemoveVersion",
                "Effect": "Allow",
                "Action": list(REMOVAL_ALLOWED_OBJECT_ACTIONS),
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            },
            {
                "Sid": "NeverReadWriteNorRelock",
                "Effect": "Deny",
                "Action": list(REMOVAL_DENIED_ACTIONS),
                "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"],
            },
        ],
    }


class AdminError(RuntimeError):
    """An admin call failed. Carries the path and status only, never a key or a body."""


class _Admin:
    def __init__(self, root: StorageSettings, client: httpx.Client) -> None:
        self._endpoint = str(root.objectstore_endpoint).rstrip("/")
        self._credentials = Credentials(
            root.objectstore_access_key.get_secret_value(),
            root.objectstore_secret_key.get_secret_value(),
        )
        self._http = client

    def put(self, path: str, params: dict[str, str], body: dict[str, Any]) -> None:
        url = str(httpx.URL(f"{self._endpoint}{_ADMIN}/{path}", params=params))
        data = json.dumps(body).encode()
        request = AWSRequest(method="PUT", url=url, data=data)
        S3SigV4Auth(self._credentials, "s3", _SIGNING_REGION).add_auth(request)
        response = self._http.put(url, content=data, headers=dict(request.headers.items()))
        if response.status_code != 200:
            raise AdminError(f"PUT {_ADMIN}/{path}: HTTP {response.status_code}")


def _ensure_user(
    root: StorageSettings,
    access_key: str,
    secret_key: SecretStr,
    policy_name: str,
    policy: dict[str, Any],
    client: httpx.Client | None,
) -> None:
    with client or httpx.Client(timeout=10.0) as http:
        admin = _Admin(root, http)
        admin.put("add-canned-policy", {"name": policy_name}, policy)
        admin.put(
            "add-user",
            {"accessKey": access_key},
            {"secretKey": secret_key.get_secret_value(), "status": "enabled"},
        )
        admin.put(
            "set-user-or-group-policy",
            {"policyName": policy_name, "userOrGroup": access_key, "isGroup": "false"},
            {},
        )


def ensure_app_user(
    root: StorageSettings, app: AppCredentialSettings, client: httpx.Client | None = None
) -> None:
    """Create or update the app user, its policy and the binding. Safe to run repeatedly."""
    access_key = app.access_key.get_secret_value()
    if access_key == root.objectstore_access_key.get_secret_value():
        raise ValueError("the app credential must not be the root credential")
    _ensure_user(
        root, access_key, app.secret_key, APP_POLICY_NAME, app_policy(root.objectstore_bucket),
        client,
    )  # fmt: skip


def ensure_seal_user(
    root: StorageSettings,
    seal: SealCredentialSettings,
    app: AppCredentialSettings,
    client: httpx.Client | None = None,
) -> None:
    """Create or update the sealing user, its policy and the binding. Safe to run repeatedly."""
    access_key = seal.access_key.get_secret_value()
    others = {root.objectstore_access_key.get_secret_value(), app.access_key.get_secret_value()}
    if access_key in others:
        raise ValueError("the sealing credential must differ from the root and app credentials")
    _ensure_user(
        root, access_key, seal.secret_key, SEAL_POLICY_NAME, seal_policy(root.objectstore_bucket),
        client,
    )  # fmt: skip


def ensure_removal_user(
    root: StorageSettings,
    removal: RemovalCredentialSettings,
    others: Sequence[AppCredentialSettings | SealCredentialSettings],
    client: httpx.Client | None = None,
) -> None:
    """Create or update the removal user, its policy and the binding. Safe to run repeatedly."""
    access_key = removal.access_key.get_secret_value()
    taken = {root.objectstore_access_key.get_secret_value()}
    taken |= {other.access_key.get_secret_value() for other in others}
    if access_key in taken:
        raise ValueError("the removal credential must differ from every other credential")
    _ensure_user(
        root, access_key, removal.secret_key, REMOVAL_POLICY_NAME,
        removal_policy(root.objectstore_bucket), client,
    )  # fmt: skip
