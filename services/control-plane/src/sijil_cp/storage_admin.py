"""The least-privilege object-store user every running service uses (retention-lock layer 4).

Only the one-shot `migrate` service holds the RustFS root key. It creates (idempotently) the user
`SIJIL_OBJECTSTORE_APP_ACCESS_KEY` with a policy that can put, read and list originals and
nothing else: no delete of any kind, no BypassGovernanceRetention, no legal hold, no bucket
configuration. Putting an object with a lock needs `s3:PutObjectRetention` (S3 semantics, and
RustFS 1.0.4 enforces it); without the bypass permission the store itself refuses to shorten a
GOVERNANCE retention or replace it, so that grant can only ever extend a lock.

RustFS 1.0.4 has a MinIO-style admin API under `/rustfs/admin/v3/` that takes plain JSON and
S3 SigV4 signatures. The sealing path (M3/M4: COMPLIANCE, governance removal) needs a separate,
narrower credential; it is not this one.
"""

import json
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


class AppCredentialSettings(BaseSettings):
    """The services' object-store credential, as `migrate` provisions it."""

    model_config = SettingsConfigDict(
        env_prefix="SIJIL_OBJECTSTORE_APP_", extra="ignore", hide_input_in_errors=True
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


def ensure_app_user(
    root: StorageSettings, app: AppCredentialSettings, client: httpx.Client | None = None
) -> None:
    """Create or update the app user, its policy and the binding. Safe to run repeatedly."""
    access_key = app.access_key.get_secret_value()
    if access_key == root.objectstore_access_key.get_secret_value():
        raise ValueError("the app credential must not be the root credential")
    with client or httpx.Client(timeout=10.0) as http:
        admin = _Admin(root, http)
        admin.put(
            "add-canned-policy",
            {"name": APP_POLICY_NAME},
            app_policy(root.objectstore_bucket),
        )
        admin.put(
            "add-user",
            {"accessKey": access_key},
            {"secretKey": app.secret_key.get_secret_value(), "status": "enabled"},
        )
        admin.put(
            "set-user-or-group-policy",
            {"policyName": APP_POLICY_NAME, "userOrGroup": access_key, "isGroup": "false"},
            {},
        )
