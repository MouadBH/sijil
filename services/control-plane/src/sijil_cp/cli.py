"""`sijil-cp` console entry: `serve` runs the API, `bootstrap-storage` prepares the object store."""

import argparse
import asyncio
import sys
import time
from collections.abc import Sequence
from typing import TYPE_CHECKING
from uuid import UUID

import httpx
from botocore.exceptions import BotoCoreError, ClientError

from sijil_core.storage import S3ObjectStore, StorageSettings
from sijil_cp.storage_admin import AppCredentialSettings, ensure_app_user

if TYPE_CHECKING:
    from sijil_cp.settings import CpSettings

_BOOTSTRAP_ATTEMPTS = 30


def serve() -> None:
    from sijil_cp.main import run

    run()


def bootstrap_storage() -> None:
    """Create the object-locked bucket and the services' least-privilege user if missing.

    Runs with the root key (only `migrate` holds it). Safe to run repeatedly. The store may
    still be starting when the one-shot `migrate` service reaches this point, so connection
    errors are retried for about a minute.
    """
    settings = StorageSettings()
    app = AppCredentialSettings()
    store = S3ObjectStore(settings)
    for attempt in range(1, _BOOTSTRAP_ATTEMPTS + 1):
        try:
            asyncio.run(store.ensure_bucket())
            ensure_app_user(settings, app)
        except (BotoCoreError, ClientError, httpx.TransportError) as exc:
            if attempt == _BOOTSTRAP_ATTEMPTS:
                raise
            print(  # noqa: T201
                f"object store not ready ({type(exc).__name__}), retrying", file=sys.stderr
            )
            time.sleep(2)
        else:
            print(f"bucket {settings.objectstore_bucket} and app user ready")  # noqa: T201
            return


def audit_verify(workspace_id: UUID, settings: "CpSettings | None" = None) -> int:
    """Recompute a workspace's audit chain.

    Exit code 0 when intact, 1 when broken, 2 when it could not be verified at all (an unknown
    workspace, or the database unreachable): an empty chain for a mistyped id is not "ok".
    """
    from sijil_core import audit
    from sijil_core.db.models import Workspace
    from sijil_cp.db import make_engine, make_sessionmaker
    from sijil_cp.settings import CpSettings

    async def _run() -> audit.VerifyResult | None:
        engine = make_engine(settings or CpSettings())
        try:
            async with make_sessionmaker(engine)() as db:
                if await db.get(Workspace, workspace_id) is None:
                    return None
                return await audit.verify(db, workspace_id)
        finally:
            await engine.dispose()

    try:
        result = asyncio.run(_run())
    except Exception as exc:
        # The type only: messages can carry the connection string.
        print(f"database error: {type(exc).__name__}", file=sys.stderr)  # noqa: T201
        return 2
    if result is None:
        print("unknown workspace", file=sys.stderr)  # noqa: T201
        return 2
    print(f"ok {result.count}" if result.ok else f"broken at seq {result.first_bad_seq}")  # noqa: T201
    return 0 if result.ok else 1


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="sijil-cp")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="run the API server")
    sub.add_parser("bootstrap-storage", help="create the object-locked original bucket")
    audit_parser = sub.add_parser("audit", help="audit log tools")
    audit_sub = audit_parser.add_subparsers(dest="audit_command", required=True)
    verify = audit_sub.add_parser("verify", help="verify a workspace's audit hash chain")
    verify.add_argument("--workspace", type=UUID, required=True)
    args = parser.parse_args(argv)
    if args.command == "serve":
        serve()
    elif args.command == "audit":
        sys.exit(audit_verify(args.workspace))
    else:
        bootstrap_storage()


if __name__ == "__main__":
    main()
