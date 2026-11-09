"""`sijil-cp` console entry: `serve` runs the API, `bootstrap-storage` prepares the object store,
`audit verify` recomputes a workspace's audit chain, `export verify` checks an export ZIP and
`search backfill` has the worker rebuild a workspace's search index."""

import argparse
import asyncio
import os
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

import httpx
from botocore.exceptions import BotoCoreError, ClientError

from sijil_core.storage import S3ExportStore, S3ObjectStore, StorageSettings
from sijil_cp.storage_admin import (
    AppCredentialSettings,
    ExportCredentialSettings,
    RemovalCredentialSettings,
    SealCredentialSettings,
    ensure_app_user,
    ensure_export_user,
    ensure_removal_user,
    ensure_seal_user,
)

if TYPE_CHECKING:
    from sijil_cp.enqueue import Enqueuer
    from sijil_cp.settings import CpSettings

_BOOTSTRAP_ATTEMPTS = 30


def serve() -> None:
    from sijil_cp.main import run

    run()


def bootstrap_storage() -> None:
    """Create the object-locked bucket, the plain exports bucket and, if missing, the four users
    the running services hold: the app user (most services), the seal user (`worker-seal`), the
    removal user (`worker-removal`) and the export user (`worker-export`), each with its own
    policy (sijil_cp.storage_admin).

    Runs with the root key (only `migrate` holds it). Safe to run repeatedly. The store may
    still be starting when the one-shot `migrate` service reaches this point, so connection
    errors are retried for about a minute.
    """
    settings = StorageSettings()
    app = AppCredentialSettings()
    sealer = SealCredentialSettings()
    remover = RemovalCredentialSettings()
    exporter = ExportCredentialSettings()
    store = S3ObjectStore(settings)
    exports = S3ExportStore(settings)
    for attempt in range(1, _BOOTSTRAP_ATTEMPTS + 1):
        try:
            asyncio.run(store.ensure_bucket())
            asyncio.run(exports.ensure_bucket())
            ensure_app_user(settings, app)
            ensure_seal_user(settings, sealer, app)
            ensure_removal_user(settings, remover, [app, sealer])
            ensure_export_user(settings, exporter, [app, sealer, remover])
        except (BotoCoreError, ClientError, httpx.TransportError) as exc:
            if attempt == _BOOTSTRAP_ATTEMPTS:
                raise
            print(  # noqa: T201
                f"object store not ready ({type(exc).__name__}), retrying", file=sys.stderr
            )
            time.sleep(2)
        else:
            print(  # noqa: T201
                f"buckets {settings.objectstore_bucket} and its exports,"
                " app, seal, removal and export users ready"
            )
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


def search_backfill(
    workspace_id: UUID,
    settings: "CpSettings | None" = None,
    enqueuer: "Enqueuer | None" = None,
) -> int:
    """Send `sijil.search_backfill` for a workspace of this region, which rebuilds its every
    live search row (sijil_worker.search). Exit code 0 when sent, 2 for an unknown workspace,
    one of another region, or a database or broker that cannot be reached."""
    from sqlalchemy import select

    from sijil_core.db.models import Organisation, Workspace
    from sijil_core.region import RegionMismatch, assert_region
    from sijil_cp.db import make_engine, make_sessionmaker
    from sijil_cp.enqueue import CeleryEnqueuer
    from sijil_cp.settings import CpSettings

    cp = settings or CpSettings()

    async def _region() -> str | None:
        engine = make_engine(cp)
        try:
            async with make_sessionmaker(engine)() as db:
                found: str | None = await db.scalar(
                    select(Organisation.region)
                    .join(Workspace, Workspace.org_id == Organisation.id)
                    .where(Workspace.id == workspace_id)
                )
                return found
        finally:
            await engine.dispose()

    try:
        region = asyncio.run(_region())
    except Exception as exc:
        # The type only: messages can carry the connection string.
        print(f"database error: {type(exc).__name__}", file=sys.stderr)  # noqa: T201
        return 2
    if region is None:
        print("unknown workspace", file=sys.stderr)  # noqa: T201
        return 2
    try:
        assert_region(region, cp)
    except RegionMismatch:
        print(f"workspace is in region {region}, not this one", file=sys.stderr)  # noqa: T201
        return 2
    try:
        (enqueuer or CeleryEnqueuer(str(cp.redis_url))).send_search_backfill(workspace_id)
    except Exception as exc:
        print(f"broker error: {type(exc).__name__}", file=sys.stderr)  # noqa: T201
        return 2
    print(f"queued {workspace_id}")  # noqa: T201
    return 0


async def _ledger_problems(report: dict[str, Any], database_url: str) -> list[str]:
    """`ledger.mismatch:<id>` for each reported document whose ledger row is missing, belongs
    to another workspace, or holds another `sha256` or `fields_sha256`."""
    from sqlalchemy import select

    from sijil_core.db.engine import make_engine, make_sessionmaker
    from sijil_core.db.models import DocumentLedger

    docs = [d for d in report["documents"] if isinstance(d, dict)]
    ids: list[UUID] = []
    for doc in docs:
        try:
            ids.append(UUID(str(doc.get("id"))))
        except ValueError:
            continue
    engine = make_engine(database_url)
    try:
        async with make_sessionmaker(engine)() as db:
            rows = {
                row.id: row
                for row in await db.scalars(
                    select(DocumentLedger).where(DocumentLedger.id.in_(ids))
                )
            }
    finally:
        await engine.dispose()
    problems = []
    for doc in docs:
        try:
            row = rows.get(UUID(str(doc.get("id"))))
        except ValueError:
            row = None
        if (
            row is None
            or str(row.workspace_id) != report.get("workspace_id")
            or row.sha256 != doc.get("sha256")
            or row.fields_sha256 != doc.get("fields_sha256")
        ):
            problems.append(f"ledger.mismatch:{doc.get('id')}")
    return problems


def export_verify(
    path: Path,
    *,
    public_key_b64: str | None = None,
    against_ledger: bool = False,
    database_url: str | None = None,
) -> int:
    """Check an export ZIP: its signature against the region's public key (`public_key_b64`,
    else SIJIL_EXPORT_PUBLIC_KEY), the key id, and every file's hash; with `against_ledger`,
    each document's `sha256` and `fields_sha256` against the ledger too (`database_url`, else
    SIJIL_DATABASE_URL).

    Prints "ok", or one problem code per line. Exit code 0 when intact, 1 when not, 2 when it
    could not be verified at all (no usable public key, no database to compare with)."""
    from sijil_core.export_report import decode_key, read_report, verify_zip

    raw = public_key_b64 or os.environ.get("SIJIL_EXPORT_PUBLIC_KEY", "")
    try:
        public = decode_key(raw)
    except ValueError:
        print("no usable public key (--public-key or SIJIL_EXPORT_PUBLIC_KEY)", file=sys.stderr)  # noqa: T201
        return 2
    problems = list(verify_zip(path, public).problems)
    if against_ledger:
        url = database_url or os.environ.get("SIJIL_DATABASE_URL")
        report = read_report(path)
        if not url:
            print("no database (SIJIL_DATABASE_URL) to compare with", file=sys.stderr)  # noqa: T201
            return 2
        if report is not None:
            try:
                problems += asyncio.run(_ledger_problems(report, url))
            except Exception as exc:
                # The type only: messages can carry the connection string.
                print(f"database error: {type(exc).__name__}", file=sys.stderr)  # noqa: T201
                return 2
    for problem in problems or ["ok"]:
        print(problem)  # noqa: T201
    return 1 if problems else 0


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="sijil-cp")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("serve", help="run the API server")
    sub.add_parser("bootstrap-storage", help="create the object-locked original bucket")
    audit_parser = sub.add_parser("audit", help="audit log tools")
    audit_sub = audit_parser.add_subparsers(dest="audit_command", required=True)
    verify = audit_sub.add_parser("verify", help="verify a workspace's audit hash chain")
    verify.add_argument("--workspace", type=UUID, required=True)
    export_parser = sub.add_parser("export", help="period export tools")
    export_sub = export_parser.add_subparsers(dest="export_command", required=True)
    check = export_sub.add_parser("verify", help="verify an export ZIP and its signed report")
    check.add_argument("zip", type=Path)
    check.add_argument("--public-key", help="base64 of the region's raw Ed25519 public key")
    check.add_argument(
        "--against-ledger", action="store_true", help="compare each document with the ledger"
    )
    search_parser = sub.add_parser("search", help="search index tools")
    search_sub = search_parser.add_subparsers(dest="search_command", required=True)
    backfill = search_sub.add_parser("backfill", help="rebuild a workspace's search index")
    backfill.add_argument("workspace", type=UUID)
    args = parser.parse_args(argv)
    if args.command == "serve":
        serve()
    elif args.command == "search":
        sys.exit(search_backfill(args.workspace))
    elif args.command == "audit":
        sys.exit(audit_verify(args.workspace))
    elif args.command == "export":
        sys.exit(
            export_verify(
                args.zip, public_key_b64=args.public_key, against_ledger=args.against_ledger
            )
        )
    else:
        bootstrap_storage()


if __name__ == "__main__":
    main()
