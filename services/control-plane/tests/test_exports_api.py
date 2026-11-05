"""The period export API (plan 4.6, ARC-5): request with a step-up, build (in-process, as the
export worker does), download through the control plane, verify; and the published key."""

import asyncio
import base64
import hashlib
import io
import uuid
import zipfile
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator
from datetime import UTC, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession, create_async_engine

from sijil_core import index
from sijil_core.db.models import AuditEvent, DocumentState, Membership, Role, User, Workspace
from sijil_core.export_report import (
    INDEX_NAME,
    SIGNATURE_NAME,
    key_id,
    sign,
    verify_zip,
)
from sijil_cp.cli import export_verify
from tests.support.export_keys import TEST_PUBLIC_KEY, TEST_PUBLIC_KEY_B64, TEST_SEED
from tests.support.exports import add_exportable, invoice_values, run_export
from tests.support.index_db import throwaway_index_db
from tests.support.memory_store import MemoryExportStore, MemoryObjectStore

Actor = Any
MakeActor = Callable[[str], Awaitable[Actor]]

pytestmark = pytest.mark.db

REASON = "Avoir du fournisseur reçu après le scellement (SAMPLE)."


@pytest.fixture(scope="module")
def index_db_url() -> Iterator[str]:
    yield from throwaway_index_db()


@pytest.fixture
async def index_conn(index_db_url: str) -> AsyncIterator[AsyncConnection]:
    engine = create_async_engine(index_db_url, isolation_level="AUTOCOMMIT")
    async with engine.connect() as conn:
        await index.ensure_schema(conn)
        yield conn
    await engine.dispose()


async def _member(make_actor: MakeActor, db: AsyncSession, ws: Workspace, role: Role) -> Actor:
    actor = await make_actor(f"{role.value}-{uuid.uuid4().hex[:6]}@example.com")
    db.add(Membership(user_id=actor.user_id, workspace_id=ws.id, role=role))
    await db.commit()
    return actor


@pytest.fixture
async def owner(make_actor: MakeActor, db: AsyncSession, workspace: Workspace) -> Actor:
    actor = await _member(make_actor, db, workspace, Role.owner)
    await actor.enable_totp()
    await actor.step_up()
    return actor


def _period(clock: Any) -> str:
    return f"{clock.now:%Y-%m}"


def _url(ws: Workspace, suffix: str = "") -> str:
    return f"/v1/workspaces/{ws.id}/exports{suffix}"


def _code(r: httpx.Response) -> str:
    return str(r.json()["error"]["code"])


async def _events(db: AsyncSession, ws: Workspace, action: str) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == ws.id, AuditEvent.action == action)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def _exported(
    owner: Actor, ws: Workspace, app: FastAPI, store: MemoryObjectStore,
    exports: MemoryExportStore, index_db_url: str, clock: Any, tmp_path: Path,
) -> tuple[Path, str]:  # fmt: skip
    """Request, build (as the export worker does) and download this month's export; the ZIP's
    path and the job id."""
    r = await owner.client.post(_url(ws), json={"period": _period(clock)})
    assert r.status_code == 202, r.text
    job_id = r.json()["id"]
    sessions = app.state.sessionmaker
    assert await run_export(
        uuid.UUID(job_id), sessions=sessions, index_db_url=index_db_url, store=store,
        exports=exports, now=clock.now,
    )  # fmt: skip
    r = await owner.client.get(_url(ws, f"/{job_id}/download"))
    assert r.status_code == 200, r.text
    path = tmp_path / f"{job_id}.zip"
    path.write_bytes(r.content)
    return path, job_id


async def _documents(
    db: AsyncSession, store: MemoryObjectStore, conn: AsyncConnection, ws: Workspace,
    owner: Actor, clock: Any,
) -> None:  # fmt: skip
    """Three documents of this month: sealed with a correction, filed, and undated."""
    today = f"{clock.now:%Y-%m-%d}"
    who = await db.get(User, owner.user_id)
    assert who is not None
    args = (db, store, conn, ws, who)
    arrived = clock.now - timedelta(minutes=30)
    sealed = await add_exportable(
        *args, state=DocumentState.sealed, arrived=arrived, values=invoice_values(date=today)
    )
    await index.add_correction(
        conn, sealed.id, "total_ttc", "1200.00", REASON, created_by=who.id, now=clock.now
    )
    await add_exportable(*args, arrived=arrived, values=invoice_values(date=today))
    await add_exportable(*args, state=DocumentState.received, arrived=arrived)


async def test_integrity_report_verifies(
    owner: Actor, workspace: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    export_store: MemoryExportStore, index_conn: AsyncConnection, index_db_url: str,
    clock: Any, tmp_path: Path, migrated_db: Any, cp_app: FastAPI,
) -> None:  # fmt: skip
    await _documents(db, memory_store, index_conn, workspace, owner, clock)
    path, job_id = await _exported(
        owner, workspace, cp_app, memory_store, export_store, index_db_url, clock, tmp_path
    )
    status = (await owner.client.get(_url(workspace, f"/{job_id}"))).json()
    assert status["state"] == "ready"
    assert status["document_count"] == 3
    assert status["sha256"] == hashlib.sha256(path.read_bytes()).hexdigest()
    outcome = verify_zip(path, TEST_PUBLIC_KEY)
    assert outcome.ok, outcome.problems
    assert export_verify(path, public_key_b64=TEST_PUBLIC_KEY_B64) == 0
    assert (
        await asyncio.to_thread(  # the CLI runs its own event loop
            export_verify,
            path,
            public_key_b64=TEST_PUBLIC_KEY_B64,
            against_ledger=True,
            database_url=migrated_db.app_url,
        )
        == 0
    )


def _rewrite(src: Path, dst: Path, change: Callable[[str, bytes], bytes]) -> Path:
    with zipfile.ZipFile(src) as zin, zipfile.ZipFile(dst, "w") as zout:
        for name in zin.namelist():
            zout.writestr(name, change(name, zin.read(name)))
    return dst


async def test_tampered_zip_fails_verify(
    owner: Actor, workspace: Workspace, db: AsyncSession, memory_store: MemoryObjectStore,
    export_store: MemoryExportStore, index_conn: AsyncConnection, index_db_url: str,
    clock: Any, tmp_path: Path, capsys: pytest.CaptureFixture[str], cp_app: FastAPI,
) -> None:  # fmt: skip
    await _documents(db, memory_store, index_conn, workspace, owner, clock)
    path, _job = await _exported(
        owner, workspace, cp_app, memory_store, export_store, index_db_url, clock, tmp_path
    )
    with zipfile.ZipFile(path) as zf:
        original = next(n for n in zf.namelist() if n.startswith("originals/"))

    def one_byte(name: str, data: bytes) -> bytes:
        return data[:-1] + bytes([data[-1] ^ 1]) if name == original else data

    def one_cell(name: str, data: bytes) -> bytes:
        return data.replace(b"1500.00", b"1500.01", 1) if name == INDEX_NAME else data

    def other_signature(name: str, data: bytes) -> bytes:
        forged = base64.b64encode(sign(b"another report", TEST_SEED))
        return forged if name == SIGNATURE_NAME else data

    cases = [
        (one_byte, f"file.hash:{original}"),
        (one_cell, "index_csv.hash"),
        (other_signature, "signature.invalid"),
    ]
    for change, problem in cases:
        tampered = _rewrite(path, tmp_path / f"{change.__name__}.zip", change)
        assert verify_zip(tampered, TEST_PUBLIC_KEY).problems == [problem]
        capsys.readouterr()
        assert export_verify(tampered, public_key_b64=TEST_PUBLIC_KEY_B64) == 1
        assert capsys.readouterr().out.splitlines() == [problem]


async def test_export_requires_step_up_and_is_audited(
    make_actor: MakeActor, owner: Actor, workspace: Workspace, db: AsyncSession,
    memory_store: MemoryObjectStore, export_store: MemoryExportStore, index_db_url: str,
    clock: Any, enqueuer: Any, cp_app: FastAPI,
) -> None:  # fmt: skip
    period = _period(clock)
    member = await _member(make_actor, db, workspace, Role.member)
    r = await member.client.post(_url(workspace), json={"period": period})
    assert (r.status_code, _code(r)) == (403, "forbidden")
    admin = await _member(make_actor, db, workspace, Role.admin)  # no TOTP: never stepped up
    r = await admin.client.post(_url(workspace), json={"period": period})
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    clock.advance(timedelta(minutes=11))  # the owner's step-up is now too old
    r = await owner.client.post(_url(workspace), json={"period": period})
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    assert enqueuer.exports == []
    assert await _events(db, workspace, "export.requested") == []

    await owner.step_up()
    r = await owner.client.post(_url(workspace), json={"period": period})
    assert r.status_code == 202, r.text
    assert r.json() == {"id": r.json()["id"], "state": "queued"}
    job_id = uuid.UUID(r.json()["id"])
    assert enqueuer.exports == [job_id]
    [requested] = await _events(db, workspace, "export.requested")
    assert (requested.actor_id, requested.target_type, requested.target_id, requested.data) == (
        str(owner.user_id), "export", str(job_id), {"period": period},
    )  # fmt: skip
    assert await run_export(
        job_id, sessions=cp_app.state.sessionmaker, index_db_url=index_db_url, store=memory_store,
        exports=export_store, now=clock.now,
    )  # fmt: skip

    clock.advance(timedelta(minutes=11))
    r = await owner.client.get(_url(workspace, f"/{job_id}/download"))
    assert (r.status_code, _code(r)) == (403, "auth.step_up_required")
    r = await member.client.get(_url(workspace, f"/{job_id}/download"))
    assert (r.status_code, _code(r)) == (403, "forbidden")
    assert await _events(db, workspace, "export.downloaded") == []
    await owner.step_up()
    r = await owner.client.get(_url(workspace, f"/{job_id}/download"))
    assert r.status_code == 200
    assert r.headers["content-type"] == "application/zip"
    assert f"{job_id}.zip" in r.headers["content-disposition"]
    assert zipfile.ZipFile(io.BytesIO(r.content)).namelist()
    [downloaded] = await _events(db, workspace, "export.downloaded")
    assert (downloaded.actor_id, downloaded.target_id, downloaded.data) == (
        str(owner.user_id), str(job_id), {"period": period},
    )  # fmt: skip


async def test_download_not_ready_is_409(owner: Actor, workspace: Workspace, clock: Any) -> None:
    r = await owner.client.post(_url(workspace), json={"period": _period(clock)})
    job_id = r.json()["id"]
    assert (await owner.client.get(_url(workspace, f"/{job_id}"))).json()["state"] == "queued"
    r = await owner.client.get(_url(workspace, f"/{job_id}/download"))
    assert (r.status_code, _code(r)) == (409, "export.not_ready")
    r = await owner.client.get(_url(workspace, f"/{uuid.uuid4()}/download"))
    assert (r.status_code, _code(r)) == (404, "export.not_found")


async def test_a_bad_period_is_refused(
    owner: Actor, workspace: Workspace, clock: Any, enqueuer: Any
) -> None:
    created = workspace.created_at.astimezone(UTC)
    before = created.replace(day=1) - timedelta(days=1)
    for period in (
        None, "", "2026-13", "26", "2026/10", f"{clock.now.year + 1}",
        f"{(clock.now + timedelta(days=40)):%Y-%m}", f"{before:%Y-%m}", f"{created.year - 1}",
    ):  # fmt: skip
        r = await owner.client.post(_url(workspace), json={"period": period})
        assert (r.status_code, _code(r)) == (422, "export.period_invalid"), period
    assert enqueuer.exports == []
    # The year the workspace was created in, and its month, are fine.
    for period in (f"{created.year}", f"{created:%Y-%m}"):
        r = await owner.client.post(_url(workspace), json={"period": period})
        assert r.status_code == 202, (period, r.text)


async def test_export_key_endpoint_matches_signing_key(
    cp_app: FastAPI, csrf_client_for: Callable[..., httpx.AsyncClient]
) -> None:
    async with csrf_client_for(cp_app) as anonymous:
        r = await anonymous.get("/v1/regions/ma-cas/export-key")
        assert r.status_code == 200
        assert r.json() == {
            "region": "ma-cas",
            "key_id": key_id(TEST_PUBLIC_KEY),
            "public_key": TEST_PUBLIC_KEY_B64,
        }
        # The published key verifies what the signing seed signs.
        published = base64.b64decode(r.json()["public_key"])
        Ed25519PublicKey.from_public_bytes(published).verify(sign(b"report", TEST_SEED), b"report")
        r = await anonymous.get("/v1/regions/eu-fra/export-key")
        assert (r.status_code, r.json()["error"]["code"]) == (421, "region.mismatch")
