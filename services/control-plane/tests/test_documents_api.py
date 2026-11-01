"""Documents list and detail: ledger rows merged with Paperless titles, audited views."""

import base64
import uuid
from collections.abc import Awaitable, Callable
from datetime import UTC, date, datetime, timedelta
from typing import Any

import httpx
import pytest
import respx
from fastapi import FastAPI
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core.crypto import seal
from sijil_core.db.models import (
    AuditEvent,
    CredentialKind,
    DocumentLedger,
    DocumentSource,
    DocumentState,
    IngestState,
    Membership,
    Organisation,
    OrgType,
    Role,
    TenantCredential,
    Workspace,
    WorkspaceState,
)

Actor = Any

pytestmark = pytest.mark.db

PL = "http://paperless.test"
TOKEN = "svc-token-secret"
T0 = datetime(2026, 10, 1, 12, 0, tzinfo=UTC)
PUBLIC_KEYS = {
    "id",
    "state",
    "ingest_state",
    "source",
    "content_kind",
    "size_bytes",
    "sha256",
    "ingested_at",
    "retention_until",
    "title",
    "original_file_name",
}


def _url(workspace: Workspace, suffix: str = "") -> str:
    return f"/v1/workspaces/{workspace.id}/documents{suffix}"


async def _row(
    db: AsyncSession,
    workspace: Workspace,
    n: int,
    *,
    at: datetime | None = None,
    paperless_doc_id: int | None = None,
    id: uuid.UUID | None = None,
) -> DocumentLedger:
    row = DocumentLedger(
        id=id or uuid.uuid4(),
        workspace_id=workspace.id,
        sha256=f"{n:064x}",
        md5=f"{n:032x}",
        size_bytes=100 + n,
        content_kind="pdf",
        source=DocumentSource.upload,
        object_key=f"secret-ref/{n}",
        object_version_id=f"secret-version-{n}",
        state=DocumentState.filed if paperless_doc_id else DocumentState.received,
        ingest_state=IngestState.consumed if paperless_doc_id else IngestState.queued,
        paperless_task_id="secret-task" if paperless_doc_id else None,
        paperless_doc_id=paperless_doc_id,
        retention_until=date(2036, 12, 31),
        ingested_at=at or T0 + timedelta(minutes=n),
    )
    db.add(row)
    await db.commit()
    return row


@pytest.fixture
async def engine(db: AsyncSession, workspace: Workspace, cp_app: FastAPI) -> Workspace:
    workspace.paperless_url = PL
    env = seal(
        TOKEN.encode(),
        cp_app.state.keys,
        workspace.kms_key_id,
        f"{workspace.id}:{CredentialKind.service_token}",
    )
    db.add(
        TenantCredential(
            workspace_id=workspace.id, kind=CredentialKind.service_token, envelope=env.to_bytes()
        )
    )
    await db.commit()
    return workspace


async def _member(
    make_actor: Callable[[str], Awaitable[Actor]],
    db: AsyncSession,
    workspace: Workspace,
    email: str,
    role: Role,
) -> Actor:
    actor = await make_actor(email)
    db.add(Membership(user_id=actor.user_id, workspace_id=workspace.id, role=role))
    await db.commit()
    return actor


@pytest.fixture
async def reader(
    make_actor: Callable[[str], Awaitable[Actor]], db: AsyncSession, engine: Workspace
) -> Actor:
    return await _member(make_actor, db, engine, "reader@example.com", Role.readonly)


async def _events(db: AsyncSession, workspace: Workspace) -> list[AuditEvent]:
    rows = await db.scalars(
        select(AuditEvent)
        .where(AuditEvent.workspace_id == workspace.id)
        .order_by(AuditEvent.seq)
        .execution_options(populate_existing=True)
    )
    return list(rows)


def _paperless_ok(rows: list[dict[str, Any]]) -> respx.Route:
    return respx.get(f"{PL}/api/documents/").mock(
        return_value=httpx.Response(200, json={"count": len(rows), "results": rows})
    )


@respx.mock
async def test_list_merges_ledger_and_paperless_title(
    reader: Actor, engine: Workspace, db: AsyncSession
) -> None:
    await _row(db, engine, 1, paperless_doc_id=11)
    await _row(db, engine, 2)  # not yet consumed: no title
    route = _paperless_ok(
        [{"id": 11, "title": "Facture EDF", "created": "2026-09-30", "original_file_name": "e.pdf"}]
    )
    r = await reader.client.get(_url(engine))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["engine"] == "ok"
    assert body["next_cursor"] is None
    by_size = {i["size_bytes"]: i for i in body["items"]}
    assert by_size[101]["title"] == "Facture EDF"
    assert by_size[101]["original_file_name"] == "e.pdf"
    assert by_size[102]["title"] is None
    assert by_size[102]["original_file_name"] is None
    assert [i["size_bytes"] for i in body["items"]] == [102, 101]  # newest first
    request = route.calls.last.request
    assert request.headers["Authorization"] == f"Token {TOKEN}"
    assert request.url.params["id__in"] == "11"


@respx.mock
async def test_list_survives_paperless_down(
    reader: Actor, engine: Workspace, db: AsyncSession
) -> None:
    await _row(db, engine, 1, paperless_doc_id=11)
    respx.get(f"{PL}/api/documents/").mock(side_effect=httpx.ConnectError("down"))
    r = await reader.client.get(_url(engine))
    assert r.status_code == 200
    body = r.json()
    assert body["engine"] == "unavailable"
    [item] = body["items"]
    assert item["title"] is None
    assert item["state"] == "filed"
    assert TOKEN not in r.text


@respx.mock
async def test_list_survives_paperless_garbage_and_missing_credential(
    reader: Actor, engine: Workspace, db: AsyncSession
) -> None:
    await _row(db, engine, 1, paperless_doc_id=11)
    respx.get(f"{PL}/api/documents/").mock(return_value=httpx.Response(200, json=["nope"]))
    assert (await reader.client.get(_url(engine))).json()["engine"] == "unavailable"
    respx.get(f"{PL}/api/documents/").mock(return_value=httpx.Response(500))
    assert (await reader.client.get(_url(engine))).json()["engine"] == "unavailable"
    cred = await db.scalar(
        select(TenantCredential).where(TenantCredential.workspace_id == engine.id)
    )
    assert cred is not None
    cred.envelope = b"garbage"
    await db.commit()
    r = await reader.client.get(_url(engine))
    assert r.status_code == 200
    assert r.json()["engine"] == "unavailable"


@respx.mock
async def test_list_paginates_with_cursor(
    reader: Actor, engine: Workspace, db: AsyncSession
) -> None:
    # Five rows, three sharing the same ingested_at: ties must neither repeat nor skip.
    ids = []
    for n in range(5):
        row = await _row(db, engine, n, at=T0 if n >= 2 else T0 + timedelta(minutes=n + 10))
        ids.append(str(row.id))
    seen: list[str] = []
    cursor: str | None = None
    for _ in range(5):
        params: dict[str, Any] = {"limit": 2}
        if cursor:
            params["cursor"] = cursor
        r = await reader.client.get(_url(engine), params=params)
        assert r.status_code == 200, r.text
        body = r.json()
        assert len(body["items"]) <= 2
        seen += [i["id"] for i in body["items"]]
        cursor = body["next_cursor"]
        if cursor is None:
            break
    assert sorted(seen) == sorted(ids)
    assert len(seen) == len(set(seen)) == 5
    # Newest first; ties broken by id descending.
    tied = [i for i in seen[2:]]
    assert tied == sorted(tied, reverse=True)
    assert cursor is None


async def test_list_rejects_bad_cursor_and_limit(reader: Actor, engine: Workspace) -> None:
    for cursor in ("!!!", "bm90LWEtY3Vyc29y", base64.urlsafe_b64encode(b"x|y").decode()):
        r = await reader.client.get(_url(engine), params={"cursor": cursor})
        assert r.status_code == 422, cursor
        assert r.json()["error"]["code"] == "validation"
    for limit in (0, 101):
        r = await reader.client.get(_url(engine), params={"limit": limit})
        assert r.status_code == 422


@respx.mock
async def test_readonly_can_list_member_of_other_ws_cannot(
    make_actor: Callable[[str], Awaitable[Actor]],
    reader: Actor,
    engine: Workspace,
    db: AsyncSession,
) -> None:
    _paperless_ok([])
    assert (await reader.client.get(_url(engine))).status_code == 200
    # A member of a different workspace sees this one as not found.
    org = await db.get(Organisation, engine.org_id)
    assert org is not None
    other_org = Organisation(name="Other", type=OrgType.business, country="MA", region="ma-cas")
    db.add(other_org)
    await db.commit()
    slug = f"ws-{uuid.uuid4().hex[:8]}"
    other = Workspace(
        org_id=other_org.id,
        name="Other",
        slug=slug,
        state=WorkspaceState.active,
        db_name=f"paperless_{slug}",
        index_db_name=f"index_{slug}",
        kms_key_id=f"kek-{slug}",
    )
    db.add(other)
    await db.commit()
    outsider = await _member(make_actor, db, other, "outsider@example.com", Role.owner)
    r = await outsider.client.get(_url(engine))
    assert r.status_code == 404
    assert (await outsider.client.get(_url(other))).status_code in (200, 202)


@respx.mock
async def test_document_view_is_audited(reader: Actor, engine: Workspace, db: AsyncSession) -> None:
    row = await _row(db, engine, 1, paperless_doc_id=11)
    await _row(db, engine, 2)
    _paperless_ok([{"id": 11, "title": "T", "created": None, "original_file_name": "a.pdf"}])
    assert (await reader.client.get(_url(engine))).status_code == 200
    r = await reader.client.get(_url(engine, f"/{row.id}"))
    assert r.status_code == 200, r.text
    assert r.json()["title"] == "T"
    listed, viewed = await _events(db, engine)
    assert listed.action == "documents.listed"
    assert listed.data == {"count": 2}
    assert listed.actor_id == str(reader.user_id)
    assert viewed.action == "document.viewed"
    assert viewed.target_type == "document"
    assert viewed.target_id == str(row.id)
    assert viewed.actor_id == str(reader.user_id)


@respx.mock
async def test_detail_other_workspace_404(
    reader: Actor, engine: Workspace, db: AsyncSession
) -> None:
    _paperless_ok([])
    slug = f"ws-{uuid.uuid4().hex[:8]}"
    other = Workspace(
        org_id=engine.org_id,
        name="Sibling",
        slug=slug,
        state=WorkspaceState.active,
        db_name=f"paperless_{slug}",
        index_db_name=f"index_{slug}",
        kms_key_id=f"kek-{slug}",
    )
    db.add(other)
    await db.commit()
    foreign = await _row(db, other, 7)
    r = await reader.client.get(_url(engine, f"/{foreign.id}"))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "document.not_found"
    r = await reader.client.get(_url(engine, f"/{uuid.uuid4()}"))
    assert r.status_code == 404
    assert r.json()["error"]["code"] == "document.not_found"
    assert await _events(db, engine) == []  # a miss is not a view


@respx.mock
async def test_document_out_never_exposes_internal_fields(
    reader: Actor, engine: Workspace, db: AsyncSession
) -> None:
    row = await _row(db, engine, 1, paperless_doc_id=11)
    _paperless_ok([{"id": 11, "title": "T", "created": None, "original_file_name": "a.pdf"}])
    listing = await reader.client.get(_url(engine))
    detail = await reader.client.get(_url(engine, f"/{row.id}"))
    assert set(listing.json()) == {"items", "next_cursor", "engine"}
    assert set(listing.json()["items"][0]) == PUBLIC_KEYS
    assert set(detail.json()) == PUBLIC_KEYS
    for text in (listing.text, detail.text):
        for secret in ("secret-ref", "secret-version", "secret-task", TOKEN, PL, "object_key"):
            assert secret not in text
