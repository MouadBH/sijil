"""Ask (plan 5.3, ARC-2): questions about the workspace's archive, answered asynchronously by
the Ask worker (`sijil_worker.ask`) on the in-region Ollama.

`POST …/ask` (`documents_read`) takes `{question (3 to 500 characters), locale}` (else 422
`ask.question_invalid`), at most 20 per user per minute (Redis; else 429 `ask.rate_limited`).
The question is document content: it is written to the workspace's index database
(`ask_store.create_ask`), never to the control-plane database, and audited `ask.asked`
`{locale}`, never its text. After that commit `sijil.answer_question` goes to the Ask worker
(queue `ask`), and the answer is 202 `{id, state: "queued"}`.

`GET …/ask/{id}` returns the question's state and, once answered, the answer: the model's
`answer_text`, or the `answer_code` of the templated fallback (an `ask.answer.*` key the client
renders with the figures), the figures (money as strings), how many matching documents were
left out because they await review, and the cited documents. Only the asker, an Owner or an
Admin reads it; anyone else gets 404 `ask.not_found`, as for an unknown id. An answer quoting a
document since removed or being deleted is 404 too: its purge deletes the answer, and nothing
of the document is shown meanwhile. The index database unreachable is 503 `ask.unavailable`.
"""

import logging
from datetime import date
from typing import Annotated, Final, Literal
from uuid import UUID, uuid4

from fastapi import APIRouter, Depends, Request
from pydantic import BaseModel, ConfigDict, StringConstraints
from sqlalchemy.exc import SQLAlchemyError

from sijil_core import ask_store, audit
from sijil_core.db.models import ActorType, Role
from sijil_cp.api.review import CurrentUser, Db, Now, Reading, _engines, _ip
from sijil_cp.api.search import _parse
from sijil_cp.auth import ratelimit
from sijil_cp.deps import require_totp_compliance
from sijil_cp.enqueue import get_enqueuer
from sijil_cp.errors import ApiError

log = logging.getLogger(__name__)

router = APIRouter(tags=["ask"], dependencies=[Depends(require_totp_compliance)])

NOT_FOUND: Final = "ask.not_found"
QUESTION_INVALID: Final = "ask.question_invalid"
RATE_LIMITED: Final = "ask.rate_limited"
UNAVAILABLE: Final = "ask.unavailable"
# Besides the asker, these may read an answer.
READERS: Final = frozenset({Role.owner, Role.admin})


class AskIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    question: Annotated[str, StringConstraints(strip_whitespace=True, min_length=3, max_length=500)]
    locale: Literal["fr", "ar", "en"]


class AskQueued(BaseModel):
    id: UUID
    state: str


class FigureOut(BaseModel):
    measure: str
    value: str  # money is a string
    currency: str | None
    document_count: int


class ExcludedOut(BaseModel):
    needs_review: int


class CitationOut(BaseModel):
    id: UUID
    supplier_name: str | None
    basis_date: date
    total_ttc: str | None  # money is a string
    currency: str | None


class AskOut(BaseModel):
    id: UUID
    state: str
    locale: str
    answer_text: str | None
    answer_code: str | None
    figures: list[FigureOut]
    excluded: ExcludedOut
    citations: list[CitationOut]
    error_code: str | None


@router.post("/workspaces/{workspace_id}/ask", response_model=AskQueued, status_code=202)
async def ask(request: Request, ctx: Reading, user: CurrentUser, db: Db, now: Now) -> AskQueued:
    workspace = ctx.workspace
    body = _parse(AskIn, await request.body(), QUESTION_INVALID)
    key = ratelimit.user_key("ask", user.id)
    if not await ratelimit.hit(
        request.app.state.redis, key, ratelimit.ASK_LIMIT, ratelimit.ASK_WINDOW_S
    ):
        raise ApiError(RATE_LIMITED, 429)
    ask_id = uuid4()
    try:
        async with (await _engines(request).engine(workspace)).begin() as conn:
            await ask_store.create_ask(
                conn, ask_id, question=body.question, locale=body.locale, created_by=user.id,
                now=now,
            )  # fmt: skip
    except (SQLAlchemyError, OSError) as exc:
        log.warning("question not queued in workspace %s: %s", workspace.id, type(exc).__name__)
        raise ApiError(UNAVAILABLE, 503) from None
    await audit.append(
        db,
        workspace_id=workspace.id,
        action="ask.asked",
        actor_type=ActorType.user,
        actor_id=str(user.id),
        target_type="ask",
        target_id=str(ask_id),
        ip=_ip(request),
        data={"locale": body.locale},
        now=now,
    )
    await db.commit()
    get_enqueuer(request).send_ask(ask_id, workspace.id)
    return AskQueued(id=ask_id, state="queued")


@router.get("/workspaces/{workspace_id}/ask/{ask_id}", response_model=AskOut)
async def answer(ask_id: UUID, request: Request, ctx: Reading, user: CurrentUser) -> AskOut:
    try:
        async with (await _engines(request).engine(ctx.workspace)).connect() as conn:
            record = await ask_store.read_ask(conn, ask_id)
            if record is None or (record.created_by != user.id and ctx.role not in READERS):
                raise ApiError(NOT_FOUND, 404)
            if not await ask_store.answer_is_live(conn, record):
                raise ApiError(NOT_FOUND, 404)
            cited = await ask_store.cited_documents(conn, record.citations)
    except (SQLAlchemyError, OSError) as exc:
        log.warning("answer %s not read: %s", ask_id, type(exc).__name__)
        raise ApiError(UNAVAILABLE, 503) from None
    return AskOut(
        id=record.id,
        state=record.state,
        locale=record.locale,
        answer_text=record.answer_text,
        answer_code=record.answer_code,
        figures=[FigureOut.model_validate(f) for f in record.figures],
        excluded=ExcludedOut(needs_review=int(record.excluded.get("needs_review", 0))),
        citations=[
            CitationOut(
                id=doc.id,
                supplier_name=doc.supplier_name,
                basis_date=doc.basis_date,
                total_ttc=None if doc.total_ttc is None else str(doc.total_ttc),
                currency=doc.currency,
            )
            for doc in cited
        ],
        error_code=record.error_code,
    )
