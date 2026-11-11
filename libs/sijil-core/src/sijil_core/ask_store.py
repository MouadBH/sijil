"""Ask's questions and answers in the workspace's index database (`ask_query`, index schema v6).

A question is document content (it names suppliers, amounts), so it lives beside the documents,
never in the shared control-plane database. Its life: `queued` (the control plane,
`create_ask`), `answering` (the Ask worker claimed it, `claim_ask`), then `answered`
(`store_answer`) or `failed` (`fail_ask`). Questions are deleted 90 days after they were asked
(`expire_asks`, daily); the table has no lock trigger, since it is not document data.

An answer quotes documents: those it cites (`citations`, at most 20, shown) and those its
figures counted (`counted`, every one). It is stored only while each of them has a live search
row, held FOR SHARE until the answer commits, so a removal marked first is never quoted; one
marked or purged after is caught by the purge (`index.purge_document`), which deletes every
answer citing or counting the purged document.
"""

import json
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import Any, Final
from uuid import UUID

from sqlalchemy import bindparam, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncConnection

RETENTION_DAYS: Final = 90
_LIVE: Final = "state NOT IN ('removed', 'deleting', 'deleted')"


@dataclass(frozen=True, kw_only=True)
class Answer:
    """What the Ask worker stores: `answer_text` (the model's, checked) or `answer_code` (the
    templated fallback the client renders), never both."""

    intent: dict[str, Any] | None
    answer_text: str | None
    answer_code: str | None
    figures: list[dict[str, Any]] = field(default_factory=list)
    citations: list[UUID] = field(default_factory=list)
    counted: list[UUID] = field(default_factory=list)
    excluded: dict[str, int] = field(default_factory=dict)


@dataclass(frozen=True, kw_only=True)
class AskRecord:
    id: UUID
    question: str
    locale: str
    state: str
    intent: dict[str, Any] | None
    answer_text: str | None
    answer_code: str | None
    figures: list[dict[str, Any]]
    citations: list[UUID]
    counted: list[UUID]
    excluded: dict[str, int]
    error_code: str | None
    created_by: UUID
    created_at: datetime
    answered_at: datetime | None


@dataclass(frozen=True)
class CitedDocument:
    id: UUID
    supplier_name: str | None
    basis_date: date
    total_ttc: Decimal | None
    currency: str | None


_COLUMNS: Final = (
    "id, question, locale, state, intent, answer_text, answer_code, figures, citations, "
    "counted, excluded, error_code, created_by, created_at, answered_at"
)


def _json(value: Any) -> Any:
    return json.loads(value) if isinstance(value, str) else value


def _record(row: Any) -> AskRecord:
    return AskRecord(
        id=row.id,
        question=row.question,
        locale=row.locale,
        state=row.state,
        intent=_json(row.intent),
        answer_text=row.answer_text,
        answer_code=row.answer_code,
        figures=list(_json(row.figures)),
        citations=list(row.citations),
        counted=list(row.counted),
        excluded=dict(_json(row.excluded)),
        error_code=row.error_code,
        created_by=row.created_by,
        created_at=row.created_at,
        answered_at=row.answered_at,
    )


async def create_ask(
    conn: AsyncConnection, ask_id: UUID, *, question: str, locale: str, created_by: UUID,
    now: datetime,
) -> None:  # fmt: skip
    await conn.execute(
        text(
            "INSERT INTO ask_query (id, question, locale, created_by, created_at) "
            "VALUES (:id, :question, :locale, :by, :now)"
        ),
        {"id": ask_id, "question": question, "locale": locale, "by": created_by, "now": now},
    )


async def read_ask(conn: AsyncConnection, ask_id: UUID) -> AskRecord | None:
    result = await conn.execute(
        text(f"SELECT {_COLUMNS} FROM ask_query WHERE id = :id"),  # noqa: S608
        {"id": ask_id},
    )
    row = result.first()
    return None if row is None else _record(row)


async def claim_ask(conn: AsyncConnection, ask_id: UUID) -> AskRecord | None:
    """Moves a `queued` question to `answering` and returns it; one already `answering` (a
    retry) is returned as it is. None when it is answered, failed or gone: nothing to do."""
    result = await conn.execute(
        text(
            f"UPDATE ask_query SET state = 'answering' WHERE id = :id "  # noqa: S608
            f"AND state IN ('queued', 'answering') RETURNING {_COLUMNS}"
        ),
        {"id": ask_id},
    )
    row = result.first()
    return None if row is None else _record(row)


async def _live(conn: AsyncConnection, ids: Sequence[UUID]) -> bool:
    """Whether every document has a live search row, each held FOR SHARE until the caller's
    transaction ends."""
    wanted = set(ids)
    if not wanted:
        return True
    result = await conn.execute(
        text(
            f"SELECT ledger_id FROM search_doc WHERE ledger_id = ANY(:ids) AND {_LIVE} "  # noqa: S608
            "ORDER BY ledger_id FOR SHARE"
        ),
        {"ids": sorted(wanted)},
    )
    return set(result.scalars()) == wanted


async def store_answer(
    conn: AsyncConnection, ask_id: UUID, answer: Answer, *, now: datetime
) -> bool:
    """Stores the answer (`answered`) in one transaction (opened if none is active), only while
    every document it cites or counted is live. False, writing nothing, when one is not (removed,
    being deleted or purged since it was read: the caller answers again) or when the question is
    no longer `queued` or `answering`."""
    if conn.in_transaction():
        return await _store(conn, ask_id, answer, now)
    async with conn.begin():
        return await _store(conn, ask_id, answer, now)


async def _store(conn: AsyncConnection, ask_id: UUID, answer: Answer, now: datetime) -> bool:
    if not await _live(conn, [*answer.citations, *answer.counted]):
        return False
    result = await conn.execute(
        text(
            """
            UPDATE ask_query SET state = 'answered', intent = :intent,
                answer_text = :answer_text, answer_code = :answer_code, figures = :figures,
                citations = CAST(:citations AS uuid[]), counted = CAST(:counted AS uuid[]),
                excluded = :excluded, error_code = NULL, answered_at = :now
            WHERE id = :id AND state IN ('queued', 'answering')
            RETURNING id
            """
        ).bindparams(
            bindparam("intent", type_=JSONB),
            bindparam("figures", type_=JSONB),
            bindparam("excluded", type_=JSONB),
        ),
        {
            "id": ask_id,
            "intent": answer.intent,
            "answer_text": answer.answer_text,
            "answer_code": answer.answer_code,
            "figures": answer.figures,
            "citations": [str(c) for c in answer.citations],
            "counted": [str(c) for c in answer.counted],
            "excluded": answer.excluded,
            "now": now,
        },
    )
    return result.first() is not None


async def fail_ask(conn: AsyncConnection, ask_id: UUID, code: str, *, now: datetime) -> bool:
    """`failed` with `code`, unless it was answered (or failed) already. True when it moved."""
    result = await conn.execute(
        text(
            "UPDATE ask_query SET state = 'failed', error_code = :code, answered_at = :now "
            "WHERE id = :id AND state IN ('queued', 'answering') RETURNING id"
        ),
        {"id": ask_id, "code": code, "now": now},
    )
    return result.first() is not None


async def answer_is_live(conn: AsyncConnection, record: AskRecord) -> bool:
    """Whether every document the answer cites or counted is still live (a removed one is
    waiting for its purge, which deletes the answer)."""
    ids = {*record.citations, *record.counted}
    if not ids:
        return True
    found = await conn.scalar(
        text(f"SELECT count(*) FROM search_doc WHERE ledger_id = ANY(:ids) AND {_LIVE}"),  # noqa: S608
        {"ids": sorted(ids)},
    )
    return int(found or 0) == len(ids)


async def cited_documents(conn: AsyncConnection, ids: Sequence[UUID]) -> list[CitedDocument]:
    """The live documents among `ids`, in the order given."""
    result = await conn.execute(
        text(
            "SELECT ledger_id, supplier_name, basis_date, total_ttc, currency FROM search_doc "  # noqa: S608
            f"WHERE ledger_id = ANY(:ids) AND {_LIVE}"
        ),
        {"ids": list(ids)},
    )
    found = {r.ledger_id: CitedDocument(*r) for r in result}
    return [found[i] for i in ids if i in found]


async def expire_asks(conn: AsyncConnection, before: datetime) -> int:
    """Deletes the questions asked before `before`; how many went."""
    result = await conn.execute(
        text("DELETE FROM ask_query WHERE created_at < :before"), {"before": before}
    )
    return int(getattr(result, "rowcount", 0) or 0)
