"""The workspace's supplier records (index schema v7, `supplier`; REC-5).

When a reviewer confirms or corrects a supplier (a field edit, a seal, a correction note), the
worker learns it here (`sijil_worker.learning`): one record per ICE, its confirmed `name`, and
as `aliases` the names it was known by before and the spellings a reviewer confirmed (what the
model read on a confirmed document). Extraction reads the records for its hints and to name a
known ICE's supplier; Ask's resolver matches a supplier's name or alias.

A record lives in the workspace's index database, like every value derived from documents. It
is the knowledge of the documents carrying its ICE: `index.purge_document` deletes it with the
last live one (index schema v8).
"""

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, Literal
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from sijil_core.text import search_normalise

MAX_ALIASES: Final = 10
SupplierSource = Literal["review", "seal", "correction"]

_ICE: Final = re.compile(r"^[0-9]{15}$")
_COLUMNS: Final = "ice, name, aliases, source, paperless_correspondent_id, updated_at, updated_by"


@dataclass(frozen=True, kw_only=True)
class Supplier:
    """One `supplier` row, column for column."""

    ice: str
    name: str
    aliases: tuple[str, ...]
    source: str
    paperless_correspondent_id: int | None
    updated_at: datetime
    updated_by: UUID | None


@dataclass(frozen=True)
class SupplierChange:
    """What `learn` did: the record as it now stands (None when nothing was learnt), whether it
    was created, and whether its name changed (`changed` is also true for a new record)."""

    supplier: Supplier | None
    new: bool = False
    changed: bool = False


def _supplier(row: Mapping[Any, Any]) -> Supplier:
    data = dict(row)
    data["aliases"] = tuple(data["aliases"] or ())
    return Supplier(**data)


def _fold(name: str) -> str:
    return search_normalise(name).strip()


def _aliases(
    previous: str | None, aliases: Sequence[str], name: str, spellings: Sequence[str]
) -> list[str]:
    """The aliases once `name` replaces `previous` (None: no rename) and the confirmed
    `spellings` join: none equal to `name`, none twice (after `search_normalise`), the newest
    last, the MAX_ALIASES newest kept."""
    kept = list(aliases)
    for added in ([previous] if previous is not None else []) + list(spellings):
        folded = _fold(added)
        if folded:
            kept = [a for a in kept if _fold(a) != folded]
            kept.append(added)
    folded_name = _fold(name)
    return [a for a in kept if _fold(a) != folded_name][-MAX_ALIASES:]


async def get(conn: AsyncConnection, ice: str) -> Supplier | None:
    found = (
        (
            await conn.execute(
                text(f"SELECT {_COLUMNS} FROM supplier WHERE ice = :ice"),  # noqa: S608
                {"ice": ice},
            )
        )
        .mappings()
        .first()
    )
    return None if found is None else _supplier(found)


async def find(conn: AsyncConnection, ices: Sequence[str]) -> list[Supplier]:
    """The records of these ICEs (unknown ones are absent), by ICE."""
    if not ices:
        return []
    result = await conn.execute(
        text(f"SELECT {_COLUMNS} FROM supplier WHERE ice = ANY(:ices) ORDER BY ice"),  # noqa: S608
        {"ices": list(ices)},
    )
    return [_supplier(m) for m in result.mappings()]


async def learn(
    conn: AsyncConnection,
    *,
    ice: str | None,
    name: str | None,
    source: SupplierSource,
    actor: UUID | None,
    now: datetime,
    aliases: Sequence[str] = (),
) -> SupplierChange:
    """Upserts the record of `ice` with the confirmed `name`. A new name replaces the record's,
    whose previous name joins the aliases; so do `aliases`, spellings of this supplier a
    reviewer confirmed (`_aliases`). Without a 15-digit ICE or a name, nothing is learnt.
    `changed` is about the name only. Race-safe: the row is created once and then locked FOR
    UPDATE (a transaction is opened if none is active)."""
    if ice is None or not _ICE.match(ice) or not name or not name.strip():
        return SupplierChange(None)
    if conn.in_transaction():
        return await _learn(conn, ice, name.strip(), source, actor, now, aliases)
    async with conn.begin():
        return await _learn(conn, ice, name.strip(), source, actor, now, aliases)


async def _learn(
    conn: AsyncConnection,
    ice: str,
    name: str,
    source: SupplierSource,
    actor: UUID | None,
    now: datetime,
    spellings: Sequence[str],
) -> SupplierChange:
    params = {"ice": ice, "name": name, "source": source, "now": now, "by": actor}
    insert = f"""
        INSERT INTO supplier (ice, name, aliases, source, updated_at, updated_by)
        VALUES (:ice, :name, CAST(:aliases AS text[]), :source, :now, :by)
        ON CONFLICT (ice) DO NOTHING RETURNING {_COLUMNS}
    """  # noqa: S608
    first = {**params, "aliases": _aliases(None, (), name, spellings)}
    created = (await conn.execute(text(insert), first)).mappings().first()
    if created is not None:
        return SupplierChange(_supplier(created), new=True, changed=True)
    locked = text(f"SELECT {_COLUMNS} FROM supplier WHERE ice = :ice FOR UPDATE")  # noqa: S608
    existing = _supplier((await conn.execute(locked, {"ice": ice})).mappings().one())
    renamed = existing.name != name
    aliases = _aliases(existing.name if renamed else None, existing.aliases, name, spellings)
    if not renamed and tuple(aliases) == existing.aliases:
        return SupplierChange(existing)
    if renamed:
        update = f"""
            UPDATE supplier SET name = :name, aliases = CAST(:aliases AS text[]),
                source = :source, updated_at = :now, updated_by = :by
            WHERE ice = :ice RETURNING {_COLUMNS}
        """  # noqa: S608
    else:  # only confirmed spellings: the name, its source and its author stay
        update = f"""
            UPDATE supplier SET aliases = CAST(:aliases AS text[])
            WHERE ice = :ice RETURNING {_COLUMNS}
        """  # noqa: S608
    found = await conn.execute(text(update), {**params, "aliases": aliases})
    return SupplierChange(_supplier(found.mappings().one()), changed=renamed)


async def set_correspondent_id(conn: AsyncConnection, ice: str, correspondent_id: int) -> None:
    """The Paperless correspondent the record was given (`sijil_worker.learning`)."""
    await conn.execute(
        text("UPDATE supplier SET paperless_correspondent_id = :cid WHERE ice = :ice"),
        {"cid": correspondent_id, "ice": ice},
    )
