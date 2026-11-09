"""Search over a workspace's `search_doc` rows (index schema v3+), shared by the search API and
Ask, and the saved views (`saved_view`, v4) that store a search.

`build_search` turns `SearchFilters` into one SELECT. Whatever the filters, only live documents
are returned (`received`, `needs_review`, `filed`, `sealed`): a row in `removed`, `deleting` or
`deleted` never is, and `status` cannot ask for one.

`q` is split on whitespace:
- a 15-digit token (Arabic-Indic digits folded) must equal `supplier_ice`;
- the other tokens are "the rest": the document matches when `tsv` matches
  `websearch_to_tsquery('simple', search_normalise(rest))`, when `supplier_norm` is
  trigram-similar to it (`%`, pg_trgm's default threshold 0.3), or when a token shaped like an
  invoice number (`^[A-Za-z0-9/_-]{3,}$`) equals `number` exactly.
Both sides are folded by `search_normalise`, so hamza, tatweel, harakat, teh marbuta, accents,
case and Arabic-Indic digits do not matter. Relevance is `ts_rank_cd` plus that similarity.

Paging is by keyset: the cursor holds the sort, the last row's sort key and its ledger id (the
tiebreak), so a document arriving between two pages is neither repeated nor makes one skipped.
Documents with no amount come last in both amount sorts. `relevance` with no rest of `q` to rank
by is `date_desc`.

`snippet` is `ts_headline` over the page's `content`: plain text, at most 200 characters, the
hits between `«` and `»` (those two characters in the text itself become `"`). It is computed
for the page's rows only. It parses with `sijil_headline` (index schema v4: Latin accents and
case dropped), so `telecom` marks `Télécom`; an Arabic hit found only through folding (a hamza
or teh marbuta the query wrote differently) is returned but may not be marked.
"""

import base64
import binascii
import json
import re
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import Annotated, Any, Final, Literal, get_args
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import (
    Date,
    Float,
    Numeric,
    Select,
    Text,
    and_,
    bindparam,
    cast,
    column,
    func,
    literal,
    literal_column,
    null,
    or_,
    select,
    table,
    text,
    tuple_,
)
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, TSVECTOR
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncConnection
from sqlalchemy.sql.elements import ColumnElement

from sijil_core.text import search_normalise

QUERY_INVALID: Final = "search.query_invalid"
MAX_QUERY: Final = 200
MAX_LIMIT: Final = 100
SNIPPET_MAX: Final = 200
# Tags and document types a filter may list (a document has at most 20 tags).
MAX_LIST: Final = 20

Sort = Literal["relevance", "date_desc", "date_asc", "amount_desc", "amount_asc"]
LiveState = Literal["needs_review", "filed", "sealed", "received"]
SORTS: Final[tuple[str, ...]] = get_args(Sort)
LIVE_STATES: Final[tuple[str, ...]] = get_args(LiveState)

_ICE = re.compile(r"[0-9]{15}")
_NUMBER = re.compile(r"[A-Za-z0-9/_-]{3,}")
_SPACE = re.compile(r"\s+")
_HEADLINE: Final = (
    'StartSel=«, StopSel=», MaxWords=20, MinWords=8, MaxFragments=2, FragmentDelimiter=" … "'
)


class QueryInvalid(ValueError):  # noqa: N818 - named for the machine code it maps to
    """A cursor this module did not make, or one of another sort (`search.query_invalid`)."""


class SearchFilters(BaseModel):
    """What a search, a saved view and Ask filter on. Every filter given must match."""

    model_config = ConfigDict(extra="forbid")

    q: Annotated[str, Field(max_length=MAX_QUERY)] | None = None
    document_type: Annotated[list[str], Field(max_length=MAX_LIST)] = []
    # An ICE when 15 digits, else part of the supplier's name.
    supplier: Annotated[str, Field(max_length=MAX_QUERY)] | None = None
    # Inclusive, on the retention basis date (`basis_date`), as exports and sums.
    period_from: date | None = None
    period_to: date | None = None
    # Inclusive, on `total_ttc`.
    amount_min: Decimal | None = None
    amount_max: Decimal | None = None
    # Every one of them.
    tags: Annotated[list[str], Field(max_length=MAX_LIST)] = []
    status: list[LiveState] = []


def filter_names(filters: SearchFilters) -> str:
    """The names of the filters used, comma-separated, in declaration order: what the audit log
    may say about a search (never the values)."""
    used = []
    for name in SearchFilters.model_fields:
        value = getattr(filters, name)
        if isinstance(value, str):
            value = value.strip()
        if value not in (None, "", []):
            used.append(name)
    return ",".join(used)


@dataclass(frozen=True)
class SearchHit:
    id: UUID
    state: str
    document_type: str | None
    basis_date: date
    supplier_name: str | None
    number: str | None
    total_ttc: Decimal | None
    currency: str | None
    tags: list[str]
    corrected: list[str]
    snippet: str | None


@dataclass(frozen=True)
class SearchPage:
    items: list[SearchHit]
    next_cursor: str | None


_doc = table(
    "search_doc",
    column("ledger_id", PG_UUID(as_uuid=True)),
    column("state", Text),
    column("basis_date", Date),
    column("document_type", Text),
    column("supplier_name", Text),
    column("supplier_ice", Text),
    column("number", Text),
    column("total_ttc", Numeric(14, 2)),
    column("currency", Text),
    column("tags", ARRAY(Text)),
    column("corrected", ARRAY(Text)),
    column("content", Text),
    column("supplier_norm", Text),
    column("tsv", TSVECTOR),
)
_d = _doc.c
_SIMPLE: ColumnElement[Any] = literal_column("'simple'")
_HEADLINE_CONFIG: ColumnElement[Any] = literal_column("'sijil_headline'")


@dataclass(frozen=True)
class _Text:
    """`q` split as the module docstring says."""

    ices: list[str]
    numbers: list[str]
    rest: str  # as typed
    norm: str  # search_normalise(rest)


def _split(q: str | None) -> _Text:
    ices: list[str] = []
    numbers: list[str] = []
    rest: list[str] = []
    for token in (q or "").split():
        folded = search_normalise(token)
        if _ICE.fullmatch(folded):
            ices.append(folded)
            continue
        rest.append(token)
        if _NUMBER.fullmatch(token):
            numbers.append(token)
    joined = " ".join(rest)
    return _Text(ices, numbers, joined, search_normalise(joined).strip())


def _effective(sort: str, words: _Text) -> str:
    if sort not in SORTS:
        raise QueryInvalid(sort)
    return "date_desc" if sort == "relevance" and not words.norm else sort


# Cursors ------------------------------------------------------------------------------------


def _encode(sort: str, key: Any, ledger_id: UUID) -> str:
    raw = json.dumps([sort, None if key is None else str(key), str(ledger_id)])
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def _decode(cursor: str, sort: str) -> tuple[Any, UUID]:
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        kind, key, ledger_id = json.loads(raw)
        if kind != sort or (key is None and not sort.startswith("amount")):
            raise ValueError("another sort")
        parsed: Any = None
        if key is not None:
            if sort.startswith("date"):
                parsed = date.fromisoformat(key)
            elif sort.startswith("amount"):
                parsed = Decimal(key)
                if not parsed.is_finite():
                    raise ValueError("amount")
            else:
                parsed = float(key)
        return parsed, UUID(ledger_id)
    except (binascii.Error, ValueError, TypeError, InvalidOperation, UnicodeDecodeError):
        raise QueryInvalid(QUERY_INVALID) from None


def _after(sort: str, key: ColumnElement[Any], cursor: str) -> ColumnElement[bool]:
    """Rows after the cursor's in the sort's order (NULL amounts last either way)."""
    last, last_id = _decode(cursor, sort)
    desc = sort.endswith("desc") or sort == "relevance"
    ledger = _d.ledger_id
    if last is None:  # among the rows with no amount, by ledger id
        return and_(key.is_(None), ledger < last_id if desc else ledger > last_id)
    pair = tuple_(key, ledger)
    beyond = pair < (last, last_id) if desc else pair > (last, last_id)
    return or_(beyond, key.is_(None)) if sort.startswith("amount") else beyond


# The query ----------------------------------------------------------------------------------


def build_search(filters: SearchFilters, sort: str, cursor: str | None, limit: int) -> Select[Any]:
    """One page (`limit` rows) of the documents matching `filters`, in `sort` order, after
    `cursor` (`next_cursor` of the previous page). Columns: those of `SearchHit` (`ledger_id`
    for `id`) and `sort_key`. Raises QueryInvalid for a bad cursor or sort."""
    words = _split(filters.q)
    sort = _effective(sort, words)
    allowed: list[str] = list(filters.status) or list(LIVE_STATES)
    where: list[ColumnElement[bool]] = [_d.state.in_(allowed)]
    where += [_d.supplier_ice == ice for ice in words.ices]
    rank: ColumnElement[Any] = literal(0.0, Float)
    tsquery: ColumnElement[Any] | None = None
    if words.norm:
        norm = literal(words.norm, Text)
        tsquery = func.websearch_to_tsquery(_SIMPLE, norm)
        matches = [_d.tsv.op("@@")(tsquery), _d.supplier_norm.op("%")(norm)]
        if words.numbers:
            matches.append(_d.number.in_(words.numbers))
        where.append(or_(*matches))
        rank = cast(
            func.ts_rank_cd(_d.tsv, tsquery) + func.similarity(_d.supplier_norm, norm), Float
        )
    if filters.document_type:
        where.append(_d.document_type.in_(filters.document_type))
    if filters.supplier and filters.supplier.strip():
        supplier = search_normalise(filters.supplier).strip()
        if _ICE.fullmatch(supplier):
            where.append(_d.supplier_ice == supplier)
        else:
            where.append(_d.supplier_norm.contains(supplier, autoescape=True))
    if filters.period_from is not None:
        where.append(_d.basis_date >= filters.period_from)
    if filters.period_to is not None:
        where.append(_d.basis_date <= filters.period_to)
    if filters.amount_min is not None:
        where.append(_d.total_ttc >= filters.amount_min)
    if filters.amount_max is not None:
        where.append(_d.total_ttc <= filters.amount_max)
    if filters.tags:
        where.append(_d.tags.contains(cast(literal(list(filters.tags), ARRAY(Text)), ARRAY(Text))))

    key: ColumnElement[Any] = {
        "relevance": rank,
        "date_desc": _d.basis_date,
        "date_asc": _d.basis_date,
        "amount_desc": _d.total_ttc,
        "amount_asc": _d.total_ttc,
    }[sort]
    if cursor is not None:
        where.append(_after(sort, key, cursor))

    page = (
        select(
            _d.ledger_id,
            _d.state,
            _d.document_type,
            _d.basis_date,
            _d.supplier_name,
            _d.number,
            _d.total_ttc,
            _d.currency,
            _d.tags,
            _d.corrected,
            _d.content,
            key.label("sort_key"),
        )  # fmt: skip
        .where(*where)
        .order_by(*_order(sort, key, _d.ledger_id))
        .limit(limit)
        .subquery("page")
    )
    p = page.c
    snippet: ColumnElement[Any] = null()
    if tsquery is not None:
        shown = func.websearch_to_tsquery(_HEADLINE_CONFIG, literal(words.rest, Text)).op("||")(
            tsquery
        )
        snippet = func.ts_headline(
            _HEADLINE_CONFIG,
            func.translate(p.content, "«»", '""'),
            shown,
            literal(_HEADLINE, Text),
        )
    return select(
        p.ledger_id, p.state, p.document_type, p.basis_date, p.supplier_name, p.number,
        p.total_ttc, p.currency, p.tags, p.corrected, snippet.label("snippet"), p.sort_key,
    ).order_by(*_order(sort, p.sort_key, p.ledger_id))  # fmt: skip


def _order(sort: str, key: ColumnElement[Any], ledger: ColumnElement[Any]) -> list[Any]:
    """Only an amount can be NULL; leaving NULLS alone otherwise lets `date_desc` walk the
    `(basis_date, ledger_id)` index backwards."""
    if sort.endswith("asc"):
        return [key.asc().nulls_last(), ledger.asc()]
    if sort.startswith("amount"):
        return [key.desc().nulls_last(), ledger.desc()]
    return [key.desc(), ledger.desc()]


def _snippet(raw: str | None) -> str | None:
    if not raw:
        return None
    flat = _SPACE.sub(" ", raw).strip()
    if len(flat) <= SNIPPET_MAX:
        return flat or None
    cut = flat[: SNIPPET_MAX - 2]
    if cut.count("«") > cut.count("»"):
        cut += "»"
    return cut + "…"


async def run_search(
    conn: AsyncConnection, filters: SearchFilters, sort: str, cursor: str | None, limit: int
) -> SearchPage:
    """`build_search`, run: one page and the cursor of the next (None on the last page)."""
    effective = _effective(sort, _split(filters.q))
    rows = (await conn.execute(build_search(filters, sort, cursor, limit + 1))).all()
    more = len(rows) > limit
    rows = rows[:limit]
    items = [
        SearchHit(
            id=r.ledger_id,
            state=r.state,
            document_type=r.document_type,
            basis_date=r.basis_date,
            supplier_name=r.supplier_name,
            number=r.number,
            total_ttc=r.total_ttc,
            currency=r.currency,
            tags=list(r.tags),
            corrected=list(r.corrected),
            snippet=_snippet(r.snippet),
        )
        for r in rows
    ]
    last = rows[-1] if more else None
    return SearchPage(
        items, None if last is None else _encode(effective, last.sort_key, last.ledger_id)
    )


# Saved views ----------------------------------------------------------------------------------


@dataclass(frozen=True)
class SavedView:
    id: UUID
    name: str
    filters: SearchFilters
    sort: str
    created_by: UUID
    created_at: datetime
    updated_at: datetime


_VIEW_COLUMNS: Final = "id, name, filters, sort, created_by, created_at, updated_at"


def _view(row: Any) -> SavedView:
    raw = json.loads(row.filters) if isinstance(row.filters, str) else row.filters
    return SavedView(
        id=row.id,
        name=row.name,
        filters=SearchFilters.model_validate(raw),
        sort=row.sort,
        created_by=row.created_by,
        created_at=row.created_at,
        updated_at=row.updated_at,
    )


async def list_views(conn: AsyncConnection) -> list[SavedView]:
    result = await conn.execute(
        text(f"SELECT {_VIEW_COLUMNS} FROM saved_view ORDER BY name, id")  # noqa: S608
    )
    return [_view(r) for r in result]


async def get_view(conn: AsyncConnection, view_id: UUID) -> SavedView | None:
    result = await conn.execute(
        text(f"SELECT {_VIEW_COLUMNS} FROM saved_view WHERE id = :id"),  # noqa: S608
        {"id": view_id},
    )
    row = result.first()
    return None if row is None else _view(row)


async def save_view(conn: AsyncConnection, view: SavedView) -> None:
    """Inserts the view, or replaces its name, filters and sort (`updated_at` too)."""
    await conn.execute(
        text(
            f"""
            INSERT INTO saved_view ({_VIEW_COLUMNS})
            VALUES (:id, :name, :filters, :sort, :created_by, :created_at, :updated_at)
            ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name, filters = EXCLUDED.filters,
                sort = EXCLUDED.sort, updated_at = EXCLUDED.updated_at
            """  # noqa: S608
        ).bindparams(bindparam("filters", type_=JSONB)),
        {
            "id": view.id,
            "name": view.name,
            "filters": view.filters.model_dump(mode="json"),
            "sort": view.sort,
            "created_by": view.created_by,
            "created_at": view.created_at,
            "updated_at": view.updated_at,
        },
    )


async def delete_view(conn: AsyncConnection, view_id: UUID) -> None:
    await conn.execute(text("DELETE FROM saved_view WHERE id = :id"), {"id": view_id})
