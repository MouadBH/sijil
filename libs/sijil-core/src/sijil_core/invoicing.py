"""Annual MAD invoices (ADM-4, design §5.7a): amounts, numbering, the total in words, issue.

- **Amounts** (`amounts`): `vat = round(ht × 20%)`, `ttc = ht + vat`, each rounded
  ROUND_HALF_UP to 0.01. An upgrade charges `(new − old) × days_left / days_in_period`
  (`prorata`, region-local calendar days, today included), rounded, then VAT. All exact:
  `Decimal` and `Fraction`, never a float.
- **Number** (`next_number`): `SIJ-{YYYY}-{NNNNNN}`, gapless per calendar year of issue in the
  organisation's region. The year's `InvoiceCounter` row is locked FOR UPDATE by the issuing
  transaction: a concurrent issue waits for it, and a rollback gives its number back.
- **Issue** (`issue`): the buyer (org name and ICE) and the seller (`SellerSettings`) are copied
  into the row, the PDF is rendered once in the owner's locale (`ar`, else `fr`) and its
  SHA-256 stored, all in the caller's transaction (the caller commits). A trigger freezes the
  row but its payment (0016). Audited `billing.invoice_issued` `{number, kind, plan,
  amount_ttc}` on every workspace of the org.
- **Labels** (`messages`): the `invoice.*` keys of `packages/i18n/messages/{locale}.json`,
  the product's only catalogue of user-facing strings.
- **In words** (`amount_words`): `num2words` gives the integers only (it knows no MAD, and its
  Arabic puts a space after the conjunction "و", which is removed); the dirham and centime
  nouns come from the catalogue by CLDR plural category (ar: zero, one, two, few, many, other;
  fr: one, many, other), and the two parts are joined by `invoice.words.join`.
"""

import hashlib
import json
from collections.abc import Iterator, Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal
from fractions import Fraction
from functools import cache
from pathlib import Path
from typing import Any, Final
from uuid import UUID

from num2words import num2words
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core.db.models import (
    ActorType,
    Invoice,
    InvoiceCounter,
    InvoiceKind,
    InvoiceState,
    Locale,
    Membership,
    Organisation,
    Plan,
    Role,
    User,
    Workspace,
)
from sijil_core.retention import region_local_date
from sijil_core.settings import SellerSettings

VAT_RATE: Final = Decimal("20.00")  # percent
CURRENCY: Final = "MAD"
PAYMENT_DAYS: Final = 30  # an invoice is due 30 days after issue
NUMBER_PREFIX: Final = "SIJ"
_CENT: Final = 100


@dataclass(frozen=True)
class Amounts:
    ht: Decimal
    vat: Decimal
    ttc: Decimal


def _round_half_up(value: Fraction) -> Decimal:
    """`value` to 0.01, half a cent up (amounts are never negative here)."""
    if value < 0:
        raise ValueError("negative amount")
    cents = (value * _CENT * 2 + 1) // 2  # floor(x + 1/2), exact
    return (Decimal(int(cents)) / _CENT).quantize(Decimal("0.01"))


def amounts(price_ht: Decimal, fraction: Fraction | None) -> Amounts:
    """HT (the price, or its `fraction` for an upgrade), VAT at 20% of the rounded HT, and
    TTC = HT + VAT exactly."""
    ht = _round_half_up(Fraction(price_ht) * (fraction if fraction is not None else 1))
    vat = _round_half_up(Fraction(ht) * Fraction(VAT_RATE) / 100)
    return Amounts(ht=ht, vat=vat, ttc=ht + vat)


def prorata(period_start: datetime, period_end: datetime, now: datetime, region: str) -> Fraction:
    """The share of `[period_start, period_end)` left on `now`, in the region's calendar days,
    today included: 1 on the first day, 1/days on the last."""
    start = region_local_date(period_start, region)
    end = region_local_date(period_end, region)
    today = region_local_date(now, region)
    days = (end - start).days
    if days <= 0 or not start <= today < end:
        raise ValueError("now is outside the period")
    return Fraction((end - today).days, days)


# Labels ----------------------------------------------------------------------------------------


def _catalogue_dir() -> Path:
    """`packages/i18n/messages`, from the repository (or the image's /src) holding this file."""
    for parent in Path(__file__).resolve().parents:
        candidate = parent / "packages" / "i18n" / "messages"
        if candidate.is_dir():
            return candidate
    raise FileNotFoundError("packages/i18n/messages not found")


class Messages(Mapping[str, Any]):
    """The `invoice.*` labels of one locale."""

    def __init__(self, locale: Locale, keys: Mapping[str, Any]) -> None:
        self.locale = locale
        self._keys = keys

    def __getitem__(self, key: str) -> Any:
        return self._keys[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self._keys)

    def __len__(self) -> int:
        return len(self._keys)


def invoice_locale(locale: Locale | str) -> Locale:
    """An invoice is in Arabic for an Arabic-speaking owner, in French otherwise."""
    return Locale.ar if Locale(locale) is Locale.ar else Locale.fr


@cache
def messages(locale: Locale | str) -> Messages:
    """The `invoice.*` keys of the locale's catalogue."""
    code = Locale(locale)
    data = json.loads((_catalogue_dir() / f"{code.value}.json").read_text(encoding="utf-8"))
    return Messages(code, data["invoice"])


# In words --------------------------------------------------------------------------------------


def _plural(n: int, locale: Locale) -> str:
    """The CLDR plural category of the whole number `n`."""
    if locale is Locale.ar:
        if n in (0, 1, 2):
            return ("zero", "one", "two")[n]
        if 3 <= n % 100 <= 10:
            return "few"
        if 11 <= n % 100 <= 99:
            return "many"
        return "other"
    if locale is Locale.fr:
        if n in (0, 1):
            return "one"
        return "many" if n % 1_000_000 == 0 else "other"
    return "one" if n == 1 else "other"


def _cardinal(n: int, locale: Locale) -> str:
    words = str(num2words(n, lang=locale.value))
    if locale is Locale.ar:
        words = words.replace(" و ", " و")
    return words


def _counted(n: int, unit: Mapping[str, str], locale: Locale) -> str:
    return unit[_plural(n, locale)].replace("{n}", _cardinal(n, locale))


def amount_words(amount: Decimal, labels: Messages) -> str:
    """The amount in MAD in words: dirhams, and centimes when there are any."""
    cents = int(amount.quantize(Decimal("0.01")) * _CENT)
    if cents < 0:
        raise ValueError("negative amount")
    whole, rest = divmod(cents, _CENT)
    words = labels["words"]
    dirhams = _counted(whole, words["dirham"], labels.locale)
    if rest == 0:
        return dirhams
    centimes = _counted(rest, words["centime"], labels.locale)
    return str(words["join"]).replace("{dirhams}", dirhams).replace("{centimes}", centimes)


# Numbering and issue ---------------------------------------------------------------------------


async def next_number(db: AsyncSession, year: int) -> str:
    """The year's next number, its counter row locked FOR UPDATE until the caller's
    transaction ends (a rollback gives the number back)."""
    await db.execute(insert(InvoiceCounter).values(year=year, last=0).on_conflict_do_nothing())
    counter = await db.scalar(
        select(InvoiceCounter)
        .where(InvoiceCounter.year == year)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if counter is None:  # unreachable: inserted above
        raise LookupError("invoice counter missing")
    counter.last += 1
    await db.flush()
    return f"{NUMBER_PREFIX}-{year:04d}-{counter.last:06d}"


async def owner_locale(db: AsyncSession, org_id: UUID) -> Locale:
    """The locale of the org's first Owner (French when it has none)."""
    found = await db.scalar(
        select(User.locale)
        .join(Membership, Membership.user_id == User.id)
        .where(Membership.org_id == org_id, Membership.role == Role.owner)
        .order_by(Membership.created_at, Membership.id)
        .limit(1)
    )
    return invoice_locale(found or Locale.fr)


async def audit_org(
    db: AsyncSession,
    org_id: UUID,
    action: str,
    data: dict[str, str],
    now: datetime,
    *,
    target_type: str = "org",
    target_id: str | None = None,
) -> None:
    """A system event on every workspace of the org, in the caller's transaction."""
    ids = await db.scalars(
        select(Workspace.id).where(Workspace.org_id == org_id).order_by(Workspace.id)
    )
    for workspace_id in list(ids):
        await audit.append(
            db,
            workspace_id=workspace_id,
            action=action,
            actor_type=ActorType.system,
            actor_id=None,
            target_type=target_type,
            target_id=target_id or str(org_id),
            data=data,
            now=now,
        )


async def issue(
    db: AsyncSession,
    org: Organisation,
    kind: InvoiceKind,
    plan: Plan,
    period: tuple[datetime, datetime],
    now: datetime,
    *,
    seller: SellerSettings,
    price_ht: Decimal | None = None,
    fraction: Fraction | None = None,
) -> Invoice:
    """Issues an invoice for `plan` over `period` (see the module docstring): the plan's price,
    or `price_ht` (an upgrade's difference) times `fraction`. In the caller's transaction."""
    from sijil_core import invoice_pdf  # it imports this module (Messages, amount_words)

    if org.ice is None:
        raise ValueError("an invoice needs the buyer's ICE")
    year = region_local_date(now, org.region).year
    number = await next_number(db, year)
    total = amounts(plan.price_ht if price_ht is None else price_ht, fraction)
    locale = await owner_locale(db, org.id)
    invoice = Invoice(
        org_id=org.id,
        number=number,
        year=year,
        kind=kind,
        plan_code=plan.code,
        period_start=period[0],
        period_end=period[1],
        amount_ht=total.ht,
        vat_rate=VAT_RATE,
        amount_vat=total.vat,
        amount_ttc=total.ttc,
        currency=CURRENCY,
        issued_at=now,
        due_at=now + timedelta(days=PAYMENT_DAYS),
        state=InvoiceState.issued,
        buyer={"name": org.name, "ice": org.ice},
        seller=seller.seller(),
        locale=locale,
    )
    invoice.pdf = invoice_pdf.render(invoice, messages(locale), region=org.region)
    invoice.pdf_sha256 = hashlib.sha256(invoice.pdf).hexdigest()
    db.add(invoice)
    await db.flush()
    await audit_org(
        db,
        org.id,
        "billing.invoice_issued",
        {
            "number": number,
            "kind": kind.value,
            "plan": plan.code,
            "amount_ttc": f"{total.ttc:.2f}",
        },
        now,
        target_type="invoice",
        target_id=str(invoice.id),
    )
    return invoice
