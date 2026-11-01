"""Org-level audit events are written to every workspace of the org."""

from collections.abc import Mapping
from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import Request
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from sijil_core import audit
from sijil_core.db.models import ActorType, User, Workspace


async def audit_org_event(
    db: AsyncSession,
    request: Request,
    *,
    org_id: UUID,
    actor: User,
    action: str,
    target_type: str,
    target_id: str,
    data: Mapping[str, Any] | None = None,
    now: datetime,
) -> None:
    """Append in the caller's transaction (the caller commits with the change itself)."""
    ids = await db.scalars(
        select(Workspace.id).where(Workspace.org_id == org_id).order_by(Workspace.id)
    )
    ip = request.client.host if request.client else None
    for workspace_id in list(ids):
        await audit.append(
            db,
            workspace_id=workspace_id,
            action=action,
            actor_type=ActorType.user,
            actor_id=str(actor.id),
            target_type=target_type,
            target_id=target_id,
            ip=ip,
            data=data,
            now=now,
        )
