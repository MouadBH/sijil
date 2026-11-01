from sqlalchemy.ext.asyncio import AsyncEngine

from sijil_core.db.engine import make_engine as _make_engine
from sijil_core.db.engine import make_sessionmaker
from sijil_cp.settings import CpSettings

__all__ = ["make_engine", "make_sessionmaker"]


def make_engine(settings: CpSettings) -> AsyncEngine:
    return _make_engine(str(settings.database_url))
