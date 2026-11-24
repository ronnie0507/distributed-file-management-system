import os
from collections.abc import Generator
from functools import lru_cache

from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker


@lru_cache(maxsize=1)
def _session_factory() -> sessionmaker[Session]:
    database_url = os.getenv(
        "DATABASE_URL",
        "postgresql+psycopg://fileadmin:change-me@localhost:5432/file_metadata",
    )
    engine = create_engine(database_url, pool_pre_ping=True)
    return sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)


def get_db() -> Generator[Session, None, None]:
    db = _session_factory()()
    try:
        yield db
    finally:
        db.close()


def get_session_factory() -> sessionmaker[Session]:
    return _session_factory()
