"""
Database initialization and session management for Resolver.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
License. You may obtain a copy of the License at
http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import logging
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Protocol

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker

from db_models import Base

logger = logging.getLogger(__name__)


class _SessionFactory(Protocol):
    def __call__(self) -> Session: ...


def _new_session(factory: _SessionFactory) -> Session:
    return factory()


_ENGINE: Engine | None = None
_SESSION_FACTORY: _SessionFactory | None = None


def _ensure_postgres_database_exists(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("postgresql"):
        return

    target_db = (url.database or "").strip()
    if not target_db:
        return

    if not re.fullmatch(r"[A-Za-z0-9_]+", target_db):
        raise RuntimeError(f"Invalid database name in RESOLVER_DATABASE_URL: {target_db!r}")

    admin_url = url.set(database="postgres")
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT", pool_pre_ping=True)
    try:
        with admin_engine.connect() as conn:
            exists = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :name"),
                {"name": target_db},
            ).scalar()
            if exists:
                return
            conn.exec_driver_sql(f'CREATE DATABASE "{target_db}"')
    finally:
        admin_engine.dispose()


def init_database(database_url: str) -> None:
    if _ENGINE is not None and _SESSION_FACTORY is not None:
        return
    _ensure_postgres_database_exists(database_url)
    engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=int(os.getenv("RESOLVER_DB_POOL_SIZE", "10")),
        max_overflow=int(os.getenv("RESOLVER_DB_MAX_OVERFLOW", "20")),
        pool_timeout=int(os.getenv("RESOLVER_DB_POOL_TIMEOUT", "30")),
        pool_recycle=int(os.getenv("RESOLVER_DB_POOL_RECYCLE", "1800")),
    )
    factory = sessionmaker(bind=engine, autoflush=False, expire_on_commit=False)
    globals()["_ENGINE"] = engine
    globals()["_SESSION_FACTORY"] = factory


def _require_session_factory() -> _SessionFactory:
    factory = _SESSION_FACTORY
    if factory is None or not callable(factory):
        raise RuntimeError("Database not initialized")
    return factory


@contextmanager
def get_db_session() -> Iterator[Session]:
    if _ENGINE is None:
        raise RuntimeError("Database not initialized")
    session = _new_session(_require_session_factory())
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    if _ENGINE is None:
        raise RuntimeError("Database not initialized")
    logger.info("Initializing Resolver database tables...")
    Base.metadata.create_all(bind=_ENGINE)
    logger.info("Resolver database tables created successfully")


def connection_test() -> bool:
    if _ENGINE is None:
        return False
    try:
        with _ENGINE.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        return False


def dispose_database() -> None:
    globals()["_SESSION_FACTORY"] = None
    if _ENGINE is not None:
        _ENGINE.dispose()
        globals()["_ENGINE"] = None
