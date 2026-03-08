"""
Database initialization and session management for Be Certain.

Copyright (c) 2026 Stefan Kumarasinghe

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
"""

from __future__ import annotations

import os
import re
import logging
from contextlib import contextmanager
from typing import Iterator, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.orm import Session, sessionmaker

from db_models import Base

logger = logging.getLogger(__name__)
_engine: Optional[Engine] = None
_session_factory: Optional[sessionmaker] = None


def _ensure_postgres_database_exists(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("postgresql"):
        return

    target_db = (url.database or "").strip()
    if not target_db:
        return

    if not re.fullmatch(r"[A-Za-z0-9_]+", target_db):
        raise RuntimeError(f"Invalid database name in BECERTAIN_DATABASE_URL: {target_db!r}")

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
    global _engine, _session_factory
    if _engine is not None:
        return
    _ensure_postgres_database_exists(database_url)
    _engine = create_engine(
        database_url,
        pool_pre_ping=True,
        pool_size=int(os.getenv("BECERTAIN_DB_POOL_SIZE", "10")),
        max_overflow=int(os.getenv("BECERTAIN_DB_MAX_OVERFLOW", "20")),
        pool_timeout=int(os.getenv("BECERTAIN_DB_POOL_TIMEOUT", "30")),
        pool_recycle=int(os.getenv("BECERTAIN_DB_POOL_RECYCLE", "1800")),
    )
    _session_factory = sessionmaker(bind=_engine, autocommit=False, autoflush=False, expire_on_commit=False)


@contextmanager
def get_db_session() -> Iterator[Session]:
    if _session_factory is None:
        raise RuntimeError("Database not initialized")
    session = _session_factory()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def init_db() -> None:
    if _engine is None:
        raise RuntimeError("Database not initialized")
    logger.info("Initializing BeCertain database tables...")
    Base.metadata.create_all(bind=_engine)
    logger.info("BeCertain database tables created successfully")


def connection_test() -> bool:
    if _engine is None:
        return False
    try:
        with _engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except SQLAlchemyError:
        return False


def dispose_database() -> None:
    global _engine, _session_factory
    _session_factory = None
    if _engine is not None:
        _engine.dispose()
        _engine = None
