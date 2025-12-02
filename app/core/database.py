"""Database engine and session management.

Optimized for Universal ETL workloads.
"""

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from contextlib import contextmanager

from app.core.config import settings

# =============================================================
# Engine initialization
# =============================================================
engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,   # avoid stale connections
    pool_timeout=30,
    echo=settings.LOG_LEVEL == "DEBUG",
    future=True,
)

# =============================================================
# Session factory
# =============================================================
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True,
    expire_on_commit=False,
)

# =============================================================
# Context manager for sessions
# =============================================================
@contextmanager
def get_db_session():
    """
    Provides a transactional scope around a series of operations.
    Prevents connection leaks and standardizes usage.
    """
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
