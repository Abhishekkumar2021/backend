from typing import Generator
from app.core.database import SessionLocal

def get_db():
    """
    Dependency for FastAPI routes to get a database session.
    Yields a session and closes it after the request is finished.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
