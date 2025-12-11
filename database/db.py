"""
Database connection utilities for notebooks and scripts.
Provides simplified imports for common database operations.
"""

from pathlib import Path
from .models import get_engine, init_database, get_session

# Default database path (relative to project root)
DEFAULT_DB_PATH = Path(__file__).parent.parent.parent / 'data' / 'database' / 'movie_recommender.db'
DEFAULT_DB_URL = f'sqlite:///{DEFAULT_DB_PATH}'

# Engine will be created when needed
_engine = None

def get_or_create_engine():
    """Get existing engine or create a new one"""
    global _engine
    if _engine is None:
        # Create the database directory if it doesn't exist
        DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _engine = get_engine(DEFAULT_DB_URL)
    return _engine

def SessionLocal():
    """Create a new database session"""
    return get_session(get_or_create_engine())

def init_db():
    """Initialize database with default path"""
    # Create the database directory if it doesn't exist
    DEFAULT_DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    init_database(DEFAULT_DB_URL)
    global _engine
    _engine = get_engine(DEFAULT_DB_URL)
    return _engine

__all__ = ['SessionLocal', 'init_db', 'get_or_create_engine', 'DEFAULT_DB_URL', 'DEFAULT_DB_PATH']
