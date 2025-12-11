"""
Database package for movie recommendation system.
"""

from .models import (
    Base,
    Movie,
    MovieSearchTerm,
    Review,
    MovieEmbedding,
    UserRating,
    ScrapingLog,
    get_engine,
    init_database,
    get_session
)

__all__ = [
    'Base',
    'Movie',
    'MovieSearchTerm',
    'Review',
    'MovieEmbedding',
    'UserRating',
    'ScrapingLog',
    'get_engine',
    'init_database',
    'get_session'
]
