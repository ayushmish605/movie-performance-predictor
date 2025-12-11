"""Data ingestion package"""

from .tmdb_loader import TMDBDataLoader, load_tmdb_data

__all__ = ['TMDBDataLoader', 'load_tmdb_data']
