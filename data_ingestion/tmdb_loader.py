"""
TMDB CSV Data Loader
Loads movie metadata from TMDB dataset and populates database
"""

import os
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from sqlalchemy.orm import Session
from database.models import Movie, ScrapingLog
from database.db import SessionLocal, init_db

logger = logging.getLogger(__name__)


class TMDBDataLoader:
    """Loads and validates TMDB dataset"""
    
    def __init__(self, csv_path: Optional[str] = None):
        """
        Initialize loader with optional CSV path
        
        Args:
            csv_path: Path to TMDB CSV file (optional)
        """
        self.csv_path = csv_path
        self.df = None
        self.stats = {
            'total_movies': 0,
            'loaded': 0,
            'skipped': 0,
            'errors': 0
        }
    
    def load_csv(self, csv_path: Optional[str] = None) -> pd.DataFrame:
        """Load and validate CSV file"""
        # Use provided path or instance path
        path = csv_path or self.csv_path
        if not path:
            raise ValueError("No CSV path provided")
        
        if not os.path.exists(path):
            raise FileNotFoundError(f"CSV file not found: {path}")
        
        logger.info(f"Loading TMDB data from {path}")
        
        # Load CSV with proper encoding
        try:
            self.df = pd.read_csv(path, encoding='utf-8')
        except UnicodeDecodeError:
            self.df = pd.read_csv(path, encoding='latin-1')
        
        self.stats['total_movies'] = len(self.df)
        logger.info(f"Loaded {self.stats['total_movies']} movies from CSV")
        
        # Log column names for debugging
        logger.info(f"CSV columns: {list(self.df.columns)}")
        
        return self.df
    
    def _parse_genres(self, genre_str: str) -> List[str]:
        """Parse genre string into list"""
        if pd.isna(genre_str) or not genre_str:
            return []
        
        # Handle different formats: "Action|Adventure|Sci-Fi" or "Action, Adventure"
        if '|' in str(genre_str):
            return [g.strip() for g in str(genre_str).split('|') if g.strip()]
        elif ',' in str(genre_str):
            return [g.strip() for g in str(genre_str).split(',') if g.strip()]
        else:
            return [str(genre_str).strip()]
    
    def _parse_date(self, date_str: str) -> Optional[datetime]:
        """Parse release date string"""
        if pd.isna(date_str) or not date_str:
            return None
        
        # Try multiple date formats
        formats = [
            '%Y-%m-%d',
            '%m/%d/%Y',
            '%Y/%m/%d',
            '%d-%m-%Y',
            '%Y'
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(str(date_str), fmt)
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def _extract_year(self, row: pd.Series) -> Optional[int]:
        """Extract release year from row"""
        # Try dedicated year column first
        if 'release_year' in row.index and not pd.isna(row['release_year']):
            try:
                return int(row['release_year'])
            except (ValueError, TypeError):
                pass
        
        # Try extracting from release_date
        if 'release_date' in row.index:
            date = self._parse_date(row['release_date'])
            if date:
                return date.year
        
        return None
    
    def _get_column_value(self, row: pd.Series, possible_names: List[str]) -> Optional[str]:
        """Get value from row trying multiple possible column names"""
        for name in possible_names:
            if name in row.index and not pd.isna(row[name]):
                return str(row[name]).strip()
        return None
    
    def parse_movie(self, row: pd.Series) -> Optional[Dict]:
        """
        Parse a CSV row into movie dictionary
        
        Expected TMDB columns (flexible matching):
        - title, original_title, movie_title
        - release_date, release_year
        - genres, genre
        - overview, plot, description
        - vote_average, rating, imdb_rating
        - vote_count, vote_count
        - popularity
        - budget
        - revenue
        - runtime
        - original_language, language
        """
        try:
            # Extract title (required)
            title = self._get_column_value(row, ['title', 'original_title', 'movie_title', 'name'])
            if not title:
                logger.warning("Skipping row with no title")
                return None
            
            # Extract year
            year = self._extract_year(row)
            
            # Extract genres
            genre_str = self._get_column_value(row, ['genres', 'genre', 'genre_ids'])
            genres = self._parse_genres(genre_str) if genre_str else []
            
            # Extract overview/plot
            overview = self._get_column_value(row, ['overview', 'plot', 'description', 'synopsis'])
            
            # Extract ratings (TMDB uses vote_average 0-10 scale)
            rating_str = self._get_column_value(row, ['vote_average', 'rating', 'tmdb_rating'])
            tmdb_rating = None
            if rating_str:
                try:
                    tmdb_rating = float(rating_str)
                except (ValueError, TypeError):
                    pass
            
            # Extract vote count
            vote_count_str = self._get_column_value(row, ['vote_count', 'votes'])
            vote_count = None
            if vote_count_str:
                try:
                    vote_count = int(float(vote_count_str))
                except (ValueError, TypeError):
                    pass
            
            # Extract other metadata
            popularity_str = self._get_column_value(row, ['popularity'])
            popularity = float(popularity_str) if popularity_str else None
            
            runtime_str = self._get_column_value(row, ['runtime'])
            runtime = int(float(runtime_str)) if runtime_str else None
            
            language = self._get_column_value(row, ['original_language', 'language'])
            
            return {
                'title': title,
                'release_year': year,
                'genres': genres,
                'overview': overview,
                'tmdb_rating': tmdb_rating,
                'tmdb_vote_count': vote_count,
                'popularity': popularity,
                'runtime': runtime,
                'language': language
            }
            
        except Exception as e:
            logger.error(f"Error parsing movie row: {e}")
            return None
    
    def load_into_database(self, db: Session, update_existing: bool = False) -> Dict:
        """
        Load movies from CSV into database
        
        Args:
            db: Database session
            update_existing: Whether to update existing movies
        
        Returns:
            Statistics dictionary
        """
        if self.df is None:
            self.load_csv()
        
        for idx, row in self.df.iterrows():
            try:
                movie_data = self.parse_movie(row)
                if not movie_data:
                    self.stats['skipped'] += 1
                    continue
                
                # Check if movie exists
                existing = db.query(Movie).filter(
                    Movie.title == movie_data['title'],
                    Movie.release_year == movie_data['release_year']
                ).first()
                
                if existing:
                    if update_existing:
                        # Update metadata from TMDB (but keep scraped data)
                        existing.overview = movie_data['overview'] or existing.overview
                        existing.tmdb_rating = movie_data['tmdb_rating']
                        existing.tmdb_vote_count = movie_data['tmdb_vote_count']
                        existing.popularity = movie_data['popularity']
                        existing.runtime = movie_data['runtime']
                        existing.language = movie_data['language']
                        
                        # Update genres if new ones exist
                        if movie_data['genres']:
                            existing.genres = '|'.join(movie_data['genres'])
                        
                        logger.info(f"Updated existing movie: {movie_data['title']} ({movie_data['release_year']})")
                    else:
                        logger.debug(f"Skipping existing movie: {movie_data['title']}")
                        self.stats['skipped'] += 1
                        continue
                else:
                    # Create new movie
                    movie = Movie(
                        title=movie_data['title'],
                        release_year=movie_data['release_year'],
                        genres='|'.join(movie_data['genres']) if movie_data['genres'] else None,
                        overview=movie_data['overview'],
                        tmdb_rating=movie_data['tmdb_rating'],
                        tmdb_vote_count=movie_data['tmdb_vote_count'],
                        popularity=movie_data['popularity'],
                        runtime=movie_data['runtime'],
                        language=movie_data['language'],
                        # These will be populated by scrapers
                        imdb_rating=None,
                        imdb_id=None,
                        scraped_at=None
                    )
                    db.add(movie)
                    logger.info(f"Added new movie: {movie_data['title']} ({movie_data['release_year']})")
                
                self.stats['loaded'] += 1
                
                # Commit every 100 movies
                if idx % 100 == 0:
                    db.commit()
                    logger.info(f"Progress: {idx}/{self.stats['total_movies']} movies processed")
            
            except Exception as e:
                logger.error(f"Error loading movie at row {idx}: {e}")
                self.stats['errors'] += 1
                continue
        
        # Final commit
        db.commit()
        
        # Log scraping metadata
        log = ScrapingLog(
            source='tmdb_csv',
            reviews_collected=0,
            status='completed'
        )
        db.add(log)
        db.commit()
        
        logger.info(f"TMDB Data Load Complete: {self.stats}")
        return self.stats
    
    def load_movies_from_csv(self, csv_path: str, update_existing: bool = False) -> int:
        """
        Convenience method to load movies from CSV file
        
        Args:
            csv_path: Path to TMDB CSV file
            update_existing: Whether to update existing movies
        
        Returns:
            Number of movies loaded
        """
        self.csv_path = csv_path
        self.load_csv()
        
        db = SessionLocal()
        try:
            self.load_into_database(db, update_existing)
            return self.stats['loaded']
        finally:
            db.close()


def load_tmdb_data(csv_path: str, update_existing: bool = False) -> Dict:
        logger.info(f"TMDB Data Load Complete: {self.stats}")
        return self.stats


def load_tmdb_data(csv_path: str, update_existing: bool = False) -> Dict:
    """
    Convenience function to load TMDB data
    
    Args:
        csv_path: Path to TMDB CSV file
        update_existing: Whether to update existing movies
    
    Returns:
        Statistics dictionary
    """
    # Initialize database
    init_db()
    
    # Load data
    loader = TMDBDataLoader(csv_path)
    db = SessionLocal()
    try:
        stats = loader.load_into_database(db, update_existing)
        return stats
    finally:
        db.close()


if __name__ == '__main__':
    import sys
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    if len(sys.argv) < 2:
        print("Usage: python -m src.data_ingestion.tmdb_loader <path_to_csv>")
        sys.exit(1)
    
    csv_path = sys.argv[1]
    stats = load_tmdb_data(csv_path, update_existing=False)
    
    print("\n" + "="*60)
    print("TMDB DATA LOAD SUMMARY")
    print("="*60)
    print(f"Total movies in CSV: {stats['total_movies']}")
    print(f"Successfully loaded: {stats['loaded']}")
    print(f"Skipped (duplicates): {stats['skipped']}")
    print(f"Errors: {stats['errors']}")
    print("="*60)
