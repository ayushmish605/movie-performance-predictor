"""
Initialize the database and load initial movie data.
"""

import sys
import os
import pandas as pd
from pathlib import Path
import yaml
from datetime import datetime

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from database.models import init_database, get_session, Movie
from utils.logger import setup_logger

logger = setup_logger(__name__)


def load_config():
    """Load configuration from YAML file"""
    config_path = Path(__file__).parent.parent.parent / 'config' / 'config.yaml'
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def load_movies_from_csv(csv_path, session, limit=None):
    """
    Load movies from TMDB CSV into database.
    
    Args:
        csv_path: Path to the TMDB movies CSV file
        session: Database session
        limit: Maximum number of movies to load
    """
    logger.info(f"Loading movies from {csv_path}")
    
    try:
        df = pd.read_csv(csv_path)
        
        if limit:
            df = df.head(limit)
        
        logger.info(f"Found {len(df)} movies in CSV")
        
        # Expected columns (adjust based on actual CSV structure)
        # Common TMDB columns: id, title, release_date, genres, overview, vote_average, vote_count, popularity
        
        movies_added = 0
        movies_updated = 0
        
        for idx, row in df.iterrows():
            try:
                # Check if movie already exists
                tmdb_id = row.get('id') or row.get('tmdb_id')
                existing_movie = session.query(Movie).filter_by(tmdb_id=tmdb_id).first()
                
                # Parse genres (often stored as JSON string)
                genres = row.get('genres', '[]')
                if isinstance(genres, str):
                    import json
                    try:
                        genres = json.loads(genres.replace("'", '"'))
                    except:
                        genres = []
                
                # Parse release date
                release_date = row.get('release_date')
                if pd.notna(release_date):
                    try:
                        release_date = pd.to_datetime(release_date)
                    except:
                        release_date = None
                else:
                    release_date = None
                
                # Extract year
                year = None
                if release_date:
                    year = release_date.year
                elif 'year' in row and pd.notna(row['year']):
                    year = int(row['year'])
                
                if existing_movie:
                    # Update existing movie
                    existing_movie.title = row.get('title', existing_movie.title)
                    existing_movie.year = year
                    existing_movie.genres = genres
                    existing_movie.overview = row.get('overview')
                    existing_movie.vote_average = row.get('vote_average')
                    existing_movie.vote_count = row.get('vote_count')
                    existing_movie.popularity = row.get('popularity')
                    existing_movie.release_date = release_date
                    existing_movie.updated_at = datetime.utcnow()
                    movies_updated += 1
                else:
                    # Create new movie
                    movie = Movie(
                        tmdb_id=tmdb_id,
                        imdb_id=row.get('imdb_id'),
                        title=row.get('title'),
                        year=year,
                        genres=genres,
                        overview=row.get('overview'),
                        runtime=row.get('runtime'),
                        language=row.get('original_language'),
                        vote_average=row.get('vote_average'),
                        vote_count=row.get('vote_count'),
                        popularity=row.get('popularity'),
                        release_date=release_date
                    )
                    session.add(movie)
                    movies_added += 1
                
                # Commit every 100 movies
                if (movies_added + movies_updated) % 100 == 0:
                    session.commit()
                    logger.info(f"Processed {movies_added + movies_updated} movies...")
                    
            except Exception as e:
                logger.error(f"Error processing movie at row {idx}: {e}")
                continue
        
        # Final commit
        session.commit()
        logger.info(f"Successfully loaded {movies_added} new movies and updated {movies_updated} existing movies")
        
    except Exception as e:
        logger.error(f"Error loading movies from CSV: {e}")
        session.rollback()
        raise


def main():
    """Main initialization function"""
    # Load configuration
    config = load_config()
    
    # Create data directories
    data_dir = Path(__file__).parent.parent.parent / 'data'
    (data_dir / 'database').mkdir(parents=True, exist_ok=True)
    (data_dir / 'raw' / 'reviews').mkdir(parents=True, exist_ok=True)
    (data_dir / 'processed').mkdir(parents=True, exist_ok=True)
    
    # Initialize database
    logger.info("Initializing database...")
    db_path = config['data']['database']
    db_full_path = Path(__file__).parent.parent.parent / db_path
    db_url = f"sqlite:///{db_full_path}"
    
    engine = init_database(db_url)
    logger.info("Database schema created successfully")
    
    # Load movies from CSV if available
    csv_path = input("Enter path to TMDB movies CSV (or press Enter to skip): ").strip()
    
    if csv_path and os.path.exists(csv_path):
        session = get_session(engine)
        try:
            max_movies = config['scraping'].get('max_movies', 2000)
            load_movies_from_csv(csv_path, session, limit=max_movies)
        finally:
            session.close()
    else:
        logger.info("Skipping CSV import. You can manually import later.")
    
    logger.info("Database initialization complete!")


if __name__ == "__main__":
    main()
