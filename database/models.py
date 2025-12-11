"""
Database models and schema for the movie recommendation system.
"""

from typing import Optional, Dict
from pathlib import Path
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, ForeignKey, 
    Boolean, JSON, Index, create_engine
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
from datetime import datetime

Base = declarative_base()


class Movie(Base):
    """Movies table - stores movie metadata"""
    __tablename__ = 'movies'
    
    id = Column(Integer, primary_key=True)
    tmdb_id = Column(Integer, unique=True, nullable=True)
    imdb_id = Column(String(20), unique=True, nullable=True)
    title = Column(String(500), nullable=False)
    release_year = Column(Integer, nullable=True)  # Renamed from 'year' for clarity
    genres = Column(String(500), nullable=True)  # Pipe-separated: "Action|Sci-Fi|Thriller"
    overview = Column(Text, nullable=True)
    runtime = Column(Integer, nullable=True)
    language = Column(String(10), nullable=True)
    
    # TMDB ratings (from CSV dataset)
    tmdb_rating = Column(Float, nullable=True)  # vote_average from TMDB (0-10 scale)
    tmdb_vote_count = Column(Integer, nullable=True)  # vote_count from TMDB
    popularity = Column(Float, nullable=True)  # TMDB popularity score
    
    # IMDb ratings (scraped live)
    imdb_rating = Column(Float, nullable=True)  # Live scraped rating (0-10 scale)
    imdb_vote_count = Column(Integer, nullable=True)  # Live scraped vote count
    scraped_at = Column(DateTime, nullable=True)  # When IMDb data was last scraped
    
    # Rotten Tomatoes ratings
    rt_tomatometer = Column(Float, nullable=True)  # RT critics score (0-100)
    rt_tomatometer_out_of_10 = Column(Float, nullable=True)  # Converted to 0-10 scale
    rt_slug = Column(String(200), nullable=True)  # RT URL slug (e.g., "the_matrix_1999")
    
    # Sentiment analysis averages (computed from reviews)
    sentiment_imdb_avg = Column(Float, nullable=True)  # Average sentiment from IMDb reviews (-1 to 1)
    sentiment_rt_top_critics_avg = Column(Float, nullable=True)  # RT top critics sentiment
    sentiment_rt_all_critics_avg = Column(Float, nullable=True)  # RT all critics sentiment
    sentiment_rt_verified_audience_avg = Column(Float, nullable=True)  # RT verified audience sentiment
    sentiment_rt_all_audience_avg = Column(Float, nullable=True)  # RT all audience sentiment
    
    # Metadata
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationships
    reviews = relationship("Review", back_populates="movie", cascade="all, delete-orphan")
    search_terms = relationship("MovieSearchTerm", back_populates="movie", cascade="all, delete-orphan")
    embeddings = relationship("MovieEmbedding", back_populates="movie", cascade="all, delete-orphan")
    
    def get_best_rating(self) -> Optional[Float]:
        """
        Select best rating using intelligent strategy:
        1. If IMDb scraped recently (< 7 days), prefer it
        2. If both exist, use weighted average by vote count
        3. Otherwise use whichever exists
        
        Returns:
            Rating value (0-10 scale) or None
        """
        # Strategy 1: Fresh IMDb scrape (< 7 days old)
        if self.imdb_rating and self.scraped_at:
            days_old = (datetime.utcnow() - self.scraped_at).days
            if days_old < 7:
                return self.imdb_rating
        
        # Strategy 2: Both exist - weighted average
        if self.tmdb_rating and self.imdb_rating:
            tmdb_weight = self.tmdb_vote_count or 1
            imdb_weight = self.imdb_vote_count or 1
            total_weight = tmdb_weight + imdb_weight
            
            weighted = (
                (self.tmdb_rating * tmdb_weight + self.imdb_rating * imdb_weight) 
                / total_weight
            )
            return round(weighted, 2)
        
        # Strategy 3: Use whatever exists
        return self.imdb_rating or self.tmdb_rating
    
    def get_rating_metadata(self) -> Dict:
        """
        Get detailed rating information for transparency
        
        Returns:
            Dictionary with rating sources and metadata
        """
        result = {
            'recommended_rating': self.get_best_rating(),
            'sources': []
        }
        
        if self.tmdb_rating:
            result['sources'].append({
                'source': 'tmdb_csv',
                'rating': self.tmdb_rating,
                'votes': self.tmdb_vote_count,
                'age_days': None  # Unknown - depends on CSV export date
            })
        
        if self.imdb_rating:
            age_days = None
            if self.scraped_at:
                age_days = (datetime.utcnow() - self.scraped_at).days
            
            result['sources'].append({
                'source': 'imdb_scraped',
                'rating': self.imdb_rating,
                'votes': self.imdb_vote_count,
                'age_days': age_days,
                'scraped_at': self.scraped_at.isoformat() if self.scraped_at else None
            })
        
        # Calculate difference if both exist
        if self.tmdb_rating and self.imdb_rating:
            result['difference'] = round(abs(self.tmdb_rating - self.imdb_rating), 2)
            result['note'] = (
                'Large difference - investigate' if result['difference'] > 1.0 
                else 'Ratings are similar'
            )
        
        return result
    
    # Indexes
    __table_args__ = (
        Index('idx_movie_title', 'title'),
        Index('idx_movie_year', 'release_year'),
        Index('idx_movie_imdb', 'imdb_id'),
    )


class MovieSearchTerm(Base):
    """Stores Gemini-generated search terms and hashtags for each movie"""
    __tablename__ = 'movie_search_terms'
    
    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    search_term = Column(String(200), nullable=False)
    term_type = Column(String(50))  # 'hashtag', 'keyword', 'phrase'
    source = Column(String(50))  # 'reddit', 'twitter', 'general'
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    movie = relationship("Movie", back_populates="search_terms")
    
    __table_args__ = (
        Index('idx_search_movie', 'movie_id'),
    )


class Review(Base):
    """Reviews table - stores all scraped reviews"""
    __tablename__ = 'reviews'
    
    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    source = Column(String(50), nullable=False)  # 'imdb', 'reddit', 'twitter', 'rotten_tomatoes'
    source_id = Column(String(200), unique=True)  # Original ID from source
    review_category = Column(String(50), nullable=True)  # For RT: 'top_critics', 'all_critics', 'verified_audience', 'all_audience'
    
    # Review content
    text = Column(Text, nullable=False)
    rating = Column(Float, nullable=True)  # Numeric rating if available
    title = Column(String(500), nullable=True)  # Review title
    
    # Author info
    author = Column(String(200), nullable=True)
    author_id = Column(String(200), nullable=True)
    
    # Engagement metrics
    upvotes = Column(Integer, default=0)
    downvotes = Column(Integer, default=0)
    helpful_count = Column(Integer, default=0)
    not_helpful_count = Column(Integer, default=0)  # IMDb not helpful votes
    reply_count = Column(Integer, default=0)
    
    # Temporal info
    review_date = Column(DateTime, nullable=True)
    scraped_at = Column(DateTime, default=datetime.utcnow)
    
    # Review quality metrics
    review_length = Column(Integer)  # Character count
    word_count = Column(Integer)
    quality_score = Column(Float, default=0.0)  # Weighted quality score
    
    # Sentiment analysis (to be filled later)
    sentiment_score = Column(Float, nullable=True)  # -1 to 1
    sentiment_label = Column(String(20), nullable=True)  # 'positive', 'negative', 'neutral'
    sentiment_confidence = Column(Float, nullable=True)
    
    # Processed flags
    is_processed = Column(Boolean, default=False)
    is_valid = Column(Boolean, default=True)  # False if spam/invalid
    
    # Relationships
    movie = relationship("Movie", back_populates="reviews")
    
    __table_args__ = (
        Index('idx_review_movie', 'movie_id'),
        Index('idx_review_source', 'source'),
        Index('idx_review_date', 'review_date'),
        Index('idx_review_quality', 'quality_score'),
    )


class MovieEmbedding(Base):
    """Stores precomputed embeddings for movies"""
    __tablename__ = 'movie_embeddings'
    
    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    embedding_type = Column(String(50))  # 'content', 'review_aggregate', 'tfidf'
    embedding_vector = Column(JSON)  # Store as JSON array
    model_name = Column(String(100))
    created_at = Column(DateTime, default=datetime.utcnow)
    
    # Relationships
    movie = relationship("Movie", back_populates="embeddings")
    
    __table_args__ = (
        Index('idx_embedding_movie', 'movie_id'),
        Index('idx_embedding_type', 'embedding_type'),
    )


class UserRating(Base):
    """User ratings table - for collaborative filtering"""
    __tablename__ = 'user_ratings'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, nullable=False)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=False)
    rating = Column(Float, nullable=False)
    timestamp = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        Index('idx_rating_user', 'user_id'),
        Index('idx_rating_movie', 'movie_id'),
        Index('idx_rating_user_movie', 'user_id', 'movie_id'),
    )


class ScrapingLog(Base):
    """Logs scraping activities for monitoring and debugging"""
    __tablename__ = 'scraping_logs'
    
    id = Column(Integer, primary_key=True)
    movie_id = Column(Integer, ForeignKey('movies.id'), nullable=True)
    source = Column(String(50), nullable=False)
    status = Column(String(20))  # 'success', 'failed', 'rate_limited'
    reviews_collected = Column(Integer, default=0)
    error_message = Column(Text, nullable=True)
    timestamp = Column(DateTime, default=datetime.utcnow)
    duration_seconds = Column(Float, nullable=True)
    
    __table_args__ = (
        Index('idx_log_source', 'source'),
        Index('idx_log_timestamp', 'timestamp'),
    )


# Database utility functions
def get_engine(database_url='sqlite:///data/database/movie_recommender.db'):
    """Create and return database engine"""
    return create_engine(database_url, echo=False)


def init_database(database_url='sqlite:///data/database/movie_recommender.db'):
    """Initialize database and create all tables"""
    # Create the database directory if it doesn't exist (for SQLite)
    if database_url.startswith('sqlite:///'):
        db_path = Path(database_url.replace('sqlite:///', ''))
        db_path.parent.mkdir(parents=True, exist_ok=True)
    
    engine = get_engine(database_url)
    Base.metadata.create_all(engine)
    return engine


def get_session(engine):
    """Create and return database session"""
    Session = sessionmaker(bind=engine)
    return Session()
