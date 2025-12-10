#!/usr/bin/env python3
"""
Comprehensive database health checker
- Shows counts and statistics
- Detects data quality issues
- Identifies potential problems
- Provides actionable recommendations
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import func, distinct

sys.path.insert(0, str(Path(__file__).parent / 'src'))

from src.database.db import SessionLocal
from src.database.models import Movie, Review, MovieSearchTerm

def check_database_health():
    """Run comprehensive database health checks"""
    db = SessionLocal()
    issues = []
    warnings = []
    
    try:
        print("\n" + "=" * 80)
        print(" DATABASE HEALTH CHECK")
        print("=" * 80)
        
        # ============================================================
        # SECTION 1: BASIC COUNTS
        # ============================================================
        print("\n BASIC STATISTICS")
        print("-" * 80)
        
        movies_count = db.query(Movie).count()
        reviews_count = db.query(Review).count()
        terms_count = db.query(MovieSearchTerm).count()
        
        print(f"  Total Movies:        {movies_count:>10,}")
        print(f"  Total Reviews:       {reviews_count:>10,}")
        print(f"  Search Terms:        {terms_count:>10,}")
        
        if reviews_count > 0:
            avg_reviews_per_movie = reviews_count / max(movies_count, 1)
            print(f"  Avg Reviews/Movie:   {avg_reviews_per_movie:>10.1f}")
        
        # ============================================================
        # SECTION 2: REVIEW SOURCE BREAKDOWN
        # ============================================================
        if reviews_count > 0:
            print("\n REVIEW SOURCES")
            print("-" * 80)
            
            imdb_count = db.query(Review).filter(Review.source == 'imdb').count()
            rt_count = db.query(Review).filter(Review.source == 'rotten_tomatoes').count()
            reddit_count = db.query(Review).filter(Review.source == 'reddit').count()
            twitter_count = db.query(Review).filter(Review.source == 'twitter').count()
            
            print(f"  IMDb:                {imdb_count:>10,} ({imdb_count/reviews_count*100:>5.1f}%)")
            print(f"  Rotten Tomatoes:     {rt_count:>10,} ({rt_count/reviews_count*100:>5.1f}%)")
            if reddit_count > 0:
                print(f"  Reddit:              {reddit_count:>10,} ({reddit_count/reviews_count*100:>5.1f}%)")
            if twitter_count > 0:
                print(f"  Twitter:             {twitter_count:>10,} ({twitter_count/reviews_count*100:>5.1f}%)")
            
            # RT review type breakdown
            if rt_count > 0:
                print("\n   Rotten Tomatoes Breakdown:")
                top_critics = db.query(Review).filter(
                    Review.source == 'rotten_tomatoes',
                    Review.sentiment_label == 'top_critic'
                ).count()
                critics = db.query(Review).filter(
                    Review.source == 'rotten_tomatoes',
                    Review.sentiment_label == 'critic'
                ).count()
                verified = db.query(Review).filter(
                    Review.source == 'rotten_tomatoes',
                    Review.sentiment_label == 'verified_audience'
                ).count()
                audience = db.query(Review).filter(
                    Review.source == 'rotten_tomatoes',
                    Review.sentiment_label == 'audience'
                ).count()
                
                print(f"     Top Critics:      {top_critics:>10,}")
                print(f"     Critics:          {critics:>10,}")
                print(f"     Verified Aud:     {verified:>10,}")
                print(f"     Audience:         {audience:>10,}")
        
        # ============================================================
        # SECTION 3: DATA QUALITY CHECKS
        # ============================================================
        print("\n DATA QUALITY CHECKS")
        print("-" * 80)
        
        # Check for movies without reviews
        movies_without_reviews = db.query(Movie).outerjoin(Review).filter(
            Review.id == None
        ).count()
        
        if movies_without_reviews > 0:
            pct = movies_without_reviews / max(movies_count, 1) * 100
            warnings.append(f"{movies_without_reviews} movies ({pct:.1f}%) have no reviews")
            print(f"  ⚠️  Movies w/o reviews: {movies_without_reviews:>10,} ({pct:>5.1f}%)")
        else:
            print(f"    All movies have reviews")
        
        # Check for reviews without text
        empty_reviews = db.query(Review).filter(
            (Review.text == None) | (Review.text == '')
        ).count()
        
        if empty_reviews > 0:
            issues.append(f"{empty_reviews} reviews have empty text")
            print(f"    Empty review text:   {empty_reviews:>10,}")
        else:
            print(f"    No empty reviews")
        
        # Check for very short reviews (< 20 chars)
        short_reviews = db.query(Review).filter(
            Review.review_length < 20
        ).count()
        
        if short_reviews > 0:
            warnings.append(f"{short_reviews} reviews are very short (< 20 chars)")
            print(f"  ⚠️  Very short reviews:  {short_reviews:>10,}")
        
        # Check for duplicate source_ids
        duplicate_source_ids = db.query(Review.source_id, func.count(Review.source_id)).group_by(
            Review.source_id
        ).having(func.count(Review.source_id) > 1).count()
        
        if duplicate_source_ids > 0:
            issues.append(f"{duplicate_source_ids} duplicate source_ids found")
            print(f"    Duplicate source_ids: {duplicate_source_ids:>10,}")
        else:
            print(f"    No duplicate source_ids")
        
        # Check for reviews without source_id
        missing_source_id = db.query(Review).filter(
            (Review.source_id == None) | (Review.source_id == '')
        ).count()
        
        if missing_source_id > 0:
            issues.append(f"{missing_source_id} reviews missing source_id")
            print(f"    Missing source_id:    {missing_source_id:>10,}")
        else:
            print(f"    All reviews have source_id")
        
        # Check for movies without year
        missing_year = db.query(Movie).filter(Movie.release_year == None).count()
        if missing_year > 0:
            warnings.append(f"{missing_year} movies missing release year")
            print(f"  ⚠️  Movies w/o year:     {missing_year:>10,}")
        
        # Check for movies without genres
        missing_genres = db.query(Movie).filter(
            (Movie.genres == None) | (Movie.genres == '')
        ).count()
        if missing_genres > 0:
            warnings.append(f"{missing_genres} movies missing genres")
            print(f"  ⚠️  Movies w/o genres:   {missing_genres:>10,}")
        
        # ============================================================
        # SECTION 4: RATING ANALYSIS
        # ============================================================
        if movies_count > 0:
            print("\n RATING COVERAGE")
            print("-" * 80)
            
            tmdb_rated = db.query(Movie).filter(Movie.tmdb_rating != None).count()
            imdb_rated = db.query(Movie).filter(Movie.imdb_rating != None).count()
            
            print(f"  TMDB ratings:        {tmdb_rated:>10,} ({tmdb_rated/movies_count*100:>5.1f}%)")
            print(f"  IMDb ratings:        {imdb_rated:>10,} ({imdb_rated/movies_count*100:>5.1f}%)")
            
            # Check for ratings outside valid range
            invalid_tmdb = db.query(Movie).filter(
                (Movie.tmdb_rating < 0) | (Movie.tmdb_rating > 10)
            ).count()
            invalid_imdb = db.query(Movie).filter(
                (Movie.imdb_rating < 0) | (Movie.imdb_rating > 10)
            ).count()
            
            if invalid_tmdb > 0:
                issues.append(f"{invalid_tmdb} movies have invalid TMDB ratings")
                print(f"    Invalid TMDB ratings: {invalid_tmdb:>10,}")
            if invalid_imdb > 0:
                issues.append(f"{invalid_imdb} movies have invalid IMDb ratings")
                print(f"    Invalid IMDb ratings: {invalid_imdb:>10,}")
        
        # ============================================================
        # SECTION 5: SCRAPING FRESHNESS
        # ============================================================
        if reviews_count > 0:
            print("\n DATA FRESHNESS")
            print("-" * 80)
            
            # Most recent scrape
            latest_scrape = db.query(func.max(Review.scraped_at)).scalar()
            if latest_scrape:
                days_ago = (datetime.now() - latest_scrape).days
                print(f"  Latest scrape:       {latest_scrape.strftime('%Y-%m-%d %H:%M')}")
                print(f"                       ({days_ago} days ago)")
                
                if days_ago > 7:
                    warnings.append(f"Latest data is {days_ago} days old")
            
            # Oldest scrape
            oldest_scrape = db.query(func.min(Review.scraped_at)).scalar()
            if oldest_scrape:
                print(f"  Oldest scrape:       {oldest_scrape.strftime('%Y-%m-%d %H:%M')}")
        
        # ============================================================
        # SECTION 6: REVIEW METRICS
        # ============================================================
        if reviews_count > 0:
            print("\n REVIEW METRICS")
            print("-" * 80)
            
            avg_length = db.query(func.avg(Review.review_length)).scalar() or 0
            avg_words = db.query(func.avg(Review.word_count)).scalar() or 0
            
            print(f"  Avg review length:   {avg_length:>10.0f} chars")
            print(f"  Avg word count:      {avg_words:>10.0f} words")
            
            # IMDb helpful votes
            if imdb_count > 0:
                total_helpful = db.query(func.sum(Review.helpful_count)).filter(
                    Review.source == 'imdb'
                ).scalar() or 0
                total_not_helpful = db.query(func.sum(Review.not_helpful_count)).filter(
                    Review.source == 'imdb'
                ).scalar() or 0
                
                print(f"\n  IMDb Engagement:")
                print(f"    Helpful votes:     {total_helpful:>10,}")
                print(f"    Not helpful:       {total_not_helpful:>10,}")
                if total_helpful + total_not_helpful > 0:
                    helpful_ratio = total_helpful / (total_helpful + total_not_helpful) * 100
                    print(f"    Helpful ratio:     {helpful_ratio:>10.1f}%")
        
        # ============================================================
        # SECTION 7: TOP MOVIES BY REVIEWS
        # ============================================================
        if reviews_count > 0:
            print("\n TOP 5 MOVIES BY REVIEW COUNT")
            print("-" * 80)
            
            top_movies = db.query(
                Movie.title,
                Movie.release_year,
                func.count(Review.id).label('review_count')
            ).join(Review).group_by(Movie.id).order_by(
                func.count(Review.id).desc()
            ).limit(5).all()
            
            for i, (title, year, count) in enumerate(top_movies, 1):
                print(f"  {i}. {title[:50]} ({year}): {count} reviews")
        
        # ============================================================
        # SECTION 8: SUMMARY & RECOMMENDATIONS
        # ============================================================
        print("\n" + "=" * 80)
        print(" SUMMARY")
        print("=" * 80)
        
        if not issues and not warnings:
            print("\n DATABASE HEALTH: EXCELLENT")
            print("   No issues or warnings detected!")
        else:
            if issues:
                print(f"\n CRITICAL ISSUES ({len(issues)}):")
                for issue in issues:
                    print(f"   • {issue}")
            
            if warnings:
                print(f"\n⚠️  WARNINGS ({len(warnings)}):")
                for warning in warnings:
                    print(f"   • {warning}")
            
            print("\n RECOMMENDATIONS:")
            if movies_without_reviews > 0:
                print("   • Run scraping pipeline to collect more reviews")
            if empty_reviews > 0:
                print("   • Clean up or remove reviews with empty text")
            if duplicate_source_ids > 0:
                print("   • Investigate and remove duplicate reviews")
            if missing_source_id > 0:
                print("   • Add source_id to all reviews for proper deduplication")
        
        print("\n" + "=" * 80 + "\n")
        
    except Exception as e:
        print(f"\n ERROR during health check: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        db.close()

if __name__ == "__main__":
    check_database_health()
