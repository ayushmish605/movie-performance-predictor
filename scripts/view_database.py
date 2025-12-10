#!/usr/bin/env python3
"""
Interactive Database Viewer

Run this script to view the contents of your movie recommendation database.
Works in terminal with rich formatting or opens a simple web interface.

Usage:
    python3 view_database.py              # Terminal viewer
    python3 view_database.py --web        # Web interface (requires flask)
    python3 view_database.py --export     # Export to CSV files
"""

import sys
from pathlib import Path
import argparse

# Add src to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root / 'src'))

from src.database.db import SessionLocal
from src.database.models import Movie, Review, MovieSearchTerm
from datetime import datetime


def print_separator(char="=", length=80):
    """Print a separator line"""
    print(char * length)


def view_summary():
    """Display database summary statistics"""
    db = SessionLocal()
    
    print("\n DATABASE SUMMARY")
    print_separator()
    
    try:
        # Count records
        movie_count = db.query(Movie).count()
        review_count = db.query(Review).count()
        search_term_count = db.query(MovieSearchTerm).count()
        
        # Count by source
        imdb_reviews = db.query(Review).filter(Review.source == 'imdb').count()
        rt_reviews = db.query(Review).filter(Review.source == 'rotten_tomatoes').count()
        
        # Movies with ratings
        movies_with_imdb = db.query(Movie).filter(Movie.imdb_rating.isnot(None)).count()
        movies_with_tmdb = db.query(Movie).filter(Movie.tmdb_rating.isnot(None)).count()
        movies_with_rt = db.query(Movie).filter(Movie.rt_tomatometer.isnot(None)).count()
        
        # Sentiment analysis stats
        reviews_with_sentiment = db.query(Review).filter(Review.sentiment_score.isnot(None)).count()
        positive_reviews = db.query(Review).filter(Review.sentiment_label == 'positive').count()
        negative_reviews = db.query(Review).filter(Review.sentiment_label == 'negative').count()
        neutral_reviews = db.query(Review).filter(Review.sentiment_label == 'neutral').count()
        
        print(f"{'Total Movies':<40} {movie_count:>10,}")
        print(f"{'  - With TMDB Rating':<40} {movies_with_tmdb:>10,}")
        print(f"{'  - With IMDb Rating':<40} {movies_with_imdb:>10,}")
        print(f"{'  - With RT Tomatometer':<40} {movies_with_rt:>10,}")
        print()
        print(f"{'Total Reviews':<40} {review_count:>10,}")
        print(f"{'  - IMDb Reviews':<40} {imdb_reviews:>10,}")
        print(f"{'  - Rotten Tomatoes Reviews':<40} {rt_reviews:>10,}")
        print(f"{'  - With Sentiment Analysis':<40} {reviews_with_sentiment:>10,}")
        if reviews_with_sentiment > 0:
            print(f"{'    • Positive':<40} {positive_reviews:>10,} ({positive_reviews/reviews_with_sentiment*100:>5.1f}%)")
            print(f"{'    • Negative':<40} {negative_reviews:>10,} ({negative_reviews/reviews_with_sentiment*100:>5.1f}%)")
            print(f"{'    • Neutral':<40} {neutral_reviews:>10,} ({neutral_reviews/reviews_with_sentiment*100:>5.1f}%)")
        print()
        print(f"{'Search Terms Generated':<40} {search_term_count:>10,}")
        
        # Average review metrics
        if review_count > 0:
            avg_length = db.query(Review).filter(Review.review_length.isnot(None)).all()
            if avg_length:
                avg_len = sum(r.review_length for r in avg_length) / len(avg_length)
                print()
                print(f"{'Average Review Length':<40} {avg_len:>10,.0f} chars")
        
    finally:
        db.close()
    
    print_separator()


def view_movies(limit=20, search=None):
    """Display movies table"""
    db = SessionLocal()
    
    print("\n MOVIES")
    print_separator()
    
    try:
        query = db.query(Movie)
        
        if search:
            query = query.filter(Movie.title.ilike(f'%{search}%'))
        
        movies = query.limit(limit).all()
        
        if not movies:
            print("No movies found in database.")
            return
        
        print(f"{'ID':<5} {'Title':<32} {'Year':<6} {'TMDB':<6} {'IMDb':<6} {'RT%':<6} {'RT/10':<6} {'Sent':<7} {'Revs':<5}")
        print_separator("-")
        
        for movie in movies:
            review_count = db.query(Review).filter(Review.movie_id == movie.id).count()
            title = movie.title[:29] + '...' if len(movie.title) > 32 else movie.title
            tmdb = f"{movie.tmdb_rating:.1f}" if movie.tmdb_rating else "N/A"
            imdb_r = f"{movie.imdb_rating:.1f}" if movie.imdb_rating else "N/A"
            rt_pct = f"{movie.rt_tomatometer:.0f}" if movie.rt_tomatometer else "N/A"
            rt_10 = f"{movie.rt_tomatometer_out_of_10:.1f}" if movie.rt_tomatometer_out_of_10 else "N/A"
            
            # Calculate overall sentiment average from all categories
            sentiment_values = [
                movie.sentiment_imdb_avg,
                movie.sentiment_rt_top_critics_avg,
                movie.sentiment_rt_all_critics_avg,
                movie.sentiment_rt_verified_audience_avg,
                movie.sentiment_rt_all_audience_avg
            ]
            valid_sentiments = [s for s in sentiment_values if s is not None]
            sentiment = f"{sum(valid_sentiments)/len(valid_sentiments):.3f}" if valid_sentiments else "N/A"
            
            print(f"{movie.id:<5} {title:<32} {movie.release_year:<6} {tmdb:<6} {imdb_r:<6} {rt_pct:<6} {rt_10:<6} {sentiment:<7} {review_count:<5}")
        
        total = db.query(Movie).count()
        print_separator("-")
        print(f"Showing {len(movies)} of {total:,} total movies")
        
    finally:
        db.close()


def view_reviews(limit=20, movie_title=None):
    """Display reviews table"""
    db = SessionLocal()
    
    print("\n REVIEWS")
    print_separator()
    
    try:
        query = db.query(Review).order_by(Review.scraped_at.desc())
        
        if movie_title:
            # Find movie first
            movie = db.query(Movie).filter(Movie.title.ilike(f'%{movie_title}%')).first()
            if movie:
                query = query.filter(Review.movie_id == movie.id)
                print(f"Filtering by movie: {movie.title}\n")
            else:
                print(f"Movie not found: {movie_title}\n")
        
        reviews = query.limit(limit).all()
        
        if not reviews:
            print("No reviews found.")
            return
        
        for i, review in enumerate(reviews, 1):
            movie = db.query(Movie).filter(Movie.id == review.movie_id).first()
            print(f"\n{i}. {movie.title if movie else 'Unknown'} ({review.source.upper()})")
            print(f"   Author: {review.author or 'Anonymous'}")
            
            if review.rating:
                print(f"   Rating: {review.rating}/10")
            
            # Sentiment information
            if review.sentiment_score is not None:
                sentiment_emoji = "" if review.sentiment_label == 'positive' else "" if review.sentiment_label == 'negative' else ""
                print(f"   Sentiment: {review.sentiment_score:.4f} ({review.sentiment_label} {sentiment_emoji})")
                if review.sentiment_confidence:
                    print(f"   Confidence: {review.sentiment_confidence:.4f}")
            
            # Category for RT reviews
            if review.source == 'rotten_tomatoes' and review.review_category:
                category_display = review.review_category.replace('_', ' ').title()
                print(f"   Category: {category_display}")
            
            if review.helpful_count:
                print(f"   Helpful: {review.helpful_count:,} votes")
            
            print(f"   Length: {review.word_count or 0} words")
            
            # Show preview
            preview = review.text[:150] + "..." if review.text and len(review.text) > 150 else review.text
            print(f"   \"{preview}\"")
            
            if i < len(reviews):
                print_separator("-")
        
        total = db.query(Review).count()
        print(f"\nShowing {len(reviews)} of {total:,} total reviews")
        
    finally:
        db.close()


def view_search_terms(limit=20):
    """Display search terms table"""
    db = SessionLocal()
    
    print("\n SEARCH TERMS")
    print_separator()
    
    try:
        terms = db.query(MovieSearchTerm).limit(limit).all()
        
        if not terms:
            print("No search terms found. Run Step 11c to generate them.")
            return
        
        for i, term in enumerate(terms, 1):
            movie = db.query(Movie).filter(Movie.id == term.movie_id).first()
            print(f"{i}. {movie.title if movie else 'Unknown'}")
            print(f"   Term: \"{term.search_term}\"")
            print(f"   Source: {term.source}")
            print()
        
        total = db.query(MovieSearchTerm).count()
        print(f"Showing {len(terms)} of {total:,} total search terms")
        
    finally:
        db.close()


def export_to_csv():
    """Export database to CSV files"""
    import pandas as pd
    
    db = SessionLocal()
    export_dir = project_root / "exports"
    export_dir.mkdir(exist_ok=True)
    
    print("\n EXPORTING TO CSV")
    print_separator()
    
    try:
        # Export movies
        movies = db.query(Movie).all()
        if movies:
            df_movies = pd.DataFrame([{
                'id': m.id,
                'title': m.title,
                'year': m.release_year,
                'genres': m.genres,
                'tmdb_rating': m.tmdb_rating,
                'imdb_rating': m.imdb_rating,
                'tmdb_votes': m.tmdb_vote_count,
                'imdb_votes': m.imdb_vote_count,
                'rt_tomatometer': m.rt_tomatometer,
                'rt_tomatometer_out_of_10': m.rt_tomatometer_out_of_10,
                'sentiment_imdb_avg': m.sentiment_imdb_avg,
                'sentiment_rt_top_critics_avg': m.sentiment_rt_top_critics_avg,
                'sentiment_rt_all_critics_avg': m.sentiment_rt_all_critics_avg,
                'sentiment_rt_verified_audience_avg': m.sentiment_rt_verified_audience_avg,
                'sentiment_rt_all_audience_avg': m.sentiment_rt_all_audience_avg
            } for m in movies])
            
            movies_file = export_dir / f"movies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_movies.to_csv(movies_file, index=False)
            print(f" Exported {len(movies):,} movies to: {movies_file}")
        
        # Export reviews
        reviews = db.query(Review).all()
        if reviews:
            df_reviews = pd.DataFrame([{
                'movie_id': r.movie_id,
                'source': r.source,
                'author': r.author,
                'rating': r.rating,
                'text': r.text,
                'review_date': r.review_date,
                'helpful_count': r.helpful_count,
                'not_helpful_count': r.not_helpful_count,
                'word_count': r.word_count,
                'sentiment_score': r.sentiment_score,
                'sentiment_label': r.sentiment_label,
                'sentiment_confidence': r.sentiment_confidence,
                'review_category': r.review_category
            } for r in reviews])
            
            reviews_file = export_dir / f"reviews_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df_reviews.to_csv(reviews_file, index=False)
            print(f" Exported {len(reviews):,} reviews to: {reviews_file}")
        
        print(f"\n Export directory: {export_dir}")
        
    finally:
        db.close()


def interactive_menu():
    """Display interactive menu"""
    while True:
        print("\n" + "="*60)
        print(" MOVIE RECOMMENDATION DATABASE VIEWER")
        print("="*60)
        print("\n1. View Summary")
        print("2. View Movies")
        print("3. View Reviews")
        print("4. View Search Terms")
        print("5. Search Movies by Title")
        print("6. View Reviews for Specific Movie")
        print("7. Export to CSV")
        print("0. Exit")
        
        choice = input("\nEnter your choice: ").strip()
        
        if choice == '1':
            view_summary()
        elif choice == '2':
            limit = input("How many movies to show? (default: 20): ").strip()
            limit = int(limit) if limit.isdigit() else 20
            view_movies(limit=limit)
        elif choice == '3':
            limit = input("How many reviews to show? (default: 20): ").strip()
            limit = int(limit) if limit.isdigit() else 20
            view_reviews(limit=limit)
        elif choice == '4':
            view_search_terms()
        elif choice == '5':
            search = input("Enter movie title to search: ").strip()
            if search:
                view_movies(limit=50, search=search)
        elif choice == '6':
            movie_title = input("Enter movie title: ").strip()
            if movie_title:
                view_reviews(limit=100, movie_title=movie_title)
        elif choice == '7':
            export_to_csv()
        elif choice == '0':
            print("\n Goodbye!")
            break
        else:
            print("\n Invalid choice. Please try again.")
        
        input("\nPress Enter to continue...")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='View movie recommendation database')
    parser.add_argument('--summary', action='store_true', help='Show summary only')
    parser.add_argument('--movies', type=int, metavar='N', help='Show N movies')
    parser.add_argument('--reviews', type=int, metavar='N', help='Show N reviews')
    parser.add_argument('--search', type=str, help='Search movies by title')
    parser.add_argument('--export', action='store_true', help='Export to CSV')
    
    args = parser.parse_args()
    
    # If no arguments, show interactive menu
    if len(sys.argv) == 1:
        interactive_menu()
        return
    
    # Handle specific commands
    if args.summary:
        view_summary()
    
    if args.movies:
        view_movies(limit=args.movies, search=args.search)
    
    if args.reviews:
        view_reviews(limit=args.reviews)
    
    if args.export:
        export_to_csv()


if __name__ == '__main__':
    main()
