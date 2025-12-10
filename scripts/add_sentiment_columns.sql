-- SQL script to add sentiment analysis columns to existing database
-- Run with: sqlite3 data/database/movie_recommender.db < add_sentiment_columns.sql

-- Add RT score columns to movies table
ALTER TABLE movies ADD COLUMN rt_tomatometer REAL;
ALTER TABLE movies ADD COLUMN rt_tomatometer_out_of_10 REAL;

-- Add sentiment average columns to movies table
ALTER TABLE movies ADD COLUMN sentiment_imdb_avg REAL;
ALTER TABLE movies ADD COLUMN sentiment_rt_top_critics_avg REAL;
ALTER TABLE movies ADD COLUMN sentiment_rt_all_critics_avg REAL;
ALTER TABLE movies ADD COLUMN sentiment_rt_verified_audience_avg REAL;
ALTER TABLE movies ADD COLUMN sentiment_rt_all_audience_avg REAL;

-- Add review category column to reviews table
ALTER TABLE reviews ADD COLUMN review_category TEXT;

-- Verify columns were added
.schema movies
.schema reviews
