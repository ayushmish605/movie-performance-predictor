# Movie Performance Predictor - Data Collection

This directory contains scripts for data collection and database management.

## Scripts

### add_rt_slug_column.py
Adds the `rt_slug` column to the movies table for storing Rotten Tomatoes movie identifiers.

### add_sentiment_columns.sql
SQL migration to add sentiment analysis columns to the database schema.

### check_status.py
Checks the status of data collection:
- Number of movies with IMDb ratings
- Number of movies with RT scores
- Number of reviews collected by source
- Sentiment analysis progress

### view_database.py
Interactive database viewer for exploring collected data.
