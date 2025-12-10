# Movie Performance Predictor

A data collection and feature engineering pipeline for predicting movie performance (hit or flop) using reviews, ratings, and sentiment analysis from multiple sources.

## Overview

This repository contains the **data collection component** of a movie performance prediction system. It scrapes movie reviews and ratings from IMDb and Rotten Tomatoes, performs sentiment analysis, and prepares features for machine learning classification.

## Features

- **Multi-source data collection**: Scrapes ratings and reviews from:
  - IMDb (ratings, user reviews)
  - Rotten Tomatoes (Tomatometer scores, critic and audience reviews)
- **Sentiment analysis**: Uses VADER to analyze review sentiment
- **Data enrichment**: Combines TMDB metadata with scraped reviews
- **Feature engineering**: Aggregates sentiment scores by source and category

## Dataset

The pipeline processes 2,188 commercial movies released between 2016-2024, sourced from The Movie Database (TMDB).

## Repository Structure

```
movie-performance-predictor/
├── notebooks/
│   └── data_collection.ipynb    # Main data collection workflow
├── scrapers/
│   ├── imdb_scraper.py          # IMDb scraper
│   └── rotten_tomatoes_selenium.py  # Rotten Tomatoes scraper
├── scripts/
│   ├── add_rt_slug_column.py    # Database migration scripts
│   ├── add_sentiment_columns.sql
│   ├── check_status.py          # Data collection status checker
│   └── view_database.py         # Database viewer utility
├── data/
│   └── tmdb_commercial_movies_2016_2024.csv  # Source dataset
├── config/
│   └── api_keys.example.yaml    # Configuration template
└── requirements.txt             # Python dependencies
```

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/movie-performance-predictor.git
cd movie-performance-predictor
```

2. Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Set up configuration:
```bash
cp config/api_keys.example.yaml config/api_keys.yaml
# Edit api_keys.yaml with your API credentials if needed
```

## Usage

### Running the Data Collection Notebook

Open and run `notebooks/data_collection.ipynb` to:
1. Load movies from the TMDB CSV dataset
2. Scrape ratings and reviews from IMDb and Rotten Tomatoes
3. Perform sentiment analysis on collected reviews
4. Generate enriched features for ML model training

### Utility Scripts

**Check collection status:**
```bash
python scripts/check_status.py
```

**View database contents:**
```bash
python scripts/view_database.py
```

**Run database migrations:**
```bash
python scripts/add_rt_slug_column.py
```

## Data Collection Challenges

### Social Media Sources
Initial plans included scraping Twitter and Reddit for real-time audience sentiment, but were abandoned due to:

- **Twitter**: Free API limited to 500 tweets/month; Pro plan ($100/month) not feasible for academic research
- **Reddit**: API restricted as of November 2024, requiring academic approval from Reddit Devvit team

### Current Approach
Focus on IMDb and Rotten Tomatoes as primary review sources, which provide comprehensive critic and audience perspectives without API restrictions.

## Machine Learning Pipeline

This repository handles **data collection and feature engineering only**. The scraped data is exported and fed into a separate ML classifier for predicting movie performance (hit/flop classification).

### Features Generated
- TMDB ratings and popularity scores
- IMDb ratings and review sentiment
- Rotten Tomatoes Tomatometer scores
- Aggregated sentiment scores by source:
  - IMDb user reviews
  - RT top critics
  - RT all critics
  - RT verified audience
  - RT all audience

## Dependencies

Key libraries:
- `selenium` - Web scraping for dynamic content
- `beautifulsoup4` - HTML parsing
- `requests` - HTTP requests
- `pandas` - Data manipulation
- `sqlalchemy` - Database operations
- `vaderSentiment` - Sentiment analysis
- `fuzzywuzzy` - Fuzzy string matching for movie search

See `requirements.txt` for complete list.

## Contributing

This is an academic project. If you find issues or have suggestions, please open an issue or submit a pull request.

## License

MIT License - see LICENSE file for details.

## Acknowledgments

- TMDB for movie metadata
- IMDb and Rotten Tomatoes for review data
- VADER Sentiment Analysis tool
