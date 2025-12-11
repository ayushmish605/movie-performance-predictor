CREATE TABLE movies (
    tmdb_id               INTEGER PRIMARY KEY,
    title                 TEXT,
    original_title        TEXT,
    release_date          DATE,
    year                  INTEGER,
    original_language     TEXT,
    genres                TEXT,
    budget                BIGINT,
    revenue_worldwide     BIGINT,
    runtime               INTEGER,
    popularity            DOUBLE PRECISION,
    vote_average          DOUBLE PRECISION,
    vote_count            INTEGER,
    production_companies  TEXT,
    production_countries  TEXT,
    director              TEXT,
    top_3_cast            TEXT,
    overview              TEXT,
    adult                 BOOLEAN
);

\copy movies (
    tmdb_id,
    title,
    original_title,
    release_date,
    year,
    original_language,
    genres,
    budget,
    revenue_worldwide,
    runtime,
    popularity,
    vote_average,
    vote_count,
    production_companies,
    production_countries,
    director,
    top_3_cast,
    overview,
    adult
)
FROM '/Users/Downloads/tmdb_commercial_movies_2016_2025.csv'
CSV HEADER;

CREATE TABLE rt_reviews_raw (
    id SERIAL PRIMARY KEY,
    tmdb_id INTEGER REFERENCES movies(tmdb_id),
    review_source TEXT,            
    review_text TEXT,
    sentiment_score DOUBLE PRECISION,
    rating_out_of_10 DOUBLE PRECISION,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE imdb_reviews_raw (
    id SERIAL PRIMARY KEY,
    tmdb_id INTEGER REFERENCES movies(tmdb_id),
    sentiment_imdb_avg DOUBLE PRECISION,
    sentiment_imdb_count INTEGER,
    created_at TIMESTAMP DEFAULT NOW()
);



