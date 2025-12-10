"""
Movie review scrapers for data collection.
"""

from .imdb_scraper import IMDbScraper
from .rotten_tomatoes_selenium import RottenTomatoesSeleniumScraper

__all__ = ['IMDbScraper', 'RottenTomatoesSeleniumScraper']
