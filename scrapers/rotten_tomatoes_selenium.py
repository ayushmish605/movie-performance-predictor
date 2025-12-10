"""
Rotten Tomatoes Selenium Scraper

Uses Selenium WebDriver to scrape JavaScript-rendered reviews from Rotten Tomatoes.
This is slower but works with RT's dynamic content loading.

Installation:
    pip install selenium webdriver-manager

Configuration:
    Set USE_SELENIUM=True in config.yaml to enable this scraper
"""

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import time
import re
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import sys
from pathlib import Path
from urllib.parse import quote
from difflib import SequenceMatcher

sys.path.append(str(Path(__file__).parent.parent))
from utils.logger import setup_logger

logger = setup_logger(__name__)


class RottenTomatoesSeleniumScraper:
    """Scrape reviews from Rotten Tomatoes using Selenium for JavaScript-rendered content"""
    
    BASE_URL = "https://www.rottentomatoes.com"
    
    # Review endpoints
    ENDPOINTS = {
        'top_critics': '/reviews/top-critics',
        'all_critics': '/reviews/all-critics',
        'verified_audience': '/reviews/verified-audience',
        'all_audience': '/reviews/all-audience'
    }
    
    # Priority weights for deduplication
    PRIORITIES = {
        'top_critic': 4,
        'critic': 3,
        'verified_audience': 2,
        'audience': 1
    }
    
    def __init__(self, rate_limit: float = 3.0, headless: bool = True):
        """
        Initialize Selenium scraper.
        
        Args:
            rate_limit: Seconds between requests
            headless: Run browser in headless mode (no GUI)
        """
        self.rate_limit = rate_limit
        self.headless = headless
        self.driver = None
        logger.info("Initializing Rotten Tomatoes Selenium scraper")
    
    def _init_driver(self):
        """Initialize Chrome WebDriver with optimal settings"""
        if self.driver:
            return
        
        logger.info("Setting up Chrome WebDriver...")
        
        chrome_options = Options()
        if self.headless:
            chrome_options.add_argument("--headless")  # Simple headless mode that works on macOS
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
        
        # Disable images for faster loading
        prefs = {
            "profile.managed_default_content_settings.images": 2,
            "profile.default_content_setting_values.notifications": 2
        }
        chrome_options.add_experimental_option("prefs", prefs)
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        try:
            # Use simple initialization like your working script - no Service object
            self.driver = webdriver.Chrome(options=chrome_options)
            self.driver.set_page_load_timeout(30)
            logger.info(" Chrome WebDriver initialized")
        except Exception as e:
            logger.error(f"Failed to initialize ChromeDriver: {e}")
            logger.info(" Tip: If on macOS, you may need to allow ChromeDriver in System Preferences > Security")
            raise
        def _init_driver(self, force_restart=False):
            """Initialize Chrome WebDriver with optimal settings, with retry logic"""
            if self.driver and not force_restart:
                return
            if self.driver:
                try:
                    self.driver.quit()
                except Exception:
                    pass
                self.driver = None
            logger.info("Setting up Chrome WebDriver...")
            chrome_options = Options()
            if self.headless:
                chrome_options.add_argument("--headless")
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")
            chrome_options.add_argument("--user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36")
            prefs = {
                "profile.managed_default_content_settings.images": 2,
                "profile.default_content_setting_values.notifications": 2
            }
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
            for attempt in range(3):
                try:
                    self.driver = webdriver.Chrome(options=chrome_options)
                    self.driver.set_page_load_timeout(30)
                    logger.info(f" Chrome WebDriver initialized (attempt {attempt+1})")
                    return
                except Exception as e:
                    logger.error(f"Failed to initialize ChromeDriver (attempt {attempt+1}): {e}")
                    time.sleep(2)
            logger.error(" Could not initialize ChromeDriver after 3 attempts.")
            raise RuntimeError("Failed to initialize ChromeDriver")
    
    def _close_driver(self):
        """Close WebDriver and clean up"""
        if self.driver:
            self.driver.quit()
            self.driver = None
            logger.info("WebDriver closed")
    
    def _generate_slug(self, title: str, year: Optional[int] = None) -> str:
        """
        Generate URL slug from title (fallback method).
        
        Args:
            title: Movie title
            year: Release year
        
        Returns:
            RT movie slug guess (e.g., 'deadpool_and_wolverine')
        """
        # Simple slug generation
        slug = title.lower()
        
        # Replace special characters but keep spaces for now
        # Special handling for & -> _and_
        slug = slug.replace('&', ' and ')
        
        # Remove all non-alphanumeric except spaces
        slug = re.sub(r'[^a-z0-9\s]', '', slug)
        
        # Replace spaces with underscores
        slug = '_'.join(slug.split())
        
        # Add year if provided
        if year:
            slug = f"{slug}_{year}"
        
        return slug
    
    def get_tomatometer_score(self, movie_slug: str) -> Optional[float]:
        """
        Get the Tomatometer (critics) score for a movie, with fallback to Popcornmeter.
        
        Strategy:
        1. Try to get Tomatometer (critics score)
        2. If not available (shows "- -"), fall back to Popcornmeter (audience score)
        
        Args:
            movie_slug: RT movie slug (e.g., 'the_batman')
        
        Returns:
            Score as percentage (0-100), or None if not found
        """
    def get_tomatometer_score(self, movie_slug: str) -> Optional[float]:
        """
        Get the Tomatometer (critics) score for a movie, with fallback to Popcornmeter.
        Improved: Always initializes driver, checks driver before use, adds retry logic and error handling.
        """
        try:
            self._init_driver()
            url = f"{self.BASE_URL}/m/{movie_slug}"
            logger.info(f"Getting RT score from: {url}")
            driver = self.driver
            if not driver:
                logger.error("WebDriver not initialized!")
                return None
            for attempt in range(3):
                try:
                    driver.get(url)
                    wait = WebDriverWait(driver, 20)
                    wait.until(
                        EC.presence_of_element_located(
                            (By.CSS_SELECTOR, 'div.score-wrap')
                        )
                    )
                    break
                except Exception as e:
                    logger.warning(f"Attempt {attempt+1}: Error loading page: {e}")
                    if attempt == 2:
                        logger.error(f"Page source on error:\n{driver.page_source if driver else 'No driver'}")
                        return None
                    time.sleep(2 + attempt)

            # First try: Look for the Tomatometer (critics score)
            try:
                empty_critics = driver.find_element(By.CSS_SELECTOR, 'rt-text.critics-score-empty')
                crit_class = empty_critics.get_attribute('class') if empty_critics else None
                if crit_class and 'hide' not in crit_class:
                    logger.info(f"Tomatometer shows '- -', trying Popcornmeter...")
                    raise NoSuchElementException("Critics score is empty")
            except Exception:
                pass

            # Try to get critics score
            try:
                score_element = driver.find_element(By.CSS_SELECTOR, 'rt-text[slot="criticsScore"]')
                score_text = score_element.text.strip() if score_element else None
                if score_text and score_text != '- -' and '%' in score_text:
                    logger.info(f" Found Tomatometer score: {score_text}")
                    score_value = float(score_text.replace('%', ''))
                    return score_value
                else:
                    raise NoSuchElementException("Invalid critics score")
            except Exception:
                logger.info(f"Tomatometer not available, trying Popcornmeter...")
                try:
                    empty_audience = driver.find_element(By.CSS_SELECTOR, 'rt-text.audience-score-empty')
                    aud_class = empty_audience.get_attribute('class') if empty_audience else None
                    if aud_class and 'hide' not in aud_class:
                        logger.warning(f"⚠️  Both Tomatometer and Popcornmeter show '- -' for: {movie_slug}")
                        return None
                except Exception:
                    pass
                try:
                    popcorn_element = driver.find_element(By.CSS_SELECTOR, 'rt-text[slot="audienceScore"]')
                    popcorn_text = popcorn_element.text.strip() if popcorn_element else None
                    if popcorn_text and popcorn_text != '- -' and '%' in popcorn_text:
                        logger.info(f" Found Popcornmeter score (fallback): {popcorn_text}")
                        popcorn_value = float(popcorn_text.replace('%', ''))
                        return popcorn_value
                    else:
                        logger.warning(f"⚠️  Neither Tomatometer nor Popcornmeter available for: {movie_slug}")
                        return None
                except Exception:
                    logger.warning(f"⚠️  Neither Tomatometer nor Popcornmeter found for: {movie_slug}")
                    return None
        except Exception as e:
            logger.warning(f"⚠️  Error getting RT score for '{movie_slug}': {str(e)}")
            return None
    
    def search_movie(self, title: str, year: Optional[int] = None) -> Optional[str]:
        """
        Search for movie using RT search feature with fallback to slug generation.
        
        Strategy:
        1. Try search WITHOUT year (RT URLs often don't include year)
        2. If that fails and year provided, try search WITH year
        3. If both fail, fallback to slug generation without year
        4. Last resort: slug generation with year
        
        Args:
            title: Movie title
            year: Release year
        
        Returns:
            RT movie slug (e.g., 'deadpool_and_wolverine') or None if not found
        """
        # STRATEGY 1: Search WITHOUT year (most common RT URL format)
        logger.info(f" Strategy 1: Searching without year for '{title}'")
        slug = self._search_via_selenium(title, year=None)
        
        if slug:
            logger.info(f" Found via search (no year): {slug}")
            return slug
        
        # STRATEGY 2: If year provided, try search WITH year
        if year:
            logger.info(f" Strategy 2: Searching with year for '{title}' ({year})")
            slug = self._search_via_selenium(title, year=year)
            
            if slug:
                logger.info(f" Found via search (with year): {slug}")
                return slug
        
        # STRATEGY 3: Fallback to slug generation WITHOUT year
        logger.warning(f"⚠️ Search failed, trying slug generation without year")
        slug = self._generate_slug(title, year=None)
        logger.info(f"Generated slug (no year): {slug}")
        
        # We'll return this and let the scraper validate if it works
        return slug
    
    def _search_via_selenium(self, title: str, year: Optional[int] = None) -> Optional[str]:
        """
        Use RT's search feature to find the actual movie URL.
        
        Args:
            title: Movie title
            year: Release year for verification
        
        Returns:
            RT movie slug if found, None otherwise
        """
        try:
            self._init_driver()
            driver = self.driver
            if not driver:
                logger.error("WebDriver not initialized!")
                return None
            # URL-encode the search query for special characters
            encoded_title = quote(title)
            search_url = f"{self.BASE_URL}/search?search={encoded_title}"
            logger.info(f"Searching RT: {search_url}")
            driver.get(search_url)
            # Wait for search results to load (increased timeout)
            wait = WebDriverWait(driver, 15)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "search-page-media-row")))
            # Find all movie results (search-page-media-row elements)
            results = driver.find_elements(By.TAG_NAME, "search-page-media-row")
            
            if not results:
                logger.warning(f"No search results found for: {title}")
                return None
            
            logger.info(f"Found {len(results)} search results for '{title}'")
            
            # Log all results for debugging
            all_results_info = []
            for i, result in enumerate(results[:10], 1):  # Log first 10
                try:
                    link = result.find_element(By.CSS_SELECTOR, 'a[data-qa="info-name"]')
                    res_title = link.text.strip()
                    res_url = link.get_attribute('href')
                    res_year = result.get_attribute('startyear')
                    res_type = 'TV' if '/tv/' in str(res_url) else 'Movie'
                    all_results_info.append(f"  {i}. {res_title} ({res_year}) [{res_type}]")
                except:
                    pass
            
            if all_results_info:
                logger.debug(f"Search results:\n" + "\n".join(all_results_info))
            
            # Look for best match
            best_match = None
            best_score = 0.0
            
            for result in results:
                try:
                    # Get movie link and title
                    link_element = result.find_element(By.CSS_SELECTOR, 'a[data-qa="info-name"]')
                    result_title = link_element.text.strip()
                    result_url = link_element.get_attribute('href')
                    
                    # Get release year if available
                    result_year = result.get_attribute('startyear')
                    
                    # Skip TV shows (URLs contain /tv/)
                    if result_url and '/tv/' in result_url:
                        logger.debug(f"Skipping TV show: {result_title}")
                        continue
                    
                    # Calculate match score
                    match_score = self._calculate_match_score(title, result_title)
                    
                    # Year bonus/penalty
                    year_compatible = True
                    if year and result_year:
                        try:
                            result_year_int = int(result_year)
                            year_diff = abs(result_year_int - year)
                            if year_diff <= 2:  # Increased tolerance from 1 to 2 years
                                match_score += 0.1  # Bonus for matching year
                            else:
                                year_compatible = False
                                logger.debug(f"Year mismatch: {result_title} ({result_year} vs {year}), score: {match_score:.2f}")
                        except ValueError:
                            pass
                    
                    # Track best match
                    if year_compatible and match_score > best_score:
                        best_score = match_score
                        best_match = (result_title, result_url, result_year, match_score)
                    
                except Exception as e:
                    logger.debug(f"Error parsing search result: {e}")
                    continue
            
            # Accept match if score is high enough (threshold: 0.7)
            if best_match and best_score >= 0.7:
                result_title, result_url, result_year, score = best_match
                # Extract slug from URL
                slug = result_url.replace(f"{self.BASE_URL}/m/", "").rstrip('/')
                logger.info(f" Match found: '{result_title}' ({result_year}) -> {slug} [score: {score:.2f}]")
                return slug
            elif best_match:
                logger.warning(f"⚠️ Best match score too low ({best_score:.2f}): '{best_match[0]}' for '{title}'")
            else:
                logger.warning(f"No matching movie found in search results for: {title}")
            
            return None
            
        except TimeoutException:
            logger.warning(f"Search timeout for: {title} (page took >15s to load)")
            return None
        except Exception as e:
            logger.error(f"Search error for '{title}': {e}")
            return None
    
    def _calculate_match_score(self, search_title: str, result_title: str) -> float:
        """
        Calculate fuzzy match score between search title and result title.
        Uses multiple matching strategies and returns best score.
        
        Args:
            search_title: Original search title
            result_title: Title from search result
        
        Returns:
            Match score between 0.0 and 1.0 (higher is better)
        """
        # Normalize both titles
        def normalize(title):
            title = title.lower()
            # Remove articles from beginning
            for article in ['the ', 'a ', 'an ']:
                if title.startswith(article):
                    title = title[len(article):]
            # Remove special characters but keep spaces
            title = re.sub(r'[^a-z0-9\s]', '', title)
            # Normalize spaces
            title = ' '.join(title.split())
            return title
        
        search_norm = normalize(search_title)
        result_norm = normalize(result_title)
        
        # Strategy 1: Exact match
        if search_norm == result_norm:
            return 1.0
        
        # Strategy 2: One contains the other
        if search_norm in result_norm or result_norm in search_norm:
            # Penalize if one is much longer
            len_ratio = min(len(search_norm), len(result_norm)) / max(len(search_norm), len(result_norm))
            return 0.9 * len_ratio
        
        # Strategy 3: Sequence matching (fuzzy)
        seq_ratio = SequenceMatcher(None, search_norm, result_norm).ratio()
        
        # Strategy 4: Word overlap
        search_words = set(search_norm.split())
        result_words = set(result_norm.split())
        if search_words and result_words:
            word_overlap = len(search_words & result_words) / max(len(search_words), len(result_words))
        else:
            word_overlap = 0.0
        
        # Return best score from strategies
        return max(seq_ratio, word_overlap)
    
    def _titles_match(self, search_title: str, result_title: str) -> bool:
        """
        Check if two titles match (fuzzy matching).
        DEPRECATED: Use _calculate_match_score instead.
        
        Args:
            search_title: Original search title
            result_title: Title from search result
        
        Returns:
            True if titles match
        """
        score = self._calculate_match_score(search_title, result_title)
        return score >= 0.7  # 70% match threshold
    
    def scrape_reviews(
        self,
        movie_slug: str,
        max_reviews: int = 20,
        endpoints: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Scrape reviews from multiple endpoints with deduplication.
        
        Args:
            movie_slug: RT movie URL slug
            max_reviews: Max reviews per endpoint
            endpoints: List of endpoints to scrape (default: all)
        
        Returns:
            List of review dictionaries with deduplication
        """
        if not endpoints:
            endpoints = list(self.ENDPOINTS.keys())
        
        logger.info(f"Scraping RT reviews for: {movie_slug}")
        logger.info(f"Endpoints: {', '.join(endpoints)}")
        
        all_reviews = []
        seen_texts = {}  # text_hash -> (review, priority)
        
        for endpoint_key in endpoints:
            if endpoint_key not in self.ENDPOINTS:
                logger.warning(f"Unknown endpoint: {endpoint_key}")
                continue
            
            endpoint_path = self.ENDPOINTS[endpoint_key]
            reviews = self._scrape_endpoint(movie_slug, endpoint_path, endpoint_key, max_reviews)
            
            # Deduplicate while preserving higher priority
            for review in reviews:
                text_hash = hash(review['text'])
                review_type = review.get('review_type', 'audience')
                priority = self.PRIORITIES.get(review_type, 0)
                
                if text_hash in seen_texts:
                    existing_priority = seen_texts[text_hash][1]
                    if priority > existing_priority:
                        # Replace with higher priority version
                        seen_texts[text_hash] = (review, priority)
                        logger.debug(f"Updated duplicate review to higher priority: {review_type}")
                else:
                    seen_texts[text_hash] = (review, priority)
            
            time.sleep(self.rate_limit)
        
        # Extract deduplicated reviews
        all_reviews = [review for review, _ in seen_texts.values()]
        
        logger.info(f" Collected {len(all_reviews)} unique reviews after deduplication")
        return all_reviews
    
    def _scrape_endpoint(
        self,
        movie_slug: str,
        endpoint_path: str,
        endpoint_type: str,
        max_reviews: int
    ) -> List[Dict]:
        """
        Scrape reviews from a specific endpoint using Selenium.
        
        Args:
            movie_slug: RT movie URL slug
            endpoint_path: Endpoint path (e.g., '/reviews/all-critics')
            endpoint_type: Type identifier (e.g., 'top_critics')
            max_reviews: Maximum reviews to collect
        
        Returns:
            List of review dictionaries
        """
        url = f"{self.BASE_URL}/m/{movie_slug}{endpoint_path}"
        logger.info(f"Scraping: {url}")
        
        try:
            self._init_driver()
            driver = self.driver
            if not driver:
                logger.error("WebDriver not initialized!")
                return []
            logger.info(f"Loading page: {url}")
            driver.get(url)
            # Wait for page to fully load
            time.sleep(2)
            # Wait for review cards to load (shorter timeout, quick failure)
            try:
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "review-card"))
                )
                logger.debug("Review cards loaded successfully")
                # Give extra time for content to render
                time.sleep(2)
            except TimeoutException:
                page_source_lower = driver.page_source.lower() if driver else ""
                page_title = driver.title if driver else ""
                if movie_slug.replace('_', ' ') not in page_title.lower():
                    logger.info(f"ℹ️  Movie not found (redirected to different page): {endpoint_type}")
                elif "404" in page_title.lower() or "not found" in page_source_lower:
                    logger.info(f"ℹ️  404 Not Found: {endpoint_type}")
                elif "no reviews" in page_source_lower or "no user reviews" in page_source_lower:
                    logger.info(f"ℹ️  No reviews available for {endpoint_type}")
                else:
                    logger.info(f"ℹ️  No review cards found at {endpoint_type}")
                return []
            # Scroll to load more reviews
            self._scroll_to_load_reviews(max_reviews)
            # Find all review-card elements
            review_cards = driver.find_elements(By.TAG_NAME, "review-card") if driver else []
            logger.debug(f"Found {len(review_cards)} review cards on page")
            if len(review_cards) == 0:
                logger.info(f"ℹ️  No review cards found for {endpoint_type}")
                return []
            
            reviews = []
            for i, card in enumerate(review_cards[:max_reviews], 1):
                review_data = self._parse_review_card_selenium(card, endpoint_type)
                if review_data:
                    reviews.append(review_data)
            
            logger.info(f" Scraped {len(reviews)} reviews from {endpoint_type}")
            return reviews
            
        except TimeoutException:
            # Already handled above, shouldn't reach here but just in case
            logger.info(f"ℹ️  Timeout for {endpoint_type}")
            return []
            
        except Exception as e:
            # Gracefully handle any errors without crashing
            error_msg = str(e)
            
            # Network/connection errors - common and not alarming
            if any(err in error_msg for err in ['Connection aborted', 'RemoteDisconnected', 
                                                   'ConnectionError', 'Max retries exceeded']):
                logger.info(f"ℹ️  Network error for {endpoint_type} (connection lost)")
            # Driver closed/None errors
            elif 'NoneType' in error_msg or 'driver' in error_msg.lower():
                logger.info(f"ℹ️  Driver closed for {endpoint_type}")
            else:
                # Unknown error - log a warning
                logger.warning(f"⚠️  Error scraping {endpoint_type}: {error_msg[:100]}")
            
            return []  # Always return empty list gracefully
    
    def _scroll_to_load_reviews(self, target_count: int):
        """Scroll page to trigger lazy loading of reviews"""
        driver = self.driver
        if not driver:
            logger.error("WebDriver not initialized for scrolling!")
            return
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_attempts = 0
        max_scrolls = 5
        while scroll_attempts < max_scrolls:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
            new_height = driver.execute_script("return document.body.scrollHeight")
            current_count = len(driver.find_elements(By.TAG_NAME, "review-card"))
            if current_count >= target_count:
                break
            if new_height == last_height:
                break
            last_height = new_height
            scroll_attempts += 1
        logger.debug(f"Scrolled {scroll_attempts} times to load reviews")
    
    def _parse_review_card_selenium(self, card_element, endpoint_type: str) -> Optional[Dict]:
        """
        Parse a single review-card web element.
        
        Args:
            card_element: Selenium WebElement for <review-card>
            endpoint_type: Endpoint identifier for priority assignment
        
        Returns:
            Review dictionary or None
        """
        try:
            # Review text is NOT in shadow DOM - it's in light DOM slots!
            # Structure: <review-card><drawer-more slot="review"><span slot="content">TEXT</span></drawer-more></review-card>
            
            text = None
            
            # Extract review text from drawer-more > span[slot="content"]
            try:
                drawer_element = card_element.find_element(By.CSS_SELECTOR, 'drawer-more[slot="review"]')
                text_element = drawer_element.find_element(By.CSS_SELECTOR, 'span[slot="content"]')
                
                # Get text with proper spacing using JavaScript to avoid button text
                driver = self.driver
                if driver:
                    text = driver.execute_script("""
                        const elem = arguments[0];
                        const clone = elem.cloneNode(true);
                        // Remove any buttons or interactive elements
                        const buttons = clone.querySelectorAll('button, a.ipc-link');
                        buttons.forEach(btn => btn.remove());
                        // Get text with spacing
                        return clone.textContent.trim();
                    """, text_element)
                else:
                    text = text_element.text.strip() if text_element else ""
                
                # Fallback to direct text if JS fails
                if not text:
                    text = text_element.text.strip() if text_element else ""
                
                if text and len(text) >= 10:
                    # Clean up common artifacts
                    text = text.replace('Content collapsed.', '').replace('See Less', '').replace('See More', '')
                    text = text.strip()
                    logger.debug(f"Found review text in drawer-more slot")
                else:
                    # Try alternative: sometimes the text is directly in drawer-more
                    text = drawer_element.text.strip()
                    text = text.replace('Content collapsed.', '').replace('See Less', '').replace('See More', '')
                    text = text.strip()
            except NoSuchElementException:
                # Try fallback selectors
                try:
                    text_element = card_element.find_element(By.CSS_SELECTOR, 'span[slot="content"]')
                    text = text_element.text.strip() if text_element else ""
                    text = text.replace('Content collapsed.', '').replace('See Less', '').replace('See More', '')
                    text = text.strip()
                except NoSuchElementException:
                    pass
            
            if not text or len(text) < 10:
                logger.debug(f"No valid review text found (length: {len(text) if text else 0})")
                return None
            
            # Extract author name from rt-link[slot="name"]
            author = None
            try:
                author_element = card_element.find_element(By.CSS_SELECTOR, 'rt-link[slot="name"]')
                author = author_element.text.strip()
            except NoSuchElementException:
                logger.debug("Could not find author name")
            
            # Extract date from span[slot="timestamp"]
            review_date = None
            try:
                date_element = card_element.find_element(By.CSS_SELECTOR, 'span[slot="timestamp"]')
                date_text = date_element.text.strip()
                if date_text:
                    review_date = self._parse_relative_timestamp(date_text)
            except NoSuchElementException:
                logger.debug("Could not find review date")
            
            # Determine review type and priority
            is_critic = 'critic' in endpoint_type
            is_top = 'top' in endpoint_type
            is_verified = 'verified' in endpoint_type
            
            if is_critic:
                review_type = 'top_critic' if is_top else 'critic'
            else:
                review_type = 'verified_audience' if is_verified else 'audience'
            
            # Generate unique source ID
            source_id = f"rt_{endpoint_type}_{hash(text)}"
            
            logger.debug(f"Successfully parsed review: {len(text)} chars, author: {author}, type: {review_type}")
            
            return {
                'source_id': source_id,
                'text': text,
                'rating': None,  # RT uses binary fresh/rotten, not numeric
                'title': None,
                'author': author,
                'review_date': review_date,
                'helpful_count': 0,  # RT doesn't provide helpful counts
                'not_helpful_count': 0,  # RT doesn't provide not helpful counts
                'review_length': len(text),
                'word_count': len(text.split()),
                'review_type': review_type
            }
            
        except Exception as e:
            logger.debug(f"Error parsing review card: {e}")
            import traceback
            logger.debug(f"Traceback: {traceback.format_exc()}")
            return None
    
    def _parse_relative_timestamp(self, date_str: str) -> Optional[datetime]:
        """
        Convert relative timestamps to datetime.
        
        Args:
            date_str: Date string - can be:
                - Relative: "2d", "3w", "1mo"
                - Month+Day: "Aug 26", "Jul 4"
                - Full date: "Aug 26, 2024"
        
        Returns:
            Datetime object or None
        """
        if not date_str:
            return None
        
        date_str = date_str.strip()
        
        # Try to parse month+day format (e.g., "Aug 26")
        try:
            # If it's just "Month Day" assume current year
            if ',' not in date_str:
                # Try parsing as "Month Day"
                parsed = datetime.strptime(date_str + f", {datetime.now().year}", "%b %d, %Y")
                # If the parsed date is in the future, it was from last year
                if parsed > datetime.now():
                    parsed = datetime.strptime(date_str + f", {datetime.now().year - 1}", "%b %d, %Y")
                return parsed
            else:
                # Full date with year
                return datetime.strptime(date_str, "%b %d, %Y")
        except ValueError:
            pass
        
        # Try relative format: number + unit
        match = re.match(r'(\d+)([smhdwMy])', date_str)
        if not match:
            return None
        
        value = int(match.group(1))
        unit = match.group(2)
        
        now = datetime.now()
        
        if unit == 's':
            return now - timedelta(seconds=value)
        elif unit == 'm':
            return now - timedelta(minutes=value)
        elif unit == 'h':
            return now - timedelta(hours=value)
        elif unit == 'd':
            return now - timedelta(days=value)
        elif unit == 'w':
            return now - timedelta(weeks=value)
        elif unit == 'M':
            return now - timedelta(days=value * 30)
        elif unit == 'y':
            return now - timedelta(days=value * 365)
        
        return None
    
    def scrape_movie_reviews(
        self,
        title: str,
        year: Optional[int] = None,
        max_reviews: int = 20
    ) -> List[Dict]:
        """
        Main method: Search for movie and scrape reviews.
        
        Note: search_movie() now automatically tries without year first,
        then with year if provided. No need for fallback here.
        
        Args:
            title: Movie title
            year: Release year (used for verification, not required in URL)
            max_reviews: Max reviews per endpoint
        
        Returns:
            List of review dictionaries
        """
        try:
            # Search for movie (automatically tries without year first)
            movie_slug = self.search_movie(title, year)
            if not movie_slug:
                logger.warning(f"Could not find slug for: {title}")
                return []
            
            logger.info(f"Using slug: {movie_slug}")
            
            # Scrape all endpoints with the found slug
            reviews = self.scrape_reviews(movie_slug, max_reviews)
            
            return reviews
            
        except Exception as e:
            logger.warning(f"⚠️  Error in scrape_movie_reviews for '{title}': {str(e)[:80]}")
            return []
            
        finally:
            # Always clean up driver
            self._close_driver()
    
    def __del__(self):
        """Cleanup on deletion"""
        self._close_driver()


# Test functionality
if __name__ == "__main__":
    scraper = RottenTomatoesSeleniumScraper(headless=False)
    
    print("Testing Rotten Tomatoes Selenium Scraper")
    print("=" * 60)
    
    # Test movie
    test_title = "Zootopia 2"
    test_year = 2025
    
    print(f"\nScraping reviews for: {test_title} ({test_year})")
    reviews = scraper.scrape_movie_reviews(test_title, test_year, max_reviews=10)
    
    if reviews:
        print(f"\n Scraped {len(reviews)} reviews")
        
        # Show priority distribution
        from collections import Counter
        priorities = Counter(r['review_type'] for r in reviews)
        print(f"\n Priority Distribution:")
        for ptype, count in priorities.items():
            print(f"   {ptype}: {count}")
        
        # Show sample reviews
        print(f"\n Sample Reviews:")
        for i, review in enumerate(reviews[:3], 1):
            print(f"\n{i}. [{review['review_type']}] {review['author'] or 'Anonymous'}")
            print(f"   {review['text'][:150]}...")
    else:
        print("\n⚠️  No reviews found")
