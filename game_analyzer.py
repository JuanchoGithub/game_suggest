import streamlit as st
import pandas as pd
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser
import time
import os
import sqlite3
import logging
import re

# Configure logging
LOG_FILE = 'game_analyzer.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

CACHE_FILE = 'wishlist_cache.csv'
CACHE_DIR = 'html_cache'
DATABASE_FILE = 'game_database.db'

def create_database():
    """Create the game database if it doesn't exist"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS games (
            title TEXT PRIMARY KEY,
            current_price REAL,
            metascore INTEGER,
            openscore INTEGER,
            steam_score INTEGER,
            last_discount TEXT,
            avg_days_between_discounts REAL,
            days_since_last_discount INTEGER
        )
    ''')
    conn.commit()
    conn.close()

def game_exists_in_db(title):
    """Check if a game already exists in the database"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM games WHERE title=?", (title,))
    exists = cursor.fetchone() is not None
    conn.close()
    return exists

def save_game_to_db(game):
    """Save a game's data to the database"""
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    try:
        cursor.execute(
            '''
            INSERT OR REPLACE INTO games (
                title, current_price, metascore, openscore, steam_score, 
                last_discount, avg_days_between_discounts, days_since_last_discount
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''',
            (
                game['title'], game['current_price'], game['metascore'], game['openscore'],
                game['steam_score'], game['last_discount'], game['avg_days_between_discounts'],
                game['days_since_last_discount']
            )
        )
        conn.commit()
        logging.info(f"Saved game to database: {game['title']}")
    except Exception as e:
        logging.error(f"Error saving game to database: {game['title']}: {e}, Game data: {game}")
    finally:
        conn.close()

def get_html_cache_filename(url):
    """Generate a cache filename from the URL"""
    return os.path.join(CACHE_DIR, f'{url.replace("/", "_").replace(":", "_")}.html')

def get_html_from_cache(url):
    """Retrieve HTML content from the cache file"""
    cache_file = get_html_cache_filename(url)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logging.error(f"Error reading cache file {cache_file}: {e}")
    return None

def save_html_to_cache(url, html_content):
    """Save HTML content to the cache file"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = get_html_cache_filename(url)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logging.info(f"HTML saved to cache: {cache_file}")
    except Exception as e:
        logging.error(f"Error saving HTML to cache file {cache_file}: {e}")

def get_steamdb_cache_filename(url):
    """Generate a cache filename for SteamDB URLs"""
    return os.path.join(CACHE_DIR, f'steamdb_{url.replace("/", "_").replace(":", "_")}.html')

def get_steamdb_html_from_cache(url):
    """Retrieve SteamDB HTML content from the cache file"""
    cache_file = get_steamdb_cache_filename(url)
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                return f.read()
        except Exception as e:
            logging.error(f"Error reading SteamDB cache file {cache_file}: {e}")
    return None

def save_steamdb_html_to_cache(url, html_content):
    """Save SteamDB HTML content to the cache file"""
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = get_steamdb_cache_filename(url)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        logging.info(f"SteamDB HTML saved to cache: {cache_file}")
    except Exception as e:
        logging.error(f"Error saving SteamDB HTML to cache file {cache_file}: {e}")

def search_steamdb(game_title):
    """Search SteamDB for the game and return the app ID"""
    search_url = f'https://steamdb.info/search/?a=app&q={game_title}'
    logging.info(f"Searching SteamDB for {game_title} using URL: {search_url}")

    # Check if HTML is cached
    html_content = get_steamdb_html_from_cache(search_url)
    if html_content:
        logging.info(f"Loading SteamDB search HTML from cache for {search_url}")
        soup = BeautifulSoup(html_content, 'html.parser')
    else:
        try:
            response = requests.get(search_url)
            response.raise_for_status()
            html_content = response.text
            save_steamdb_html_to_cache(search_url, html_content)
            soup = BeautifulSoup(html_content, 'html.parser')
        except requests.exceptions.RequestException as e:
            logging.error(f"Error searching SteamDB for {game_title}: {e}")
            return None

    # Find the first search result
    result_link = soup.select_one('.app a')
    if result_link:
        app_id = result_link['href'].split('/')[2]
        logging.info(f"Found SteamDB app ID {app_id} for {game_title}")
        return app_id
    else:
        logging.warning(f"No SteamDB search results found for {game_title}")
        logging.warning(f"Full search URL: {search_url}")
        logging.warning(f"Response content: {soup.prettify()}")
        return None

def get_steam_rating(app_id):
    """Get the Steam rating from the SteamDB app page"""
    if not app_id:
        return None

    app_url = f'https://steamdb.info/app/{app_id}/'
    logging.info(f"Fetching Steam rating for app ID {app_id} from URL: {app_url}")

    # Check if HTML is cached
    html_content = get_steamdb_html_from_cache(app_url)
    if html_content:
        logging.info(f"Loading SteamDB app HTML from cache for {app_url}")
        soup = BeautifulSoup(html_content, 'html.parser')
    else:
        try:
            response = requests.get(app_url)
            response.raise_for_status()
            html_content = response.text
            save_steamdb_html_to_cache(app_url, html_content)
            soup = BeautifulSoup(html_content, 'html.parser')
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching SteamDB app page for {app_id}: {e}")
            return None

    # Find the review element
    review_element = soup.select_one('a[href*="#reviews"]')
    if review_element:
        # Extract the rating from the aria-label attribute
        aria_label = review_element.get('aria-label')
        if aria_label:
            match = re.search(r'([\d\.]+)%', aria_label)
            if match:
                steam_score = match.group(1)
                logging.info(f"Extracted Steam score {steam_score} for app ID {app_id}")
                return steam_score
        else:
            logging.warning(f"No aria-label found on SteamDB for app ID {app_id}")
            logging.warning(f"Full app URL: {app_url}")
            logging.warning(f"Response content: {soup.prettify()}")
            return None
    else:
        logging.warning(f"No Steam rating found on SteamDB for app ID {app_id}")
        logging.warning(f"Full app URL: {app_url}")
        logging.warning(f"Response content: {soup.prettify()}")
        return None

def get_game_data(base_url, force_refresh=False):
    """Get game data, prioritizing database, then cache, then web scraping."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }

    session = requests.Session()
    all_games = []

    # Create database if it doesn't exist
    create_database()

    # --- Loading Logic ---
    if not force_refresh:
        # 1. Try loading from Database
        st.write("Attempting to load data from database...")
        logging.info("Attempting to load data from database...")
        try:
            conn = sqlite3.connect(DATABASE_FILE)
            # Check if the table is empty first
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM games")
            count = cursor.fetchone()[0]
            if count > 0:
                df = pd.read_sql_query("SELECT * FROM games", conn)
                conn.close()
                if not df.empty:
                    st.success("Data loaded successfully from database.")
                    logging.info("Data loaded successfully from database.")
                    # Ensure correct types after loading from DB
                    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
                    for col in ['metascore', 'openscore', 'steam_score', 'days_since_last_discount']:
                         df[col] = pd.to_numeric(df[col], errors='coerce')
                    df['avg_days_between_discounts'] = pd.to_numeric(df['avg_days_between_discounts'], errors='coerce')
                    return df
                else:
                    st.write("Database table exists but is empty.")
                    logging.info("Database table exists but is empty.")
            else:
                 st.write("Database table is empty.")
                 logging.info("Database table is empty.")
            conn.close()
        except Exception as e:
            st.warning(f"Could not load from database: {e}")
            logging.warning(f"Could not load from database: {e}")

        # 2. Try loading from CSV Cache (as fallback if DB fails/is empty)
        st.write("Attempting to load data from CSV cache...")
        logging.info("Attempting to load data from CSV cache...")
        if os.path.exists(CACHE_FILE):
            try:
                df = pd.read_csv(CACHE_FILE)
                # Basic validation
                if not df.empty and 'title' in df.columns:
                     st.success("Data loaded successfully from CSV cache.")
                     logging.info("Data loaded successfully from CSV cache.")
                     # Ensure correct types after loading from CSV
                     df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
                     for col in ['metascore', 'openscore', 'steam_score', 'days_since_last_discount']:
                          df[col] = pd.to_numeric(df[col], errors='coerce')
                     df['avg_days_between_discounts'] = pd.to_numeric(df['avg_days_between_discounts'], errors='coerce')
                     return df
                else:
                     st.warning("CSV cache file is empty or invalid.")
                     logging.warning("CSV cache file is empty or invalid.")
            except Exception as e:
                st.warning(f"Error loading CSV cache: {e}")
                logging.warning(f"Error loading CSV cache: {e}")
        else:
            st.write("CSV cache file not found.")
            logging.info("CSV cache file not found.")

        # If both DB and Cache fail or are empty, proceed to scrape
        st.write("No data found in database or cache. Proceeding to fetch from web.")
        logging.info("No data found in database or cache. Proceeding to fetch from web.")
        force_refresh = True # Force scraping if no cached/DB data found

    # --- Web Scraping Logic (only runs if force_refresh is True) ---
    if force_refresh:
        st.write("Fetching data from web...")
        logging.info("Fetching data from web...")
        page = 1
        games_processed_count = 0
        while True:
            st.write(f"Fetching page {page}...")
            url = f"{base_url}?page={page}" if page > 1 else base_url

            # Always fetch from web when force_refresh is True
            st.write(f"Fetching HTML from {url}")
            logging.info(f"Fetching HTML from {url}")
            try:
                st.write(f"Sending GET request to: {url}")
                response = session.get(url, headers=headers, timeout=15) # Added timeout
                response.raise_for_status()
                st.write(f"Received response with status code: {response.status_code}")
                html_content = response.text
                save_html_to_cache(url, html_content) # Save fetched HTML
                soup = BeautifulSoup(html_content, 'html.parser')
            except requests.exceptions.RequestException as e:
                st.error(f"Error fetching page {page}: {e}")
                logging.error(f"Error fetching page {page}: {e}")
                break # Stop if a page fails

            game_cards = soup.select('.list-view')
            st.write(f"Found {len(game_cards)} game cards on page {page}")
            logging.info(f"Found {len(game_cards)} game cards on page {page}")

            if not game_cards:
                st.write("No more game cards found, stopping.")
                logging.info("No more game cards found, stopping.")
                break

            for card in game_cards:
                game = {}
                title_elem = card.select_one('.main-link h6')
                if not title_elem:
                    st.warning("Skipping card: No title found.")
                    logging.warning("Skipping card: No title found.")
                    continue
                game['title'] = title_elem.text.strip()

                # --- Check DB *before* scraping details ---
                # This prevents re-scraping details for games already saved.
                if game_exists_in_db(game['title']):
                    st.write(f"Skipping game (already in database): {game['title']}")
                    logging.info(f"Skipping game (already in database): {game['title']}")
                    continue # Skip to the next card

                st.write(f"Processing game: {game['title']}")
                logging.info(f"Processing game: {game['title']}")
                games_processed_count += 1

                # Get current price
                price_elem = card.select_one('strong')
                if price_elem:
                    price_text = price_elem.text.strip()
                    # 1. Remove currency symbols (like ARS$, $, €) and whitespace
                    # Keep digits, comma, and dot for now
                    cleaned_price_str = re.sub(r'[^\d,.]', '', price_text)

                    try:
                        # Check if the original cleaned string contained a comma (decimal separator)
                        if ',' in cleaned_price_str:
                            # Treat '.' as thousands, ',' as decimal
                            numeric_str = cleaned_price_str.replace('.', '').replace(',', '.')
                            game['current_price'] = float(numeric_str) if numeric_str else 0.0
                        # Check if the original cleaned string contained a dot but no comma (e.g., $19.99)
                        elif '.' in cleaned_price_str:
                             # Treat '.' as decimal, ignore potential thousands commas if any were missed (shouldn't happen with current regex)
                             numeric_str = cleaned_price_str.replace(',', '') # Remove commas if any snuck in
                             game['current_price'] = float(numeric_str) if numeric_str else 0.0
                        # Only digits found (e.g., "1135000")
                        elif cleaned_price_str.isdigit():
                            # Assume last two digits are decimals
                            if len(cleaned_price_str) >= 2:
                                numeric_str = cleaned_price_str[:-2] + '.' + cleaned_price_str[-2:]
                                game['current_price'] = float(numeric_str)
                            elif len(cleaned_price_str) == 1:
                                # Treat single digit as dollars/euros/etc. (e.g., "5")
                                game['current_price'] = float(cleaned_price_str)
                            else: # Empty string after cleaning
                                game['current_price'] = 0.0
                        else: # Handle cases where cleaning resulted in non-standard format
                             game['current_price'] = 0.0
                             st.warning(f"Could not parse price for {game['title']} from text '{price_text}' (cleaned: '{cleaned_price_str}' - unexpected format)")
                             logging.warning(f"Could not parse price for {game['title']} from text '{price_text}' (cleaned: '{cleaned_price_str}' - unexpected format)")

                    except ValueError:
                        game['current_price'] = 0.0
                        st.warning(f"Could not parse price for {game['title']} from text '{price_text}' (cleaned: '{cleaned_price_str}') - ValueError")
                        logging.warning(f"Could not parse price for {game['title']} from text '{price_text}' (cleaned: '{cleaned_price_str}') - ValueError")
                else:
                    game['current_price'] = 0.0
                    st.warning(f"No price found for {game['title']}")
                    logging.warning(f"No price found for {game['title']}")

                # Get game detail URL
                detail_elem = card.select_one('.main-link')
                if detail_elem and detail_elem.get('href'):
                    detail_url = f"https://www.dekudeals.com{detail_elem['href']}"
                    game['detail_url'] = detail_url
                    time.sleep(0.5) # Be polite

                    try:
                        # --- Detail Page Scraping ---
                        detail_html = None
                        # Check cache first even in refresh mode for detail pages to speed up
                        detail_html_cache = get_html_from_cache(detail_url)
                        if detail_html_cache:
                             detail_soup = BeautifulSoup(detail_html_cache, 'html.parser')
                        else:
                             detail_response = session.get(detail_url, headers=headers, timeout=15) # Added timeout
                             detail_response.raise_for_status()
                             detail_html = detail_response.text
                             save_html_to_cache(detail_url, detail_html) # Cache detail page
                             detail_soup = BeautifulSoup(detail_html, 'html.parser')

                        # Scores
                        metacritic_elem = detail_soup.select_one("li.list-group-item strong:contains('Metacritic') + a")
                        game['metascore'] = int(metacritic_elem.text.strip()) if metacritic_elem and metacritic_elem.text.strip().isdigit() else None

                        opencritic_elem = detail_soup.select_one("li.list-group-item strong:contains('OpenCritic') + a")
                        game['openscore'] = int(opencritic_elem.text.strip()) if opencritic_elem and opencritic_elem.text.strip().isdigit() else None

                        # Steam Score (consider caching results here too)
                        app_id = search_steamdb(game['title']) # search_steamdb uses its own cache
                        steam_score_str = get_steam_rating(app_id) # get_steam_rating uses its own cache
                        game['steam_score'] = float(steam_score_str) if steam_score_str else None

                        # Price History
                        game['last_discount'] = None
                        game['avg_days_between_discounts'] = None
                        game['days_since_last_discount'] = None
                        history_table = detail_soup.select_one('.price-history table')
                        if history_table:
                            dates = []
                            rows = history_table.select('tr')
                            for row in rows[1:]:
                                date_cell = row.select_one('td')
                                if date_cell:
                                    try:
                                        # Attempt to parse various date formats robustly
                                        date_str = date_cell.text.strip()
                                        if date_str: # Ensure not empty
                                             dates.append(parser.parse(date_str))
                                    except Exception as date_e:
                                        logging.warning(f"Could not parse date '{date_cell.text.strip()}' for {game['title']}: {date_e}")
                                        continue
                            if dates:
                                dates.sort(reverse=True)
                                game['last_discount'] = dates[0].strftime('%Y-%m-%d')
                                game['days_since_last_discount'] = (datetime.now() - dates[0]).days
                                if len(dates) > 1:
                                    diff_days = [(dates[i] - dates[i+1]).days for i in range(len(dates)-1) if (dates[i] - dates[i+1]).days >= 0] # Ensure positive diff
                                    if diff_days:
                                         game['avg_days_between_discounts'] = sum(diff_days) / len(diff_days)
                        else:
                            st.warning(f"No price history table found for {game['title']}")
                            logging.warning(f"No price history table found for {game['title']}")

                    except requests.exceptions.RequestException as e:
                        st.error(f"Error fetching detail page for {game['title']}: {e}")
                        logging.error(f"Error fetching detail page for {game['title']}: {e}")
                        # Don't add game if details failed, but maybe log it?
                        continue # Skip adding this game
                    except Exception as detail_e:
                         st.error(f"Error processing details for {game['title']}: {detail_e}")
                         logging.error(f"Error processing details for {game['title']}: {detail_e}")
                         continue # Skip adding this game
                else:
                    st.warning(f"No detail URL found for game card: {game.get('title', 'N/A')}")
                    logging.warning(f"No detail URL found for game card: {game.get('title', 'N/A')}")
                    # Decide if you want to add games without details
                    # game['metascore'] = None ... etc.
                    continue # Skip adding this game for now

                # Only add and save if details were successfully processed
                all_games.append(game)
                save_game_to_db(game) # Save the newly scraped game

            # Check for next page link more reliably
            next_page_link = soup.select_one('a.page-link[rel="next"]') # Standard rel="next"
            if not next_page_link:
                 # Fallback check if rel="next" isn't used
                 current_active = soup.select_one('li.page-item.active span.page-link')
                 if current_active:
                      next_li = current_active.find_parent('li').find_next_sibling('li')
                      if next_li and next_li.select_one('a.page-link'):
                           next_page_link = next_li.select_one('a.page-link')
                      else:
                           next_page_link = None # Truly no next page element found
                 else:
                      next_page_link = None # Cannot determine next page

            if not next_page_link:
                st.write("No next page link found, stopping.")
                logging.info("No next page link found, stopping.")
                break

            page += 1
            # Optional: Add a limit to prevent infinite loops during testing
            # if page > 5: # Limit to 5 pages for testing
            #     st.warning("Reached page limit for testing.")
            #     break

        # --- Post-Scraping Processing ---
        if not all_games and games_processed_count == 0:
             st.warning("Web scraping finished, but no new games were processed or added.")
             logging.warning("Web scraping finished, but no new games were processed or added.")
             # Attempt to load from DB again in case it was populated by another run
             try:
                  conn = sqlite3.connect(DATABASE_FILE)
                  df = pd.read_sql_query("SELECT * FROM games", conn)
                  conn.close()
                  if not df.empty:
                       st.info("Loaded data from existing database after scraping yielded no new games.")
                       logging.info("Loaded data from existing database after scraping yielded no new games.")
                       # Ensure types
                       df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
                       for col in ['metascore', 'openscore', 'steam_score', 'days_since_last_discount']:
                            df[col] = pd.to_numeric(df[col], errors='coerce')
                       df['avg_days_between_discounts'] = pd.to_numeric(df['avg_days_between_discounts'], errors='coerce')
                       return df
                  else:
                       return pd.DataFrame() # Return empty df if still nothing
             except Exception as e:
                  st.error(f"Failed to load from database after scraping: {e}")
                  logging.error(f"Failed to load from database after scraping: {e}")
                  return pd.DataFrame() # Return empty df on error

        elif not all_games and games_processed_count > 0:
             st.error("Web scraping processed games but resulted in an empty list. Check logs for errors during detail processing.")
             logging.error("Web scraping processed games but resulted in an empty list.")
             return pd.DataFrame() # Return empty df

        else:
             # Combine newly scraped games with existing DB data if needed?
             # For now, just return the newly scraped ones.
             df = pd.DataFrame(all_games)
             st.success(f"Web scraping complete. Processed {games_processed_count} new games.")
             logging.info(f"Web scraping complete. Processed {games_processed_count} new games.")

             # Save the newly scraped data to CSV cache as well
             try:
                 # Optional: Load existing cache, append, drop duplicates, save
                 # Or just overwrite cache with the latest scrape results
                 df.to_csv(CACHE_FILE, index=False)
                 st.info("Updated CSV cache with newly scraped data.")
                 logging.info("Updated CSV cache with newly scraped data.")
             except Exception as e:
                 st.error(f"Error saving updated CSV cache: {e}")
                 logging.error(f"Error saving updated CSV cache: {e}")

             # Ensure correct types before returning
             df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
             for col in ['metascore', 'openscore', 'steam_score', 'days_since_last_discount']:
                  df[col] = pd.to_numeric(df[col], errors='coerce')
             df['avg_days_between_discounts'] = pd.to_numeric(df['avg_days_between_discounts'], errors='coerce')
             return df

    # Should not be reached if logic is correct, but return empty DF as fallback
    st.error("Reached end of get_game_data without returning data.")
    logging.error("Reached end of get_game_data without returning data.")
    return pd.DataFrame()

def analyze_and_recommend(df):
    # Convert price and scores to numeric values
    df['current_price'] = pd.to_numeric(df['current_price'], errors='coerce').fillna(0)
    for col in ['metascore', 'openscore', 'steam_score']:
        df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Calculate average score
    df['avg_score'] = df[['metascore', 'openscore', 'steam_score']].mean(axis=1)
    
    # Normalize scores and prices
    max_price = df['current_price'].max()
    if max_price > 0:
        df['normalized_price'] = 1 - (df['current_price'] / max_price)
    else:
        df['normalized_price'] = 0
    
    score_range = df['avg_score'].max() - df['avg_score'].min()
    if score_range > 0:
        df['normalized_score'] = (df['avg_score'] - df['avg_score'].min()) / score_range
    else:
        df['normalized_score'] = 0
    
    # Calculate discount probability
    df['days_since_last_discount'] = pd.to_numeric(df['days_since_last_discount'], errors='coerce').fillna(365)
    df['discount_probability'] = 1 / (df['days_since_last_discount'] + 1)
    
    # Calculate recommendation score
    df['recommendation_score'] = (
        0.4 * df['normalized_score'].fillna(0) +  # Higher weight for game quality
        0.3 * df['normalized_price'].fillna(0) +  # Price value
        0.3 * df['discount_probability'].fillna(0)  # Likelihood of future discount
    )
    
    return df.sort_values('recommendation_score', ascending=False)

def display_results(wishlist_df, recommendations):
    """Displays the processed data in a scrollable container."""
    # --- Debug Start ---
    st.write("--- Debug: Entering display_results ---")
    if recommendations is None:
        st.write("Debug: 'recommendations' DataFrame is None.")
        return # Cannot proceed if recommendations are None
    elif recommendations.empty:
        st.write("Debug: 'recommendations' DataFrame is empty.")
    else:
        st.write(f"Debug: 'recommendations' columns: {recommendations.columns.tolist()}")
        st.write("Debug: 'recommendations' head:")
        st.dataframe(recommendations.head(2)) # Show a couple of rows for inspection
    # --- Debug End ---

    with st.container(height=600): # You can adjust the height as needed
        # Display raw data
        st.subheader('Raw Data')
        # --- Debug Start ---
        if wishlist_df is None:
             st.write("Debug: 'wishlist_df' is None.")
        elif wishlist_df.empty:
             st.write("Debug: 'wishlist_df' is empty.")
        else:
             st.write("Debug: Displaying Raw Data...")
             st.dataframe(wishlist_df[['title', 'current_price', 'metascore', 'openscore', 'steam_score', 'last_discount', 'days_since_last_discount']].head(2))
        # --- Debug End ---
        # Original line: st.dataframe(wishlist_df[['title', ...]]) # Keep original display if needed

        # Display recommendations
        st.header('Recommendations')
        st.write("Here are your game recommendations, sorted by best value:")
        display_cols = [
            'title', 'current_price', 'avg_score',
            'days_since_last_discount', 'avg_days_between_discounts',
            'recommendation_score'
        ]
        # --- Debug Start ---
        st.write("Debug: Checking columns for Recommendations Table...")
        missing_display_cols = [col for col in display_cols if col not in recommendations.columns]
        if not missing_display_cols:
             st.write("Debug: Displaying Recommendations Table...")
             st.dataframe(recommendations[display_cols])
        else:
             st.warning(f"Debug: Cannot display recommendations table. Missing columns: {missing_display_cols}")
        # --- Debug End ---

        # Display statistical insights
        st.header('Statistical Insights')
        st.write(f"Total games in wishlist: {len(recommendations)}")
        # --- Debug Start ---
        st.write("Debug: Calculating Averages...")
        # --- Debug End ---
        # Calculate averages for filtering
        avg_price = None
        if 'current_price' in recommendations.columns and pd.api.types.is_numeric_dtype(recommendations['current_price']) and recommendations['current_price'].notna().any():
            avg_price = recommendations['current_price'].mean()
            st.write(f"Average price of games: ARS${avg_price:.2f}")
        else:
            st.write("Average price calculation skipped (column missing, non-numeric, or all NaN).")
        # --- Debug Start ---
        st.write(f"Debug: avg_price = {avg_price}")
        # --- Debug End ---

        avg_overall_score = None
        if 'avg_score' in recommendations.columns and pd.api.types.is_numeric_dtype(recommendations['avg_score']) and recommendations['avg_score'].notna().any():
             avg_overall_score = recommendations['avg_score'].mean()
             # Display other averages if needed
             if 'metascore' in recommendations.columns and pd.api.types.is_numeric_dtype(recommendations['metascore']) and not recommendations['metascore'].isna().all():
                 st.write(f"Average metascore: {recommendations['metascore'].mean():.1f}")
             if 'avg_days_between_discounts' in recommendations.columns and pd.api.types.is_numeric_dtype(recommendations['avg_days_between_discounts']) and not recommendations['avg_days_between_discounts'].isna().all():
                 st.write(f"Average days between discounts: {recommendations['avg_days_between_discounts'].mean():.1f}")
        else:
             st.write("Average score calculation skipped (column missing, non-numeric, or all NaN).")
        # --- Debug Start ---
        st.write(f"Debug: avg_overall_score = {avg_overall_score}")
        # --- Debug End ---


        # Show top recommendations by category
        st.subheader("Top Games by Category")
        # --- Debug Start ---
        st.write("Debug: Checking conditions for Category Display...")
        # --- Debug End ---

        st.write("Best Value Games (High Score, Low Price - Top 5 Overall):")
        if not recommendations.empty:
            # --- Debug Start ---
            st.write("Debug: Displaying Best Value Games...")
            # --- Debug End ---
            value_games = recommendations.head(5)[['title', 'current_price', 'avg_score', 'recommendation_score']]
            st.dataframe(value_games)
        else:
            st.write("No recommendations to display for Best Value.")


        st.write("Most Likely to be Discounted Soon:")
        # Ensure 'discount_probability' exists before sorting
        if 'discount_probability' in recommendations.columns:
            # --- Debug Start ---
            st.write("Debug: Displaying Discounted Soon Games...")
            # --- Debug End ---
            discount_games = recommendations.sort_values('discount_probability', ascending=False).head(5)[
                ['title', 'current_price', 'days_since_last_discount', 'avg_days_between_discounts']]
            st.dataframe(discount_games)
        else:
            # --- Debug Start ---
            st.write("Debug: Discount probability data not available.")
            # --- Debug End ---
            st.write("Discount probability data not available.")

        # New Category: Below Average Price, Above Average Rating
        st.write("Good Deals (Below Avg Price, Above Avg Rating):")
        good_deals_condition = avg_price is not None and avg_overall_score is not None and 'avg_score' in recommendations.columns and 'current_price' in recommendations.columns
        # --- Debug Start ---
        st.write(f"Debug: Good Deals condition check: avg_price={avg_price}, avg_overall_score={avg_overall_score}, has 'avg_score'={'avg_score' in recommendations.columns}, has 'current_price'={'current_price' in recommendations.columns} -> {good_deals_condition}")
        # --- Debug End ---
        if good_deals_condition:
            good_deals = recommendations[
                (recommendations['current_price'] < avg_price) &
                (recommendations['avg_score'] > avg_overall_score)
            ].sort_values('recommendation_score', ascending=False).head(5) # Sort by recommendation score

            if not good_deals.empty:
                # --- Debug Start ---
                st.write("Debug: Displaying Good Deals Games...")
                # --- Debug End ---
                st.dataframe(good_deals[['title', 'current_price', 'avg_score', 'recommendation_score']])
            else:
                st.write("No games found matching this criteria.")
        else:
            st.write("Cannot calculate this category due to missing average price or score data.")


        st.write("Highest Rated Games:")
        # Ensure 'avg_score' exists before sorting
        if 'avg_score' in recommendations.columns:
             # --- Debug Start ---
             st.write("Debug: Displaying Highest Rated Games...")
             # --- Debug End ---
             top_rated = recommendations.sort_values('avg_score', ascending=False).head(5)[
                ['title', 'current_price', 'metascore', 'openscore', 'steam_score', 'avg_score']]
             st.dataframe(top_rated)
        else:
             # --- Debug Start ---
             st.write("Debug: Average score data not available for Highest Rated.")
             # --- Debug End ---
             st.write("Average score data not available.")


def main():
    st.title('Game Deals Analyzer')
    wishlist_url = 'https://www.dekudeals.com/wishlist/8byr34kdnr'

    # --- Attempt initial load from DB ---
    initial_data_loaded = False
    if 'processed_data' not in st.session_state: # Only try initial load once per session
        try:
            conn = sqlite3.connect(DATABASE_FILE)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM games")
            count = cursor.fetchone()[0]
            conn.close()
            if count > 0:
                st.write("Found existing data in database, attempting initial load...")
                logging.info("Found existing data in database, attempting initial load...")
                # Use get_game_data with force_refresh=False to prioritize DB
                initial_df = get_game_data(wishlist_url, force_refresh=False)
                if initial_df is not None and not initial_df.empty:
                    st.session_state.raw_data = initial_df
                    st.session_state.processed_data = analyze_and_recommend(initial_df.copy())
                    st.success("Initial data loaded and processed from database.")
                    logging.info("Initial data loaded and processed from database.")
                    initial_data_loaded = True
                else:
                    st.warning("Database has entries, but failed to load/process initial data.")
                    logging.warning("Database has entries, but failed to load/process initial data.")
                    # Ensure session state is initialized if initial load fails
                    st.session_state.processed_data = None
                    st.session_state.raw_data = None
            else:
                 st.info("Database is empty. Use processing options to fetch data.")
                 logging.info("Database is empty on initial check.")
                 # Ensure session state is initialized if DB is empty
                 st.session_state.processed_data = None
                 st.session_state.raw_data = None
        except Exception as e:
            st.error(f"Error during initial database check/load: {e}")
            logging.error(f"Error during initial database check/load: {e}")
            # Ensure session state is initialized on error
            st.session_state.processed_data = None
            st.session_state.raw_data = None
    # --- End initial load attempt ---


    # Initialize session state (if not already set by initial load)
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'raw_data' not in st.session_state:
        st.session_state.raw_data = None
    if 'show_processing_options' not in st.session_state:
        # Only hide options initially if data was loaded successfully
        st.session_state.show_processing_options = not initial_data_loaded

    # Main button to reveal processing options
    if st.button("Process Wishlist Data"):
        st.session_state.show_processing_options = not st.session_state.show_processing_options # Toggle visibility

    # Show processing options if the main button has been clicked OR if initial load failed
    if st.session_state.show_processing_options:
        col1, col2 = st.columns(2)
        with col1:
            if st.button("Process using Cache/DB"): # Renamed for clarity
                with st.spinner("Processing using cache/DB..."):
                    # Clear previous results before processing
                    st.session_state.raw_data = None
                    st.session_state.processed_data = None
                    wishlist_df = get_game_data(wishlist_url, force_refresh=False)
                    if wishlist_df is not None and not wishlist_df.empty:
                        recommendations = analyze_and_recommend(wishlist_df.copy())
                        st.session_state.raw_data = wishlist_df
                        st.session_state.processed_data = recommendations
                        st.success("Processing complete using cache/DB.")
                    else:
                        st.error("No games found or error during processing (Cache/DB).")
                        # Ensure state reflects failure
                        st.session_state.raw_data = None
                        st.session_state.processed_data = None
                st.session_state.show_processing_options = False # Hide buttons after processing

        with col2:
            if st.button("Process from Scratch (Refresh Web Data)"):
                 with st.spinner("Processing and refreshing data from web..."):
                    # Clear previous results before processing
                    st.session_state.raw_data = None
                    st.session_state.processed_data = None
                    wishlist_df = get_game_data(wishlist_url, force_refresh=True)
                    if wishlist_df is not None and not wishlist_df.empty:
                        recommendations = analyze_and_recommend(wishlist_df.copy())
                        st.session_state.raw_data = wishlist_df
                        st.session_state.processed_data = recommendations
                        st.success("Processing complete with refreshed data.")
                    else:
                        st.error("No games found or error during processing (Refresh).")
                        # Ensure state reflects failure
                        st.session_state.raw_data = None
                        st.session_state.processed_data = None
                 st.session_state.show_processing_options = False # Hide buttons after processing

    # Display results if data has been processed and exists
    # --- Debug Start ---
    st.write(f"--- Debug: Checking display condition ---")
    st.write(f"Debug: processed_data is None: {st.session_state.processed_data is None}")
    st.write(f"Debug: raw_data is None: {st.session_state.raw_data is None}")
    st.write(f"Debug: show_processing_options: {st.session_state.show_processing_options}")
    # --- Debug End ---

    if st.session_state.processed_data is not None and st.session_state.raw_data is not None:
        st.markdown("---") # Add a separator
        # --- Debug Start ---
        st.write("--- Debug: Calling display_results ---")
        # --- Debug End ---
        display_results(st.session_state.raw_data, st.session_state.processed_data)
    # Optionally, add a message if processing was attempted but failed OR if no initial data and no processing done yet
    elif st.session_state.processed_data is None and not st.session_state.show_processing_options:
         # --- Debug Start ---
         st.write("--- Debug: Displaying 'Processing finished, no data' message ---")
         # --- Debug End ---
         # Check if initial load was attempted and failed vs just no data yet
         if not initial_data_loaded and 'raw_data' in st.session_state and st.session_state.raw_data is None: # Check if state exists but is None
              st.info("No data loaded. Use processing options above.")
         elif initial_data_loaded and st.session_state.processed_data is None: # Initial load happened but failed processing?
              st.warning("Initial data loaded but processing failed or resulted in no recommendations.")
         else: # Covers cases where processing buttons were clicked but failed
              st.info("Processing finished, but no data was generated or found. Cannot display results.")

    elif st.session_state.show_processing_options:
         # --- Debug Start ---
         st.write("--- Debug: Displaying 'Select processing option' message ---")
         # --- Debug End ---
         st.info("Select a processing option above.") # Initial state or after clicking main button
    # Add a case for when options are hidden but data is missing (might overlap with the first elif)
    elif st.session_state.raw_data is None and not st.session_state.show_processing_options:
         # --- Debug Start ---
         st.write("--- Debug: Displaying 'Processing finished, raw data missing' message ---")
         # --- Debug End ---
         # This case might be redundant with the other elif, but kept for debug clarity
         st.info("No data available to display. Use 'Process Wishlist Data' button.")


if __name__ == "__main__":
    main()
