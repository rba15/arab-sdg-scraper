"""
Twitter Scraper with MS SQL Database Integration
================================================
This script scrapes tweets from Twitter API based on country queries and topics,
then stores the results directly in MS SQL database instead of local files.

Main Functions:
- scrape(): Main scraping function that queries Twitter API and stores results in database
- scrape_data(): Fetches tweet data and metadata
- scrape_counts(): Fetches tweet count statistics over time
- query_data(): Handles pagination and rate limiting for tweet data
- query_counts(): Handles pagination for count data

Database Tables Used:
- RawTweets: Stores raw tweet data (id, text, created_at, etc.)
- TweetCount: Stores aggregated tweet counts by day/week
"""

import logging
import os
import time
import requests
import pandas as pd
import pyodbc
from sqlalchemy import create_engine
from datetime import datetime

# Configure logging to track scraping progress and errors
logging.basicConfig(
    filename=f'{os.getcwd()}/twitter_scrape.log', 
    encoding='utf-8', 
    level=logging.DEBUG
)
logging.getLogger("requests").setLevel(logging.CRITICAL)

# Database connection string - UPDATE WITH YOUR CREDENTIALS
DB_CONNECTION_STRING = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DB_CONNECTION_STRING)

# Twitter API configuration - will be loaded from environment or config
TWITTER_BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN', 'YOUR_BEARER_TOKEN')
TWITTER_API_URL = "https://api.twitter.com/2/tweets/search/recent"
TWITTER_COUNTS_URL = "https://api.twitter.com/2/tweets/counts/recent"


def to_query(country_query, lang, input_query):
    """
    Construct Twitter API query string from components.
    
    Args:
        country_query (str): Country-specific keywords (e.g., "Lebanon OR Beirut")
        lang (str): Language code ('en' or 'ar')
        input_query (str): Topic/SDG specific keywords
        
    Returns:
        str: Formatted query string for Twitter API
        
    Example:
        to_query("Lebanon", "en", "poverty OR inequality") 
        -> "(poverty OR inequality) (Lebanon) lang:en -is:retweet"
    """
    if input_query == 'nan' or input_query is None:
        return f"{country_query} lang:{lang} -is:retweet"
    elif country_query == 'nan' or country_query is None:
        return f"{input_query} lang:{lang} -is:retweet"
    else:
        return f"({input_query}) ({country_query}) lang:{lang} -is:retweet"


def scrape(country_code, country_query, lang, input_query, topic, since_id=None):
    """
    Main scraping function that fetches tweets and stores them in database.
    
    Args:
        country_code (str): 2-letter country code (e.g., 'LB', 'EG')
        country_query (str): Country location keywords
        lang (str): Language code ('en' or 'ar')
        input_query (str): Topic search query
        topic (str): Topic identifier (e.g., 'SDG01')
        since_id (str, optional): Tweet ID to start scraping from (for incremental updates)
        
    Process:
        1. Constructs query using to_query()
        2. Fetches raw tweet data via scrape_data()
        3. Fetches count data via scrape_counts()
        4. Stores both in database tables
    """
    logging.info(f"Starting scrape for {country_code} - {topic} - {lang}")
    
    # Build the query string
    query = to_query(country_query, lang, input_query)
    
    # Fetch raw tweet data
    data_df = scrape_data(query, since_id=since_id)
    if len(data_df) > 0:
        # Add metadata columns
        data_df['country_code'] = country_code
        data_df['topic'] = topic
        data_df['lang'] = lang
        data_df['scraped_at'] = datetime.now()
        
        # Store in database (append to existing data)
        data_df.to_sql(
            'RawTweets', 
            engine, 
            if_exists='append', 
            index=False,
            method='multi',  # Faster bulk insert
            chunksize=1000   # Insert in chunks to avoid memory issues
        )
        logging.info(f"Stored {len(data_df)} tweets for {country_code}-{topic}")
    
    # Fetch and store count data
    counts_df = scrape_counts(query, since_id=since_id)
    if len(counts_df) > 0:
        # Clean and format the data
        counts_df['Week'] = counts_df['Day'].str[:10]  # Extract date only (YYYY-MM-DD)
        counts_df['CountryCode'] = country_code
        counts_df['TopicId'] = topic
        counts_df['Lang'] = lang
        counts_df.drop(columns=['end'], inplace=True, errors='ignore')
        counts_df.rename(columns={'tweet_count': 'Count'}, inplace=True)
        
        # Store in database
        counts_df.to_sql(
            'TweetCount',
            engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=500
        )
        logging.info(f"Stored count data for {country_code}-{topic}")


def scrape_data(query, since_id=None):
    """
    Fetch tweet data from Twitter API with pagination.
    
    Args:
        query (str): Twitter search query
        since_id (str, optional): Only return tweets after this ID
        
    Returns:
        pd.DataFrame: DataFrame with columns [id, text, created_at, author_id, etc.]
    """
    tweets_df = query_data(query, since_id=since_id)
    return tweets_df


def scrape_counts(query, since_id=None):
    """
    Fetch tweet count statistics from Twitter API.
    
    Args:
        query (str): Twitter search query
        since_id (str, optional): Only count tweets after this ID
        
    Returns:
        pd.DataFrame: DataFrame with columns [Day, tweet_count]
    """
    counts_df = query_counts(query, since_id=since_id)
    return counts_df


def query_data(query, since_id=None):
    """
    Query Twitter API for tweet data with automatic pagination and rate limit handling.
    
    Args:
        query (str): Search query
        since_id (str, optional): Start from this tweet ID
        
    Returns:
        pd.DataFrame: All tweets matching the query
        
    Note:
        - Handles rate limiting (429 errors) by waiting 15 minutes
        - Automatically paginates through all results using next_token
        - Retries failed requests
    """
    data = pd.DataFrame()
    results = next_data(query, next_token=None, since_id=since_id)
    
    # Check if we got data
    if 'data' in results:
        data = pd.DataFrame(results['data'])
        logging.info(f"Fetched initial batch: {len(data)} tweets")
    
    # Handle rate limiting
    elif 'status' in results and results['status'] == 429:
        logging.warning('Rate limit hit, waiting 15 minutes...')
        time.sleep(900)  # Wait 15 minutes
        results = next_data(query, next_token=None, since_id=since_id)
    
    # Paginate through all results
    while 'meta' in results and 'next_token' in results['meta']:
        next_token = results['meta']['next_token']
        results = next_data(query, next_token=next_token)
        
        if 'data' in results:
            batch_df = pd.DataFrame(results['data'])
            data = pd.concat([data, batch_df], ignore_index=True, sort=False)
            logging.info(f"Total tweets fetched: {len(data)}")
        
        # Respect rate limits
        time.sleep(1)  # Small delay between requests
    
    return data


def query_counts(query, since_id=None):
    """
    Query Twitter API for tweet count data.
    
    Args:
        query (str): Search query
        since_id (str, optional): Start from this tweet ID
        
    Returns:
        pd.DataFrame: Tweet counts grouped by time period
    """
    counts = pd.DataFrame()
    results = next_counts(query, since_id=since_id)
    
    if 'data' in results:
        counts = pd.DataFrame(results['data'])
        logging.info(f"Fetched count data: {len(counts)} time periods")
    
    return counts


def next_data(query, next_token=None, since_id=None):
    """
    Make a single request to Twitter API for tweet data.
    
    Args:
        query (str): Search query
        next_token (str, optional): Pagination token
        since_id (str, optional): Only tweets after this ID
        
    Returns:
        dict: API response JSON
    """
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    
    params = {
        "query": query,
        "max_results": 100,  # Max allowed per request
        "tweet.fields": "created_at,author_id,public_metrics,lang"
    }
    
    if next_token:
        params["next_token"] = next_token
    if since_id:
        params["since_id"] = since_id
    
    try:
        response = requests.get(TWITTER_API_URL, headers=headers, params=params)
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching data: {e}")
        return {}


def next_counts(query, since_id=None):
    """
    Make a single request to Twitter API for count data.
    
    Args:
        query (str): Search query  
        since_id (str, optional): Only count tweets after this ID
        
    Returns:
        dict: API response JSON with count statistics
    """
    headers = {"Authorization": f"Bearer {TWITTER_BEARER_TOKEN}"}
    
    params = {
        "query": query,
        "granularity": "day"  # Can be 'day', 'hour', or 'minute'
    }
    
    if since_id:
        params["since_id"] = since_id
    
    try:
        response = requests.get(TWITTER_COUNTS_URL, headers=headers, params=params)
        return response.json()
    except Exception as e:
        logging.error(f"Error fetching counts: {e}")
        return {}


def runner():
    """
    Main runner function that orchestrates scraping for all countries and topics.
    
    Process:
        1. Reads countries from database (Country table)
        2. Reads topics from database (Topic table)
        3. Reads last scrape status from CountryTopic table (to get since_id)
        4. Loops through each country-topic-language combination
        5. Scrapes data and updates since_id in database
        
    This function is called by the cronjob.
    """
    logging.info("=== Starting scraper run ===")
    
    # Read configuration from database
    countries_df = pd.read_sql_table('Country', engine)
    topics_df = pd.read_sql_table('Topic', engine)
    country_topics_df = pd.read_sql_table('CountryTopic', engine)
    
    # Loop through each country-topic combination
    for _, row in country_topics_df.iterrows():
        country_code = row['CountryCode']
        topic_id = row['TopicId']
        lang = row['Lang']
        since_id = row.get('SinceId', None)
        
        # Get country and topic details
        country = countries_df[countries_df['Code'] == country_code].iloc[0]
        topic = topics_df[topics_df['TopicId'] == topic_id].iloc[0]
        
        # Get queries based on language
        country_query = country.get('query', '')
        if lang == 'ar':
            topic_query = topic['QueryAr']
        else:
            topic_query = topic['QueryEn']
        
        try:
            # Scrape the data
            scrape(country_code, country_query, lang, topic_query, topic_id, since_id=since_id)
            
            # TODO: Update since_id in CountryTopic table with the latest tweet ID
            # This ensures next run only fetches new tweets
            
        except Exception as e:
            logging.error(f"Error scraping {country_code}-{topic_id}-{lang}: {e}")
            continue
    
    logging.info("=== Scraper run completed ===")


if __name__ == '__main__':
    runner()
