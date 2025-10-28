"""
Keyword Statistics Calculator with MS SQL Database Integration
===============================================================
Calculates statistics based on specific keywords/phrases in tweets.

Different from stats.py which calculates overall statistics, this module:
- Analyzes specific keywords within SDG topics
- Calculates sentiment for each keyword
- Identifies most/least discussed keywords
- Tracks keyword-specific positive/negative sentiment

Database Tables Used:
- RawTweets: Read tweet text and sentiment
- ProcessedTweets: Read processed sentiment data
- KeywordStatistics: Write keyword-specific statistics
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json
import logging

# Configure logging
logging.basicConfig(
    filename='keyword_stats.log',
    encoding='utf-8',
    level=logging.INFO
)

# Database connection - UPDATE WITH YOUR CREDENTIALS  
DB_CONNECTION_STRING = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DB_CONNECTION_STRING)


def get_stats(keywords, country_code):
    """
    Calculate statistics for specific keywords within a country.
    
    Args:
        keywords (list): List of keywords/phrases to analyze
        country_code (str): Country code to filter tweets
        
    Returns:
        dict: Statistics containing:
            - total: Total tweets analyzed
            - sdgTotal: Total SDG-related tweets
            - max: {value, sdg} - Most discussed keyword
            - min: {value, sdg} - Least discussed keyword  
            - max_pos: Keyword with highest positive sentiment
            - max_neg: Keyword with highest negative sentiment
            
    Process:
        1. Query all processed tweets for this country
        2. For each keyword, count matching tweets
        3. Calculate sentiment percentages for each keyword
        4. Find max/min counts and sentiment extremes
    """
    logging.info(f"Calculating keyword stats for {country_code}")
    
    # Query processed tweets with sentiment from database
    query = f"""
        SELECT text, sentiment, topic
        FROM ProcessedTweets  
        WHERE country_code = '{country_code}'
    """
    
    tweets_df = pd.read_sql_query(query, engine)
    
    if len(tweets_df) == 0:
        logging.warning(f"No processed tweets found for {country_code}")
        return {
            'stats': {
                'total': '0',
                'sdgTotal': '0',
                'max': {'value': '0', 'sdg': ''},
                'min': {'value': '0', 'sdg': ''},
                'max_pos': '',
                'max_neg': ''
            }
        }
    
    # Initialize tracking variables
    max_count = 0
    max_keyword = ''
    min_count = float('inf')
    min_keyword = ''
    sdg_total = 0
    total = len(tweets_df)
    
    # Sentiment tracking
    max_neg_pct = 0
    neg_keyword = ''
    max_pos_pct = 0
    pos_keyword = ''
    
    # Analyze each keyword
    for keyword in keywords:
        pos = 0
        neg = 0
        keyword_count = 0
        
        # Convert keyword to regex pattern (handle multi-word keywords)
        # For "climate change", search for tweets containing both "climate" AND "change"
        keyword_parts = keyword.split(' ')
        keyword_pattern = '&'.join(keyword_parts)  # Use & as AND operator
        
        # Find tweets matching this keyword
        # Case-insensitive search for all keyword parts
        matched_tweets = tweets_df[
            tweets_df['text'].str.contains(keyword_pattern, case=False, na=False, regex=False)
        ]
        
        keyword_count = len(matched_tweets)
        sdg_total += keyword_count
        
        # Count sentiment for this keyword
        if keyword_count > 0:
            pos = len(matched_tweets[matched_tweets['sentiment'] == 'positive'])
            neg = len(matched_tweets[matched_tweets['sentiment'] == 'negative'])
            
            # Calculate sentiment percentages
            pos_pct = pos / keyword_count if keyword_count > 0 else 0
            neg_pct = neg / keyword_count if keyword_count > 0 else 0
            
            # Track most positive keyword
            if pos_pct > max_pos_pct:
                max_pos_pct = pos_pct
                pos_keyword = keyword
                logging.debug(f"New max positive: {pos_keyword} ({max_pos_pct:.2%})")
            
            # Track most negative keyword  
            if neg_pct > max_neg_pct:
                max_neg_pct = neg_pct
                neg_keyword = keyword
                logging.debug(f"New max negative: {neg_keyword} ({max_neg_pct:.2%})")
        
        # Track most discussed keyword
        if keyword_count > max_count:
            max_keyword = keyword
            max_count = keyword_count
        
        # Track least discussed keyword (but not zero)
        if keyword_count < min_count and keyword_count > 0:
            min_keyword = keyword
            min_count = keyword_count
    
    # Handle edge case
    if min_count == float('inf'):
        min_count = 0
    
    return {
        'stats': {
            'total': str(total),
            'sdgTotal': str(sdg_total),
            'max': {
                'value': str(max_count),
                'sdg': max_keyword
            },
            'min': {
                'value': str(min_count),
                'sdg': min_keyword
            },
            'max_pos': pos_keyword,
            'max_neg': neg_keyword
        }
    }


def export_stats(keywords, country_code):
    """
    Calculate keyword statistics and store in database.
    
    Args:
        keywords (list): Keywords to analyze
        country_code (str): Country code
        
    Process:
        1. Calculate stats using get_stats()
        2. Format as JSON
        3. Store in KeywordStatistics table
    """
    logging.info(f"Exporting keyword stats for {country_code}")
    
    try:
        # Calculate statistics
        stats = get_stats(keywords, country_code)
        
        # Prepare for database insertion
        data = {
            'CountryCode': country_code,
            'Statistics': json.dumps(stats, ensure_ascii=False)
        }
        
        # Delete existing stats
        with engine.connect() as conn:
            conn.execute(f"DELETE FROM KeywordStatistics WHERE CountryCode = '{country_code}'")
        
        # Insert new stats
        pd.DataFrame([data]).to_sql(
            'KeywordStatistics',
            engine,
            if_exists='append',
            index=False
        )
        
        logging.info(f"Successfully exported keyword stats for {country_code}")
        
    except Exception as e:
        logging.error(f"Error exporting keyword stats for {country_code}: {e}")
        raise


def runner():
    """
    Main runner that calculates keyword statistics for all countries.
    
    Process:
        1. Read countries from database
        2. Read topics and extract keywords from queries
        3. Calculate keyword stats for each country
        
    Note: Keywords are extracted from the topic queries (ar_query and en_query columns)
    """
    logging.info("=== Starting keyword statistics calculation ===")
    
    try:
        # Read configuration
        countries_df = pd.read_sql_table('Country', engine)
        topics_df = pd.read_sql_table('Topic', engine)
        
        # Extract keywords from queries
        # Parse the query strings to get individual keywords
        queries = np.array(topics_df['QueryAr'])  # Using Arabic queries as example
        keywords = []
        
        for query in queries:
            # Parse query format: "(keyword1 OR keyword2) AND (keyword3)"
            # Remove parentheses and quotes, split by OR
            cleaned = query.replace('(', '').replace(')', '').replace('"', '')
            parts = [q.strip() for q in cleaned.split('OR')]
            keywords.extend(parts)
        
        # Remove duplicates
        keywords = list(set(keywords))
        
        # Calculate stats for each country
        for _, country in countries_df.iterrows():
            country_code = country['Code']
            export_stats(keywords, country_code)
        
        logging.info("=== Keyword statistics calculation completed ===")
        
    except Exception as e:
        logging.error(f"Error in keyword statistics runner: {e}")
        raise


if __name__ == '__main__':
    runner()
