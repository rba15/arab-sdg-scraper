"""
Statistics Calculator with MS SQL Database Integration
======================================================
Calculates statistics from scraped tweet data stored in MS SQL database.

Statistics Calculated:
1. Count Statistics:
   - Total tweets across all topics
   - Total SDG-related tweets  
   - Most discussed SDG (max count)
   - Least discussed SDG (min count)

2. Sentiment Statistics:
   - SDG with most positive sentiment
   - SDG with most negative sentiment

Database Tables Used:
- TweetCount: Read tweet counts per topic
- SentimentDistribution: Read sentiment breakdowns
- Statistics: Write calculated statistics

Output:
- Stores statistics in Statistics table for each country
- Can be queried by web dashboard
"""

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import json
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    filename='stats_calculation.log',
    encoding='utf-8',
    level=logging.INFO
)

# Database connection - UPDATE WITH YOUR CREDENTIALS
DB_CONNECTION_STRING = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DB_CONNECTION_STRING)


def count_stats(country_code):
    """
    Calculate tweet count statistics for a given country.
    
    Args:
        country_code (str): 2-letter country code (e.g., 'LB', 'EG', 'ARAB')
        
    Returns:
        dict: Statistics containing:
            - total: Total tweets (all topics including SDG0 baseline)
            - sdgTotal: Total SDG-specific tweets
            - max: {value, sdg} - Most discussed SDG
            - min: {value, sdg} - Least discussed SDG
            
    Process:
        1. Query TweetCount table for all topics of this country
        2. Sum up tweet counts per topic
        3. Find max and min (excluding SDG0 which is baseline)
        4. Calculate totals
    """
    logging.info(f"Calculating count stats for {country_code}")
    
    # Query all tweet counts for this country from database
    query = f"""
        SELECT TopicId, SUM(Count) as TotalCount
        FROM TweetCount
        WHERE CountryCode = '{country_code}'
        GROUP BY TopicId
    """
    counts_df = pd.read_sql_query(query, engine)
    
    if len(counts_df) == 0:
        logging.warning(f"No data found for {country_code}")
        return {
            'total': '0',
            'sdgTotal': '0',
            'max': {'value': '0', 'sdg': ''},
            'min': {'value': '0', 'sdg': ''}
        }
    
    max_count = 0
    max_sdg = ''
    min_count = float('inf')
    min_sdg = ''
    sdg_total = 0
    total = 0
    
    # Iterate through each topic's count
    for _, row in counts_df.iterrows():
        topic_id = row['TopicId']
        count = row['TotalCount']
        
        # SDG0 is baseline (all tweets), treat separately
        if topic_id == 'SDG00' or topic_id == 'SDG0':
            total = count
        else:
            # Track SDG-specific tweets
            sdg_total += count
            
            # Track maximum
            if count > max_count:
                max_count = count
                max_sdg = topic_id
            
            # Track minimum (but not zero)
            if count < min_count and count > 0:
                min_count = count
                min_sdg = topic_id
    
    # Handle edge case where no SDGs found
    if min_count == float('inf'):
        min_count = 0
    
    return {
        'total': str(total),
        'sdgTotal': str(sdg_total),
        'max': {
            'value': str(max_count),
            'sdg': max_sdg
        },
        'min': {
            'value': str(min_count),
            'sdg': min_sdg
        }
    }


def sentiment_stats(country_code):
    """
    Calculate sentiment statistics for a given country.
    
    Args:
        country_code (str): 2-letter country code
        
    Returns:
        dict: Statistics containing:
            - max_pos: SDG with highest positive sentiment percentage
            - max_neg: SDG with highest negative sentiment percentage
            
    Process:
        1. Query SentimentDistribution table for all topics
        2. Calculate sentiment percentages (positive/total, negative/total)
        3. Find SDG with highest positive % and highest negative %
    """
    logging.info(f"Calculating sentiment stats for {country_code}")
    
    # Query sentiment distribution from database
    query = f"""
        SELECT TopicId, 
               SUM(NbPositive) as Positive,
               SUM(NbNegative) as Negative, 
               SUM(NbNeutral) as Neutral
        FROM SentimentDistribution
        WHERE CountryCode = '{country_code}' AND IsOverall = 1
        GROUP BY TopicId
    """
    sentiment_df = pd.read_sql_query(query, engine)
    
    if len(sentiment_df) == 0:
        logging.warning(f"No sentiment data found for {country_code}")
        return {
            'max_pos': '',
            'max_neg': ''
        }
    
    max_neg_pct = 0
    neg_sdg = ''
    max_pos_pct = 0
    pos_sdg = ''
    
    # Calculate percentages for each SDG
    for _, row in sentiment_df.iterrows():
        topic_id = row['TopicId']
        pos = row['Positive']
        neg = row['Negative']
        neut = row['Neutral']
        
        # Calculate total tweets with sentiment
        total_tweets = pos + neg + neut
        
        # Skip if no tweets
        if total_tweets == 0:
            continue
        
        # Calculate percentages
        pos_pct = pos / total_tweets
        neg_pct = neg / total_tweets
        
        # Track most positive SDG
        if pos_pct > max_pos_pct:
            max_pos_pct = pos_pct
            pos_sdg = topic_id
        
        # Track most negative SDG
        if neg_pct > max_neg_pct:
            max_neg_pct = neg_pct
            neg_sdg = topic_id
        
        logging.debug(f"{topic_id}: {pos_pct:.2%} positive, {neg_pct:.2%} negative")
    
    return {
        'max_pos': pos_sdg,
        'max_neg': neg_sdg
    }


def export_stats(country_code):
    """
    Calculate all statistics and store in database.
    
    Args:
        country_code (str): Country to calculate stats for
        
    Process:
        1. Calculate count statistics
        2. Calculate sentiment statistics
        3. Combine into single stats object
        4. Store in Statistics table (or update if exists)
    """
    logging.info(f"Exporting stats for {country_code}")
    
    try:
        # Calculate both types of statistics
        counts = count_stats(country_code)
        sentiment = sentiment_stats(country_code)
        
        # Combine into single object
        stats = {
            'CountryCode': country_code,
            'Total': counts['total'],
            'SdgTotal': counts['sdgTotal'],
            'MaxValue': counts['max']['value'],
            'MaxSdg': counts['max']['sdg'],
            'MinValue': counts['min']['value'],
            'MinSdg': counts['min']['sdg'],
            'MaxPosSdg': sentiment['max_pos'],
            'MaxNegSdg': sentiment['max_neg'],
            'CalculatedAt': datetime.now()
        }
        
        # Convert to DataFrame for SQL insertion
        stats_df = pd.DataFrame([stats])
        
        # Store in database (replace if exists)
        # First delete existing stats for this country
        with engine.connect() as conn:
            conn.execute(f"DELETE FROM Statistics WHERE CountryCode = '{country_code}'")
        
        # Insert new stats
        stats_df.to_sql(
            'Statistics',
            engine,
            if_exists='append',
            index=False
        )
        
        logging.info(f"Successfully exported stats for {country_code}")
        
    except Exception as e:
        logging.error(f"Error exporting stats for {country_code}: {e}")
        raise


def runner():
    """
    Main runner that calculates statistics for all countries.
    
    Process:
        1. Read list of countries from database
        2. Calculate and export stats for each country
        3. Also calculate aggregate stats for 'ARAB' region
        
    Called by cronjob after scraping is complete.
    """
    logging.info("=== Starting statistics calculation ===")
    
    try:
        # Read countries from database
        countries_df = pd.read_sql_table('Country', engine)
        
        # Calculate stats for each country
        for _, country in countries_df.iterrows():
            country_code = country['Code']
            export_stats(country_code)
        
        # Calculate aggregate stats for Arab region
        export_stats('ARAB')
        
        logging.info("=== Statistics calculation completed ===")
        
    except Exception as e:
        logging.error(f"Error in statistics runner: {e}")
        raise


if __name__ == '__main__':
    runner()
