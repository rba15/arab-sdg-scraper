"""
Word Cloud Generator with MS SQL Database Integration
=====================================================
Generates word clouds from tweet text stored in database.

Process:
1. Reads tweets from RawTweets table
2. Cleans and tokenizes text (removes stopwords, URLs, etc.)
3. Counts word frequencies
4. Stores top N words in Wordcloud table

Database Tables Used:
- RawTweets: Read tweet text
- Wordcloud: Write word frequencies

Features:
- Supports both Arabic and English
- Removes language-specific stopwords
- Removes URLs, mentions, and special characters
- Stores top 30 most frequent words per country-topic combination
"""

import re
import numpy as np
import pandas as pd
from nltk.corpus import stopwords
from collections import Counter
from sqlalchemy import create_engine
import json
import logging

# Configure logging
logging.basicConfig(
    filename='wordcloud_generation.log',
    encoding='utf-8',
    level=logging.INFO
)

# Database connection - UPDATE WITH YOUR CREDENTIALS
DB_CONNECTION_STRING = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
engine = create_engine(DB_CONNECTION_STRING)

# Arabic stopwords (common words to exclude)
ar_stopwords = np.array(stopwords.words('arabic'))
ar_stopwords = np.append(ar_stopwords, [
    'تم', 'اذا', 'أنّ', 'و', 'الله', 'مش', 'او', 'عم', 'انو', 'كان', 
    'شو', 'شي', 'يلي', 'الى', 'ان', 'انا', 'بسبب', 'في', 'ما', 'من', 
    'على', 'أن', 'كل', 'شو', 'عم', 'بس', 'ال', 'لا', 'مش', 'بكل', 
    'الان', 'ع', 'ام', 'اما'
])

# English stopwords (common words to exclude)
en_stopwords = np.array(stopwords.words('english'))
en_stopwords = np.append(en_stopwords, [
    'https', 'amp', 'really', 'us', 'every', 'like', 'please', 'still',
    'via', 'get', 'could', 'love', 'one', 'new', 'never', 'even',
    'thing', 'soon', 'try', 'god', 'day', 'retweet', 'gt', 'got',
    'would', 'ur', 'always', 'think', 'means', 'dont', 'go', 'much',
    'sir', 'take', 'everyone', 'everything', 'subscribe', 'yes', 'look'
])


def text_cleanup(text, country, lang):
    """
    Clean and tokenize tweet text for word cloud generation.
    
    Args:
        text (str): Raw tweet text
        country (str): Country name/keywords (will be excluded from word cloud)
        lang (str): Language code ('ar' or 'en')
        
    Returns:
        list: List of cleaned word tokens
        
    Process:
        1. Convert to lowercase (English only)
        2. Remove URLs, mentions, hashtags
        3. Remove special characters (keep only letters)
        4. Tokenize (split into words)
        5. Remove stopwords
        6. Remove country-specific keywords
    """
    
    if lang == 'en':
        # English text processing
        text = text.lower()
        text = text.replace('\n', ' ')
        
        # Remove URLs, mentions, hashtags
        text = re.sub(r'http\S+', '', text)
        text = re.sub(r'@\w+', '', text)
        text = re.sub(r'#\w+', '', text)
        
        # Keep only letters and spaces
        regex = re.compile('[^a-zA-Z\s_]')
        text = regex.sub('', text)
    
    if lang == 'ar':
        # Arabic text processing - extract only Arabic characters
        text = ' '.join(re.findall(r'[\u0600-\u06FF]+', text))
    
    # Tokenize
    tokens = text.split()
    
    # Remove stopwords and country keywords
    if lang == 'ar':
        tokens = [w for w in tokens if w not in ar_stopwords and w not in country]
    else:
        country_lower = country.lower()
        tokens = [w for w in tokens if w not in en_stopwords and w not in country_lower]
    
    return tokens


def get_wordcloud(tweets_df, country, lang, top_n=30):
    """
    Generate word cloud data from tweets DataFrame.
    
    Args:
        tweets_df (pd.DataFrame): DataFrame with 'text' column containing tweets
        country (str): Country name for filtering
        lang (str): Language code
        top_n (int): Number of top words to return (default: 30)
        
    Returns:
        pd.DataFrame: DataFrame with columns [word, count] sorted by count descending
        
    Process:
        1. Concatenate all tweet text
        2. Clean and tokenize using text_cleanup()
        3. Count word frequencies using Counter
        4. Return top N most common words
    """
    
    if len(tweets_df) == 0:
        logging.warning(f"No tweets provided for word cloud")
        return pd.DataFrame(columns=['word', 'count'])
    
    # Combine all tweets into one text
    text = ' '.join(tweet for tweet in tweets_df['text'] if isinstance(tweet, str))
    
    # Clean and tokenize
    tokens = text_cleanup(text, country, lang)
    
    # Count word frequencies
    counter = Counter(tokens)
    most_common = dict(counter.most_common(top_n))
    
    # Convert to DataFrame
    wordcloud = pd.DataFrame.from_dict(most_common, orient='index').reset_index()
    wordcloud.rename(columns={"index": "word", 0: "count"}, inplace=True)
    
    logging.info(f"Generated word cloud with {len(wordcloud)} words")
    
    return wordcloud


def calculate_wordclouds(country_code, topic_id, lang, is_overall=True):
    """
    Calculate word cloud for a specific country-topic-language combination.
    
    Args:
        country_code (str): Country code
        topic_id (str): Topic/SDG identifier
        lang (str): Language code
        is_overall (bool): Whether this is overall wordcloud (vs time-specific)
        
    Process:
        1. Query tweets from database for this combination
        2. Generate word cloud using get_wordcloud()
        3. Store results in Wordcloud table as JSON
    """
    logging.info(f"Calculating word cloud for {country_code}-{topic_id}-{lang}")
    
    try:
        # Query tweets from database
        query = f"""
            SELECT text
            FROM RawTweets
            WHERE country_code = '{country_code}'
              AND topic = '{topic_id}'
              AND lang = '{lang}'
        """
        tweets_df = pd.read_sql_query(query, engine)
        
        if len(tweets_df) == 0:
            logging.warning(f"No tweets found for {country_code}-{topic_id}-{lang}")
            return
        
        # Get country name for stopword filtering
        country_query = pd.read_sql_query(
            f"SELECT query FROM Country WHERE Code = '{country_code}'", 
            engine
        )
        country_name = country_query.iloc[0]['query'] if len(country_query) > 0 else ''
        
        # Generate word cloud
        wordcloud_df = get_wordcloud(tweets_df, country_name, lang)
        
        if len(wordcloud_df) == 0:
            logging.warning(f"No words generated for {country_code}-{topic_id}-{lang}")
            return
        
        # Convert to JSON format for storage
        wordcloud_json = wordcloud_df.to_json(orient='records', force_ascii=False)
        
        # Prepare data for database insertion
        data = {
            'CountryCode': country_code,
            'TopicId': topic_id,
            'Lang': lang,
            'IsOverall': is_overall,
            'WordCloud': wordcloud_json
        }
        
        # Delete existing word cloud if any
        with engine.connect() as conn:
            conn.execute(f"""
                DELETE FROM Wordcloud 
                WHERE CountryCode = '{country_code}' 
                  AND TopicId = '{topic_id}' 
                  AND Lang = '{lang}'
                  AND IsOverall = {1 if is_overall else 0}
            """)
        
        # Insert new word cloud
        pd.DataFrame([data]).to_sql(
            'Wordcloud',
            engine,
            if_exists='append',
            index=False
        )
        
        logging.info(f"Stored word cloud for {country_code}-{topic_id}-{lang}")
        
    except Exception as e:
        logging.error(f"Error calculating word cloud: {e}")
        raise


def runner():
    """
    Main runner that generates word clouds for all country-topic combinations.
    
    Process:
        1. Read all countries from database
        2. Read all topics from database
        3. Generate word clouds for each combination (both languages)
        4. Also generate aggregate word clouds for 'ARAB' region
        
    Called by cronjob after scraping is complete.
    """
    logging.info("=== Starting word cloud generation ===")
    
    try:
        # Read configuration from database
        countries_df = pd.read_sql_table('Country', engine)
        topics_df = pd.read_sql_table('Topic', engine)
        
        # Generate word cloud for each country-topic-language combination
        for _, country in countries_df.iterrows():
            country_code = country['Code']
            
            for _, topic in topics_df.iterrows():
                topic_id = topic['TopicId']
                
                # Generate for both languages
                for lang in ['en', 'ar']:
                    calculate_wordclouds(country_code, topic_id, lang, is_overall=True)
        
        logging.info("=== Word cloud generation completed ===")
        
    except Exception as e:
        logging.error(f"Error in word cloud runner: {e}")
        raise


if __name__ == '__main__':
    runner()
