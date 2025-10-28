# Twitter Sentiment Analysis - Python Scripts

## Overview
These Python scripts scrape Twitter data, calculate sentiment statistics, and store results in MS SQL database.

## Files
- **scraper_db.py** - Scrapes tweets from Twitter API → stores in database
- **stats_db.py** - Calculates overall statistics (max/min SDGs, totals)
- **wordcloud_db.py** - Generates word clouds from tweet text
- **keyword_stats_db.py** - Calculates keyword-specific statistics
- **cronjob_runner.py** - Master script that runs all components

## Setup

### 1. Install Dependencies
```bash
pip install pandas numpy sqlalchemy pyodbc requests nltk
python -m nltk.downloader stopwords
```

### 2. Configure Database Connection
Update the connection string in each file:
```python
DB_CONNECTION_STRING = "mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server"
```

### 3. Set Twitter API Credentials
Set environment variable or update in scraper_db.py:
```bash
export TWITTER_BEARER_TOKEN="your_token_here"
```

## Usage

### Manual Run
```bash
python cronjob_runner.py
```

### Schedule with Cron (Daily at 2 AM)
```bash
crontab -e
# Add this line:
0 2 * * * cd /path/to/project/python && python cronjob_runner.py >> logs/cronjob.log 2>&1
```

## Database Tables Required

Your MS SQL database should have these tables:
- Country (Code, NameAr, NameEn, query)
- Topic (TopicId, QueryEn, QueryAr)
- CountryTopic (CountryCode, TopicId, Lang, SinceId)
- RawTweets (id, text, created_at, country_code, topic, lang)
- TweetCount (CountryCode, TopicId, Lang, Week, Count)
- Statistics (CountryCode, Total, SdgTotal, MaxValue, MaxSdg, etc.)
- Wordcloud (CountryCode, TopicId, Lang, IsOverall, WordCloud)
