"""
Twitter Sentiment Analysis Cronjob Runner
==========================================
Master script that orchestrates the entire data pipeline.

Execution Order:
1. Scrape tweets from Twitter API (scraper_db.py)
2. Calculate statistics (stats_db.py)
3. Generate word clouds (wordcloud_db.py)
4. Calculate keyword statistics (keyword_stats_db.py)

Usage:
    python cronjob_runner.py

Schedule with cron (example - runs daily at 2 AM):
    0 2 * * * cd /path/to/project && python cronjob_runner.py >> logs/cronjob.log 2>&1
"""

import logging
import sys
from datetime import datetime

# Import all processing modules
from scraper_db import runner as scraper_runner
from stats_db import runner as stats_runner
from wordcloud_db import runner as wordcloud_runner
from keyword_stats_db import runner as keyword_stats_runner

# Configure logging
logging.basicConfig(
    filename='cronjob_runner.log',
    encoding='utf-8',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)


def main():
    """
    Main execution function that runs the entire pipeline.
    
    Process:
        1. Scrape new tweets from Twitter API
        2. Calculate overall statistics
        3. Generate word clouds
        4. Calculate keyword-specific statistics
        
    Each step is wrapped in try-except to ensure pipeline continues
    even if one step fails.
    """
    start_time = datetime.now()
    logging.info("="*60)
    logging.info(f"CRONJOB STARTED at {start_time}")
    logging.info("="*60)
    
    try:
        # Step 1: Scrape tweets
        logging.info("STEP 1/4: Scraping tweets from Twitter API")
        scraper_runner()
        logging.info("✓ Scraping completed successfully")
        
    except Exception as e:
        logging.error(f"✗ Scraping failed: {e}")
        # Continue to next steps even if scraping fails
    
    try:
        # Step 2: Calculate statistics
        logging.info("STEP 2/4: Calculating statistics")
        stats_runner()
        logging.info("✓ Statistics calculation completed")
        
    except Exception as e:
        logging.error(f"✗ Statistics calculation failed: {e}")
    
    try:
        # Step 3: Generate word clouds
        logging.info("STEP 3/4: Generating word clouds")
        wordcloud_runner()
        logging.info("✓ Word cloud generation completed")
        
    except Exception as e:
        logging.error(f"✗ Word cloud generation failed: {e}")
    
    try:
        # Step 4: Calculate keyword statistics
        logging.info("STEP 4/4: Calculating keyword statistics")
        keyword_stats_runner()
        logging.info("✓ Keyword statistics completed")
        
    except Exception as e:
        logging.error(f"✗ Keyword statistics failed: {e}")
    
    # Log completion
    end_time = datetime.now()
    duration = end_time - start_time
    logging.info("="*60)
    logging.info(f"CRONJOB COMPLETED at {end_time}")
    logging.info(f"Total duration: {duration}")
    logging.info("="*60)
    
    print(f"Cronjob completed in {duration}")


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        logging.critical(f"CRITICAL ERROR in cronjob: {e}")
        sys.exit(1)
