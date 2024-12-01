import json
from datetime import datetime, timedelta
from news_gdelt import GDELTNewsFetcher
from download_news_from_url import NewsDownloader
import asyncio
import os
from json_indexer import NewsIndexer
from k_line import KLine
import logging
import subprocess
import news_to_json_gm_single
import filter
import glob

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

keywords = ["Apple", "AAPL"]
output_folder = f'gm_single_{keywords[1]}'
if not os.path.exists(output_folder):
    os.makedirs(output_folder)

async def process_inflection_point(inflection_date, price):
    logger.info(f"Processing inflection point for date: {inflection_date}")
    
    # Convert date string to datetime
    date = datetime.strptime(inflection_date, '%Y-%m-%d')
    logger.info(f"Converted inflection date to datetime: {date}")
    
    # Add date validation before proceeding
    min_allowed_date = datetime(2016, 1, 1)
    if date < min_allowed_date:
        logger.warning(f"Skipping date {inflection_date} - GDELT only supports dates from 2015 onwards")
        return
    
    # Calculate date range
    start_date = (date - timedelta(days=14)).strftime('%Y-%m-%d')
    end_date = (date + timedelta(days=7)).strftime('%Y-%m-%d')
    logger.info(f"Calculated date range: {start_date} to {end_date}")
    
    # Initialize GDELT fetcher
    logger.info("Initializing GDELT fetcher")
    fetcher = GDELTNewsFetcher(
        keywords=["Occidental Petroleum", "OXY"],
        themes=["ECON_STOCKMARKET", "ECON_TRADE"],
        domains=["cnbc.com", "businessinsider.com", "seekingalpha.com", 
                "investing.com", "finance.yahoo.com", "marketwatch.com"],
        start_date=start_date,
        end_date=end_date,
        num_articles=20
    )
    
    # Fetch and download articles
    logger.info("Fetching and downloading articles")
    await fetcher.run()
    
    # Download news content
    logger.info("Downloading news content")
    downloader = NewsDownloader(
        input_file='gdelt_articles_with_content.json',
        output_file=f'scraped_articles_{inflection_date}.json'
    )
    await downloader.run()
    
    # Process news articles using news_to_json_gm_single
    logger.info("Processing news articles with Gemini")
    news_output_folder = news_to_json_gm_single.main(f'scraped_articles_{inflection_date}.json')
    
    # Apply filter.py
    logger.info("Applying filter to processed articles")
    filter.filtered_articles(str(news_output_folder), output_folder)
    
    logger.info(f"Completed processing for inflection point: {inflection_date}")

async def main():
    logger.info("Starting main process")
    
    # Run k_line.py to generate inflection_points_OXY.json
    logger.info("Running k_line.py to generate inflection points")
    kline = KLine('OXY', show_chart=False)
    kline.fetch_data()
    kline.process_data()
    kline.analyze()
    logger.info("K-line analysis completed")
    
    # Load inflection points data
    logger.info("Loading inflection points data")
    with open('inflection_points_OXY.json', 'r') as f:
        inflection_points = json.load(f)
    logger.info(f"Loaded {len(inflection_points)} inflection points")
    
    # Process each inflection point
    logger.info("Processing inflection points")
    for point in inflection_points:
        logger.info(f"Processing inflection point: {point['date']}")
        await process_inflection_point(point['date'], point['price'])
        
    # Only proceed with indexing if we have a valid output folder
    if output_folder:
        
        # Combine all filtered_articles.json files into one
        logger.info("Combining filtered articles from all inflection points")
        
        # Get all filtered_articles.json files from gm_single_* folders
        filtered_files = glob.glob(f'{output_folder}/filtered_articles_*.json')
        
        if filtered_files:
            combined_articles = []
            
            # Read and combine all files
            for file_path in filtered_files:
                logger.info(f"Reading articles from {file_path}")
                try:
                    with open(file_path, 'r') as f:
                        articles = json.load(f)
                        combined_articles.extend(articles)
                except Exception as e:
                    logger.error(f"Error reading {file_path}: {str(e)}")
                    
            # Write combined articles to new file
            output_path = f'{output_folder}/combined_filtered_articles.json'
            logger.info(f"Writing {len(combined_articles)} combined articles to {output_path}")
            try:
                with open(output_path, 'w') as f:
                    json.dump(combined_articles, f, indent=2)
            except Exception as e:
                logger.error(f"Error writing combined articles: {str(e)}")
        else:
            logger.warning("No filtered_articles.json files found to combine")
        
        logger.info("Indexing the news")
        indexer = NewsIndexer(
            f'{output_folder}/combined_filtered_articles.json',
            'inflection_points_OXY.json',
            f'{output_folder}/news_by_inflection_OXY.json'
        )
        indexer.run()
        logger.info("News indexing completed")
    else:
        logger.info("Skipping indexing due to invalid date or missing output")

if __name__ == "__main__":
    logger.info("Script started")
    asyncio.run(main())
    logger.info("Script completed")
