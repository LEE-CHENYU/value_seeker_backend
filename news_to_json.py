import json
import anthropic
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
import time

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_article_with_claude(article_text, client):
    logging.info("Starting to process article with Claude")

    prompt = """You are a financial news analyzer. Your task is to extract structured information from financial news articles and output it in JSON format. Follow these specific guidelines:

1. Output Structure Required:
{{
    "news_article": {{
        "id": string,            // Create unique based on ticker_date_type
        "title": string,
        "publishedDate": string, // ISO format
        "source": string,
        "url": string,
        "type": string          // Type of news article
    }},
    "company": {{
        "ticker": string,
        "name": string,
        "exchange": string
    }},
    "market_event": {{
        "type": string,         // Main event type
        "key_points": object,   // Key numerical or factual points
        "major_shareholders": [ // If ownership related
            {{
                "name": string,
                "ownership_percentage": number,
                "type": string
            }}
        ]
    }},
    "analysis": {{
        "key_findings": string[],
        "sentiment": string,    // positive, negative, neutral
        "risk_factors": string[]
    }},
    "event_classification": {{
        "primary_type": string, // Corporate Governance, Financial, Product, Market
        "sub_type": string,     // More specific classification
        "severity": number,     // 1-5 scale
        "confidence": number,   // 0-1 scale
        "impact_duration": string // SHORT_TERM, MEDIUM_TERM, LONG_TERM
    }}
}}

Please process the following news article and output the JSON according to these specifications:

{article_text}

Important: Only output the JSON structure with no additional explanation or commentary. Ensure the JSON is valid and properly formatted."""

    logging.info("Sending request to Claude")
    max_retries = 5
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            message = client.messages.create(
                model="claude-3-opus-20240229",
                max_tokens=4000,
                temperature=0,
                messages=[
                    {
                        "role": "user",
                        "content": prompt.format(article_text=article_text)
                    }
                ]
            )
            logging.info("Received response from Claude")

            try:
                parsed_response = json.loads(message.content[0].text)
                logging.info("Successfully parsed Claude's response as JSON")
                return parsed_response
            except json.JSONDecodeError:
                logging.error("Failed to parse Claude's response as JSON")
                return {"error": "Failed to parse Claude's response as JSON"}

        except anthropic.RateLimitError as e:
            if attempt < max_retries - 1:
                logging.warning(f"Rate limit exceeded. Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error("Max retries reached. Unable to process article.")
                return {"error": "Rate limit exceeded"}

        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            return {"error": str(e)}

def process_articles_batch(articles):
    """
    Process a batch of articles and save results to JSON
    
    Args:
        articles: List of dicts containing article info including 'content' field
    """
    logging.info(f"Starting to process batch of {len(articles)} articles")
    results = []
    
    api_key = os.getenv('ANTHROPIC_API_KEY')
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY environment variable is not set")
        
    client = anthropic.Anthropic(api_key=api_key)
    logging.info("Anthropic client initialized")
    
    for i, article in enumerate(articles):
        logging.info(f"Processing article {i+1}/{len(articles)}")
        if article.get('content'):
            processed = process_article_with_claude(article['content'], client)
            results.append(processed)
        else:
            logging.warning(f"Article {i+1} has no content, skipping")
        
        # Add a delay between requests to avoid rate limiting
        time.sleep(3)  # Adjust this value as needed
            
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'processed_articles_{timestamp}.json'
    
    logging.info(f"Saving results to {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
        
    logging.info("Batch processing completed")
    return output_file

if __name__ == "__main__":
    logging.info("Script started")
    try:
        with open('scraped_articles_results.json', 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        logging.info("Successfully loaded scraped_articles_results.json")
        
        # Process only successfully scraped articles
        successful_articles = scraped_data['successful_articles']
        logging.info(f"Found {len(successful_articles)} successful articles to process")
        output_file = process_articles_batch(successful_articles)
        logging.info(f"Processing completed. Results saved to {output_file}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    logging.info("Script finished")