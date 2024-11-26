import json
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
import time
import google.generativeai as genai
import re

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_article_with_gemini(article_text):
    logging.info("Starting to process article with Gemini")

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

    logging.info("Sending request to Gemini")
    max_retries = 5
    retry_delay = 1

    genai.configure(api_key=os.environ["GEMINI_API_KEY"])

    generation_config = {
        "temperature": 0,
        "top_p": 0.95,
        "top_k": 40,
        "max_output_tokens": 8192,
        "response_mime_type": "text/plain",
    }

    model = genai.GenerativeModel(
        model_name="gemini-1.5-flash",
        generation_config=generation_config,
    )

    chat_session = model.start_chat(history=[])

    for attempt in range(max_retries):
        try:
            response = chat_session.send_message(prompt.format(article_text=article_text))
            logging.info("Received response from Gemini")

            try:
                # Remove ```json and ``` from the response
                json_content = re.sub(r'^```json\s*|\s*```$', '', response.text.strip())
                parsed_response = json.loads(json_content)
                logging.info("Successfully parsed Gemini's response as JSON")
                return parsed_response
            except json.JSONDecodeError as e:
                logging.error("Failed to parse Gemini's response as JSON")
                logging.error(f"Raw response: {response.text}")
                logging.error(f"JSON parse error: {str(e)}")
                return {
                    "error": "Failed to parse Gemini's response as JSON",
                    "raw_response": response.text,
                    "parse_error": str(e)
                }

        except Exception as e:
            if attempt < max_retries - 1:
                logging.warning(f"Request failed. Retrying in {retry_delay} seconds... Error: {str(e)}")
                time.sleep(retry_delay)
                retry_delay *= 2  # Exponential backoff
            else:
                logging.error(f"Max retries reached. Unable to process article. Error: {str(e)}")
                return {"error": f"Max retries reached. Error: {str(e)}"}

def process_articles_batch(articles):
    """
    Process a batch of articles and save results to JSON
    
    Args:
        articles: List of dicts containing article info including 'content' field
    """
    logging.info(f"Starting to process batch of {len(articles)} articles")
    results = []
    failures = []
    
    for i, article in enumerate(articles):
        logging.info(f"Processing article {i+1}/{len(articles)}")
        if article.get('content'):
            processed = process_article_with_gemini(article['content'])
            if 'error' in processed:
                failures.append({
                    'article_index': i,
                    'error': processed['error'],
                    'raw_response': processed.get('raw_response'),
                    'parse_error': processed.get('parse_error')
                })
                logging.error(f"Failed to process article {i+1}. Error: {processed['error']}")
            else:
                results.append(processed)
        else:
            failures.append({
                'article_index': i,
                'error': 'Article has no content'
            })
            logging.warning(f"Article {i+1} has no content, skipping")
        
        # Add a delay between requests to avoid rate limiting
        time.sleep(3)  # Adjust this value as needed
            
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'processed_articles_{timestamp}.json'
    failures_file = f'processing_failures_{timestamp}.json'
    
    logging.info(f"Saving results to {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(results, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Saving failures to {failures_file}")
    with open(failures_file, 'w', encoding='utf-8') as f:
        json.dump(failures, f, indent=2, ensure_ascii=False)
        
    logging.info("Batch processing completed")
    return output_file, failures_file

if __name__ == "__main__":
    logging.info("Script started")
    try:
        with open('scraped_articles_results.json', 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        logging.info("Successfully loaded scraped_articles_results.json")
        
        # Process only successfully scraped articles
        successful_articles = scraped_data['successful_articles']
        logging.info(f"Found {len(successful_articles)} successful articles to process")
        output_file, failures_file = process_articles_batch(successful_articles)
        logging.info(f"Processing completed. Results saved to {output_file}")
        logging.info(f"Failures logged to {failures_file}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    logging.info("Script finished")