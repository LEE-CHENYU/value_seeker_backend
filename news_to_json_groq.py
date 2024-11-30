import json
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
import time
from groq import Groq
import re

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

def process_article_with_gemini(article_text):
    logging.info("Starting to process article with Groq")

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
        "key_points": string[],   // Key numerical or factual points
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

Important: Only output the JSON structure with no additional explanation or commentary. Ensure the JSON is valid and properly formatted.

Important formatting rules:
1. All string values must be in double quotes
2. All object keys must be in double quotes
3. Numbers with units or symbols must be quoted strings
4. Pure numbers without units can be unquoted
5. No comments or extra text
6. Use proper JSON syntax with colons and commas
7. All key_points entries must have values (either true for text or quoted strings for numbers with units)
8. The JSON output must start with {{ and end with }}, with no markdown markers before or after

Process this article:
{article_text}"""

    logging.info("Sending request to Groq")
    max_retries = 5
    retry_delay = 1

    for attempt in range(max_retries):
        try:
            chat_completion = client.chat.completions.create(
                messages=[
                    {
                        "role": "user",
                        "content": prompt.format(article_text=article_text),
                    }
                ],
                model="llama-3.1-8b-instant",
                stream=False,
            )

            logging.info("Received response from Groq")
            json_content = chat_completion.choices[0].message.content
            
            # More aggressive cleaning to remove any text before the JSON
            # Find the first occurrence of '{'
            start_idx = json_content.find('{')
            if start_idx == -1:
                raise json.JSONDecodeError("No JSON object found", json_content, 0)
            
            # Find the last occurrence of '}'
            end_idx = json_content.rfind('}')
            if end_idx == -1:
                raise json.JSONDecodeError("No JSON object found", json_content, 0)
            
            # Extract only the JSON part
            json_content = json_content[start_idx:end_idx + 1]
            
            # Remove any control characters while preserving newlines
            json_content = ''.join(char if char.isprintable() or char in '\n\r' else '' for char in json_content)
            
            # Log the cleaned content for debugging
            logging.debug(f"Cleaned JSON content: {json_content}")
            
            try:
                parsed_json = json.loads(json_content)
                logging.info("Successfully parsed Groq's response as JSON")
                
                # Save the parsed JSON to a file
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f'processed_article_{timestamp}.json'
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(parsed_json, f, indent=2, ensure_ascii=False)
                logging.info(f"Saved processed article to {output_file}")
                
                return parsed_json
            except json.JSONDecodeError as e:
                logging.warning(f"Initial parsing failed: {e}, attempting more aggressive cleaning")
                # Remove all whitespace and try again
                json_content = re.sub(r'\s+', '', json_content)
                parsed_json = json.loads(json_content)
                
                # Save the parsed JSON to a file
                timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
                output_file = f'processed_article_{timestamp}.json'
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(parsed_json, f, indent=2, ensure_ascii=False)
                logging.info(f"Saved processed article to {output_file}")
                
                return parsed_json

        except Exception as e:
            logging.error(f"Failed to process article: {str(e)}")
            logging.error(f"Raw response: {chat_completion.choices[0].message.content}")
            return None

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
            if processed is None or 'error' in processed:
                failures.append({
                    'article_index': i,
                    'error': processed.get('error') if processed else 'Unknown error',
                    'raw_response': processed.get('raw_response') if processed else None,
                    'parse_error': processed.get('parse_error') if processed else None
                })
                logging.error(f"Failed to process article {i+1}. Error: {processed.get('error') if processed else 'Unknown error'}")
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
    output_file = f'processed_articles_batch_{timestamp}.json'
    failures_file = f'processing_failures_{timestamp}.json'
    
    logging.info(f"Saving batch results to {output_file}")
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