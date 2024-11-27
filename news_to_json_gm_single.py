import json
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
import google.generativeai as genai
import re
import atexit
import google.api_core.grpc_helpers
from google.cloud import aiplatform

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_articles_with_gemini(articles):
    logging.info("Starting to process articles with Gemini")

    # Load inflection points
    with open('inflection_points_OXY.json', 'r') as f:
        inflection_points = json.load(f)

    prompt = """You are a financial news analyzer. Your task is to extract structured information from multiple financial news articles and output it in JSON format. Follow these specific guidelines:

1. Output Structure Required:
[
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
    }},
    "inflection_point": {{
        "date": string,
        "price": number,
        "relevance_ranking": number,
        "reasoning": string     // Chain of thought in 140 characters
    }}
  }}
]

Consider these inflection points for OXY stock:
{inflection_points}

Please process the following news articles and output the JSON according to these specifications:

{article_texts}

For each article, identify the most relevant inflection point based on the event classification, specific impact on financials, and overall market sentiment. Rank the relevance of each article to its identified inflection point (1 being most relevant). Include a concise chain of thought (140 characters) explaining the relevance.

Important: Only output the JSON structure with no additional explanation or commentary. Ensure the JSON is valid and properly formatted. The output should be an array of JSON objects, one for each article processed."""

    logging.info("Sending request to Gemini")

    genai.configure(api_key=os.environ["GEMINI_API_KEY"])

    generation_config = {
        "temperature": 0,
        "top_p": 0.95,
        "top_k": 40,
        "max_output_tokens": 8192,
        "response_mime_type": "text/plain",
    }

    model = genai.GenerativeModel(
        model_name="gemini-1.5-pro",
        generation_config=generation_config,
    )

    article_texts = "\n\n---ARTICLE SEPARATOR---\n\n".join([article['content'] for article in articles if article.get('content')])
    
    try:
        response = model.generate_content(prompt.format(
            inflection_points=json.dumps(inflection_points, indent=2),
            article_texts=article_texts
        ))
        logging.info("Received response from Gemini")

        try:
            # Clean up the response text
            json_content = response.text.strip()
            # Remove any markdown code block indicators
            json_content = re.sub(r'^```json\s*|\s*```$', '', json_content)
            # Remove any trailing commas before closing brackets/braces
            json_content = re.sub(r',(\s*[}\]])', r'\1', json_content)
            # Clean up any potential whitespace issues
            json_content = json_content.strip()
            
            try:
                parsed_response = json.loads(json_content)
                logging.info("Successfully parsed Gemini's response as JSON")
                return parsed_response
            except json.JSONDecodeError as e:
                logging.error(f"JSON parse error: {str(e)}")
                logging.error(f"Problematic JSON content: {json_content}")
                logging.error(f"Error position: {e.pos}")
                logging.error(f"Error line number: {e.lineno}")
                logging.error(f"Error column number: {e.colno}")
                logging.error(f"Error message: {e.msg}")
                
                # Log the surrounding context of the error
                error_context = json_content[max(0, e.pos - 50):min(len(json_content), e.pos + 50)]
                logging.error(f"Error context: {error_context}")
                
                # Attempt to identify specific JSON formatting issues
                if "Expecting property name enclosed in double quotes" in str(e):
                    logging.error("Possible missing quotes around a property name")
                elif "Expecting ',' delimiter" in str(e):
                    logging.error("Possible missing comma between elements")
                elif "Expecting value" in str(e):
                    logging.error("Possible missing value for a property")
                
                # Return error information instead of empty list
                return [{
                    "error": "JSON parse error",
                    "error_details": str(e),
                    "error_position": e.pos,
                    "error_line": e.lineno,
                    "error_column": e.colno,
                    "error_context": error_context,
                    "raw_response": json_content
                }]

        except Exception as e:
            logging.error(f"Error cleaning response text: {str(e)}")
            logging.error(f"Raw response: {response.text}")
            return [{
                "error": "Error cleaning response text",
                "error_details": str(e),
                "raw_response": response.text
            }]

    except Exception as e:
        logging.error(f"Error processing articles: {str(e)}")
        return [{
            "error": "Error processing articles",
            "error_details": str(e)
        }]

def save_batch_results(batch_results, batch_number):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'processed_articles_batch_{batch_number}_{timestamp}.json'
    
    logging.info(f"Saving batch {batch_number} results to {output_file}")
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Batch {batch_number} results saved to {output_file}")

def cleanup_grpc():
    try:
        # Force cleanup of gRPC channels
        aiplatform.initializer.global_pool.close()
    except Exception as e:
        logging.warning(f"gRPC cleanup warning: {e}")

def main():
    try:
        # Register cleanup function
        atexit.register(cleanup_grpc)
        
        logging.info("Script started")
        try:
            with open('scraped_articles_results.json', 'r', encoding='utf-8') as f:
                scraped_data = json.load(f)
            logging.info("Successfully loaded scraped_articles_results.json")
            
            successful_articles = scraped_data['successful_articles']
            total_articles = len(successful_articles)
            logging.info(f"Total successful articles: {total_articles}")
            
            batch_size = 100
            all_results = []
            
            for i in range(0, total_articles, batch_size):
                batch = successful_articles[i:i+batch_size]
                batch_number = i//batch_size + 1
                logging.info(f"Processing batch {batch_number} of {(total_articles + batch_size - 1)//batch_size}")
                
                batch_results = process_articles_with_gemini(batch)
                all_results.extend(batch_results)
                
                save_batch_results(batch_results, batch_number)
                
                logging.info(f"Completed processing batch {batch_number}")
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_file = f'processed_articles_all_{timestamp}.json'
            
            logging.info(f"Saving all results to {output_file}")
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Processing completed. All results saved to {output_file}")
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            logging.exception("Exception details:")
        logging.info("Script finished")
    finally:
        # Ensure cleanup happens even if there's an error
        cleanup_grpc()

if __name__ == "__main__":
    main()