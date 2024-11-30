import json
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
from groq import Groq
import re
import atexit

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

client = Groq(
    api_key=os.environ.get("GROQ_API_KEY"),
)

def process_articles_with_groq(articles):
    logging.info("Starting to process articles with Groq")

    # Load inflection points
    with open('inflection_points_OXY.json', 'r') as f:
        inflection_points = json.load(f)

    prompt = """You are a financial news analyzer. Your task is to extract structured information from financial news articles and output it in JSON format. Follow these specific guidelines:

Output Structure Required:
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
]

Please process the following news articles and output the JSON according to these specifications:

{article_texts}

Important: Only output the JSON structure with no additional explanation or commentary. Ensure the JSON is valid and properly formatted. The output should be an array of JSON objects, one for each article processed.

Important formatting rules:
1. All string values must be in double quotes
2. All object keys must be in double quotes
3. Numbers with units or symbols must be quoted strings
4. Pure numbers without units can be unquoted
5. No comments or extra text
6. Use proper JSON syntax with colons and commas
7. All key_points entries must have values (either true for text or quoted strings for numbers with units)
8. The JSON output must start with [ and end with ], with no markdown markers before or after"""

    logging.info("Sending request to Groq")

    article_texts = "\n\n---ARTICLE SEPARATOR---\n\n".join([article['content'] for article in articles if article.get('content')])
    
    try:
        chat_completion = client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": prompt.format(
                        inflection_points=json.dumps(inflection_points, indent=2),
                        article_texts=article_texts
                    ),
                }
            ],
            model="llama-3.1-8b-instant",
            stream=False,
        )

        logging.info("Received response from Groq")

        try:
            json_content = chat_completion.choices[0].message.content
            
            # Find the first occurrence of '[' and the last occurrence of ']'
            start_idx = json_content.find('[')
            end_idx = json_content.rfind(']')
            
            if start_idx == -1 or end_idx == -1:
                raise json.JSONDecodeError("No JSON array found", json_content, 0)
            
            # Extract only the JSON part
            json_content = json_content[start_idx:end_idx + 1]
            
            # Remove any control characters while preserving newlines
            json_content = ''.join(char if char.isprintable() or char in '\n\r' else '' for char in json_content)
            
            try:
                parsed_response = json.loads(json_content)
                logging.info("Successfully parsed Groq's response as JSON")
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
            logging.error(f"Raw response: {chat_completion.choices[0].message.content}")
            return [{
                "error": "Error cleaning response text",
                "error_details": str(e),
                "raw_response": chat_completion.choices[0].message.content
            }]

    except Exception as e:
        logging.error(f"Error processing articles: {str(e)}")
        return [{
            "error": "Error processing articles",
            "error_details": str(e)
        }]

def save_batch_results(batch_results, batch_number, output_folder):
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_file = f'processed_articles_batch_{batch_number}_{timestamp}.json'
    output_path = os.path.join(output_folder, output_file)
    
    logging.info(f"Saving batch {batch_number} results to {output_path}")
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(batch_results, f, indent=2, ensure_ascii=False)
    
    logging.info(f"Batch {batch_number} results saved to {output_path}")

def main():
    logging.info("Script started")
    try:
        with open('scraped_articles_results.json', 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        logging.info("Successfully loaded scraped_articles_results.json")
        
        successful_articles = scraped_data['successful_articles']
        total_articles = len(successful_articles)
        logging.info(f"Total successful articles: {total_articles}")
        
        batch_size = 5
        all_results = []
        
        # Create output folder
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_folder = f'groq_single_{timestamp}'
        os.makedirs(output_folder, exist_ok=True)
        
        for i in range(0, total_articles, batch_size):
            batch = successful_articles[i:i+batch_size]
            batch_number = i//batch_size + 1
            logging.info(f"Processing batch {batch_number} of {(total_articles + batch_size - 1)//batch_size}")
            
            batch_results = process_articles_with_groq(batch)
            all_results.extend(batch_results)
            
            save_batch_results(batch_results, batch_number, output_folder)
            
            logging.info(f"Completed processing batch {batch_number}")
        
        output_file = f'processed_articles_all.json'
        output_path = os.path.join(output_folder, output_file)
        
        logging.info(f"Saving all results to {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Processing completed. All results saved to {output_path}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        logging.exception("Exception details:")
    logging.info("Script finished")

if __name__ == "__main__":
    main()