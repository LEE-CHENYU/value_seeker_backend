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

class NewsProcessor:
    def __init__(self, scraped_articles_file):
        self.scraped_articles_file = scraped_articles_file
        self.inflection_points = self.load_inflection_points()
        self.prompt = self.get_prompt_template()
        self.generation_config = {
            "temperature": 0,
            "top_p": 0.95,
            "top_k": 40,
            "max_output_tokens": 8192,
            "response_mime_type": "text/plain",
        }
        self.model = self.initialize_model()

    def load_inflection_points(self):
        with open('inflection_points_OXY.json', 'r') as f:
            return json.load(f)

    def get_prompt_template(self):
        return """You are a financial news analyzer. Your task is to extract structured information from multiple financial news articles and output it in JSON format. Follow these specific guidelines:

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
7. All key_points entries must have values (either true for text or quoted strings for numbers with units)"""

    def initialize_model(self):
        genai.configure(api_key=os.environ["GEMINI_API_KEY"])
        return genai.GenerativeModel(
            model_name="gemini-1.5-pro",
            generation_config=self.generation_config,
        )

    def process_articles_with_gemini(self, articles):
        logging.info("Starting to process articles with Gemini")

        article_texts = "\n\n---ARTICLE SEPARATOR---\n\n".join([article['content'] for article in articles if article.get('content')])
        
        try:
            response = self.model.generate_content(self.prompt.format(
                inflection_points=json.dumps(self.inflection_points, indent=2),
                article_texts=article_texts
            ))
            logging.info("Received response from Gemini")

            return self.parse_gemini_response(response.text)

        except Exception as e:
            logging.error(f"Error processing articles: {str(e)}")
            return [{
                "error": "Error processing articles",
                "error_details": str(e)
            }]

    def parse_gemini_response(self, response_text):
        try:
            json_content = self.clean_response_text(response_text)
            
            try:
                with open('gemini_response.txt', 'w', encoding='utf-8') as txt_file:
                    txt_file.write(json_content)
                parsed_response = json.loads(json_content)
                logging.info("Successfully parsed Gemini's response as JSON")
                return parsed_response
            except json.JSONDecodeError as e:
                return self.handle_json_decode_error(e, json_content)

        except Exception as e:
            logging.error(f"Error cleaning response text: {str(e)}")
            logging.error(f"Raw response: {response_text}")
            return [{
                "error": "Error cleaning response text",
                "error_details": str(e),
                "raw_response": response_text
            }]

    def clean_response_text(self, text):
        cleaned = text.strip()
        cleaned = re.sub(r'^```json\s*|\s*```$', '', cleaned)
        cleaned = re.sub(r',(\s*[}\]])', r'\1', cleaned)
        return cleaned.strip()

    def handle_json_decode_error(self, error, json_content):
        logging.error(f"JSON parse error: {str(error)}")
        logging.error(f"Problematic JSON content: {json_content}")
        logging.error(f"Error position: {error.pos}")
        logging.error(f"Error line number: {error.lineno}")
        logging.error(f"Error column number: {error.colno}")
        logging.error(f"Error message: {error.msg}")
        
        error_context = json_content[max(0, error.pos - 50):min(len(json_content), error.pos + 50)]
        logging.error(f"Error context: {error_context}")
        
        if "Expecting property name enclosed in double quotes" in str(error):
            logging.error("Possible missing quotes around a property name")
        elif "Expecting ',' delimiter" in str(error):
            logging.error("Possible missing comma between elements")
        elif "Expecting value" in str(error):
            logging.error("Possible missing value for a property")
        
        return [{
            "error": "JSON parse error",
            "error_details": str(error),
            "error_position": error.pos,
            "error_line": error.lineno,
            "error_column": error.colno,
            "error_context": error_context,
            "raw_response": json_content
        }]

    def save_batch_results(self, batch_results, batch_number, output_folder):
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'processed_articles_batch_{batch_number}_{timestamp}.json'
        output_path = os.path.join(output_folder, output_file)
        
        logging.info(f"Saving batch {batch_number} results to {output_path}")
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(batch_results, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Batch {batch_number} results saved to {output_path}")

    def process_all_articles(self):
        try:
            with open(self.scraped_articles_file, 'r', encoding='utf-8') as f:
                scraped_data = json.load(f)
            logging.info(f"Successfully loaded {self.scraped_articles_file}")
            
            successful_articles = scraped_data['successful_articles']
            total_articles = len(successful_articles)
            logging.info(f"Total successful articles: {total_articles}")
            
            batch_size = 20
            all_results = []
            
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_folder = f'gm_single_{timestamp}'
            os.makedirs(output_folder, exist_ok=True)
            
            for i in range(0, total_articles, batch_size):
                batch = successful_articles[i:i+batch_size]
                batch_number = i//batch_size + 1
                logging.info(f"Processing batch {batch_number} of {(total_articles + batch_size - 1)//batch_size}")
                
                batch_results = self.process_articles_with_gemini(batch)
                all_results.extend(batch_results)
                
                self.save_batch_results(batch_results, batch_number, output_folder)
                
                logging.info(f"Completed processing batch {batch_number}")
            
            output_file = f'processed_articles_all.json'
            output_path = os.path.join(output_folder, output_file)
            
            logging.info(f"Saving all results to {output_path}")
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(all_results, f, indent=2, ensure_ascii=False)
            
            logging.info(f"Processing completed. All results saved to {output_path}")
            return output_folder
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            logging.exception("Exception details:")
            return None

def cleanup_grpc():
    try:
        aiplatform.initializer.global_pool.close()
    except Exception as e:
        logging.warning(f"gRPC cleanup warning: {e}")

def main(scraped_articles_file='scraped_articles_results.json'):
    atexit.register(cleanup_grpc)
    logging.info("Script started")
    processor = NewsProcessor(scraped_articles_file)
    processor.process_all_articles()
    logging.info("Script finished")

if __name__ == "__main__":
    import sys
    scraped_articles_file = sys.argv[1] if len(sys.argv) > 1 else 'scraped_articles_results.json'
    main(scraped_articles_file)