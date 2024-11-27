import json
import os
from datetime import datetime
import logging
from dotenv import load_dotenv
import google.generativeai as genai
import re

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
        logging.error(f"Error processing articles: {str(e)}")
        return {"error": f"Error processing articles: {str(e)}"}

if __name__ == "__main__":
    logging.info("Script started")
    try:
        with open('scraped_articles_results.json', 'r', encoding='utf-8') as f:
            scraped_data = json.load(f)
        logging.info("Successfully loaded scraped_articles_results.json")
        
        successful_articles = scraped_data['successful_articles']
        total_articles = len(successful_articles)
        logging.info(f"Total successful articles: {total_articles}")
        
        batch_size = 120
        all_results = []
        
        for i in range(0, total_articles, batch_size):
            batch = successful_articles[i:i+batch_size]
            logging.info(f"Processing batch {i//batch_size + 1} of {(total_articles + batch_size - 1)//batch_size}")
            
            batch_results = process_articles_with_gemini(batch)
            all_results.extend(batch_results)
            
            logging.info(f"Completed processing batch {i//batch_size + 1}")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_file = f'processed_articles_{timestamp}.json'
        
        logging.info(f"Saving all results to {output_file}")
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_results, f, indent=2, ensure_ascii=False)
        
        logging.info(f"Processing completed. All results saved to {output_file}")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
    logging.info("Script finished")