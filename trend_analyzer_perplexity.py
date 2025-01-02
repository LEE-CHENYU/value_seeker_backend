from numpy import identity
from openai import OpenAI
import httpx
import json
from datetime import datetime
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trend_analysis.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Perplexity API setup
PPLX_API_KEY = "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"
client = OpenAI(
    api_key=PPLX_API_KEY,
    base_url="https://api.perplexity.ai",
    http_client=httpx.Client()
)

def extract_json_from_response(content):
    """Extract JSON from response that might be wrapped in markdown code blocks"""
    logger.debug("Attempting to extract JSON from response")
    
    try:
        # Try direct JSON parsing first
        return json.loads(content)
    except json.JSONDecodeError:
        logger.debug("Direct JSON parsing failed, trying to extract from markdown")
        try:
            # Look for JSON between ```json and ``` markers
            if "```json" in content:
                # Extract content between ```json and ```
                json_content = content.split("```json")[1].split("```")[0].strip()
                logger.debug(f"Extracted JSON content:\n{json_content}")
                return json.loads(json_content)
            # Look for just ``` markers
            elif "```" in content:
                # Extract content between ``` and ```
                json_content = content.split("```")[1].split("```")[0].strip()
                logger.debug(f"Extracted JSON content:\n{json_content}")
                return json.loads(json_content)
            else:
                logger.error("No JSON markers found in response")
                return None
        except Exception as e:
            logger.error(f"Error extracting JSON from markdown: {str(e)}")
            return None

def analyze_single_trend(symbol, trend):
    """Analyze a single trend period"""
    logger.info(f"Starting analysis for {symbol} trend period: {trend['start_date']} to {trend['end_date']}")
    
    prompt = f"""Analyze this specific price trend period for {symbol}:

TREND DETAILS:
{trend['type'].upper()} from {trend['start_date']} to {trend['end_date']}
Duration: {trend['duration_months']} months
Price change: {trend['price_change_pct']}%
Price range: ${trend['start_price']} to ${trend['end_price']}
Volatility: {trend['volatility']}%

Please provide a comprehensive analysis that includes:

1. Major Market Events
- Search for significant company events, industry developments, or macro events during this period
- Identify potential catalysts that triggered trend changes
- Consider earnings reports, management changes, product launches, regulatory changes

2. Trend Pattern Analysis
- Characterize the nature of the trend (steady/volatile/cyclical)
- Identify any recurring patterns or cycles
- Note any correlation with broader market movements

3. Impact Assessment
- Evaluate how different types of events affected the stock price
- Compare the magnitude of price movements to event significance
- Identify which factors had the strongest influence on trends

4. Forward-Looking Implications
- Consider how historical patterns might inform future price movements
- Identify ongoing trends or situations that could affect the stock
- Note any structural changes in the company/industry that could alter historical patterns

Please format your response as a valid JSON object with the following structure:
{{
    "major_events": [
        {{
            "date": "YYYY-MM-DD",
            "event": "description",
            "type": "earnings/product/management/macro",
            "impact": "high/medium/low",
            "price_reaction": "description"
        }}
    ],
    "trend_analysis": {{
        "pattern": "steady/volatile/cyclical",
        "market_correlation": "strong/weak/inverse",
        "key_characteristics": [],
        "average_daily_movement": "percentage"
    }},
    "key_drivers": [
        {{
            "factor": "description",
            "importance": "high/medium/low",
            "impact_description": "text"
        }}
    ],
    "technical_factors": {{
        "support_levels": [],
        "resistance_levels": [],
        "volume_patterns": "description",
        "momentum_indicators": "description"
    }},
    "summary": "Detailed analysis of the period"
}}"""

    logger.debug(f"Generated prompt for {symbol}:\n{prompt}")

    messages = [
        {
            "role": "system",
            "content": "You are a financial analyst specializing in trend analysis and market drivers. Your expertise includes identifying correlations between market events and price movements, understanding industry dynamics, and analyzing how various factors influence stock performance. Provide detailed, fact-based analysis with specific examples and data points. Format your response as a valid JSON object.",
        },
        {
            "role": "user",
            "content": prompt,
        },
    ]

    try:
        logger.info(f"Sending request to Perplexity API for {symbol} trend period {trend['start_date']}")
        response = client.chat.completions.create(
            model="llama-3.1-sonar-huge-128k-online",
            messages=messages,
        )
        
        logger.debug(f"Raw API response:\n{response}")
        
        # Extract the content from the response
        content = response.choices[0].message.content
        logger.debug(f"Response content:\n{content}")
        
        # Try to parse the JSON using the new extraction function
        result = extract_json_from_response(content)
        if result:
            logger.info(f"Successfully parsed JSON response for {trend['start_date']}")
            return result
        else:
            logger.error(f"Failed to extract valid JSON from response for {trend['start_date']}")
            return None
            
    except Exception as e:
        logger.error(f"API request error for {trend['start_date']}: {str(e)}")
        logger.exception("Full exception details:")
        return None

def analyze_market_trends(symbol):
    """Main function to analyze market trends for a given symbol"""
    logger.info(f"Starting market trend analysis for {symbol}")
    
    try:
        # Load market analysis data
        logger.info(f"Loading market analysis data from market_analysis_{symbol}.json")
        with open(f'market_analysis_{symbol}.json', 'r') as f:
            market_data = json.load(f)
        
        output_dir = f"trend_analyses/{symbol}"
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
        
        # Analyze each trend individually
        for trend in market_data['trends']:
            logger.info(f"Processing trend period: {trend['start_date']} to {trend['end_date']}")
            
            analysis = analyze_single_trend(symbol, trend)
            if analysis:
                # Create result for this trend
                trend_result = {
                    "symbol": symbol,
                    "analysis_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "period": {
                        "start_date": trend['start_date'],
                        "end_date": trend['end_date'],
                        "duration_months": trend['duration_months'],
                        "type": trend['type'],
                        "price_change_pct": trend['price_change_pct'],
                        "price_range": {
                            "start": trend['start_price'],
                            "end": trend['end_price']
                        },
                        "volatility": trend['volatility']
                    },
                    "analysis": analysis
                }
                
                # Save individual trend analysis
                filename = f"{output_dir}/trend_{trend['start_date']}_{trend['end_date']}.json"
                logger.info(f"Saving analysis to {filename}")
                
                try:
                    with open(filename, 'w') as f:
                        json.dump(trend_result, f, indent=2)
                    logger.info(f"Successfully saved analysis for period {trend['start_date']} to {trend['end_date']}")
                except Exception as e:
                    logger.error(f"Error saving analysis to file: {str(e)}")
        
        return True
        
    except Exception as e:
        logger.error(f"Error in market trend analysis for {symbol}: {str(e)}")
        logger.exception("Full exception details:")
        return None

if __name__ == "__main__":
    logger.info("Starting trend analyzer script")
    symbol = "AAPL"
    if analyze_market_trends(symbol):
        logger.info(f"Analysis completed for {symbol}")
    else:
        logger.error(f"Analysis failed for {symbol}")