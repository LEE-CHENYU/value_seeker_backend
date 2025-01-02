from openai import OpenAI
import httpx
import json
import os

# Perplexity API setup
PPLX_API_KEY = "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"
client = OpenAI(
    api_key=PPLX_API_KEY,
    base_url="https://api.perplexity.ai",
    http_client=httpx.Client()  # Remove any proxy settings
)

def get_trend_analysis(symbol, trends):
    """Generate a detailed analysis of stock trends using Perplexity AI"""
    
    # Format trends for the prompt
    trend_descriptions = []
    for trend in trends:
        trend_desc = (
            f"{trend['type'].upper()} from {trend['start_date']} to {trend['end_date']}\n"
            f"Duration: {trend['duration_months']} months\n"
            f"Price change: {trend['price_change_pct']}%\n"
            f"Price range: ${trend['start_price']} to ${trend['end_price']}\n"
            f"Volatility: {trend['volatility']}%\n"
        )
        trend_descriptions.append(trend_desc)

    prompt = f"""Analyze {symbol}'s price trends and identify potential market drivers:

PRICE TREND PERIODS:
{'-' * 50}
{''.join(trend_descriptions)}

Please provide a comprehensive analysis that includes:

1. Major Market Events
- Search for significant company events, industry developments, or macro events during each trend period
- Identify potential catalysts that triggered trend changes
- Consider earnings reports, management changes, product launches, regulatory changes

2. Trend Pattern Analysis
- Characterize the nature of each trend (steady/volatile/cyclical)
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

Please provide specific dates, events, and data points to support the analysis. Focus on establishing clear connections between market events and price movements."""

    messages = [
        {
            "role": "system",
            "content": "You are a financial analyst specializing in trend analysis and market drivers. Your expertise includes identifying correlations between market events and price movements, understanding industry dynamics, and analyzing how various factors influence stock performance. Provide detailed, fact-based analysis with specific examples and data points.",
        },
        {
            "role": "user",
            "content": prompt,
        },
    ]

    try:
        response = client.chat.completions.create(
            model="llama-3.1-sonar-huge-128k-online",
            messages=messages,
        )
        analysis = response.choices[0].message.content

        # Save the analysis
        output_dir = "trend_analyses"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/trend_analysis_{symbol}_{timestamp}.json"
        
        analysis_data = {
            "symbol": symbol,
            "timestamp": timestamp,
            "trends": trends,
            "analysis": analysis
        }
        
        with open(filename, 'w') as f:
            json.dump(analysis_data, f, indent=2)
            
        return analysis

    except Exception as e:
        print(f"Error analyzing trends for {symbol}: {str(e)}")
        return "Trend analysis unavailable due to API error."

def analyze_market_trends(symbol):
    """Main function to analyze market trends for a given symbol"""
    try:
        # Load market analysis data
        with open(f'market_analysis_{symbol}.json', 'r') as f:
            market_data = json.load(f)
            
        # Get trend analysis
        analysis = get_trend_analysis(symbol, market_data['trends'])
        
        # Combine trend data with analysis
        result = {
            "symbol": symbol,
            "trends": market_data['trends'],
            "analysis": analysis,
            "inflection_points": market_data['inflection_points']
        }
        
        # Save combined results
        output_filename = f'trend_analysis_{symbol}_complete.json'
        with open(output_filename, 'w') as f:
            json.dump(result, f, indent=2)
            
        return result
        
    except Exception as e:
        print(f"Error in market trend analysis for {symbol}: {str(e)}")
        return None

if __name__ == "__main__":
    # Example usage
    symbol = "AAPL"
    result = analyze_market_trends(symbol)
    if result:
        print(f"Analysis completed for {symbol}")
        print("\nAnalysis Summary:")
        print(result['analysis'])