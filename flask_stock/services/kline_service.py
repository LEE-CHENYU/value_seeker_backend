import yfinance as yf
import pandas as pd

def get_stock_kline_data(symbol='AAPL', interval='1d'):
    """
    Fetch stock kline data using yfinance
    
    Args:
        symbol (str): Stock symbol (default: AAPL)
        interval (str): Data interval (default: 1d)
                       Valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    Returns:
        list: List of kline data in format required for charts
    """
    try:
        # Create a Ticker object
        ticker = yf.Ticker(symbol)
        
        # Get historical data
        # Period is set to 1 year, adjust as needed
        df = ticker.history(interval=interval, period='1y')
        
        # Convert the data to the format needed for kline chart
        kline_data = []
        for index, row in df.iterrows():
            kline_data.append({
                'time': index.timestamp(),  # Convert datetime to timestamp
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row['Volume'])
            })
        
        return kline_data
    
    except Exception as e:
        raise Exception(f"Error fetching data for {symbol}: {str(e)}")