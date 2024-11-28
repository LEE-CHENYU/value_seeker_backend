import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

def get_stock_kline_data(symbol='AAPL', interval='1d', years=10):
    """
    Fetch stock kline data using yfinance
    
    Args:
        symbol (str): Stock symbol (default: AAPL)
        interval (str): Data interval (default: 1d)
        years (int): Number of years of historical data to fetch
    Returns:
        list: List of kline data in format required for charts
    """
    try:
        ticker = yf.Ticker(symbol)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=years * 365)
        
        df = ticker.history(start=start_date, end=end_date, interval=interval)
        
        kline_data = []
        for index, row in df.iterrows():
            kline_data.append({
                'time': int(index.timestamp()),
                'open': float(row['Open']),
                'high': float(row['High']),
                'low': float(row['Low']),
                'close': float(row['Close']),
                'volume': float(row['Volume'])
            })
        
        return kline_data
    
    except Exception as e:
        raise Exception(f"Error fetching data for {symbol}: {str(e)}")