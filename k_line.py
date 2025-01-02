import yfinance as yf
import json
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta
from enum import Enum

class TrendType(Enum):
    UPTREND = "uptrend"
    DOWNTREND = "downtrend"
    SIDEWAYS = "sideways"

class KLine:
    def __init__(self, symbol, function='TIME_SERIES_MONTHLY', years_to_display=20, 
                 significant_change_threshold=0.1, trend_threshold=0.05, show_chart=False):
        self.symbol = symbol
        self.years_to_display = years_to_display
        self.data = None
        self.filtered_dates = []
        self.filtered_prices = []
        self.best_short_period = 5
        self.best_long_period = 20
        self.max_crossovers = 0
        self.significant_change_threshold = significant_change_threshold
        self.inflection_points = []
        self.show_chart = show_chart
        self.trend_threshold = trend_threshold
        self.trends = []

    def fetch_data(self):
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.years_to_display * 365)
        self.data = yf.download(self.symbol, start=start_date, end=end_date, interval='1mo')
        self.save_json()

    def save_json(self):
        filename = f'kline_monthly_{self.symbol}.json'
        self.data.to_json(filename, date_format='iso')

    def process_data(self):
        self.filtered_dates = self.data.index.tolist()
        self.filtered_prices = self.data['Close'][self.symbol].tolist()

    @staticmethod
    def calculate_ma(data, period):
        return np.convolve(data, np.ones(period)/period, mode='valid')

    @staticmethod
    def find_crossovers(short_ma, long_ma):
        return np.where(np.diff(np.sign(short_ma - long_ma)))[0]

    def find_best_ma_periods(self):
        for short_test in range(3, 30):
            for long_test in range(short_test + 1, 50):
                short_ma = self.calculate_ma(self.filtered_prices, short_test)
                long_ma = self.calculate_ma(self.filtered_prices, long_test)
                
                min_length = min(len(short_ma), len(long_ma))
                short_ma = short_ma[-min_length:]
                long_ma = long_ma[-min_length:]
                
                crossovers = self.find_crossovers(short_ma, long_ma)
                
                if len(crossovers) > self.max_crossovers:
                    self.max_crossovers = len(crossovers)
                    self.best_short_period = short_test
                    self.best_long_period = long_test

    @staticmethod
    def find_significant_inflections(prices, window=3, threshold=0.05, min_distance=2):
        inflection_points = []
        
        def is_significant_change(current_idx, last_idx):
            return abs((prices[current_idx] - prices[last_idx]) / prices[last_idx]) >= threshold
        
        def is_local_maximum(i, window_size):
            if prices[i] <= prices[i-1] or prices[i] <= prices[i+1]:
                return False
            
            left_max = max(prices[max(0, i-window_size):i])
            right_max = max(prices[i+1:min(len(prices), i+window_size+1)])
            
            return prices[i] >= left_max and prices[i] >= right_max
        
        def is_local_minimum(i, window_size):
            if prices[i] >= prices[i-1] or prices[i] >= prices[i+1]:
                return False
            
            left_min = min(prices[max(0, i-window_size):i])
            right_min = min(prices[i+1:min(len(prices), i+window_size+1)])
            
            return prices[i] <= left_min and prices[i] <= right_min
        
        potential_points = []
        for i in range(window, len(prices) - window):
            if is_local_maximum(i, window) or is_local_minimum(i, window):
                potential_points.append(i)
        
        for point in potential_points:
            if not inflection_points or (point - inflection_points[-1] >= min_distance):
                if not inflection_points or is_significant_change(point, inflection_points[-1]):
                    inflection_points.append(point)
        
        return inflection_points

    def analyze(self):
        self.find_best_ma_periods()
        short_ma = self.calculate_ma(self.filtered_prices, self.best_short_period)
        long_ma = self.calculate_ma(self.filtered_prices, self.best_long_period)

        ma_dates = self.filtered_dates[self.best_long_period-1:]
        min_length = min(len(ma_dates), len(short_ma), len(long_ma))
        ma_dates = ma_dates[-min_length:]
        short_ma = short_ma[-min_length:]
        long_ma = long_ma[-min_length:]

        window_size = 3

        self.inflection_points = self.find_significant_inflections(self.filtered_prices, 
                                                              window=window_size,
                                                              threshold=self.significant_change_threshold)

        print(f"Best short MA period: {self.best_short_period}")
        print(f"Best long MA period: {self.best_long_period}")
        print(f"Number of crossovers: {self.max_crossovers}")
        print(f"Number of significant inflection points: {len(self.inflection_points)}")

        self.save_inflection_points()

        if self.show_chart:
            self.plot(ma_dates, short_ma, long_ma)

        # Add trend analysis
        self.analyze_trends()
        
        # Update the save_inflection_points method to include trends
        self.save_analysis_results()

    def plot(self, ma_dates, short_ma, long_ma):
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 15))
        
        # First subplot - Moving averages
        ax1.plot(self.filtered_dates, self.filtered_prices, label='Price', alpha=0.5)
        ax1.plot(ma_dates, short_ma, label=f'{self.best_short_period}-month MA')
        ax1.plot(ma_dates, long_ma, label=f'{self.best_long_period}-month MA')

        crossovers = self.find_crossovers(short_ma, long_ma)
        for idx in crossovers:
            crossover_price = self.filtered_prices[self.best_long_period-1+idx]
            ax1.plot(ma_dates[idx], crossover_price, 'ro', markersize=10, 
                     label='Crossover' if idx == crossovers[0] else "")

        ax1.set_title(f'{self.symbol} Monthly Stock Prices with Dynamic Moving Averages (Last {self.years_to_display} Years)')
        ax1.set_xlabel('Date')
        ax1.set_ylabel('Price (USD)')
        ax1.grid(True)
        ax1.legend()

        # Second subplot - Inflection points with colored trends
        ax2.plot(self.filtered_dates, self.filtered_prices, label='Price', alpha=0.3, color='gray')

        # Plot trend lines between inflection points with colors
        for i in range(len(self.inflection_points) - 1):
            start_idx = self.inflection_points[i]
            end_idx = self.inflection_points[i + 1]
            
            # Get dates and prices for this segment
            segment_dates = self.filtered_dates[start_idx:end_idx + 1]
            segment_prices = self.filtered_prices[start_idx:end_idx + 1]
            
            # Determine color based on price movement
            price_change = segment_prices[-1] - segment_prices[0]
            color = 'green' if price_change > 0 else 'red'
            
            # Plot the trend segment
            ax2.plot(segment_dates, segment_prices, color=color, linewidth=2,
                    label='Uptrend' if color == 'green' and i == 0 else 
                          'Downtrend' if color == 'red' and i == 0 else "")

        # Plot inflection points
        for idx in self.inflection_points:
            ax2.plot(self.filtered_dates[idx], self.filtered_prices[idx], 'ko', markersize=8,
                     label='Inflection Point' if idx == self.inflection_points[0] else "")

        ax2.set_title(f'{self.symbol} Monthly Stock Prices with Trend Direction')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Price (USD)')
        ax2.grid(True)
        ax2.legend()

        # Third subplot - Trend periods with detailed analysis
        ax3.plot(self.filtered_dates, self.filtered_prices, label='Price', alpha=0.3, color='gray')
        
        # Color regions based on trend type
        colors = {
            'uptrend': 'lightgreen',
            'downtrend': 'lightcoral',
            'sideways': 'lightgray'
        }
        
        # Plot trend segments with annotations
        for trend in self.trends:
            start_date = datetime.strptime(trend['start_date'], '%Y-%m-%d')
            end_date = datetime.strptime(trend['end_date'], '%Y-%m-%d')
            
            # Background shading
            ax3.axvspan(start_date, end_date, 
                       alpha=0.3, 
                       color=colors[trend['type']])
            
            # Find the price data points for this period
            mask = (np.array(self.filtered_dates) >= start_date) & (np.array(self.filtered_dates) <= end_date)
            period_dates = np.array(self.filtered_dates)[mask]
            period_prices = np.array(self.filtered_prices)[mask]
            
            # Plot the trend line
            if len(period_dates) > 0:
                color = 'green' if trend['type'] == 'uptrend' else 'red' if trend['type'] == 'downtrend' else 'gray'
                ax3.plot(period_dates, period_prices, color=color, linewidth=2,
                        label=f"{trend['type'].capitalize()} ({trend['duration_months']}m, {trend['price_change_pct']}%)")
                
                # Add annotation with trend metrics
                mid_idx = len(period_dates) // 2
                if mid_idx < len(period_dates):
                    y_pos = period_prices[mid_idx]
                    ax3.annotate(
                        f"{trend['price_change_pct']}%\n{trend['duration_months']}m",
                        xy=(period_dates[mid_idx], y_pos),
                        xytext=(10, 10), textcoords='offset points',
                        bbox=dict(boxstyle='round,pad=0.5', fc='white', alpha=0.7),
                        fontsize=8
                    )

        ax3.set_title(f'{self.symbol} Price Trends with Metrics')
        ax3.set_xlabel('Date')
        ax3.set_ylabel('Price (USD)')
        ax3.grid(True)
        ax3.legend(loc='upper left', bbox_to_anchor=(1, 1))

        plt.tight_layout()
        plt.show()

    def save_inflection_points(self):
        inflection_data = []
        for idx in self.inflection_points:
            data = {
                "date": self.filtered_dates[idx].strftime('%Y-%m-%d'),
                "price": self.filtered_prices[idx],
                "index": idx,
                "prev_date": self.filtered_dates[idx-1].strftime('%Y-%m-%d') if idx > 0 else None,
                "prev_price": self.filtered_prices[idx-1] if idx > 0 else None,
                "price_change": self.filtered_prices[idx] - self.filtered_prices[idx-1] if idx > 0 else None
            }
            inflection_data.append(data)
        
        filename = f'inflection_points_{self.symbol}.json'
        with open(filename, 'w') as f:
            json.dump(inflection_data, f, indent=4)
        print(f"Inflection points saved to {filename}")

    def analyze_trends(self):
        """Analyze price trends between inflection points"""
        trends = []
        
        for i in range(len(self.inflection_points) - 1):
            start_idx = self.inflection_points[i]
            end_idx = self.inflection_points[i + 1]
            
            start_price = self.filtered_prices[start_idx]
            end_price = self.filtered_prices[end_idx]
            price_change = (end_price - start_price) / start_price
            
            # Calculate duration in months
            start_date = self.filtered_dates[start_idx]
            end_date = self.filtered_dates[end_idx]
            duration = (end_date - start_date).days / 30  # Approximate months
            
            # Determine trend type
            if abs(price_change) < self.trend_threshold:
                trend_type = TrendType.SIDEWAYS
            else:
                trend_type = TrendType.UPTREND if price_change > 0 else TrendType.DOWNTREND
            
            # Calculate trend strength metrics
            price_series = self.filtered_prices[start_idx:end_idx + 1]
            volatility = np.std(price_series) / np.mean(price_series)
            
            trend = {
                "start_date": start_date.strftime('%Y-%m-%d'),
                "end_date": end_date.strftime('%Y-%m-%d'),
                "duration_months": round(duration, 1),
                "type": trend_type.value,
                "price_change_pct": round(price_change * 100, 2),
                "start_price": round(start_price, 2),
                "end_price": round(end_price, 2),
                "volatility": round(volatility * 100, 2)
            }
            
            trends.append(trend)
        
        self.trends = trends
        return trends

    def save_analysis_results(self):
        """Save both inflection points and trends analysis"""
        analysis_data = {
            "inflection_points": [],
            "trends": self.trends
        }
        
        for idx in self.inflection_points:
            data = {
                "date": self.filtered_dates[idx].strftime('%Y-%m-%d'),
                "price": round(self.filtered_prices[idx], 2),
                "index": idx,
                "prev_date": self.filtered_dates[idx-1].strftime('%Y-%m-%d') if idx > 0 else None,
                "prev_price": round(self.filtered_prices[idx-1], 2) if idx > 0 else None,
                "price_change": round(self.filtered_prices[idx] - self.filtered_prices[idx-1], 2) if idx > 0 else None
            }
            analysis_data["inflection_points"].append(data)
        
        filename = f'market_analysis_{self.symbol}.json'
        with open(filename, 'w') as f:
            json.dump(analysis_data, f, indent=4)
        print(f"Analysis results saved to {filename}")

if __name__ == "__main__":
# Usage example:
    kline = KLine('AAPL', show_chart=True)
    kline.fetch_data()
    kline.process_data()
    kline.analyze()