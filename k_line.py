import requests
import json
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime, timedelta

class KLine:
    def __init__(self, symbol, api_key, function='TIME_SERIES_MONTHLY', years_to_display=20, significant_change_threshold=0.05):
        self.symbol = symbol
        self.api_key = api_key
        self.function = function
        self.years_to_display = years_to_display
        self.url = f'https://www.alphavantage.co/query?function={self.function}&symbol={self.symbol}&apikey={self.api_key}'
        self.data = None
        self.filtered_dates = []
        self.filtered_prices = []
        self.best_short_period = 5
        self.best_long_period = 20
        self.max_crossovers = 0
        self.significant_change_threshold = significant_change_threshold

    def fetch_data(self):
        r = requests.get(self.url)
        self.data = r.json()
        self.save_json()

    def save_json(self):
        filename = f'kline_{self.function}_{self.symbol}.json'
        with open(filename, 'w') as f:
            json.dump(self.data, f, indent=4)

    def process_data(self):
        time_series = self.data.get('Monthly Time Series', {})
        dates = []
        prices = []
        for date, values in time_series.items():
            dates.append(datetime.strptime(date, '%Y-%m-%d'))
            prices.append(float(values['4. close']))
        dates.reverse()
        prices.reverse()

        cutoff_date = datetime.now() - timedelta(days=self.years_to_display * 365)
        self.filtered_dates = [date for date in dates if date > cutoff_date]
        self.filtered_prices = prices[-len(self.filtered_dates):]

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
    def find_significant_inflections(prices, window=3, threshold=0.05):
        inflection_points = []
        for i in range(window, len(prices) - window):
            is_max = all(prices[i] > prices[i-j] for j in range(1, window+1)) and \
                     all(prices[i] > prices[i+j] for j in range(1, window+1))
            is_min = all(prices[i] < prices[i-j] for j in range(1, window+1)) and \
                     all(prices[i] < prices[i+j] for j in range(1, window+1))
            
            if is_max or is_min:
                prev_price = prices[i-1]
                curr_price = prices[i]
                if prev_price != 0:
                    change = abs((curr_price - prev_price) / prev_price)
                    if change >= threshold:
                        inflection_points.append(i)
        
        return inflection_points

    def plot(self):
        self.find_best_ma_periods()
        short_ma = self.calculate_ma(self.filtered_prices, self.best_short_period)
        long_ma = self.calculate_ma(self.filtered_prices, self.best_long_period)

        ma_dates = self.filtered_dates[self.best_long_period-1:]
        min_length = min(len(ma_dates), len(short_ma), len(long_ma))
        ma_dates = ma_dates[-min_length:]
        short_ma = short_ma[-min_length:]
        long_ma = long_ma[-min_length:]

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 12))

        # First subplot - Moving Averages
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

        # Second subplot - Inflection Points
        window_size = 3

        inflection_points = self.find_significant_inflections(self.filtered_prices, 
                                                              window=window_size,
                                                              threshold=self.significant_change_threshold)

        ax2.plot(self.filtered_dates, self.filtered_prices, label='Price', alpha=0.8)

        for idx in inflection_points:
            ax2.plot(self.filtered_dates[idx], self.filtered_prices[idx], 'go', markersize=10,
                     label='Inflection Point' if idx == inflection_points[0] else "")

        ax2.set_title(f'{self.symbol} Monthly Stock Prices with Inflection Points (>{self.significant_change_threshold*100}% change)')
        ax2.set_xlabel('Date')
        ax2.set_ylabel('Price (USD)')
        ax2.grid(True)
        ax2.legend()

        plt.tight_layout()
        plt.show()

        print(f"Best short MA period: {self.best_short_period}")
        print(f"Best long MA period: {self.best_long_period}")
        print(f"Number of crossovers: {self.max_crossovers}")
        print(f"Number of significant inflection points: {len(inflection_points)}")

# Usage example:
kline = KLine('OXY', 'AYTLT9XYXR8L9OSZ')
kline.fetch_data()
kline.process_data()
kline.plot()