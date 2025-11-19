from bot.connectors.base import BaseConnector
import ccxt
import pandas as pd
from dotenv import load_dotenv 
import os                  

class CCXTConnector(BaseConnector):
    def __init__(self, exchange_id, paper=True, default_type='spot'):
        self.exchange_id = exchange_id
        self.paper = paper
        self.default_type = default_type
        self.exchange = None

    def init(self):
        # הערה: ב-testnet, ccxt משתמש ב־'bybit' עם enableRateLimit; 
        # אם תרצה testnet אמיתי צריך לעדכן params/urls – אבל לשלב הזה נטען שווקים.
        self.exchange = getattr(ccxt, self.exchange_id)({
            'enableRateLimit': True,
        })
        self.exchange.load_markets()

    def fetch_ohlcv(self, symbol, timeframe='15m', limit=500):
        raw = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp')
        return df
