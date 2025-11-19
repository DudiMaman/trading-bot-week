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

    class CCXTConnector(BaseConnector):
    def __init__(self, exchange_id: str, paper: bool = True, default_type: str = "spot"):
        super().__init__()

        # לטעון משתנים מה-.env (כולל BYBIT_API_KEY/BYBIT_API_SECRET)
        load_dotenv()

        self.exchange_id = exchange_id
        self.paper = paper
        self.default_type = default_type

        # ברירת מחדל – בלי מפתחות (לטסטנט / נייר / בורסות אחרות)
        params = {
            "enableRateLimit": True,
            "options": {
                "defaultType": default_type,
            },
        }

        # אם זה Bybit LIVE (paper=False) – נטען מפתחות אמיתיים מה־ENV
        if exchange_id == "bybit" and not paper:
            api_key = os.getenv("BYBIT_API_KEY")
            api_secret = os.getenv("BYBIT_API_SECRET")

            if api_key and api_secret:
                params["apiKey"] = api_key
                params["secret"] = api_secret
            else:
                print("⚠️ WARNING: BYBIT_API_KEY/BYBIT_API_SECRET not found in environment")

        # יצירת האובייקט של ccxt
        self.exchange = getattr(ccxt, exchange_id)(params)

    def fetch_ohlcv(self, symbol, timeframe='15m', limit=500):
        raw = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(raw, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.set_index('timestamp')
        return df
