from bot.connectors.base import BaseConnector
import ccxt
import pandas as pd
from dotenv import load_dotenv
import os


class CCXTConnector(BaseConnector):
    def __init__(self, exchange_id: str, paper: bool = True, default_type: str = "spot"):
        super().__init__()

        # טוען משתנים מה-.env (כולל BYBIT_API_KEY/BYBIT_API_SECRET)
        load_dotenv()

        self.exchange_id = exchange_id
        self.paper = paper
        self.default_type = default_type

        # פרמטרי בסיס ל-ccxt
        params = {
            "enableRateLimit": True,
            "options": {
                "defaultType": default_type,
            },
        }

        # אם זה Bybit LIVE (לא paper) – נשתמש במפתחות האמיתיים מה-ENV
        if exchange_id == "bybit" and not paper:
            api_key = os.getenv("BYBIT_API_KEY")
            api_secret = os.getenv("BYBIT_API_SECRET")

            if api_key and api_secret:
                params["apiKey"] = api_key
                params["secret"] = api_secret
                print("[CCXTConnector] Using real Bybit API keys from env")
            else:
                print("[CCXTConnector] WARNING: BYBIT_API_KEY/BYBIT_API_SECRET not found in env")

        # יצירת האובייקט של הבורסה
        self.exchange = getattr(ccxt, exchange_id)(params)

    def init(self):
        """Load markets once at startup."""
        self.exchange.load_markets()

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1m", limit: int = 500) -> pd.DataFrame:
        """Wrapper שמחזיר DataFrame עם אינדקס זמן."""
        ohlcv = self.exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["timestamp", "open", "high", "low", "close", "volume"])
        df["time"] = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
        df = df.set_index("time")
        return df
