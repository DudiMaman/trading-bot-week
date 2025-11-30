import os
import pandas as pd
from alpaca_trade_api import REST
from typing import Dict, Any
from .base import BaseConnector


class AlpacaConnector(BaseConnector):
    def __init__(self, paper: bool = True):
        # תעדוף מפתחות חדשים (ALPACA_*) ואז הישנים (APCA_*)
        api_key = os.getenv("ALPACA_API_KEY_ID") or os.getenv("APCA_API_KEY_ID")
        api_secret = os.getenv("ALPACA_API_SECRET_KEY") or os.getenv("APCA_API_SECRET_KEY")

        if not api_key or not api_secret:
            raise ValueError("Missing Alpaca API keys (ALPACA_/APCA_).")

        # base_url: קודם ENV (חדש/ישן), אחרת לפי paper/live
        env_base_url = os.getenv("ALPACA_BASE_URL") or os.getenv("APCA_API_BASE_URL")
        if env_base_url:
            base_url = env_base_url
        else:
            base_url = "https://paper-api.alpaca.markets" if paper else "https://api.alpaca.markets"

        self.api = REST(api_key, api_secret, base_url=base_url)
        self.paper = paper

    def init(self):
        pass

    @staticmethod
    def _normalize_timeframe(timeframe: str) -> str:
        """
        ממיר timeframes מהעולם של הקריפטו ('5m', '15m', '1h', '1d')
        לפורמט שאלפקה מצפה לו ('5Min', '15Min', '1Hour', '1Day').
        אם לא נמצא מיפוי – מחזיר כמו שהוא.
        """
        mapping = {
            "1m": "1Min",
            "3m": "3Min",
            "5m": "5Min",
            "15m": "15Min",
            "30m": "30Min",
            "1h": "1Hour",
            "2h": "2Hour",
            "4h": "4Hour",
            "1d": "1Day",
        }
        return mapping.get(timeframe, timeframe)

    def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 600) -> pd.DataFrame:
        tf = self._normalize_timeframe(timeframe)
        bars = self.api.get_bars(symbol, tf, limit=limit).df
        df = bars.reset_index().rename(columns={'timestamp': 'ts'})
        df = df[['ts', 'open', 'high', 'low', 'close', 'volume']]
        df['ts'] = pd.to_datetime(df['ts'])
        df.set_index('ts', inplace=True)
        return df

    def create_market_order(self, symbol: str, side: str, qty: float) -> Dict[str, Any]:
        order = self.api.submit_order(
            symbol=symbol,
            qty=qty,
            side=side,
            type='market',
            time_in_force='day'
        )
        # _raw קיים באובייקטים של alpaca_trade_api; אם לא – מחזירים ID בסיסי
        return getattr(order, '_raw', {'id': str(order.id)})

    def get_precision(self, symbol: str) -> Dict[str, Any]:
        # אפשר לשפר בעתיד לפי symbol ספציפי; לעת עתה ערכי ברירת מחדל סבירים
        return {'amount_min': 1.0, 'price_tick': 0.01, 'amount_step': 1.0}

    def account_equity(self) -> float:
        return float(self.api.get_account().equity)
