# bot/live_equity.py
import os
import datetime as dt
from decimal import Decimal

import ccxt


def _as_bool(v) -> bool:
    if v is None:
        return False
    return str(v).strip().lower() in {"1", "true", "yes", "on"}


def get_live_total_equity() -> float:
    """
    מחזיר את ה-total equity בחשבון Bybit במונחי USDT (float).
    אם יש בעיה (API key, תקשורת, מבנה תשובה) – מרים Exception,
    והקוד ב-db_writer כבר יודע ליפול חזרה לערך שמגיע מהבוט (9.26 וכו').
    """
    api_key = os.getenv("BYBIT_API_KEY")
    api_secret = os.getenv("BYBIT_API_SECRET")

    if not api_key or not api_secret:
        raise RuntimeError("BYBIT_API_KEY / BYBIT_API_SECRET not set in environment")

    is_testnet = _as_bool(os.getenv("BYBIT_TESTNET")) or _as_bool(os.getenv("TESTNET"))

    exchange = ccxt.bybit({
        "apiKey": api_key,
        "secret": api_secret,
        "enableRateLimit": True,
        "options": {
            # ברירת מחדל סבירה לחשבון unified
            "defaultType": "swap",
        },
    })

    if is_testnet:
        exchange.set_sandbox_mode(True)

    # מושך מאזן
    balance = exchange.fetch_balance()

    # ccxt מחזיר בדרך כלל dict עם מפתח 'total' שבתוכו 'USDT'
    total = balance.get("total") or {}
    total_usdt = total.get("USDT")

    if total_usdt is None:
        # fallback ל-equity כללית אם קיימת
        if "info" in balance:
            # פה אפשר לפרק שדות ספציפיים ל-Bybit אם תרצה בעתיד
            raise RuntimeError("USDT total balance not found in Bybit response via ccxt")
        raise RuntimeError("USDT total balance not found in balance response")

    # מחזיר כ-float; Postgres double precision יודע להתמודד עם זה מצוין
    return float(Decimal(str(total_usdt)))
